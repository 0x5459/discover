use crate::{
    codec::{Codec, DecodeErorr, Decoder, EncodeError, Encoder},
    HashSet, Instance, Registry,
};
use futures::{ready, Future, FutureExt};
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
    time::Duration,
};
use tokio::task;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use zk_watcher::ZkWatcher;
use zookeeper::{Acl, CreateMode, ZkError, ZooKeeper};

mod zk_watcher;

pub struct Zk<EC, DC>
where
    EC: 'static,
    DC: 'static,
{
    client: Arc<ZooKeeper>,
    codec: &'static Codec<EC, DC>,
    persistent_exist_node_path: Arc<RwLock<HashSet<String>>>,
}

impl<EC, DC> Zk<EC, DC>
where
    EC: Sync,
    DC: Sync,
{
    pub fn new(
        zk_urls: &str,
        timeout: Duration,
        codec: &'static Codec<EC, DC>,
    ) -> impl Future<Output = Zk<EC, DC>> {
        let zk_urls = zk_urls.to_string();

        task::spawn_blocking(move || Zk {
            client: Arc::new(ZooKeeper::connect(zk_urls.as_str(), timeout, |_| {}).unwrap()),
            codec,
            persistent_exist_node_path: Arc::new(RwLock::new(HashSet::default())),
        })
        .map(|zk| zk.unwrap())
    }
}

#[pin_project]
pub struct RegFut {
    #[pin]
    join_handle: JoinHandle<Result<(), ZkRegError>>,
}

impl RegFut {
    pub fn new<EC>(
        client: Arc<ZooKeeper>,
        ins: Instance,
        encoder: &'static EC,
        dynamic: bool,
        persistent_exist_node_path: Arc<RwLock<HashSet<String>>>,
    ) -> Self
    where
        EC: Encoder + Sync + 'static,
    {
        RegFut {
            join_handle: task::spawn_blocking(move || {
                let last_path = String::from_utf8(
                    encoder
                        .encode(&ins)
                        .map_err(|e| -> EncodeError { e.into() })?,
                )
                .map_err(|e| EncodeError {})?;
                create_path(
                    client,
                    &(ins.appid + "/" + last_path.as_str()),
                    dynamic,
                    persistent_exist_node_path,
                )
            }),
        }
    }
}

fn create_path(
    client: Arc<ZooKeeper>,
    path: &str,
    dynamic: bool,
    persistent_exist_node_path: Arc<RwLock<HashSet<String>>>,
) -> Result<(), ZkRegError> {
    if !dynamic {
        if persistent_exist_node_path.read().unwrap().contains(path) {
            return Ok(());
        }
        if client
            .exists(path, false)
            .map_err(|e| ZkRegError::CreatePath(e))?
            .is_some()
        {
            persistent_exist_node_path
                .write()
                .unwrap()
                .insert(path.to_owned());
        }
    }

    if let Some(pos) = path.rfind('/') {
        if pos > 0 {
            create_path(
                client.clone(),
                &path[..pos],
                false,
                persistent_exist_node_path.clone(),
            )?;
        }
    }

    client
        .create(
            path,
            Vec::new(),
            Acl::open_unsafe().clone(),
            if dynamic {
                CreateMode::Ephemeral
            } else {
                CreateMode::Persistent
            },
        )
        .map_err(|e| ZkRegError::CreatePath(e))?;
    persistent_exist_node_path
        .write()
        .unwrap()
        .insert(path.to_owned());
    Ok(())
}

impl Future for RegFut {
    type Output = Result<(), ZkRegError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(match ready!(self.project().join_handle.poll(cx)) {
            Ok(out) => out,
            Err(e) => Err(ZkRegError::Join(e)),
        })
    }
}

#[derive(Debug)]
pub enum ZkRegError {
    Encode,
    Decode,
    CreatePath(ZkError),
    DeletePath(ZkError),
    Join(JoinError),
}

impl std::error::Error for ZkRegError {}

impl From<EncodeError> for ZkRegError {
    fn from(e: EncodeError) -> Self {
        todo!()
    }
}

impl From<DecodeErorr> for ZkRegError {
    fn from(e: DecodeErorr) -> Self {
        todo!()
    }
}

#[pin_project]
pub struct DeRegFut {
    #[pin]
    join_handle: JoinHandle<Result<(), ZkRegError>>,
}

impl DeRegFut {
    pub fn new<EC>(
        client: Arc<ZooKeeper>,
        ins: &Instance,
        encoder: &'static EC,
        persistent_exist_node_path: Arc<RwLock<HashSet<String>>>,
    ) -> Self
    where
        EC: Encoder + Sync + 'static,
    {
        let ins = ins.clone();
        DeRegFut {
            join_handle: task::spawn_blocking(move || {
                let last_path = String::from_utf8(
                    encoder
                        .encode(&ins)
                        .map_err(|e| -> EncodeError { e.into() })?,
                )
                .map_err(|e| EncodeError {})?;
                let path = ins.appid + "/" + last_path.as_str();
                persistent_exist_node_path
                    .write()
                    .unwrap()
                    .insert(path.clone());
                client
                    .delete(path.as_str(), None)
                    .map_err(|e| ZkRegError::DeletePath(e))
            }),
        }
    }
}

impl Future for DeRegFut {
    type Output = Result<(), ZkRegError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(ready!(self.project().join_handle.poll(cx)).unwrap())
    }
}

impl<EC, DC> Registry for Zk<EC, DC>
where
    EC: Encoder + Sync + 'static,
    DC: Decoder + Sync + 'static,
{
    type Error = ZkRegError;

    type RegFuture = RegFut;

    type DeRegFuture = DeRegFut;

    type Watcher = ZkWatcher;

    fn register(&self, ins: Instance) -> Self::RegFuture {
        let dynamic = ins
            .metadata
            .get("dynamic")
            .map(|v| v == "true")
            .unwrap_or(true);
        RegFut::new(
            self.client.clone(),
            ins,
            self.codec.get_encoder_ref(),
            dynamic,
            self.persistent_exist_node_path.clone(),
        )
    }

    fn deregister(&self, ins: &Instance) -> Self::DeRegFuture {
        DeRegFut::new(
            self.client.clone(),
            ins,
            self.codec.get_encoder_ref(),
            self.persistent_exist_node_path.clone(),
        )
    }

    fn watch(&self, appid: &'static str) -> Self::Watcher {
        ZkWatcher::new(self.client.clone(), appid, self.codec.get_decoder_ref())
    }
}
