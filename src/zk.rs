use crate::{
    codec::{Codec, DecodeErorr, Decoder, EncodeErorr, Encoder},
    Instance, Registry,
};
use futures::{ready, Future, FutureExt};
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::task;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use zk_watcher::ZkWatcher;
use zookeeper::{Acl, CreateMode, ZkError, ZooKeeper, ZooKeeperExt};

mod zk_watcher;

pub struct Zk<EC, DC>
where
    EC: 'static,
    DC: 'static,
{
    client: Arc<ZooKeeper>,
    codec: &'static Codec<EC, DC>,
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
    pub fn new<EC, DC>(
        client: Arc<ZooKeeper>,
        ins: Instance,
        codec: &'static Codec<EC, DC>,
        dynamic: bool,
    ) -> Self
    where
        EC: Encoder + Sync + 'static,
        DC: Decoder + Sync + 'static,
    {
        let encoder = codec.get_encoder_ref();

        RegFut {
            join_handle: task::spawn_blocking(move || {
                let data = encoder
                    .encode(&ins)
                    .map_err(|e| -> EncodeErorr { e.into() })?;
                client
                    .ensure_path(ins.appid.as_str())
                    .map_err(|e| ZkRegError::CreatePath(e))?;
                client
                    .create(
                        ins.appid.as_str(),
                        data,
                        Acl::open_unsafe().clone(),
                        if dynamic {
                            CreateMode::Ephemeral
                        } else {
                            CreateMode::Persistent
                        },
                    )
                    .map_err(|e| ZkRegError::CreatePath(e))?;
                Ok(())
            }),
        }
    }
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

pub enum ZkRegError {
    Encode,
    Decode,
    CreatePath(ZkError),
    DeletePath(ZkError),
    Join(JoinError),
}

impl From<EncodeErorr> for ZkRegError {
    fn from(e: EncodeErorr) -> Self {
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
    pub fn new(client: Arc<ZooKeeper>, path: String) -> Self {
        DeRegFut {
            join_handle: task::spawn_blocking(move || {
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
        RegFut::new(self.client.clone(), ins, self.codec, dynamic)
    }

    fn deregister(&self, ins: &Instance) -> Self::DeRegFuture {
        DeRegFut::new(self.client.clone(), ins.appid.to_owned())
    }

    fn watch(&self, appid: &'static str) -> Self::Watcher {
        ZkWatcher::new(self.client.clone(), appid, self.codec.get_decoder_ref())
    }
}
