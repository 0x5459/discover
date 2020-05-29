use crate::{
    codec::{Codec, Decoder, DefaultCodecError, Encoder},
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
use tokio::task::JoinHandle;
use zookeeper::{
    Acl, CreateMode, WatchedEvent, Watcher as ZookeeperWatcher, ZkError, ZooKeeper, ZooKeeperExt,
};

struct Zk<'a, EC, DE> {
    client: Arc<ZooKeeper>,
    codec: &'a Codec<EC, DE>,
}

impl<'a, EC, DE> Zk<'a, EC, DE> {
    fn new(
        zk_urls: &str,
        timeout: Duration,
        codec: &'a Codec<EC, DE>,
    ) -> impl Future<Output = Self> {
        let zk_urls = zk_urls.to_string();

        task::spawn_blocking(move || Zk {
            client: Arc::new(ZooKeeper::connect(zk_urls.as_str(), timeout, |_| {}).unwrap()),
            codec,
        })
        .map(|zk| zk.unwrap())
    }
}

#[pin_project]
struct RegFut {
    #[pin]
    join_handle: JoinHandle<Result<(), String>>,
}

impl RegFut {
    pub fn new<EC, DE>(
        client: Arc<ZooKeeper>,
        ins: &Instance,
        codec: &Codec<EC, DE>,
        dynamic: bool,
    ) -> Self
    where
        EC: Encoder,
        DE: Decoder,
    {
        let encoder = codec.get_encoder_ref();
        RegFut {
            join_handle: task::spawn_blocking(move || {
                let data = encoder.encode(ins)?;
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
    type Output = Result<(), String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(ready!(self.project().join_handle.poll(cx)).unwrap())
    }
}

pub enum ZkRegError {
    Codec(DefaultCodecError),
    CreatePath(ZkError),
    DeletePath(ZkError),
}

impl From<DefaultCodecError> for ZkRegError {
    fn from(e: DefaultCodecError) -> Self {
        ZkRegError::Codec(e)
    }
}

#[pin_project]
struct DeRegFut {
    #[pin]
    join_handle: JoinHandle<Result<(), String>>,
}

impl DeRegFut {
    fn new(client: Arc<ZooKeeper>, path: &str) -> Self {
        DeRegFut {
            join_handle: task::spawn_blocking(move || {
                client
                    .delete(path, None)
                    .map_err(|e| ZkRegError::DeletePath(e))
            }),
        }
    }
}

impl Future for DeRegFut {
    type Output = Result<(), String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(ready!(self.project().join_handle.poll(cx)).unwrap())
    }
}

impl<'a, EC, DE> Registry for Zk<'a, EC, DE> {
    type Error = ZkRegError;

    type RegFuture = RegFut;

    type DeRegFuture = DeRegFut;

    type Watcher = ZkWatcher;

    fn register(&self, ins: Instance) -> Self::RegFuture {
        RegFut::new(self.client.clone(), ins, e.dynamic)
    }

    fn deregister(&self, ins: &Instance) -> Self::DeRegFuture {
        DeRegFut::new(self.client.clone(), e.to_dubbo_path())
    }

    fn watch(&self, appid: &str) -> Self::Watcher {
        ZkWatcher::new(self.client.clone(), en)
    }
}
