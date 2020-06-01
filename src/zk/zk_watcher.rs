use crate::codec::{Codec, Decoder, Encoder};
use crate::{
    watcher::{Event, WatchEvent},
    HashSet, Instance,
};
use futures::channel::mpsc;
use futures::{ready, Stream};
use log::error;
use pin_project::pin_project;
use std::{
    sync::{Arc, Mutex},
    task::Poll,
};
use tokio::task;
use zookeeper::{WatchedEvent, Watcher, ZooKeeper};

#[pin_project]
pub struct ZkWatcher<EC, DC>
where
    EC: 'static,
    DC: 'static,
{
    client: Arc<ZooKeeper>,
    codec: &'static Codec<EC, DC>,
    #[pin]
    rx: mpsc::UnboundedReceiver<WatchEvent>,
    instances: Arc<Mutex<HashSet<Arc<Instance>>>>,
}

impl<EC, DC> ZkWatcher<EC, DC> {
    pub fn new(client: Arc<ZooKeeper>, appid: &'static str, codec: &'static Codec<EC, DC>) -> Self
    where
        EC: Encoder + Sync + 'static,
        DC: Decoder + Sync + 'static,
    {
        let (mut tx, rx) = mpsc::unbounded();
        let zk_client = client.clone();
        let decoder = codec.get_decoder_ref();
        task::spawn_blocking(move || {
            let instances = zk_client
                .get_children_w(
                    appid,
                    ZkInstanceWatchHandler {
                        client: zk_client.clone(),
                        codec,
                        tx: tx.clone(),
                    },
                )
                .unwrap_or(vec![]); // todo error

            instances.iter().for_each(|ins_str| {
                // let ins = decoder.decode(ins_str.as_bytes());
                let ins = decoder.decode(ins_str.as_bytes());
                if let Err(e) = ins {
                    error!("[ZkWather] instance decode error {}", e);
                    return;
                }
                let ins = Arc::new(ins.unwrap());
                tx.start_send(WatchEvent::new(Event::Create(ins.clone())));
                let exits = zk_client.exists_w(
                    appid,
                    ZkInstanceItemWatchHandler {
                        client: zk_client.clone(),
                        ins: ins.clone(),
                        tx: tx.clone(),
                    },
                );
                if let Ok(None) = exits {
                    tx.start_send(WatchEvent::new(Event::Delete(ins)));
                }
            });
        });

        Self {
            client,
            codec,
            rx,
            instances: Arc::new(Mutex::new(HashSet::default())),
        }
    }
}

impl<EC, DC> Stream for ZkWatcher<EC, DC>
where
    EC: Encoder + 'static,
    DC: Decoder + 'static,
{
    type Item = WatchEvent;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let event_opt: Option<Self::Item> = ready!(self.as_mut().project().rx.poll_next(cx));
        match event_opt {
            Some(ref e) => match &e.event {
                Event::Create(ins) => {
                    if self.instances.lock().unwrap().contains(ins.as_ref()) {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    } else {
                        self.instances.lock().unwrap().insert(ins.clone());
                        Poll::Ready(event_opt)
                    }
                }
                Event::Delete(ins) => {
                    if self.instances.lock().unwrap().contains(ins.as_ref()) {
                        self.instances.lock().unwrap().remove(ins.as_ref());
                        Poll::Ready(event_opt)
                    } else {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            },
            None => Poll::Ready(None),
        }
    }
}

struct ZkInstanceWatchHandler<EC, DC>
where
    EC: 'static,
    DC: 'static,
{
    client: Arc<ZooKeeper>,
    codec: &'static Codec<EC, DC>,
    tx: mpsc::UnboundedSender<WatchEvent>,
}

impl<EC, DC> Watcher for ZkInstanceWatchHandler<EC, DC>
where
    EC: Encoder + Sync + 'static,
    DC: Decoder + Sync + 'static,
{
    fn handle(&self, we: WatchedEvent) {
        if we.path.is_none() {
            return;
        }
        let path = we.path.unwrap();
        let path = path.as_str();
        let instances = self
            .client
            .get_children_w(
                path,
                ZkInstanceWatchHandler {
                    client: self.client.clone(),
                    codec: self.codec,
                    tx: self.tx.clone(),
                },
            )
            .unwrap_or(vec![]); // todo error

        let decoder = self.codec.get_decoder_ref();
        let mut tx = self.tx.clone();
        instances.iter().for_each(|ins_str| {
            let ins = decoder.decode(ins_str.as_bytes());
            if let Err(e) = ins {
                error!(
                    "[ZkInstanceWatchHandler] instance decode error {}",
                    e.to_string()
                );
                return;
            }
            let ins = Arc::new(ins.unwrap());
            tx.start_send(WatchEvent::new(Event::Create(ins.clone())));
            let exits = self.client.exists_w(
                path,
                ZkInstanceItemWatchHandler {
                    client: self.client.clone(),
                    ins: ins.clone(),
                    tx: tx.clone(),
                },
            );

            if let Ok(None) = exits {
                tx.unbounded_send(WatchEvent::new(Event::Delete(ins))).ok();
            }
        });
    }
}

struct ZkInstanceItemWatchHandler {
    client: Arc<ZooKeeper>,
    ins: Arc<Instance>,
    tx: mpsc::UnboundedSender<WatchEvent>,
}

impl Watcher for ZkInstanceItemWatchHandler {
    fn handle(&self, we: WatchedEvent) {
        if we.path.is_none() {
            return;
        }
        match we.event_type {
            zookeeper::WatchedEventType::NodeCreated => {
                self.tx
                    .unbounded_send(WatchEvent::new(Event::Create(self.ins.clone())))
                    .ok();
            }
            zookeeper::WatchedEventType::NodeDeleted => {
                self.tx
                    .unbounded_send(WatchEvent::new(Event::Delete(self.ins.clone())))
                    .ok();
            }
            _ => {}
        };

        let exits = self.client.exists_w(
            we.path.unwrap().as_str(),
            ZkInstanceItemWatchHandler {
                client: self.client.clone(),
                ins: self.ins.clone(),
                tx: self.tx.clone(),
            },
        );
        if let Ok(None) = exits {
            self.tx
                .unbounded_send(WatchEvent::new(Event::Delete(self.ins.clone())))
                .ok();
        }
    }
}
