use crate::{
    codec::Codec,
    watcher::{Event, WatchEvent},
    HashSet, Instance,
};

use futures::channel::mpsc;
use futures::{ready, Stream};
use pin_project::pin_project;
use std::{
    sync::{Arc, Mutex},
    task::Poll,
};
use tokio::task;
use zookeeper::{WatchedEvent, Watcher, ZooKeeper};

#[pin_project]
pub struct ZkWatcher<'e, 'c, EC, DC> {
    client: Arc<ZooKeeper>,
    codec: &'c Codec<EC, DC>,
    #[pin]
    rx: mpsc::UnboundedReceiver<WatchEvent<'e>>,
    instances: Arc<Mutex<HashSet<Instance>>>,
}

impl<'e, 'c, EC, DC> ZkWatcher<'e, 'c, EC, DC> {
    pub fn new(client: Arc<ZooKeeper>, appid: &str, codec: &'c Codec<EC, DC>) -> Self {
        let (mut tx, rx) = mpsc::unbounded();
        let zk_client = client.clone();

        task::spawn_blocking(move || {
            let instances = zk_client
                .get_children_w(
                    appid,
                    ZkInstanceWatchHandler {
                        client: zk_client.clone(),
                        tx: tx.clone(),
                    },
                )
                .unwrap_or(vec![]);

            instances.iter().for_each(|ins| {
                let endpoint: Endpoint = es.as_str().parse().unwrap();

                let endpoint_item_zk_path = endpoint_zk_path.clone() + "/" + es;
                tx.start_send(WatchEvent::new(Event::Create(endpoint.clone())));
                let exits = zk_client.exists_w(
                    endpoint_item_zk_path.as_str(),
                    ZkInstanceItemWatchHandler {
                        client: zk_client.clone(),
                        ins: endpoint.clone(),
                        tx: tx.clone(),
                    },
                );
                if let Ok(None) = exits {
                    tx.start_send(WatchEvent::new(Event::Delete(endpoint)));
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

impl<'e, EC, DC> Stream for ZkWatcher<'e, '_, EC, DC> {
    type Item = WatchEvent<'e>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let event_opt: Option<Self::Item> = ready!(self.as_mut().project().rx.poll_next(cx));
        match event_opt {
            Some(ref e) => match e.event {
                Event::Create(endpoint) => {
                    if self.endpoints.lock().unwrap().contains(&endpoint) {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    } else {
                        self.endpoints.lock().unwrap().insert(endpoint.clone());
                        Poll::Ready(event_opt)
                    }
                }
                Event::Delete(endpoint) => {
                    if self.endpoints.lock().unwrap().contains(endpoint) {
                        self.endpoints.lock().unwrap().remove(endpoint);
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

struct ZkInstanceWatchHandler<'a> {
    client: Arc<ZooKeeper>,
    tx: mpsc::UnboundedSender<WatchEvent<'a>>,
}

impl<'a> Watcher for ZkInstanceWatchHandler<'a> {
    fn handle(&self, we: WatchedEvent) {
        if we.path.is_none() {
            return;
        }
        let children = self
            .client
            .get_children_w(
                we.path.unwrap().as_str(),
                ZkInstanceWatchHandler {
                    client: self.client.clone(),
                    tx: self.tx.clone(),
                },
            )
            .unwrap_or(vec![]);

        let mut tx = self.tx.clone();
        children.iter().for_each(|es| {
            let endpoint: Endpoint = es.as_str().parse().unwrap();
            tx.start_send(WatchEvent::new(Event::Create(endpoint.clone())));
            let exits = self.client.exists_w(
                es,
                ZkInstanceItemWatchHandler {
                    client: self.client.clone(),
                    endpoint: endpoint.clone(),
                    tx: tx.clone(),
                },
            );

            if let Ok(None) = exits {
                tx.unbounded_send(WatchEvent::new(Event::Delete(endpoint)))
                    .ok();
            }
        });
    }
}

struct ZkInstanceItemWatchHandler<'a> {
    client: Arc<ZooKeeper>,
    ins: Instance,
    tx: mpsc::UnboundedSender<WatchEvent<'a>>,
}

impl<'a> Watcher for ZkInstanceItemWatchHandler<'a> {
    fn handle(&'a self, we: WatchedEvent) {
        if we.path.is_none() {
            return;
        }
        let mut tx = self.tx.clone();
        match we.event_type {
            zookeeper::WatchedEventType::NodeCreated => {
                tx.unbounded_send(WatchEvent::new(Event::Create(self.ins)))
                    .ok();
            }
            zookeeper::WatchedEventType::NodeDeleted => {
                tx.unbounded_send(WatchEvent::new(Event::Delete(self.ins)))
                    .ok();
            }
            _ => {}
        };

        let exits = self.client.exists_w(
            we.path.unwrap().as_str(),
            ZkInstanceItemWatchHandler {
                client: self.client.clone(),
                ins: self.ins,
                tx: self.tx.clone(),
            },
        );
        if let Ok(None) = exits {
            tx.unbounded_send(WatchEvent::new(Event::Delete(self.endpoint.clone())))
                .ok();
        }
    }
}
