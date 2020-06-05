use crate::codec::Decoder;
use crate::watcher::{Event, WatchEvent};
use crate::{HashSet, Instance};
use futures::channel::mpsc;
use futures::Stream;
use log::error;
use pin_project::pin_project;
use std::iter::FromIterator;
use std::{
    sync::{Arc, Mutex},
    task::Poll,
};
use tokio::task;
use zookeeper::{WatchedEvent, WatchedEventType, Watcher, ZooKeeper};

#[pin_project]
pub struct ZkWatcher {
    zk_client: Arc<ZooKeeper>,
    #[pin]
    watch_event_rx: mpsc::UnboundedReceiver<WatchEvent>,
}

impl ZkWatcher {
    pub fn new<D>(zk_client: Arc<ZooKeeper>, appid: &'static str, decoder: &'static D) -> Self
    where
        D: Decoder + Sync + 'static,
    {
        let (watch_event_tx, watch_event_rx) = mpsc::unbounded();
        let client = zk_client.clone();

        task::spawn_blocking(move || {
            let raw_instances = Arc::new(Mutex::new(HashSet::default()));
            *raw_instances.lock().unwrap() = client
                .get_children_w(
                    appid,
                    ZkAppWatchHandler {
                        zk_client: client.clone(),
                        raw_instances: raw_instances.clone(),
                        watch_event_tx: watch_event_tx.clone(),
                        decoder,
                    },
                )
                .map(|children| HashSet::from_iter(children.into_iter()))
                .unwrap_or(HashSet::default()); // todo error;
        });
        Self {
            zk_client,
            watch_event_rx,
        }
    }
}

impl Stream for ZkWatcher {
    type Item = WatchEvent;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.as_mut().project().watch_event_rx.poll_next(cx)
    }
}

struct ZkAppWatchHandler<D>
where
    D: 'static,
{
    zk_client: Arc<ZooKeeper>,
    raw_instances: Arc<Mutex<HashSet<String>>>,
    watch_event_tx: mpsc::UnboundedSender<WatchEvent>,
    decoder: &'static D,
}

impl<D> ZkAppWatchHandler<D>
where
    D: Decoder,
{
    fn diff_and_send_watch_event(&self, new_instances: HashSet<String>) {
        let (created_diff, deleted_diff) = {
            let mut old_instance = self.raw_instances.lock().unwrap();
            let diff = (
                new_instances
                    .difference(&old_instance)
                    .cloned()
                    .collect::<Vec<String>>(),
                old_instance
                    .difference(&new_instances)
                    .cloned()
                    .collect::<Vec<String>>(),
            );
            *old_instance = new_instances;
            diff
        };
        let created_instances_iter = created_diff.iter().filter_map(|ins| {
            decode_instance(ins, self.decoder).map(|ins| WatchEvent::new(Event::Create(ins)))
        });
        let deleted_instances_iter = deleted_diff.iter().filter_map(|ins| {
            decode_instance(ins, self.decoder).map(|ins| WatchEvent::new(Event::Delete(ins)))
        });
        for event in created_instances_iter.chain(deleted_instances_iter) {
            self.watch_event_tx.unbounded_send(event);
        }
    }
}

impl<D> Watcher for ZkAppWatchHandler<D>
where
    D: Decoder + Sync,
{
    fn handle(&self, we: WatchedEvent) {
        if let (WatchedEventType::NodeChildrenChanged, Some(path)) = (we.event_type, we.path) {
            // the children of a watched znode are created or deleted.
            let new_instances = self
                .zk_client
                .get_children_w(
                    path.as_str(),
                    ZkAppWatchHandler {
                        zk_client: self.zk_client.clone(),
                        raw_instances: self.raw_instances.clone(),
                        watch_event_tx: self.watch_event_tx.clone(),
                        decoder: self.decoder,
                    },
                )
                .map(|children| HashSet::from_iter(children.into_iter()))
                .unwrap_or(HashSet::default()); // todo error
            self.diff_and_send_watch_event(new_instances);
        }
    }
}

#[inline]
fn decode_instance<D: Decoder>(ins: &str, decoder: &D) -> Option<Instance> {
    match decoder.decode(ins.as_bytes()) {
        Ok(ins) => Some(ins),
        Err(e) => {
            error!("instance decode error. {}", e.to_string());
            None
        }
    }
}
