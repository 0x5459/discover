use crate::Instance;
use futures::Stream;
use std::{sync::Arc, time::SystemTime};

#[derive(PartialEq, Eq, Debug)]
pub enum Event {
    Create(Arc<Instance>),
    Delete(Arc<Instance>),
}

pub trait Watcher: Stream {}

impl<T> Watcher for T where T: Stream<Item = WatchEvent> {}

#[derive(Debug)]
pub struct WatchEvent {
    pub event: Event,
    pub timestamp: SystemTime,
}

impl WatchEvent {
    pub fn new(event: Event) -> WatchEvent {
        WatchEvent {
            event,
            timestamp: SystemTime::now(),
        }
    }
}
