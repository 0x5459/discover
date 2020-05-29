use crate::Instance;
use futures::Stream;
use std::time::SystemTime;
#[derive(PartialEq, Eq, Debug)]
pub enum Event<'a> {
    Create(&'a Instance),
    Delete(&'a Instance),
}

pub trait Watcher: Stream {}

impl<'a, T> Watcher for T where T: Stream<Item = WatchEvent<'a>> {}

#[derive(Debug)]
pub struct WatchEvent<'a> {
    pub event: Event<'a>,
    pub timestamp: SystemTime,
}

impl<'a> WatchEvent<'a> {
    pub fn new(event: Event) -> WatchEvent {
        WatchEvent {
            event,
            timestamp: SystemTime::now(),
        }
    }
}
