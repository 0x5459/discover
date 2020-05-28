use futures::{Future, Stream};
use pin_project::pin_project;
use std::collections::HashMap;
use tower::discover::{Change, Discover};
use watcher::{Event, WatchEvent};

mod codec;
mod watcher;

type Value = serde_json::Value;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Instance {
    zone: String,
    env: String,
    appid: String,
    hostname: String,
    addrs: Vec<String>,
    version: String,
    metadata: HashMap<String, Value>,
}

pub trait Registry {
    type Error;

    type RegFuture: Future<Output = Result<(), Self::Error>>;

    type DeRegFuture: Future<Output = Result<(), Self::Error>>;

    type Watcher: Stream<Item = WatchEvent>;

    fn register(&self, e: Instance) -> Self::RegFuture;

    fn deregister(&self, e: &Instance) -> Self::DeRegFuture;

    fn watch(&self, appid: &str) -> Self::Watcher;
}

#[pin_project]
struct AppDiscover<SB, R>
where
    R: Registry,
{
    #[pin]
    watcher: R::Watcher,
    #[pin]
    service_creater: SB,
}

impl<SB, R, S> AppDiscover<SB, R>
where
    R: Registry,
    SB: Fn(Instance) -> S,
{
    pub fn new<W>(watcher: R::Watcher, service_creater: SB) -> Self {
        Self {
            watcher,
            service_creater,
        }
    }
}

impl<SB, R, S> Discover for AppDiscover<SB, R>
where
    R: Registry,
    SB: Fn(Instance) -> S,
{
    type Key = String;
    type Service = S;
    type Error = String;

    fn poll_discover(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Change<Self::Key, Self::Service>, Self::Error>> {
        self.as_mut()
            .project()
            .watcher
            .poll_next(cx)
            .map(|watch_event_opt| match watch_event_opt {
                Some(watch_event) => match watch_event.event {
                    Event::Create(ins) => Ok(Change::Insert(
                        ins.appid.clone(),
                        (self.as_mut().project().service_creater)(ins),
                    )),
                    Event::Delete(ins) => Ok(Change::Remove(ins.appid)),
                },
                None => Err("mother fuck".to_owned()),
            })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
