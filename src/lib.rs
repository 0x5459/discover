use futures::{Future, Stream};
use fxhash;
use pin_project::pin_project;
use std::{collections::HashMap, hash::Hash};
use tower::discover::{Change, Discover};
use watcher::{Event, WatchEvent};

pub mod codec;
pub mod watcher;
pub mod zk;

pub type HashSet<T> = std::collections::HashSet<T, std::hash::BuildHasherDefault<fxhash::FxHasher>>;

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Instance {
    pub zone: String,
    pub env: String,
    pub appid: String,
    pub hostname: String,
    pub addrs: Vec<String>,
    pub version: String,
    pub metadata: HashMap<String, String>,
}

impl Hash for Instance {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.appid.hash(state);
        self.version.hash(state);
        self.env.hash(state);
        self.addrs.hash(state);
    }
}

pub trait Registry {
    type Error;

    type RegFuture: Future<Output = Result<(), Self::Error>>;

    type DeRegFuture: Future<Output = Result<(), Self::Error>>;

    type Watcher: Stream<Item = WatchEvent>;

    fn register(&self, ins: Instance) -> Self::RegFuture;

    fn deregister(&self, ins: &Instance) -> Self::DeRegFuture;

    fn watch(&self, appid: &'static str) -> Self::Watcher;
}

#[pin_project]
pub struct AppDiscover<SB, R>
where
    R: Registry,
{
    #[pin]
    watcher: R::Watcher,
    #[pin]
    service_creater: SB,
}

impl<SB, R> AppDiscover<SB, R>
where
    R: Registry,
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
    SB: Fn(&Instance) -> S,
{
    type Key = String;
    type Service = S;
    type Error = Terminated;

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
                        (self.as_mut().project().service_creater)(&ins),
                    )),
                    Event::Delete(ins) => Ok(Change::Remove(ins.appid.clone())),
                },
                None => Err(Terminated),
            })
    }
}

pub struct Terminated;
