use discover::codec::DEFAULT_CODEC;
use discover::zk::Zk;
use discover::{watcher::Event, Instance, Registry};
use futures::stream::{self, StreamExt};
use std::io::{BufRead, BufReader, Write};
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use zookeeper::ZooKeeper;

pub struct ZkCluster {
    process: Child,
    connect_string: String,
    closed: bool,
}

impl ZkCluster {
    fn start(instances: usize) -> ZkCluster {
        let mut process = match Command::new("java")
            .arg("-jar")
            .arg("zk-test-cluster/target/main.jar")
            .arg(instances.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
        {
            Ok(p) => p,
            Err(e) => panic!("failed to start ZkCluster: {}", e),
        };
        let connect_string = Self::read_connect_string(&mut process);
        ZkCluster {
            process,
            connect_string,
            closed: false,
        }
    }

    fn read_connect_string(process: &mut Child) -> String {
        let mut reader = BufReader::new(process.stdout.as_mut().unwrap());
        let mut connect_string = String::new();
        if reader.read_line(&mut connect_string).is_err() {
            panic!("Couldn't read ZK connect_string")
        }
        connect_string.pop(); // remove '\n'
        connect_string
    }

    fn kill_an_instance(&mut self) {
        self.process.stdin.as_mut().unwrap().write(b"k").unwrap();
    }

    fn shutdown(&mut self) {
        if !self.closed {
            self.process.stdin.as_mut().unwrap().write(b"q").unwrap();
            assert!(self.process.wait().unwrap().success());
            self.closed = true
        }
    }
}

impl Drop for ZkCluster {
    fn drop(&mut self) {
        self.shutdown()
    }
}

#[cfg(test)]
#[tokio::test(threaded_scheduler)]
async fn test_register_deregister() {
    let cluster = ZkCluster::start(3);
    let zk = Zk::new(
        &cluster.connect_string,
        Duration::from_millis(3000),
        &DEFAULT_CODEC,
    )
    .await;

    let ins = Instance {
        zone: "sh1".to_owned(),
        env: "test".to_owned(),
        appid: "/dubbo-rs/provider".to_owned(),
        hostname: "myhostname".to_owned(),
        addrs: vec![
            "http://172.1.1.1:8000".to_owned(),
            "grpc://172.1.1.1:9999".to_owned(),
        ],
        version: "111".to_owned(),
        metadata: [("weight".to_owned(), "10".to_owned())]
            .iter()
            .cloned()
            .collect(),
    };

    let _ = zk.register(ins.clone()).await.unwrap();

    let zk_client =
        ZooKeeper::connect(&cluster.connect_string, Duration::from_millis(3000), |_| {}).unwrap();
    let path = "/dubbo-rs/provider/zone=sh1&env=test&appid=%2Fdubbo-rs%2Fprovider&hostname=myhostname&addrs=http%3A%2F%2F172.1.1.1%3A8000&addrs=grpc%3A%2F%2F172.1.1.1%3A9999&version=111&metadata=%7B%22weight%22%3A%2210%22%7D";
    assert!(zk_client.exists(path, false).unwrap().is_some());

    let _ = zk.deregister(&ins).await;
    assert!(zk_client.exists(path, false).unwrap().is_none());
}

#[tokio::test(threaded_scheduler)]
async fn test_watch() {
    let cluster = ZkCluster::start(3);

    let zk = Zk::new(
        &cluster.connect_string,
        Duration::from_millis(3000),
        &DEFAULT_CODEC,
    )
    .await;

    let app_id = "/dubbo-rs/provider";
    let ins1 = Instance {
        zone: "sh1".to_owned(),
        env: "test".to_owned(),
        appid: app_id.to_owned(),
        hostname: "myhostname".to_owned(),
        addrs: vec![
            "http://172.1.1.1:8000".to_owned(),
            "grpc://172.1.1.1:9999".to_owned(),
        ],
        version: "111".to_owned(),
        metadata: [("weight".to_owned(), "10".to_owned())]
            .iter()
            .cloned()
            .collect(),
    };

    let ins2 = Instance {
        zone: "sh1".to_owned(),
        env: "test".to_owned(),
        appid: app_id.to_owned(),
        hostname: "myhostname".to_owned(),
        addrs: vec![
            "http://172.1.1.2:8000".to_owned(),
            "grpc://172.1.1.2:9999".to_owned(),
        ],
        version: "111".to_owned(),
        metadata: [("weight".to_owned(), "10".to_owned())]
            .iter()
            .cloned()
            .collect(),
    };

    let _ = zk.register(ins1.clone()).await;

    let mut watcher = zk.watch(app_id);

    let _ = zk.register(ins2.clone()).await;

    let _ = zk.deregister(&ins1).await;

    let _ = zk.deregister(&ins2).await;

    let first_event = watcher.next().await;
    assert!(first_event.is_some());
    let first_event = first_event.unwrap();
    assert!(matches!(first_event.event, Event::Create(..)));
    if let Event::Create(ins) = first_event.event {
        assert_eq!(ins, ins2);
    }

    let second_event = watcher.next().await;
    assert!(second_event.is_some());
    let second_event = second_event.unwrap();
    assert!(matches!(second_event.event, Event::Delete(..)));
    if let Event::Delete(ins) = second_event.event {
        assert_eq!(ins, ins1);
    }

    let third_event = watcher.next().await;
    assert!(third_event.is_some());
    let third_event = third_event.unwrap();
    assert!(matches!(third_event.event, Event::Delete(..)));
    if let Event::Delete(ins) = third_event.event {
        assert_eq!(ins, ins2);
    }
}
