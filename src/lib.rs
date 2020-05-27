use std::collections::HashMap;

mod codec;

type Value = serde_json::Value;

pub struct Instance {
    zone: String,
    env: String,
    appid: String,
    hostname: String,
    addrs: Vec<String>,
    version: String,
    metadata: HashMap<String, Value>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
