use crate::Instance;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

trait Encoder {
    type Error;

    fn encode(&self, ins: &Instance) -> Result<Vec<u8>, Self::Error>;
}

trait Decoder {
    type Error;

    fn decode(&self, key: &[u8], value: &[u8]) -> Result<Instance, Self::Error>;
}

struct Codec<KeyEncoder, ValueEncoder, D> {
    key_encoder: KeyEncoder,
    value_encoder: ValueEncoder,
    decoder: D,
}

impl<KeyEncoder, ValueEncoder, D> Codec<KeyEncoder, ValueEncoder, D>
where
    KeyEncoder: Encoder,
    ValueEncoder: Encoder,
    D: Decoder,
{
    pub fn new(key_encoder: KeyEncoder, value_encoder: ValueEncoder, decoder: D) -> Self {
        Self {
            key_encoder,
            value_encoder,
            decoder,
        }
    }

    pub fn get_key_encoder_ref(&self) -> &KeyEncoder {
        &self.key_encoder
    }

    pub fn get_value_encoder_ref(&self) -> &ValueEncoder {
        &self.value_encoder
    }

    pub fn get_decoder_ref(&self) -> &D {
        &self.decoder
    }
}

struct DefaultKeyEncoder;

impl Encoder for DefaultKeyEncoder {
    type Error = String;

    fn encode(&self, ins: &Instance) -> Result<Vec<u8>, Self::Error> {
        Ok(ins.appid.clone().into_bytes())
    }
}

pub struct DefaultValueEncoder;

impl Encoder for DefaultValueEncoder {
    type Error = String;

    fn encode(&self, ins: &Instance) -> Result<Vec<u8>, Self::Error> {
        let mut s = String::new();
        s.push_str("zone=");
        s.extend(utf8_percent_encode(&ins.zone, NON_ALPHANUMERIC));
        s.push_str("&env=");
        s.extend(utf8_percent_encode(&ins.env, NON_ALPHANUMERIC));
        s.push_str("&appid=");
        s.extend(utf8_percent_encode(&ins.appid, NON_ALPHANUMERIC));
        s.push_str("&hostname=");
        s.extend(utf8_percent_encode(&ins.hostname, NON_ALPHANUMERIC));
        for addr in ins.addrs.iter() {
            s.push_str("&addrs=");
            s.extend(utf8_percent_encode(addr, NON_ALPHANUMERIC));
        }
        s.push_str("&version=");
        s.extend(utf8_percent_encode(&ins.version, NON_ALPHANUMERIC));
        s.push_str("&metadata=");
        s.extend(utf8_percent_encode(
            &(serde_json::to_string(&ins.metadata).map_err(|err| err.to_string())?),
            NON_ALPHANUMERIC,
        ));
        Ok(s.into_bytes())
    }
}

#[cfg(test)]
mod tests {

    use super::DefaultValueEncoder;
    use crate::{Value, Instance};
    use super::Encoder;
    use serde_json::Number;

    #[test]
    fn test_default_value_encoder_encode() {

        let ins = Instance {
            zone: "sh1".to_owned(),
            env: "test".to_owned(),
            appid: "provider".to_owned(),
            hostname: "myhostname".to_owned(),
            addrs: vec!["http://172.1.1.1:8000".to_owned(), "grpc://172.1.1.1:9999".to_owned()],
            version: "111".to_owned(),
            metadata: [("Norway".to_owned(), Value::Number(Number::from(10)))].iter().cloned().collect()
        };
        let defaultValueEncoder = DefaultValueEncoder{};
        println!("{:?}", defaultValueEncoder.encode(&ins).map(|data| String::from_utf8(data)))
    }
}
