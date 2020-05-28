use crate::Instance;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet};

pub trait Encoder {
    type Error;

    fn encode(&self, ins: &Instance) -> Result<Vec<u8>, Self::Error>;
}

/// Returns a new `EncoderFn` with the given closure.
pub fn encoder_fn<T>(f: T) -> EncoderFn<T> {
    EncoderFn { f }
}

pub struct EncoderFn<T> {
    f: T,
}

impl<T, E> Encoder for EncoderFn<T>
where
    T: Fn(&Instance) -> Result<Vec<u8>, E>,
{
    type Error = E;

    fn encode(&self, ins: &Instance) -> Result<Vec<u8>, Self::Error> {
        (self.f)(ins)
    }
}

pub trait Decoder {
    type Error;

    fn decode(&self, key: &[u8], value: &[u8]) -> Result<Instance, Self::Error>;
}

/// Returns a new `DecoderFn` with the given closure.
pub fn decoder_fn<T>(f: T) -> DecoderFn<T> {
    DecoderFn { f }
}

pub struct DecoderFn<T> {
    f: T,
}

impl<T, E> Decoder for DecoderFn<T>
where
    T: Fn(&[u8], &[u8]) -> Result<Instance, E>,
{
    type Error = E;
    fn decode(&self, key: &[u8], value: &[u8]) -> Result<Instance, Self::Error> {
        (self.f)(key, value)
    }
}

pub struct Codec<KeyEncoder, ValueEncoder, D> {
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

const URL_ENCODE_SET: &AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'*')
    .remove(b'-')
    .remove(b'.')
    .remove(b'_');

pub fn new_default_codec() -> Codec<
    EncoderFn<impl Fn(&Instance) -> Result<Vec<u8>, String>>,
    EncoderFn<impl Fn(&Instance) -> Result<Vec<u8>, String>>,
    DecoderFn<impl Fn(&[u8], &[u8]) -> Result<Instance, String>>,
> {
    Codec::new(
        encoder_fn(|ins: &Instance| -> Result<_, String> { Ok(ins.appid.clone().into_bytes()) }),
        encoder_fn(|ins: &Instance| -> Result<_, String> {
            let mut s = String::new();
            s.push_str("zone=");
            s.extend(utf8_percent_encode(&ins.zone, URL_ENCODE_SET));
            s.push_str("&env=");
            s.extend(utf8_percent_encode(&ins.env, URL_ENCODE_SET));
            s.push_str("&hostname=");
            s.extend(utf8_percent_encode(&ins.hostname, URL_ENCODE_SET));
            for addr in ins.addrs.iter() {
                s.push_str("&addrs=");
                s.extend(utf8_percent_encode(addr, URL_ENCODE_SET));
            }
            s.push_str("&version=");
            s.extend(utf8_percent_encode(&ins.version, URL_ENCODE_SET));
            s.push_str("&metadata=");
            s.extend(utf8_percent_encode(
                &(serde_json::to_string(&ins.metadata).map_err(|err| err.to_string())?),
                URL_ENCODE_SET,
            ));
            Ok(s.into_bytes())
        }),
        decoder_fn(|key: &[u8], value: &[u8]| -> Result<_, String> {
            let mut ins = Instance::default();
            let value = std::str::from_utf8(value).map_err(|err| err.to_string())?;

            let pair_iter = value.split('&').map(|pair| {
                let pair = pair.splitn(2, '=').collect::<Vec<&str>>();
                if pair.len() < 2 {
                    (unsafe { *pair.get_unchecked(0) }, "")
                } else {
                    unsafe { (*pair.get_unchecked(0), *pair.get_unchecked(1)) }
                }
            });

            for (k, v) in pair_iter {
                let v = percent_decode_str(v)
                    .decode_utf8()
                    .map_err(|err| err.to_string())?;

                match k {
                    "zone" => ins.zone = v.into_owned(),
                    "env" => ins.env = v.into_owned(),
                    "hostname" => ins.env = v.into_owned(),
                    "addrs" => ins.addrs.push(v.into_owned()),
                    "version" => ins.version = v.into_owned(),
                    "metadata" => {
                        ins.metadata =
                            serde_json::from_str(v.as_ref()).map_err(|err| err.to_string())?
                    }
                    _ => {}
                }
            }
            ins.appid = std::str::from_utf8(key)
                .map_err(|err| err.to_string())?
                .to_owned();
            Ok(ins)
        }),
    )
}

#[cfg(test)]
mod tests {

    use super::new_default_codec;
    use super::Encoder;
    use crate::{Instance, Value};
    use serde_json::Number;

    #[test]
    fn test_default_value_encoder_encode() {
        let cases = [
            (Instance {
                zone: "sh1".to_owned(),
                env: "test".to_owned(),
                appid: "provider".to_owned(),
                hostname: "myhostname".to_owned(),
                addrs: vec!["http://172.1.1.1:8000".to_owned(), "grpc://172.1.1.1:9999".to_owned()],
                version: "111".to_owned(),
                metadata: [("weight".to_owned(), Value::Number(Number::from(10)))].iter().cloned().collect()
            }, "zone=sh1&env=test&hostname=myhostname&addrs=http%3A%2F%2F172.1.1.1%3A8000&addrs=grpc%3A%2F%2F172.1.1.1%3A9999&version=111&metadata=%7B%22weight%22%3A10%7D")
        ];
        let codec = new_default_codec();
        let value_encoder = codec.get_value_encoder_ref();
        for case in cases.iter() {
            let res = value_encoder.encode(&case.0);
            assert!(res.is_ok());
            assert_eq!(case.1, String::from_utf8(res.unwrap()).unwrap());
        }
    }

    #[test]
    fn test_default_key_encoder_encode() {
        let mut ins = Instance::default();
        ins.appid = "卢本伟牛逼".to_owned();
        let codec = new_default_codec();
        let key_encoder = codec.get_key_encoder_ref();
        let res = key_encoder.encode(&ins);
        assert!(res.is_ok());
        assert_eq!(ins.appid, String::from_utf8(res.unwrap()).unwrap());
    }
}
