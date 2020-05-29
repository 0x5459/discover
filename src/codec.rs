use crate::Instance;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet};
use std::str::Utf8Error;

pub trait Encoder {
    type Error;

    fn encode(&self, ins: &Instance) -> Result<Vec<u8>, Self::Error>;
}

impl<F, E> Encoder for F
where
    F: Fn(&Instance) -> Result<Vec<u8>, E>,
{
    type Error = E;
    fn encode(&self, ins: &Instance) -> Result<Vec<u8>, Self::Error> {
        self(ins)
    }
}

pub trait Decoder {
    type Error;

    fn decode(&self, data: &[u8]) -> Result<Instance, Self::Error>;
}

impl<F, E> Decoder for F
where
    F: Fn(&[u8]) -> Result<Instance, E>,
{
    type Error = E;
    fn decode(&self, data: &[u8]) -> Result<Instance, Self::Error> {
        self(data)
    }
}

pub struct Codec<E, D> {
    encoder: E,
    decoder: D,
}

impl<E, D> Codec<E, D>
where
    E: Encoder,
    D: Decoder,
{
    pub fn new(encoder: E, decoder: D) -> Self {
        Self { encoder, decoder }
    }

    pub fn get_encoder_ref(&self) -> &E {
        &self.encoder
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

#[derive(Debug)]
pub enum DefaultCodecError {
    UTF8(Utf8Error),
    MetadataSerde(serde_json::Error),
}

impl From<Utf8Error> for DefaultCodecError {
    fn from(e: Utf8Error) -> Self {
        DefaultCodecError::UTF8(e)
    }
}

pub fn new_default_codec() -> Codec<
    impl Fn(&Instance) -> Result<Vec<u8>, DefaultCodecError>,
    impl Fn(&[u8]) -> Result<Instance, DefaultCodecError>,
> {
    Codec::new(
        |ins: &Instance| -> Result<_, DefaultCodecError> {
            let mut s = String::new();
            s.push_str("zone=");
            s.extend(utf8_percent_encode(&ins.zone, URL_ENCODE_SET));
            s.push_str("&env=");
            s.extend(utf8_percent_encode(&ins.env, URL_ENCODE_SET));
            s.push_str("&appid=");
            s.extend(utf8_percent_encode(&ins.appid, URL_ENCODE_SET));
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
                &(serde_json::to_string(&ins.metadata)
                    .map_err(|e| DefaultCodecError::MetadataSerde(e))?),
                URL_ENCODE_SET,
            ));
            Ok(s.into_bytes())
        },
        |data: &[u8]| -> Result<_, DefaultCodecError> {
            let mut ins = Instance::default();
            let value = std::str::from_utf8(data)?;

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
                    .map_err(|err| DefaultCodecError::UTF8(err))?;

                match k {
                    "zone" => ins.zone = v.into_owned(),
                    "env" => ins.env = v.into_owned(),
                    "appid" => ins.appid = v.into_owned(),
                    "hostname" => ins.env = v.into_owned(),
                    "addrs" => ins.addrs.push(v.into_owned()),
                    "version" => ins.version = v.into_owned(),
                    "metadata" => {
                        ins.metadata = serde_json::from_str(v.as_ref())
                            .map_err(|e| DefaultCodecError::MetadataSerde(e))?
                    }
                    _ => {}
                }
            }
            Ok(ins)
        },
    )
}

#[cfg(test)]
mod tests {

    use super::new_default_codec;
    use super::Encoder;
    use crate::{Instance, Value};
    use serde_json::Number;

    #[test]
    fn test_default_encoder_encode() {
        let cases = [
            (Instance {
                zone: "sh1".to_owned(),
                env: "test".to_owned(),
                appid: "provider".to_owned(),
                hostname: "myhostname".to_owned(),
                addrs: vec!["http://172.1.1.1:8000".to_owned(), "grpc://172.1.1.1:9999".to_owned()],
                version: "111".to_owned(),
                metadata: [("weight".to_owned(), Value::Number(Number::from(10)))].iter().cloned().collect()
            }, "zone=sh1&env=test&appid=provider&hostname=myhostname&addrs=http%3A%2F%2F172.1.1.1%3A8000&addrs=grpc%3A%2F%2F172.1.1.1%3A9999&version=111&metadata=%7B%22weight%22%3A10%7D")
        ];
        let codec = new_default_codec();
        let encoder = codec.get_encoder_ref();
        for case in cases.iter() {
            let res = encoder.encode(&case.0);
            assert!(res.is_ok());
            assert_eq!(case.1, String::from_utf8(res.unwrap()).unwrap());
        }
    }
}
