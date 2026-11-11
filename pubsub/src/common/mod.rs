#[allow(unused)]
pub(crate) const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

pub(crate) enum PubSubMessage{
    Subscribe(String),
    Unsubscribe(String),
    Publish(String, String),
}

//TODO: un decerealise function for PubSubMessage