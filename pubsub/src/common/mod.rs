use anyhow::bail;
use quinn::RecvStream;

#[allow(unused)]
pub(crate) const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

pub enum PubSubMessage{//creer un bool qui répond au client sur ce qu'il fait (si sub le server répond avec un type sub et dit tu est bien sub ?)
    CreateTopic(String),
    DeleteTopic(String),
    Subscribe(String),
    Unsubscribe(String),
    Publish(String, Vec<u8>),
    Disconnect,
    //Ack, //pour dire que le message a été reçu et traité
    //Error,
}

pub trait MessageHandler {
    async fn on_create_topic(&self, topic: String) -> Result<(), String>;
    async fn on_delete_topic(&self, topic: String) -> Result<(), String>;
    async fn on_subscribe(&self, topic: String) -> Result<(), String>;
    async fn on_unsubscribe(&self, topic: String) -> Result<(), String>;
    async fn on_publish(&self, topic: String, message: Vec<u8>) -> Result<(), String>;
    async fn on_disconnect(&self) -> Result<(), String> {
        Ok(())
    }
}

impl PubSubMessage {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            PubSubMessage::CreateTopic(topic) => {
                let mut data = vec![0u8]; // 0 for CreateTopic
                data.extend_from_slice(topic.as_bytes());
                data
            }
            PubSubMessage::DeleteTopic(topic) => {
                let mut data = vec![1u8]; // 1 for DeleteTopic
                data.extend_from_slice(topic.as_bytes());
                data
            }
            PubSubMessage::Subscribe(topic) => {
                let mut data = vec![2u8]; // 2 for Subscribe
                data.extend_from_slice(topic.as_bytes());
                data
            }
            PubSubMessage::Unsubscribe(topic) => {
                let mut data = vec![3u8]; // 3 for Unsubscribe
                data.extend_from_slice(topic.as_bytes());
                data
            }
            PubSubMessage::Publish(topic, message) => {
                let mut data = vec![4u8]; // 4 for Publish
                data.extend_from_slice(topic.as_bytes());
                data.push(0); // separator between topic and message
                data.extend_from_slice(message);
                data
            }
            PubSubMessage::Disconnect => vec![5u8], // 5 for Disconnect
        }
    }

    pub fn deserialize(data: &[u8]) -> Result<PubSubMessage, String> {
        if data.is_empty() {
            return Err("Empty data".to_string());
        }
        match data[0] {
            0 => Ok(PubSubMessage::CreateTopic(String::from_utf8(data[1..].to_vec()).map_err(|e| e.to_string())?)),
            1 => Ok(PubSubMessage::DeleteTopic(String::from_utf8(data[1..].to_vec()).map_err(|e| e.to_string())?)),
            2 => Ok(PubSubMessage::Subscribe(String::from_utf8(data[1..].to_vec()).map_err(|e| e.to_string())?)),
            3 => Ok(PubSubMessage::Unsubscribe(String::from_utf8(data[1..].to_vec()).map_err(|e| e.to_string())?)),
            4 => {
                let separator_index = data.iter().position(|&x| x == 0).ok_or("Missing separator in Publish message".to_string())?;
                let topic = String::from_utf8(data[1..separator_index].to_vec()).map_err(|e| e.to_string())?;
                let message = data[separator_index + 1..].to_vec();
                Ok(PubSubMessage::Publish(topic, message))
            }
            5 => Ok(PubSubMessage::Disconnect),
            _ => Err("Unknown message type".to_string()),
        }
    }

    pub async fn handle_message<H: MessageHandler>(&self, handler: &H)-> Result<(), String> {
        match self {
            PubSubMessage::CreateTopic(topic) => handler.on_create_topic(topic.clone()).await,
            PubSubMessage::DeleteTopic(topic) => handler.on_delete_topic(topic.clone()).await,
            PubSubMessage::Subscribe(topic) => handler.on_subscribe(topic.clone()).await,
            PubSubMessage::Unsubscribe(topic) => handler.on_unsubscribe(topic.clone()).await,
            PubSubMessage::Publish(topic, message) => handler.on_publish(topic.clone(), message.clone()).await,
            PubSubMessage::Disconnect => handler.on_disconnect().await,
        }
    }
}

///Vérif si le message est bien formé, si oui, le retourne, sinon, retourne une erreur
pub async fn read_message(recv: &mut RecvStream) -> anyhow::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match recv.read_exact(&mut len_buf).await {
        Ok(()) => {}
        Err(quinn::ReadExactError::FinishedEarly(0)) => return Ok(None), // fermeture propre
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 1024 * 1024 {
        bail!("message trop grand : {len} octets");
    }
    let mut data = vec![0u8; len];
    recv.read_exact(&mut data).await?;
    Ok(Some(data))
}

pub async fn send_message(send: &mut quinn::SendStream, data: &[u8]) -> anyhow::Result<()> {
    let len = data.len() as u32;
    send.write_all(&len.to_be_bytes()).await?;
    send.write_all(data).await?;
    Ok(())
}