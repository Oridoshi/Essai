use std::{
    ascii, fs, io,
    net::SocketAddr,
    path::{self, Path, PathBuf},
    str,
    sync::Arc,
};

/*
trait PubSubClient {
    async fn connect(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn disconnect(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn subscribe(&mut self, topic: &str) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn unsubscribe(&mut self, topic: &str) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn publish(&mut self, topic: &str, data: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>>;
}
*/