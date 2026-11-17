use std::{
    ascii, fs, io,
    net::SocketAddr,
    path::{self, Path, PathBuf},
    str,
    sync::Arc,
};
use std::collections::{HashMap, VecDeque};
use anyhow::{Context, Result, anyhow, bail};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::pki_types::pem::PemObject;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Connection, Incoming, RecvStream};
use tracing::{error, info, info_span, Instrument};
use crate::common;
use crate::common::MessageHandler;

type Clients = Arc<tokio::sync::Mutex<HashMap<usize, Arc<Client>>>>;

struct Client {
    connection: Connection,
    send: tokio::sync::Mutex<quinn::SendStream>,
}

impl Client {
    fn new(connection: Connection, send: quinn::SendStream) -> Self {
        Client { connection, send: tokio::sync::Mutex::new(send) }
    }
}

struct Topic {
    name: String,
    subscribers: Vec<Arc<Client>>,
    inbox: VecDeque<Vec<u8>>,
    outbox: VecDeque<Vec<u8>>,
}

pub struct ServOpt {
    pub key: PathBuf,
    pub cert: PathBuf,
    pub stateless_retry: bool,
    pub listen: SocketAddr,
    pub block: Option<SocketAddr>,
    pub connection_limit: Option<usize>,
}

type Topics = Arc<tokio::sync::Mutex<HashMap<String, Topic>>>;

#[derive(Clone)]
pub struct PSServer {
    topics: Topics, //hashmap of topics with name as key and topic as value
    clients: Clients,
}


impl PSServer {
    pub fn new() -> Self {
        PSServer {
            topics: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            clients: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    pub async fn start(&self, options: ServOpt) {
        let (cert, key) = Self::get_key_and_cert(&options.cert, &options.key).unwrap();

        let mut server_crypto = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert, key)
            .expect("failed to create server config");
        server_crypto.alpn_protocols = common::ALPN_QUIC_HTTP.iter().map(|&x| x.into()).collect();

        let mut server_config =
            quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto).unwrap()));
        let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();

        let endpoint = quinn::Endpoint::server(server_config, options.listen).expect("failed to create server endpoint");
        eprintln!("listening on {}", endpoint.local_addr().unwrap());

        while let Some(conn) = endpoint.accept().await {
            if options
                .connection_limit
                .is_some_and(|n| endpoint.open_connections() >= n)
            {
                info!("refusing due to open connection limit");
                conn.refuse();
            } else if Some(conn.remote_address()) == options.block {
                info!("refusing blocked client IP address");
                conn.refuse();
            } else if options.stateless_retry && !conn.remote_address_validated() {
                info!("requiring connection to validate its address");
                conn.retry().unwrap();
            } else {
                info!("accepting connection");
                let fut = Self::handle_connection(conn, self.clone());
                tokio::spawn(async move {
                    if let Err(e) = fut.await {
                        error!("connection failed: {reason}", reason = e.to_string())
                    }
                });
            }
        }
    } // Démaré job sys ici ! Faire en sorte que tout le démarage et recive info cli soit un job ?

    fn get_key_and_cert(cert_path: &Path, key_path: &Path,) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
        let (cert, key) = {
            let key = if key_path.extension().is_some_and(|x| x == "der") {
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                    fs::read(key_path).context("failed to read private key file")?,
                ))
            } else {
                PrivateKeyDer::from_pem_file(key_path)
                    .context("failed to read PEM from private key file")?
            };

            let cert_chain = if cert_path.extension().is_some_and(|x| x == "der") {
                vec![CertificateDer::from(
                    fs::read(cert_path).context("failed to read certificate chain file")?,
                )]
            } else {
                CertificateDer::pem_file_iter(cert_path)
                    .context("failed to read PEM from certificate chain file")?
                    .collect::<Result<_, _>>()
                    .context("invalid PEM-encoded certificate")?
            };

            (cert_chain, key)
        };

        Ok((cert, key))
    }
    /// SI PAS DE CERTIFICAT ET DE CLE, CREER UN CERTIFICAT ET UNE CLE
    pub fn create_certificate(cert_path: Option<PathBuf>, key_path: Option<PathBuf>) -> Result<(PathBuf,PathBuf)> {
        let default_dir = || -> Result<PathBuf> {
            let dirs = directories_next::ProjectDirs::from("org", "orga", "pubsub")
                .context("impossible de déterminer le dossier de données")?;
            Ok(dirs.data_local_dir().join("tls"))
        };

        let cert_path = match cert_path {
            Some(p) => p,
            None => default_dir()?.join("cert.der"),
        };
        let key_path = match key_path {
            Some(p) => p,
            None => default_dir()?.join("key.der"),
        };

        if cert_path.exists() && key_path.exists() {
            return Ok((cert_path, key_path));   // déjà là, rien à faire
        }
        if cert_path.exists() || key_path.exists() {
            anyhow::bail!("un seul des deux fichiers existe : état incohérent, supprime-les");
        }

        let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .context("échec de la génération du certificat")?;
        let key = PrivatePkcs8KeyDer::from(generated.signing_key.serialize_der());

        if let Some(parent) = cert_path.parent() {
            fs::create_dir_all(parent).context("failed to create certificate directory")?;
        }
        fs::write(&cert_path, generated.cert.der()).context("failed to write certificate")?;

        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent).context("failed to create key directory")?;
        }
        fs::write(&key_path, key.secret_pkcs8_der()).context("failed to write private key")?;

        Ok((cert_path, key_path))
    }
    async fn handle_connection(incoming: Incoming, server: PSServer) -> Result<()> {
        let connection = incoming.await?;
        let id = connection.stable_id();
        info!("accepted connection from {} with id {}", connection.remote_address(), id);

        let (send, mut recv) = match connection.accept_bi().await {
            Ok(s) => s,
            Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                info!("connection closed by peer");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        let client = Arc::new(Client::new(connection, send));
        server.clients.lock().await.insert(id, Arc::clone(&client));

        let handler = ServerMessageHandler {
            server: server.clone(),
            client: Arc::clone(&client),
        };

        let result = Self::read_loop(&handler, &mut recv).await;

        // nettoyage dans tous les cas
        // nettoyage : table des clients + abonnements
        server.clients.lock().await.remove(&id);
        for t in server.topics.lock().await.values_mut() {
            t.subscribers.retain(|c| c.connection.stable_id() != id);
        }

        result
    }
    async fn read_loop(handler: &ServerMessageHandler, recv: &mut RecvStream) -> Result<()> {
        let id = handler.client.connection.stable_id();
        loop {
            let Some(data) = common::read_message(recv).await? else {
                info!("client {id} closed its stream");
                return Ok(());
            };
            let msg = common::PubSubMessage::deserialize(&data).map_err(|e| anyhow!(e))?;
            msg.handle_message(handler).await.map_err(|e| anyhow!(e))?;
        }
    }

    ///actuellement dumb methode, qui ne fait que vider l'inbox dans l'outbox
    async fn process_inbox(&self, topic: String) -> std::result::Result<(), String> {
        let mut topics = self.topics.lock().await;
        if let Some(t) = topics.get_mut(&topic) {
            while let Some(message) = t.inbox.pop_front() {
                t.outbox.push_back(message);
            }
            Ok(())
        } else {
            Err(format!("Topic {} does not exist", topic))
        }
    }

    ///check if the inbox of the topic is empty, if the topic exists, otherwise return an error
    async fn is_inbox_empty(&self, topic: String) -> std::result::Result<bool, String> {
        let topics = self.topics.lock().await;
        if let Some(t) = topics.get(&topic) {
            Ok(!t.inbox.is_empty())
        } else {
            Err(format!("Topic {} does not exist", topic))
        }
    }

    ///check if the outbox of the topic is empty, if the topic exists, otherwise return an error
    async fn is_outbox_empty(&self, topic: String) -> std::result::Result<bool, String> {
        let topics = self.topics.lock().await;
        if let Some(t) = topics.get(&topic) {
            Ok(!t.outbox.is_empty())
        } else {
            Err(format!("Topic {} does not exist", topic))
        }
    }


    async fn check_if_topic_exists(&self, topic: String) -> std::result::Result<bool, String> {
        let topics = self.topics.lock().await;
        Ok(topics.contains_key(&topic))
    }

    async fn process_outbox(&self, topic: String) -> std::result::Result<(), String> {
        let mut topics = self.topics.lock().await;
        if let Some(t) = topics.get_mut(&topic) {
            while let Some(message) = t.outbox.pop_front() {
                for subscriber in &t.subscribers {
                    let data = common::PubSubMessage::Publish(topic.clone(), message.clone()).serialize();
                    let mut send = subscriber.send.lock().await;
                    common::send_message(&mut *send, &data).await.map_err(|e| e.to_string())?;
                }
            }
            Ok(())
        } else {
            Err(format!("Topic {} does not exist", topic))
        }
    }
}

struct ServerMessageHandler{
    server: PSServer,
    client: Arc<Client>
}

impl MessageHandler for ServerMessageHandler {
    async fn on_create_topic(&self, topic: String) -> std::result::Result<(), String> {
        let mut topics = self.server.topics.lock().await;
        if topics.contains_key(&topic) {
            return Err(format!("Topic {} already exists", topic));
        }
        topics.insert(topic.clone(), Topic {
            name: topic,
            subscribers: Vec::new(),
            inbox: VecDeque::new(),
            outbox: VecDeque::new(),
        });
        Ok(())
    }

    async fn on_delete_topic(&self, topic: String) -> std::result::Result<(), String> {
        let mut topics = self.server.topics.lock().await;
        if !topics.contains_key(&topic) {
            return Err(format!("Topic {} does not exist", topic));
        }
        topics.remove(&topic);
        Ok(())
    }

    async fn on_subscribe(&self, topic: String) -> std::result::Result<(), String> {
        let mut topics = self.server.topics.lock().await;
        if let Some(t) = topics.get_mut(&topic) {
            t.subscribers.push(Arc::clone(&self.client));
            Ok(())
        } else {
            Err(format!("Topic {} does not exist", topic))
        }
    }

    async fn on_unsubscribe(&self, topic: String) -> std::result::Result<(), String> {
        let mut topics = self.server.topics.lock().await;
        if let Some(t) = topics.get_mut(&topic) {
            t.subscribers.retain(|c| c.connection.stable_id() != self.client.connection.stable_id());
            Ok(())
        } else {
            Err(format!("Topic {} does not exist", topic))
        }
    }

    ///put the message in the inbox of the topic, if the topic exists, otherwise return an error
    async fn on_publish(&self, topic: String, message: Vec<u8>) -> std::result::Result<(), String> {
        let mut topics = self.server.topics.lock().await;
        if let Some(t) = topics.get_mut(&topic) {
            t.inbox.push_back(message.clone());
            Ok(())
        } else {
            Err(format!("Topic {} does not exist", topic))
        }
    }

    async fn on_disconnect(&self) -> std::result::Result<(), String> {
        let id = self.client.connection.stable_id();
        let mut clients = self.server.clients.lock().await;
        clients.remove(&id);
        let mut topics = self.server.topics.lock().await;
        for t in topics.values_mut() {
            t.subscribers.retain(|c| c.connection.stable_id() != id);
        }
        Ok(())
    }
}

//TODO: Ce qui est un job :
// - le traitement des inbox des topics - OK
// - inbox vers outbox des topics (dumb metode pour l'instant) - OK
// - la publication des outbox des topics - OK

//TODO: TOUT PASSER SOUS JOBSYS