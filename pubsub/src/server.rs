use std::{
    ascii, fs, io,
    net::SocketAddr,
    path::{self, Path, PathBuf},
    str,
    sync::Arc,
};
use std::collections::HashMap;
use anyhow::{Context, Result, anyhow, bail};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::pki_types::pem::PemObject;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Connection, Incoming, RecvStream};
use tracing::{error, info, info_span, Instrument};
use crate::common;

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
    subscribers: Vec<Client>,
    inbox: Vec<String>,
    outbox: Vec<String>,
}

pub struct ServOpt {
    key: PathBuf,
    cert: PathBuf,
    stateless_retry: bool,
    listen: SocketAddr,
    block: Option<SocketAddr>,
    connection_limit: Option<usize>,
}

pub struct PSServer {
    topics: Vec<Topic>,
    clients: Clients,
}


impl PSServer {
    fn new(option: ServOpt) -> Self {
        PSServer {
            topics: Vec::new(),
            clients: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    async fn start(&self, options: ServOpt) {
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
                let fut = Self::handle_connection(conn, self.clients.clone());
                tokio::spawn(async move {
                    if let Err(e) = fut.await {
                        error!("connection failed: {reason}", reason = e.to_string())
                    }
                });
            }
        }
    }

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

        if cert_path.exists() || key_path.exists() {
            anyhow::bail!("Le certificat ou la clé existe déjà.");
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

    async fn handle_connection(incoming: Incoming, clients: Clients) -> Result<()> {
        let connection = incoming.await?;
        let id = connection.stable_id();

        let (send, mut recv) = match connection.accept_bi().await {
            Ok(s) => s,
            Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                info!("connection closed by peer");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        let client = Arc::new(Client::new(connection, send));
        clients.lock().await.insert(id, Arc::clone(&client));

        let result = Self::read_loop(&client, &mut recv).await;

        // nettoyage dans tous les cas
        clients.lock().await.remove(&id);

        result
    }

    async fn read_loop(client: &Client, recv: &mut RecvStream) -> Result<()> {
        let id = client.connection.stable_id();
        let mut buf = [0u8; 1024];
        loop {
            let n = match recv.read(&mut buf).await? {
                None => {
                    info!("client {id} closed its stream");
                    return Ok(());
                }
                Some(n) => n,
            };
            info!("received {n} bytes from client {id}");

            //TODO: traiter les msg pour les mettre dans l'inbox du topic correspondant
        }
    }

    fn add_subscriber() {

    }

    fn remove_subscriber() {

    }

    fn publish () {

    }

    fn disconnect () {

    }
}


//TODO: Ce qui est un job :
// - le traitement des inbox des topics
// - la publication des outbox des topics