use std::{
    ascii, fs, io,
    net::SocketAddr,
    path::{self, Path, PathBuf},
    str,
    sync::Arc,
};

use std::net::ToSocketAddrs;
use anyhow::{Result, anyhow, bail};
use rustls::pki_types::{CertificateDer};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{Connection, Endpoint, Incoming, RecvStream};
use tracing::{error, info, info_span, Instrument};
use url::Url;
use crate::common;

struct Server {
    connection: Connection,
    endpoint: Endpoint,
    send: tokio::sync::Mutex<quinn::SendStream>,
}

pub struct CliOpt {
    ///URL OF THE SERVER TO CONNECT TO
    pub url_server: Url,

    ///USE TO OVERRIDE THE HOSTNAME IN THE URL FOR CERTIFICATE VERIFICATION
    pub server_name: Option<String>,

    ///CUSTOM CERTIFICATE AUTHORITY TO TRUST, IN DER FORMAT
    pub cert: Option<PathBuf>,

    ///ADDRESS TO BIND ON
    pub bind: SocketAddr,
}

pub struct PSClient {
    server: Option<Arc<Server>>,
}


impl PSClient {
    pub fn new() -> Self {
        PSClient {
            server: None,
        }
    }

    ///Ouvre la connexion avec le serveur et ouvre un stream de communication. Le serveur doit être en écoute sur l'adresse et le port spécifiés.
    pub async fn create_and_connect(&mut self, option: CliOpt) -> Result<()> {
        let url = option.url_server;
        let url_host = url.host_str().ok_or_else(|| anyhow!("URL sans hôte"))?;
        let url_host = url_host.trim_start_matches('[').trim_end_matches(']').to_string();
        let remote = (url_host.clone(), url.port().unwrap_or(4433))
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow!("Could not resolve address"))?;

        //certificate handling
        let cert_name = option.server_name.unwrap_or(url_host);
        let mut roots = rustls::RootCertStore::empty();
        if let Some(ca_path) = option.cert {
            roots.add(CertificateDer::from(fs::read(ca_path)?))?;
        } else {
            let dirs = directories_next::ProjectDirs::from("org", "orga", "pubsub").unwrap();
            match fs::read(dirs.data_local_dir().join("tls").join("cert.der")) {
                Ok(cert) => {
                    roots.add(CertificateDer::from(cert))?;
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    info!("Local server certificate not found");
                }
                Err(e) => {
                    error!("Failed to open local server certificate: {}", e);
                }
            }
        }

        //create the client configuration
        let mut client_crypto = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client_crypto.alpn_protocols = common::ALPN_QUIC_HTTP.iter().map(|&x| x.into()).collect();

        let client_config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
        let mut endpoint = quinn::Endpoint::client(option.bind)?;
        endpoint.set_default_client_config(client_config);

        //connect to the server
        let conn = endpoint
            .connect(remote, &cert_name)?
            .await
            .map_err(|e| anyhow!("Failed to connect: {}", e))?;

        info!("Connected to {} at {}", cert_name, remote);

        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow!("Failed to open stream: {}", e))?;

        //SI BESOIN INFO AU CLIENT SUR LE SERVEUR, ON PEUT LUI ENVOYER ICI, AVANT DE LANCER LA READ_LOOP

        //store the server connection and endpoint in the PSClient struct
        let server = Arc::new(Server {
            connection: conn.clone(),
            endpoint,
            send: tokio::sync::Mutex::new(send),
        });
        self.server = Some(Arc::clone(&server));

        tokio::spawn(async move {
            if let Err(e) = Self::read_loop(&server, &mut recv).await {
                error!("read loop failed: {e}");
            }
        });

        Ok(())
    }

    async fn read_loop(server: &Server, recv: &mut RecvStream) -> Result<()> {
        let id = server.connection.stable_id();
        let mut buf = [0u8; 1024];
        loop {
            let n = match recv.read(&mut buf).await? {
                None => {
                    info!("server {id} closed its stream");
                    return Ok(());
                }
                Some(n) => n,
            };
            info!("received {n} bytes from server {id}");

            //TODO: traiter les msg pour les mettre une inbox (par rapport a un topic ?)
        }
    }
}

/*
fn disconnect() -> Result<(), Box<dyn Error + Send + Sync>>;
fn subscribe(topic: &str) -> Result<(), Box<dyn Error + Send + Sync>>;
fn unsubscribe(topic: &str) -> Result<(), Box<dyn Error + Send + Sync>>;
fn publish(topic: &str, data: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>>;
*/