use anyhow::{anyhow, Context, Result};
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

pub type BoxStream = Box<dyn AsyncStream>;

pub mod quic {
    use super::*;
    use quinn::{ClientConfig, Endpoint, RecvStream, SendStream, ServerConfig};
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};

    pub struct QuicServerEndpoint {
        endpoint: Endpoint,
        certificate_der: CertificateDer<'static>,
    }

    pub struct QuicBiStream {
        pub send: SendStream,
        pub recv: RecvStream,
        pub remote_addr: SocketAddr,
    }

    pub struct QuicStream {
        pub send: SendStream,
        pub recv: RecvStream,
        pub remote_addr: SocketAddr,
        _endpoint: Option<Endpoint>,
    }

    impl QuicServerEndpoint {
        pub fn bind(addr: SocketAddr) -> Result<Self> {
            let (server_config, certificate_der) = configure_server()?;
            let endpoint = Endpoint::server(server_config, addr)
                .with_context(|| format!("bind quic endpoint on {addr}"))?;
            Ok(Self {
                endpoint,
                certificate_der,
            })
        }

        pub fn local_addr(&self) -> Result<SocketAddr> {
            self.endpoint
                .local_addr()
                .context("read quic endpoint local address")
        }

        pub fn certificate_der(&self) -> CertificateDer<'static> {
            self.certificate_der.clone()
        }

        pub async fn accept_bi(&self) -> Result<QuicBiStream> {
            let incoming = self
                .endpoint
                .accept()
                .await
                .ok_or_else(|| anyhow!("quic endpoint closed"))?;
            let connection = incoming.await.context("accept quic connection")?;
            let remote_addr = connection.remote_address();
            let (send, recv) = connection
                .accept_bi()
                .await
                .context("accept quic bidirectional stream")?;
            Ok(QuicBiStream {
                send,
                recv,
                remote_addr,
            })
        }

        pub async fn accept_stream(&self) -> Result<QuicStream> {
            let stream = self.accept_bi().await?;
            Ok(stream.into_stream(None))
        }
    }

    impl QuicBiStream {
        pub fn into_stream(self, endpoint: Option<Endpoint>) -> QuicStream {
            QuicStream {
                send: self.send,
                recv: self.recv,
                remote_addr: self.remote_addr,
                _endpoint: endpoint,
            }
        }
    }

    impl AsyncRead for QuicStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut TaskContext<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.recv).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for QuicStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut TaskContext<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            AsyncWrite::poll_write(Pin::new(&mut self.send), cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            AsyncWrite::poll_flush(Pin::new(&mut self.send), cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut TaskContext<'_>,
        ) -> Poll<io::Result<()>> {
            AsyncWrite::poll_shutdown(Pin::new(&mut self.send), cx)
        }
    }

    pub async fn connect_bi(
        server_addr: SocketAddr,
        server_cert: CertificateDer<'static>,
    ) -> Result<(Endpoint, QuicBiStream)> {
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().expect("valid udp bind addr"))
            .context("create quic client endpoint")?;
        endpoint.set_default_client_config(configure_client(server_cert)?);

        let connection = endpoint
            .connect(server_addr, "localhost")
            .with_context(|| format!("start quic connect to {server_addr}"))?
            .await
            .with_context(|| format!("connect quic server {server_addr}"))?;
        let remote_addr = connection.remote_address();
        let (send, recv) = connection
            .open_bi()
            .await
            .context("open quic bidirectional stream")?;
        Ok((
            endpoint,
            QuicBiStream {
                send,
                recv,
                remote_addr,
            },
        ))
    }

    pub async fn connect_stream_insecure(server_addr: SocketAddr) -> Result<QuicStream> {
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().expect("valid udp bind addr"))
            .context("create quic client endpoint")?;
        endpoint.set_default_client_config(insecure_client_config()?);

        let connection = endpoint
            .connect(server_addr, "localhost")
            .with_context(|| format!("start quic connect to {server_addr}"))?
            .await
            .with_context(|| format!("connect quic server {server_addr}"))?;
        let remote_addr = connection.remote_address();
        let (send, recv) = connection
            .open_bi()
            .await
            .context("open quic bidirectional stream")?;
        Ok(QuicStream {
            send,
            recv,
            remote_addr,
            _endpoint: Some(endpoint),
        })
    }

    fn configure_client(server_cert: CertificateDer<'static>) -> Result<ClientConfig> {
        ensure_crypto_provider();
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(server_cert)
            .context("trust generated quic server certificate")?;
        ClientConfig::with_root_certificates(Arc::new(roots)).context("build quic client config")
    }

    fn insecure_client_config() -> Result<ClientConfig> {
        ensure_crypto_provider();
        let crypto = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth();
        Ok(ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
                .context("build insecure quic client config")?,
        )))
    }

    fn configure_server() -> Result<(ServerConfig, CertificateDer<'static>)> {
        ensure_crypto_provider();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .context("generate quic self-signed certificate")?;
        let cert_der = CertificateDer::from(cert.cert);
        let private_key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let mut server_config =
            ServerConfig::with_single_cert(vec![cert_der.clone()], private_key.into())
                .context("build quic server config")?;

        let transport_config =
            Arc::get_mut(&mut server_config.transport).expect("fresh transport config");
        transport_config.max_concurrent_uni_streams(0_u8.into());

        Ok((server_config, cert_der))
    }

    fn ensure_crypto_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    #[derive(Debug)]
    struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

    impl SkipServerVerification {
        fn new() -> Arc<Self> {
            Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
        }
    }

    impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp: &[u8],
            _now: UnixTime,
        ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error>
        {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &rustls::DigitallySignedStruct,
        ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
        {
            rustls::crypto::verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &rustls::DigitallySignedStruct,
        ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
        {
            rustls::crypto::verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }
}

pub mod kcp {
    use super::*;
    pub use tokio_kcp::{KcpConfig, KcpListener, KcpStream};

    pub async fn bind(addr: SocketAddr) -> Result<KcpListener> {
        KcpListener::bind(KcpConfig::default(), addr)
            .await
            .map_err(|err| anyhow!("bind kcp listener on {addr}: {err}"))
    }

    pub async fn connect(addr: SocketAddr) -> Result<KcpStream> {
        KcpStream::connect(&KcpConfig::default(), addr)
            .await
            .map_err(|err| anyhow!("connect kcp server {addr}: {err}"))
    }
}
