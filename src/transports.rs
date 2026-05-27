use anyhow::{anyhow, Context, Result};
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

pub type BoxStream = Box<dyn AsyncStream>;

pub mod websocket {
    use super::*;
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
    use sha1::{Digest, Sha1};
    use tokio::net::TcpStream;

    const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    const MAX_FRAME_SIZE: u64 = 16 * 1024 * 1024;

    pub struct WebSocketStream {
        reader: DuplexStream,
        writer: DuplexStream,
    }

    impl AsyncRead for WebSocketStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut TaskContext<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.reader).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for WebSocketStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut TaskContext<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.writer).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.writer).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut TaskContext<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.writer).poll_shutdown(cx)
        }
    }

    pub async fn accept(mut stream: TcpStream) -> Result<WebSocketStream> {
        let request = read_http_headers(&mut stream)
            .await
            .context("read websocket upgrade request")?;
        let key = websocket_header(&request, "sec-websocket-key")
            .ok_or_else(|| anyhow!("websocket request missing Sec-WebSocket-Key"))?;
        if !request.to_ascii_lowercase().contains("upgrade: websocket") {
            return Err(anyhow!("websocket request missing upgrade header"));
        }

        let accept = accept_key(&key);
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Accept: {accept}\r\n\
             \r\n"
        );
        stream
            .write_all(response.as_bytes())
            .await
            .context("write websocket upgrade response")?;
        Ok(spawn_bridge(stream, false))
    }

    pub async fn connect(server_addr: SocketAddr) -> Result<WebSocketStream> {
        let mut stream = TcpStream::connect(server_addr)
            .await
            .with_context(|| format!("connect websocket tcp server {server_addr}"))?;
        let key = BASE64.encode(b"frprs-websocket-key");
        let request = format!(
            "GET /frprs HTTP/1.1\r\n\
             Host: {server_addr}\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: {key}\r\n\
             Sec-WebSocket-Version: 13\r\n\
             \r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .context("write websocket upgrade request")?;
        let response = read_http_headers(&mut stream)
            .await
            .context("read websocket upgrade response")?;
        if !response.starts_with("HTTP/1.1 101") && !response.starts_with("HTTP/1.0 101") {
            return Err(anyhow!("websocket upgrade rejected: {response:?}"));
        }
        Ok(spawn_bridge(stream, true))
    }

    fn spawn_bridge(stream: TcpStream, mask_outgoing: bool) -> WebSocketStream {
        let (mut socket_reader, mut socket_writer) = tokio::io::split(stream);
        let (incoming_reader, mut incoming_writer) = tokio::io::duplex(64 * 1024);
        let (mut outgoing_reader, outgoing_writer) = tokio::io::duplex(64 * 1024);

        tokio::spawn(async move {
            while let Ok(Some(payload)) = read_frame(&mut socket_reader).await {
                if incoming_writer.write_all(&payload).await.is_err() {
                    break;
                }
            }
        });

        tokio::spawn(async move {
            let mut buf = vec![0_u8; 16 * 1024];
            loop {
                let n = match outgoing_reader.read(&mut buf).await {
                    Ok(0) => {
                        let _ = write_close_frame(&mut socket_writer, mask_outgoing).await;
                        break;
                    }
                    Ok(n) => n,
                    Err(_) => break,
                };
                if write_binary_frame(&mut socket_writer, &buf[..n], mask_outgoing)
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        WebSocketStream {
            reader: incoming_reader,
            writer: outgoing_writer,
        }
    }

    async fn read_http_headers(stream: &mut TcpStream) -> Result<String> {
        let mut buf = Vec::with_capacity(1024);
        let mut byte = [0_u8; 1];
        while !buf.ends_with(b"\r\n\r\n") {
            let n = stream.read(&mut byte).await.context("read http header")?;
            if n == 0 {
                return Err(anyhow!("connection closed before websocket headers"));
            }
            buf.push(byte[0]);
            if buf.len() > 16 * 1024 {
                return Err(anyhow!("websocket headers too large"));
            }
        }
        String::from_utf8(buf).context("decode websocket headers")
    }

    fn websocket_header(headers: &str, name: &str) -> Option<String> {
        headers.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.trim()
                .eq_ignore_ascii_case(name)
                .then(|| value.trim().to_string())
        })
    }

    fn accept_key(key: &str) -> String {
        let mut hasher = Sha1::new();
        hasher.update(key.as_bytes());
        hasher.update(GUID.as_bytes());
        BASE64.encode(hasher.finalize())
    }

    async fn read_frame<R>(reader: &mut R) -> Result<Option<Vec<u8>>>
    where
        R: AsyncRead + Unpin,
    {
        let mut header = [0_u8; 2];
        if reader.read_exact(&mut header).await.is_err() {
            return Ok(None);
        }
        let opcode = header[0] & 0x0f;
        let masked = header[1] & 0x80 != 0;
        let mut len = (header[1] & 0x7f) as u64;
        if len == 126 {
            let mut extended = [0_u8; 2];
            reader
                .read_exact(&mut extended)
                .await
                .context("read websocket 16-bit length")?;
            len = u16::from_be_bytes(extended) as u64;
        } else if len == 127 {
            let mut extended = [0_u8; 8];
            reader
                .read_exact(&mut extended)
                .await
                .context("read websocket 64-bit length")?;
            len = u64::from_be_bytes(extended);
        }
        if len > MAX_FRAME_SIZE {
            return Err(anyhow!("websocket frame too large: {len} bytes"));
        }

        let mut mask = [0_u8; 4];
        if masked {
            reader
                .read_exact(&mut mask)
                .await
                .context("read websocket mask")?;
        }

        let mut payload = vec![0_u8; len as usize];
        reader
            .read_exact(&mut payload)
            .await
            .context("read websocket payload")?;
        if masked {
            for (idx, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[idx % 4];
            }
        }

        match opcode {
            0x1 | 0x2 | 0x0 => Ok(Some(payload)),
            0x8 => Ok(None),
            0x9 | 0xA => Ok(Some(Vec::new())),
            _ => Err(anyhow!("unsupported websocket opcode {opcode}")),
        }
    }

    async fn write_binary_frame<W>(writer: &mut W, payload: &[u8], masked: bool) -> Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        write_frame(writer, 0x2, payload, masked).await
    }

    async fn write_close_frame<W>(writer: &mut W, masked: bool) -> Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        write_frame(writer, 0x8, &[], masked).await
    }

    async fn write_frame<W>(writer: &mut W, opcode: u8, payload: &[u8], masked: bool) -> Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let mut frame = Vec::with_capacity(payload.len() + 14);
        frame.push(0x80 | opcode);
        let mask_bit = if masked { 0x80 } else { 0 };
        if payload.len() < 126 {
            frame.push(mask_bit | payload.len() as u8);
        } else if payload.len() <= u16::MAX as usize {
            frame.push(mask_bit | 126);
            frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        } else {
            frame.push(mask_bit | 127);
            frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }

        if masked {
            let mask = [0x13, 0x37, 0x42, 0x99];
            frame.extend_from_slice(&mask);
            frame.extend(
                payload
                    .iter()
                    .enumerate()
                    .map(|(idx, byte)| byte ^ mask[idx % 4]),
            );
        } else {
            frame.extend_from_slice(payload);
        }

        writer
            .write_all(&frame)
            .await
            .context("write websocket frame")?;
        writer.flush().await.context("flush websocket frame")
    }
}

pub mod tls {
    use super::*;
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
    use tokio::net::TcpStream;
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    pub async fn accept(stream: TcpStream) -> Result<tokio_rustls::server::TlsStream<TcpStream>> {
        let acceptor = TlsAcceptor::from(Arc::new(configure_server()?));
        acceptor.accept(stream).await.context("accept tls stream")
    }

    pub async fn connect_insecure(
        server_addr: SocketAddr,
    ) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
        let stream = TcpStream::connect(server_addr)
            .await
            .with_context(|| format!("connect tls tcp server {server_addr}"))?;
        let connector = TlsConnector::from(Arc::new(insecure_client_config()?));
        let server_name = ServerName::try_from("localhost")
            .context("build tls server name")?
            .to_owned();
        connector
            .connect(server_name, stream)
            .await
            .with_context(|| format!("handshake tls server {server_addr}"))
    }

    fn configure_server() -> Result<rustls::ServerConfig> {
        ensure_crypto_provider();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .context("generate tls self-signed certificate")?;
        let cert_der = CertificateDer::from(cert.cert);
        let private_key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], private_key.into())
            .context("build tls server config")
    }

    fn insecure_client_config() -> Result<rustls::ClientConfig> {
        ensure_crypto_provider();
        Ok(rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth())
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
