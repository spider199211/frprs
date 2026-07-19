use frprs::{
    client,
    config::{
        AuthConfig, ClientConfig, ClientPluginConfig, HeaderRewriteConfig, HealthCheckConfig,
        HealthCheckType, ProxyConfig, ProxyType, ServerConfig, ServerPluginConfig, TransportConfig,
        TransportProtocol, VisitorConfig,
    },
    protocol::{read_msg, write_msg, Message, UdpPacketFrame},
    server,
};
use std::{
    io::ErrorKind,
    net::{TcpListener as StdTcpListener, UdpSocket as StdUdpSocket},
    sync::OnceLock,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{oneshot, Mutex, MutexGuard},
    time::{sleep, Duration, Instant},
};

static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

async fn acquire_test_lock() -> MutexGuard<'static, ()> {
    TEST_LOCK.get_or_init(|| Mutex::new(())).lock().await
}

fn unused_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn unused_udp_port() -> u16 {
    let socket = StdUdpSocket::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap().port()
}

async fn connect_with_retry(addr: String) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match TcpStream::connect(&addr).await {
            Ok(stream) => return stream,
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                sleep(Duration::from_millis(50)).await;
            }
            Err(err) => panic!("failed to connect to {addr}: {err}"),
        }
    }
}

async fn wait_until_connect_fails(addr: String) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match TcpStream::connect(&addr).await {
            Ok(_) if Instant::now() < deadline => sleep(Duration::from_millis(100)).await,
            Ok(_) => panic!("connection to {addr} was still accepted"),
            Err(_) => return,
        }
    }
}

async fn recv_udp_ignoring_connection_reset(
    socket: &UdpSocket,
    buf: &mut [u8],
    timeout: Duration,
) -> (usize, std::net::SocketAddr) {
    let deadline = Instant::now() + timeout;
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            panic!("timed out waiting for udp packet");
        };
        match tokio::time::timeout(remaining, socket.recv_from(buf)).await {
            Ok(Ok(received)) => return received,
            Ok(Err(err)) if err.kind() == ErrorKind::ConnectionReset => continue,
            Ok(Err(err)) => panic!("udp recv failed: {err}"),
            Err(_) => panic!("timed out waiting for udp packet"),
        }
    }
}

async fn http_request(addr: String, request: &[u8]) -> String {
    let mut stream = connect_with_retry(addr).await;
    stream.write_all(request).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8_lossy(&response).to_string()
}

fn tls_client_hello_for_sni(domain: &str) -> Vec<u8> {
    let name = domain.as_bytes();
    let mut sni = Vec::new();
    sni.extend_from_slice(&((1 + 2 + name.len()) as u16).to_be_bytes());
    sni.push(0);
    sni.extend_from_slice(&(name.len() as u16).to_be_bytes());
    sni.extend_from_slice(name);

    let mut extensions = Vec::new();
    extensions.extend_from_slice(&0_u16.to_be_bytes());
    extensions.extend_from_slice(&(sni.len() as u16).to_be_bytes());
    extensions.extend_from_slice(&sni);

    let mut body = Vec::new();
    body.extend_from_slice(&[0x03, 0x03]);
    body.extend_from_slice(&[0_u8; 32]);
    body.push(0);
    body.extend_from_slice(&2_u16.to_be_bytes());
    body.extend_from_slice(&[0x00, 0x2f]);
    body.push(1);
    body.push(0);
    body.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
    body.extend_from_slice(&extensions);

    let mut handshake = Vec::new();
    handshake.push(1);
    handshake.push(((body.len() >> 16) & 0xff) as u8);
    handshake.push(((body.len() >> 8) & 0xff) as u8);
    handshake.push((body.len() & 0xff) as u8);
    handshake.extend_from_slice(&body);

    let mut record = Vec::new();
    record.push(22);
    record.extend_from_slice(&[0x03, 0x01]);
    record.extend_from_slice(&(handshake.len() as u16).to_be_bytes());
    record.extend_from_slice(&handshake);
    record
}

fn tls_client_hello_without_sni() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0x03, 0x03]);
    body.extend_from_slice(&[0_u8; 32]);
    body.push(0);
    body.extend_from_slice(&2_u16.to_be_bytes());
    body.extend_from_slice(&[0x00, 0x2f]);
    body.push(1);
    body.push(0);
    body.extend_from_slice(&0_u16.to_be_bytes());

    let mut handshake = Vec::new();
    handshake.push(1);
    handshake.push(((body.len() >> 16) & 0xff) as u8);
    handshake.push(((body.len() >> 8) & 0xff) as u8);
    handshake.push((body.len() & 0xff) as u8);
    handshake.extend_from_slice(&body);

    let mut record = Vec::new();
    record.push(22);
    record.extend_from_slice(&[0x03, 0x01]);
    record.extend_from_slice(&(handshake.len() as u16).to_be_bytes());
    record.extend_from_slice(&handshake);
    record
}

async fn assert_tcp_proxy_forwards_with_transport(protocol: TransportProtocol) {
    let _guard = acquire_test_lock().await;
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = echo.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    stream.write_all(&buf[..n]).await.unwrap();
                }
            });
        }
    });

    let bind_port = unused_port();
    let remote_port = unused_port();
    let transport = TransportConfig {
        protocol,
        nat_hole_stun_server: None,
    };
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: transport.clone(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(150)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport,
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "echo".to_string(),
            proxy_type: ProxyType::Tcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));

    let mut stream = connect_with_retry(format!("127.0.0.1:{remote_port}")).await;
    stream.write_all(b"hello transport").await.unwrap();

    let mut got = vec![0_u8; "hello transport".len()];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(got, b"hello transport");

    client_task.abort();
    server_task.abort();
    echo_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_proxy_forwards_bytes() {
    let _guard = acquire_test_lock().await;
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = echo.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    stream.write_all(&buf[..n]).await.unwrap();
                }
            });
        }
    });

    let bind_port = unused_port();
    let remote_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 1,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "echo".to_string(),
            proxy_type: ProxyType::Tcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));

    let mut stream = connect_with_retry(format!("127.0.0.1:{remote_port}")).await;
    stream.write_all(b"hello frp-rs").await.unwrap();

    let mut got = vec![0_u8; "hello frp-rs".len()];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(got, b"hello frp-rs");

    client_task.abort();
    server_task.abort();
    echo_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_local_connect_plugin_allows_work_connection() {
    let _guard = acquire_test_lock().await;
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = echo.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                let n = stream.read(&mut buf).await.unwrap();
                stream.write_all(&buf[..n]).await.unwrap();
            });
        }
    });

    let plugin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plugin_port = plugin.local_addr().unwrap().port();
    let (plugin_tx, plugin_rx) = oneshot::channel();
    let plugin_task = tokio::spawn(async move {
        let (mut stream, _) = plugin.accept().await.unwrap();
        let mut buf = vec![0_u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let _ = plugin_tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let bind_port = unused_port();
    let remote_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: ClientPluginConfig {
            local_connect_url: Some(format!("http://127.0.0.1:{plugin_port}/local_connect")),
        },
        proxies: vec![ProxyConfig {
            name: "plugin-echo".to_string(),
            proxy_type: ProxyType::Tcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));

    let mut stream = connect_with_retry(format!("127.0.0.1:{remote_port}")).await;
    stream.write_all(b"hello plugin").await.unwrap();
    let mut got = vec![0_u8; "hello plugin".len()];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(got, b"hello plugin");

    let plugin_request = plugin_rx.await.unwrap();
    assert!(plugin_request.contains("POST /local_connect HTTP/1.1"));
    assert!(plugin_request.contains("\"op\":\"LocalConnect\""));
    assert!(plugin_request.contains("\"proxy_name\":\"plugin-echo\""));
    assert!(plugin_request.contains("\"proxy_type\":\"tcp\""));
    assert!(plugin_request.contains("\"local_addr\":\"127.0.0.1:"));

    client_task.abort();
    server_task.abort();
    echo_task.abort();
    plugin_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_local_connect_plugin_allows_udp_session() {
    let _guard = acquire_test_lock().await;
    let udp_echo = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_echo_port = udp_echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        let mut buf = [0_u8; 1024];
        loop {
            let (n, peer) = udp_echo.recv_from(&mut buf).await.unwrap();
            udp_echo.send_to(&buf[..n], peer).await.unwrap();
        }
    });

    let plugin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plugin_port = plugin.local_addr().unwrap().port();
    let (plugin_tx, plugin_rx) = oneshot::channel();
    let plugin_task = tokio::spawn(async move {
        let (mut stream, _) = plugin.accept().await.unwrap();
        let mut buf = vec![0_u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let _ = plugin_tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let bind_port = unused_port();
    let remote_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: ClientPluginConfig {
            local_connect_url: Some(format!("http://127.0.0.1:{plugin_port}/local_connect")),
        },
        proxies: vec![ProxyConfig {
            name: "plugin-udp".to_string(),
            proxy_type: ProxyType::Udp,
            local_ip: "127.0.0.1".to_string(),
            local_port: udp_echo_port,
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));
    sleep(Duration::from_millis(300)).await;

    let visitor = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    visitor
        .send_to(b"hello plugin udp", format!("127.0.0.1:{remote_port}"))
        .await
        .unwrap();
    let mut got = [0_u8; 64];
    let (n, _) =
        recv_udp_ignoring_connection_reset(&visitor, &mut got, Duration::from_secs(5)).await;
    assert_eq!(&got[..n], b"hello plugin udp");

    let plugin_request = plugin_rx.await.unwrap();
    assert!(plugin_request.contains("POST /local_connect HTTP/1.1"));
    assert!(plugin_request.contains("\"op\":\"LocalConnect\""));
    assert!(plugin_request.contains("\"proxy_name\":\"plugin-udp\""));
    assert!(plugin_request.contains("\"proxy_type\":\"udp\""));
    assert!(plugin_request.contains("\"local_addr\":\"127.0.0.1:"));

    client_task.abort();
    server_task.abort();
    echo_task.abort();
    plugin_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_rejects_tcp_proxy_outside_allow_ports() {
    let _guard = acquire_test_lock().await;
    let bind_port = unused_port();
    let allowed_port = unused_port();
    let denied_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: vec![allowed_port.to_string()],
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let mut control = connect_with_retry(format!("127.0.0.1:{bind_port}")).await;
    write_msg(
        &mut control,
        &Message::Login {
            version: "test".to_string(),
            hostname: "test".to_string(),
            os: "test".to_string(),
            arch: "test".to_string(),
            run_id: String::new(),
            token: Some("secret".to_string()),
            pool_count: 0,
        },
    )
    .await
    .unwrap();
    let run_id = match read_msg(&mut control).await.unwrap() {
        Message::LoginResp { run_id, error, .. } if error.is_empty() => run_id,
        other => panic!("unexpected login response: {other:?}"),
    };
    assert!(!run_id.is_empty());

    write_msg(
        &mut control,
        &Message::NewProxy {
            proxy_name: "denied".to_string(),
            proxy_type: "tcp".to_string(),
            remote_port: denied_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control).await.unwrap() {
        Message::NewProxyResp { error, .. } => {
            assert!(error.contains("not allowed"), "{error}");
        }
        other => panic!("unexpected proxy response: {other:?}"),
    }

    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dashboard_admin_updates_allow_ports_at_runtime() {
    let _guard = acquire_test_lock().await;
    let bind_port = unused_port();
    let allowed_port = unused_port();
    let denied_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: vec![allowed_port.to_string()],
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let mut control = connect_with_retry(format!("127.0.0.1:{bind_port}")).await;
    write_msg(
        &mut control,
        &Message::Login {
            version: "test".to_string(),
            hostname: "test".to_string(),
            os: "test".to_string(),
            arch: "test".to_string(),
            run_id: String::new(),
            token: Some("secret".to_string()),
            pool_count: 0,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control).await.unwrap() {
        Message::LoginResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected login response: {other:?}"),
    }

    write_msg(
        &mut control,
        &Message::NewProxy {
            proxy_name: "runtime-denied".to_string(),
            proxy_type: "tcp".to_string(),
            remote_port: denied_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control).await.unwrap() {
        Message::NewProxyResp { error, .. } => {
            assert!(error.contains("not allowed"), "{error}");
        }
        other => panic!("unexpected proxy response: {other:?}"),
    }

    let body = format!(r#"{{"allow_ports":["{allowed_port}","{denied_port}"]}}"#);
    let request = format!(
        "PUT /api/config/allow_ports HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer secret\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let update = http_request(format!("127.0.0.1:{dashboard_port}"), request.as_bytes()).await;
    assert!(update.contains("200 OK"));
    assert!(update.contains(&denied_port.to_string()));

    write_msg(
        &mut control,
        &Message::NewProxy {
            proxy_name: "runtime-allowed".to_string(),
            proxy_type: "tcp".to_string(),
            remote_port: denied_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control).await.unwrap() {
        Message::NewProxyResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected proxy response after allowPorts update: {other:?}"),
    }

    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_close_proxy_plugin_receives_event() {
    let _guard = acquire_test_lock().await;
    let plugin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plugin_port = plugin.local_addr().unwrap().port();
    let (plugin_tx, plugin_rx) = oneshot::channel();
    let plugin_task = tokio::spawn(async move {
        let (mut stream, _) = plugin.accept().await.unwrap();
        let mut buf = vec![0_u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let _ = plugin_tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let bind_port = unused_port();
    let remote_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig {
            close_proxy_url: Some(format!("http://127.0.0.1:{plugin_port}/close_proxy")),
            ..Default::default()
        },
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let mut control = connect_with_retry(format!("127.0.0.1:{bind_port}")).await;
    write_msg(
        &mut control,
        &Message::Login {
            version: "test".to_string(),
            hostname: "test".to_string(),
            os: "test".to_string(),
            arch: "test".to_string(),
            run_id: String::new(),
            token: Some("secret".to_string()),
            pool_count: 0,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control).await.unwrap() {
        Message::LoginResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected login response: {other:?}"),
    }

    write_msg(
        &mut control,
        &Message::NewProxy {
            proxy_name: "close-hook-echo".to_string(),
            proxy_type: "tcp".to_string(),
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control).await.unwrap() {
        Message::NewProxyResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected proxy response: {other:?}"),
    }

    write_msg(
        &mut control,
        &Message::CloseProxy {
            proxy_name: "close-hook-echo".to_string(),
        },
    )
    .await
    .unwrap();

    let plugin_request = tokio::time::timeout(Duration::from_secs(5), plugin_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(plugin_request.contains("POST /close_proxy HTTP/1.1"));
    assert!(plugin_request.contains("\"op\":\"CloseProxy\""));
    assert!(plugin_request.contains("\"event\":\"close_proxy\""));
    assert!(plugin_request.contains("\"proxy_name\":\"close-hook-echo\""));
    assert!(plugin_request.contains("\"proxy_type\":\"tcp\""));
    assert!(plugin_request.contains("\"remote_addr\":\"127.0.0.1:"));

    server_task.abort();
    plugin_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_new_user_conn_plugin_receives_tcp_event() {
    let _guard = acquire_test_lock().await;
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = echo.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                let n = stream.read(&mut buf).await.unwrap();
                stream.write_all(&buf[..n]).await.unwrap();
            });
        }
    });

    let plugin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plugin_port = plugin.local_addr().unwrap().port();
    let (plugin_tx, plugin_rx) = oneshot::channel();
    let plugin_task = tokio::spawn(async move {
        let (mut stream, _) = plugin.accept().await.unwrap();
        let mut buf = vec![0_u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let _ = plugin_tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let bind_port = unused_port();
    let remote_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig {
            new_user_conn_url: Some(format!("http://127.0.0.1:{plugin_port}/new_user_conn")),
            ..Default::default()
        },
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "new-user-hook-echo".to_string(),
            proxy_type: ProxyType::Tcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));

    let mut stream = connect_with_retry(format!("127.0.0.1:{remote_port}")).await;
    stream.write_all(b"hello user hook").await.unwrap();
    let mut got = vec![0_u8; "hello user hook".len()];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(got, b"hello user hook");

    let plugin_request = tokio::time::timeout(Duration::from_secs(5), plugin_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(plugin_request.contains("POST /new_user_conn HTTP/1.1"));
    assert!(plugin_request.contains("\"op\":\"NewUserConn\""));
    assert!(plugin_request.contains("\"proxy_name\":\"new-user-hook-echo\""));
    assert!(plugin_request.contains("\"proxy_type\":\"tcp\""));
    assert!(plugin_request.contains("\"user_addr\":\"127.0.0.1:"));
    assert!(plugin_request.contains("\"remote_addr\":\"127.0.0.1:"));

    client_task.abort();
    server_task.abort();
    echo_task.abort();
    plugin_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_new_user_conn_plugin_receives_udp_event() {
    let _guard = acquire_test_lock().await;
    let plugin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plugin_port = plugin.local_addr().unwrap().port();
    let (plugin_tx, plugin_rx) = oneshot::channel();
    let plugin_task = tokio::spawn(async move {
        let (mut stream, _) = plugin.accept().await.unwrap();
        let mut buf = vec![0_u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let _ = plugin_tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let bind_port = unused_port();
    let remote_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig {
            new_user_conn_url: Some(format!("http://127.0.0.1:{plugin_port}/new_user_conn")),
            ..Default::default()
        },
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let mut control = connect_with_retry(format!("127.0.0.1:{bind_port}")).await;
    write_msg(
        &mut control,
        &Message::Login {
            version: "test".to_string(),
            hostname: "test".to_string(),
            os: "test".to_string(),
            arch: "test".to_string(),
            run_id: String::new(),
            token: Some("secret".to_string()),
            pool_count: 0,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control).await.unwrap() {
        Message::LoginResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected login response: {other:?}"),
    }
    write_msg(
        &mut control,
        &Message::NewProxy {
            proxy_name: "udp-user-hook".to_string(),
            proxy_type: "udp".to_string(),
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control).await.unwrap() {
        Message::NewProxyResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected proxy response: {other:?}"),
    }

    let visitor = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    visitor
        .send_to(b"hello hook", format!("127.0.0.1:{remote_port}"))
        .await
        .unwrap();

    let plugin_request = tokio::time::timeout(Duration::from_secs(5), plugin_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(plugin_request.contains("POST /new_user_conn HTTP/1.1"));
    assert!(plugin_request.contains("\"op\":\"NewUserConn\""));
    assert!(plugin_request.contains("\"proxy_name\":\"udp-user-hook\""));
    assert!(plugin_request.contains("\"proxy_type\":\"udp\""));
    assert!(plugin_request.contains("\"user_addr\":\"127.0.0.1:"));
    assert!(plugin_request.contains("\"remote_addr\":\"127.0.0.1:"));

    server_task.abort();
    plugin_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_new_user_conn_plugin_receives_sudp_event() {
    let _guard = acquire_test_lock().await;
    let plugin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plugin_port = plugin.local_addr().unwrap().port();
    let (plugin_tx, plugin_rx) = oneshot::channel();
    let plugin_task = tokio::spawn(async move {
        let (mut stream, _) = plugin.accept().await.unwrap();
        let mut buf = vec![0_u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let _ = plugin_tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let bind_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig {
            new_user_conn_url: Some(format!("http://127.0.0.1:{plugin_port}/new_user_conn")),
            ..Default::default()
        },
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let mut owner = connect_with_retry(format!("127.0.0.1:{bind_port}")).await;
    write_msg(
        &mut owner,
        &Message::Login {
            version: "test".to_string(),
            hostname: "owner".to_string(),
            os: "test".to_string(),
            arch: "test".to_string(),
            run_id: String::new(),
            token: Some("secret".to_string()),
            pool_count: 0,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut owner).await.unwrap() {
        Message::LoginResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected owner login response: {other:?}"),
    }
    write_msg(
        &mut owner,
        &Message::NewProxy {
            proxy_name: "sudp-user-hook".to_string(),
            proxy_type: "sudp".to_string(),
            remote_port: 0,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: Some("private".to_string()),
        },
    )
    .await
    .unwrap();
    match read_msg(&mut owner).await.unwrap() {
        Message::NewProxyResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected sudp proxy response: {other:?}"),
    }

    let mut visitor = connect_with_retry(format!("127.0.0.1:{bind_port}")).await;
    write_msg(
        &mut visitor,
        &Message::Login {
            version: "test".to_string(),
            hostname: "visitor".to_string(),
            os: "test".to_string(),
            arch: "test".to_string(),
            run_id: String::new(),
            token: Some("secret".to_string()),
            pool_count: 0,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut visitor).await.unwrap() {
        Message::LoginResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected visitor login response: {other:?}"),
    }
    write_msg(
        &mut visitor,
        &Message::SudpPacket {
            proxy_name: "sudp-user-hook".to_string(),
            session_id: "local-sudp-visitor|127.0.0.1:50000".to_string(),
            content: b"hello hook".to_vec(),
            sk: Some("private".to_string()),
            from_visitor: true,
        },
    )
    .await
    .unwrap();

    let plugin_request = tokio::time::timeout(Duration::from_secs(5), plugin_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(plugin_request.contains("POST /new_user_conn HTTP/1.1"));
    assert!(plugin_request.contains("\"op\":\"NewUserConn\""));
    assert!(plugin_request.contains("\"proxy_name\":\"sudp-user-hook\""));
    assert!(plugin_request.contains("\"proxy_type\":\"sudp\""));
    assert!(plugin_request.contains("\"user_addr\":\"local-sudp-visitor|127.0.0.1:50000\""));
    assert!(plugin_request.contains("\"remote_addr\":\"sudp://sudp-user-hook\""));

    server_task.abort();
    plugin_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dashboard_admin_closes_client_proxies() {
    let _guard = acquire_test_lock().await;
    let bind_port = unused_port();
    let remote_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let mut control = connect_with_retry(format!("127.0.0.1:{bind_port}")).await;
    write_msg(
        &mut control,
        &Message::Login {
            version: "test".to_string(),
            hostname: "test".to_string(),
            os: "test".to_string(),
            arch: "test".to_string(),
            run_id: String::new(),
            token: Some("secret".to_string()),
            pool_count: 0,
        },
    )
    .await
    .unwrap();
    let run_id = match read_msg(&mut control).await.unwrap() {
        Message::LoginResp { run_id, error, .. } if error.is_empty() => run_id,
        other => panic!("unexpected login response: {other:?}"),
    };

    write_msg(
        &mut control,
        &Message::NewProxy {
            proxy_name: "client-owned-echo".to_string(),
            proxy_type: "tcp".to_string(),
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control).await.unwrap() {
        Message::NewProxyResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected proxy response: {other:?}"),
    }

    let closed = http_request(
        format!("127.0.0.1:{dashboard_port}"),
        format!(
            "POST /api/clients/{run_id}/close HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer secret\r\n\r\n"
        )
        .as_bytes(),
    )
    .await;
    assert!(closed.contains("200 OK"));
    assert!(closed.contains("\"closed_count\":1"));
    assert!(closed.contains("client-owned-echo"));
    wait_until_connect_fails(format!("127.0.0.1:{remote_port}")).await;

    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_group_load_balances_between_members() {
    let _guard = acquire_test_lock().await;
    let local_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_a_port = local_a.local_addr().unwrap().port();
    let task_a = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_a.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 32];
                let _ = stream.read(&mut buf).await.unwrap();
                stream.write_all(b"A").await.unwrap();
            });
        }
    });
    let local_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_b_port = local_b.local_addr().unwrap().port();
    let task_b = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_b.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 32];
                let _ = stream.read(&mut buf).await.unwrap();
                stream.write_all(b"B").await.unwrap();
            });
        }
    });

    let bind_port = unused_port();
    let remote_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let make_client_cfg = |name: &str, local_port| ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: name.to_string(),
            proxy_type: ProxyType::Tcp,
            local_ip: "127.0.0.1".to_string(),
            local_port,
            remote_port,
            group: Some("echo-group".to_string()),
            group_key: Some("group-secret".to_string()),
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_a_task = tokio::spawn(client::run(make_client_cfg("echo-a", local_a_port)));
    let client_b_task = tokio::spawn(client::run(make_client_cfg("echo-b", local_b_port)));
    sleep(Duration::from_millis(500)).await;

    let mut got = Vec::new();
    for _ in 0..4 {
        let mut stream = connect_with_retry(format!("127.0.0.1:{remote_port}")).await;
        stream.write_all(b"x").await.unwrap();
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await.unwrap();
        got.push(byte[0]);
    }
    assert!(got.contains(&b'A'), "{got:?}");
    assert!(got.contains(&b'B'), "{got:?}");

    let groups = http_request(
        format!("127.0.0.1:{dashboard_port}"),
        b"GET /api/groups HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(groups.contains("echo-group"));
    assert!(groups.contains("\"member_count\":2"));

    client_a_task.abort();
    client_b_task.abort();
    server_task.abort();
    task_a.abort();
    task_b.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_proxy_forwards_bytes_over_kcp_transport() {
    assert_tcp_proxy_forwards_with_transport(TransportProtocol::Kcp).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_proxy_forwards_bytes_over_tls_transport() {
    assert_tcp_proxy_forwards_with_transport(TransportProtocol::Tls).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_proxy_forwards_bytes_over_tcpmux_transport() {
    assert_tcp_proxy_forwards_with_transport(TransportProtocol::Tcpmux).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_proxy_forwards_bytes_over_websocket_transport() {
    assert_tcp_proxy_forwards_with_transport(TransportProtocol::Websocket).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_proxy_forwards_bytes_over_quic_transport() {
    assert_tcp_proxy_forwards_with_transport(TransportProtocol::Quic).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stcp_visitor_forwards_bytes() {
    let _guard = acquire_test_lock().await;
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = echo.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    stream.write_all(&buf[..n]).await.unwrap();
                }
            });
        }
    });

    let bind_port = unused_port();
    let visitor_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let owner_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "private-echo".to_string(),
            proxy_type: ProxyType::Stcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port: 0,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: Some("private".to_string()),
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let owner_task = tokio::spawn(client::run(owner_cfg));
    sleep(Duration::from_millis(200)).await;

    let visitor_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: Vec::new(),
        visitors: vec![VisitorConfig {
            name: "local-private-echo".to_string(),
            visitor_type: ProxyType::Stcp,
            server_name: "private-echo".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: visitor_port,
            sk: Some("private".to_string()),
        }],
    };
    let visitor_task = tokio::spawn(client::run(visitor_cfg));

    let mut stream = connect_with_retry(format!("127.0.0.1:{visitor_port}")).await;
    stream.write_all(b"hello stcp").await.unwrap();

    let mut got = vec![0_u8; "hello stcp".len()];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(got, b"hello stcp");

    visitor_task.abort();
    owner_task.abort();
    server_task.abort();
    echo_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn xtcp_visitor_forwards_bytes_directly_when_peer_reachable() {
    let _guard = acquire_test_lock().await;
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = echo.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    stream.write_all(&buf[..n]).await.unwrap();
                }
            });
        }
    });

    let bind_port = unused_port();
    let visitor_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let owner_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "xtcp-echo".to_string(),
            proxy_type: ProxyType::Xtcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port: 0,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: Some("private".to_string()),
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let owner_task = tokio::spawn(client::run(owner_cfg));
    sleep(Duration::from_millis(200)).await;

    let visitor_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: Vec::new(),
        visitors: vec![VisitorConfig {
            name: "local-xtcp-echo".to_string(),
            visitor_type: ProxyType::Xtcp,
            server_name: "xtcp-echo".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: visitor_port,
            sk: Some("private".to_string()),
        }],
    };
    let visitor_task = tokio::spawn(client::run(visitor_cfg));

    let mut stream = connect_with_retry(format!("127.0.0.1:{visitor_port}")).await;
    stream.write_all(b"hello xtcp").await.unwrap();

    let mut got = vec![0_u8; "hello xtcp".len()];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(got, b"hello xtcp");

    let metrics = http_request(
        format!("127.0.0.1:{dashboard_port}"),
        b"GET /api/metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(metrics.contains("\"visitor_connections_total\":0"));

    visitor_task.abort();
    owner_task.abort();
    server_task.abort();
    echo_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn xtcp_group_visitor_forwards_bytes_directly_when_peer_reachable() {
    let _guard = acquire_test_lock().await;
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = echo.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    stream.write_all(&buf[..n]).await.unwrap();
                }
            });
        }
    });

    let bind_port = unused_port();
    let visitor_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let owner_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "xtcp-echo-a".to_string(),
            proxy_type: ProxyType::Xtcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port: 0,
            group: Some("xtcp-echo-group".to_string()),
            group_key: Some("group-secret".to_string()),
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: Some("private".to_string()),
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let owner_task = tokio::spawn(client::run(owner_cfg));
    sleep(Duration::from_millis(200)).await;

    let visitor_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: Vec::new(),
        visitors: vec![VisitorConfig {
            name: "local-xtcp-echo-group".to_string(),
            visitor_type: ProxyType::Xtcp,
            server_name: "xtcp-echo-group".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: visitor_port,
            sk: Some("private".to_string()),
        }],
    };
    let visitor_task = tokio::spawn(client::run(visitor_cfg));

    let mut stream = connect_with_retry(format!("127.0.0.1:{visitor_port}")).await;
    stream.write_all(b"hello xtcp group").await.unwrap();

    let mut got = vec![0_u8; "hello xtcp group".len()];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(got, b"hello xtcp group");

    let metrics = http_request(
        format!("127.0.0.1:{dashboard_port}"),
        b"GET /api/metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(metrics.contains("\"visitor_connections_total\":0"));

    visitor_task.abort();
    owner_task.abort();
    server_task.abort();
    echo_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn xtcp_visitor_waits_for_delayed_owner_nat_notification() {
    let _guard = acquire_test_lock().await;
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = echo.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    stream.write_all(&buf[..n]).await.unwrap();
                }
            });
        }
    });

    let bind_port = unused_port();
    let visitor_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let visitor_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: Vec::new(),
        visitors: vec![VisitorConfig {
            name: "local-delayed-xtcp".to_string(),
            visitor_type: ProxyType::Xtcp,
            server_name: "delayed-xtcp".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: visitor_port,
            sk: Some("private".to_string()),
        }],
    };
    let visitor_task = tokio::spawn(client::run(visitor_cfg));
    sleep(Duration::from_millis(150)).await;

    let mut stream = connect_with_retry(format!("127.0.0.1:{visitor_port}")).await;
    let pending_write = tokio::spawn(async move {
        stream.write_all(b"hello delayed xtcp").await.unwrap();
        let mut got = vec![0_u8; "hello delayed xtcp".len()];
        stream.read_exact(&mut got).await.unwrap();
        got
    });

    sleep(Duration::from_millis(120)).await;
    let owner_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "delayed-xtcp".to_string(),
            proxy_type: ProxyType::Xtcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port: 0,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: Some("private".to_string()),
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let owner_task = tokio::spawn(client::run(owner_cfg));

    let got = tokio::time::timeout(Duration::from_secs(5), pending_write)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, b"hello delayed xtcp");

    let metrics = http_request(
        format!("127.0.0.1:{dashboard_port}"),
        b"GET /api/metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(metrics.contains("\"visitor_connections_total\":0"));

    owner_task.abort();
    visitor_task.abort();
    server_task.abort();
    echo_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stcp_group_load_balances_between_members() {
    let _guard = acquire_test_lock().await;
    let local_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_a_port = local_a.local_addr().unwrap().port();
    let task_a = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_a.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                let _ = stream.read(&mut buf).await.unwrap();
                stream.write_all(b"A").await.unwrap();
            });
        }
    });
    let local_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_b_port = local_b.local_addr().unwrap().port();
    let task_b = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_b.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                let _ = stream.read(&mut buf).await.unwrap();
                stream.write_all(b"B").await.unwrap();
            });
        }
    });

    let bind_port = unused_port();
    let visitor_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let make_owner_cfg = |name: &str, local_port| ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: name.to_string(),
            proxy_type: ProxyType::Stcp,
            local_ip: "127.0.0.1".to_string(),
            local_port,
            remote_port: 0,
            group: Some("private-group".to_string()),
            group_key: Some("group-secret".to_string()),
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: Some("private".to_string()),
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let owner_a_task = tokio::spawn(client::run(make_owner_cfg("private-a", local_a_port)));
    let owner_b_task = tokio::spawn(client::run(make_owner_cfg("private-b", local_b_port)));
    sleep(Duration::from_millis(300)).await;

    let visitor_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: Vec::new(),
        visitors: vec![VisitorConfig {
            name: "local-private-group".to_string(),
            visitor_type: ProxyType::Stcp,
            server_name: "private-group".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: visitor_port,
            sk: Some("private".to_string()),
        }],
    };
    let visitor_task = tokio::spawn(client::run(visitor_cfg));
    sleep(Duration::from_millis(300)).await;

    let mut got = Vec::new();
    for _ in 0..4 {
        let mut stream = connect_with_retry(format!("127.0.0.1:{visitor_port}")).await;
        stream.write_all(b"x").await.unwrap();
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await.unwrap();
        got.push(byte[0]);
    }
    assert!(got.contains(&b'A'), "{got:?}");
    assert!(got.contains(&b'B'), "{got:?}");

    let groups = http_request(
        format!("127.0.0.1:{dashboard_port}"),
        b"GET /api/groups HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(groups.contains("\"protocol\":\"stcp\""));
    assert!(groups.contains("\"group\":\"private-group\""));
    assert!(groups.contains("\"member_count\":2"));

    visitor_task.abort();
    owner_a_task.abort();
    owner_b_task.abort();
    server_task.abort();
    task_a.abort();
    task_b.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_proxy_routes_by_host_header() {
    let _guard = acquire_test_lock().await;
    let local_http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_http_port = local_http.local_addr().unwrap().port();
    let http_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_http.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                let _ = stream.read(&mut buf).await.unwrap();
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
                    .await
                    .unwrap();
            });
        }
    });

    let bind_port = unused_port();
    let vhost_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: vhost_port,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "web".to_string(),
            proxy_type: ProxyType::Http,
            local_ip: "127.0.0.1".to_string(),
            local_port: local_http_port,
            remote_port: 0,
            group: None,
            group_key: None,
            custom_domains: vec!["www.example.com".to_string()],
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));
    sleep(Duration::from_millis(300)).await;

    let mut stream = connect_with_retry(format!("127.0.0.1:{vhost_port}")).await;
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: www.example.com\r\n\r\n")
        .await
        .unwrap();

    let mut response = Vec::new();
    let mut buf = [0_u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    response.extend_from_slice(&buf[..n]);
    assert!(String::from_utf8_lossy(&response).contains("200 OK"));

    client_task.abort();
    server_task.abort();
    http_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_new_user_conn_plugin_receives_http_event() {
    let _guard = acquire_test_lock().await;
    let local_http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_http_port = local_http.local_addr().unwrap().port();
    let http_task = tokio::spawn(async move {
        let (mut stream, _) = local_http.accept().await.unwrap();
        let mut buf = [0_u8; 1024];
        let _ = stream.read(&mut buf).await.unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
            .await
            .unwrap();
    });

    let plugin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plugin_port = plugin.local_addr().unwrap().port();
    let (plugin_tx, plugin_rx) = oneshot::channel();
    let plugin_task = tokio::spawn(async move {
        let (mut stream, _) = plugin.accept().await.unwrap();
        let mut buf = vec![0_u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let _ = plugin_tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let bind_port = unused_port();
    let vhost_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: vhost_port,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig {
            new_user_conn_url: Some(format!("http://127.0.0.1:{plugin_port}/new_user_conn")),
            ..Default::default()
        },
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "http-user-hook".to_string(),
            proxy_type: ProxyType::Http,
            local_ip: "127.0.0.1".to_string(),
            local_port: local_http_port,
            remote_port: 0,
            group: None,
            group_key: None,
            custom_domains: vec!["hook.example.com".to_string()],
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));
    sleep(Duration::from_millis(300)).await;

    let mut stream = connect_with_retry(format!("127.0.0.1:{vhost_port}")).await;
    stream
        .write_all(b"GET /hook HTTP/1.1\r\nHost: hook.example.com\r\n\r\n")
        .await
        .unwrap();
    let mut response = [0_u8; 1024];
    let n = stream.read(&mut response).await.unwrap();
    assert!(String::from_utf8_lossy(&response[..n]).contains("200 OK"));

    let plugin_request = tokio::time::timeout(Duration::from_secs(5), plugin_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(plugin_request.contains("POST /new_user_conn HTTP/1.1"));
    assert!(plugin_request.contains("\"op\":\"NewUserConn\""));
    assert!(plugin_request.contains("\"proxy_name\":\"http-user-hook\""));
    assert!(plugin_request.contains("\"proxy_type\":\"http\""));
    assert!(plugin_request.contains("\"user_addr\":\"127.0.0.1:"));
    assert!(plugin_request.contains("\"remote_addr\":\"127.0.0.1:"));
    assert!(plugin_request.contains("\"dst_addr\":\"hook.example.com\""));
    assert!(plugin_request.contains("\"path\":\"/hook\""));

    client_task.abort();
    server_task.abort();
    http_task.abort();
    plugin_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_proxy_supports_path_auth_and_header_rewrite() {
    let _guard = acquire_test_lock().await;
    let api_http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_http_port = api_http.local_addr().unwrap().port();
    let (api_tx, api_rx) = oneshot::channel();
    let api_task = tokio::spawn(async move {
        let (mut stream, _) = api_http.accept().await.unwrap();
        let mut buf = [0_u8; 2048];
        let n = stream.read(&mut buf).await.unwrap();
        let _ = api_tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nAPI")
            .await
            .unwrap();
    });

    let root_http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root_http_port = root_http.local_addr().unwrap().port();
    let root_task = tokio::spawn(async move {
        let (mut stream, _) = root_http.accept().await.unwrap();
        let mut buf = [0_u8; 1024];
        let _ = stream.read(&mut buf).await.unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nROOT")
            .await
            .unwrap();
    });

    let bind_port = unused_port();
    let vhost_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: vhost_port,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let mut request_headers = HeaderRewriteConfig::default();
    request_headers
        .set
        .insert("X-Added".to_string(), "yes".to_string());
    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![
            ProxyConfig {
                name: "root-web".to_string(),
                proxy_type: ProxyType::Http,
                local_ip: "127.0.0.1".to_string(),
                local_port: root_http_port,
                remote_port: 0,
                group: None,
                group_key: None,
                custom_domains: vec!["www.example.com".to_string()],
                locations: vec!["/".to_string()],
                host_header_rewrite: None,
                request_headers: Default::default(),
                http_user: None,
                http_password: None,
                bandwidth_limit: None,
                sk: None,
                health_check: None,
            },
            ProxyConfig {
                name: "api-web".to_string(),
                proxy_type: ProxyType::Http,
                local_ip: "127.0.0.1".to_string(),
                local_port: api_http_port,
                remote_port: 0,
                group: None,
                group_key: None,
                custom_domains: vec!["*.example.com".to_string()],
                locations: vec!["/api".to_string()],
                host_header_rewrite: Some("backend.internal".to_string()),
                request_headers,
                http_user: Some("u".to_string()),
                http_password: Some("p".to_string()),
                bandwidth_limit: None,
                sk: None,
                health_check: None,
            },
        ],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));
    sleep(Duration::from_millis(400)).await;

    let mut api_stream = connect_with_retry(format!("127.0.0.1:{vhost_port}")).await;
    api_stream
        .write_all(
            b"GET /api/users HTTP/1.1\r\nHost: www.example.com\r\nAuthorization: Basic dTpw\r\n\r\n",
        )
        .await
        .unwrap();
    let mut response = vec![0_u8; 128];
    let n = api_stream.read(&mut response).await.unwrap();
    assert!(String::from_utf8_lossy(&response[..n]).contains("API"));

    let api_request = api_rx.await.unwrap();
    assert!(api_request.contains("GET /api/users HTTP/1.1"));
    assert!(api_request.contains("Host: backend.internal"));
    assert!(api_request.contains("X-Added: yes"));
    assert!(api_request.contains("X-Real-IP: 127.0.0.1"));
    assert!(api_request.contains("X-Forwarded-For: 127.0.0.1"));
    assert!(!api_request.contains("Authorization: Basic dTpw"));

    let mut root_stream = connect_with_retry(format!("127.0.0.1:{vhost_port}")).await;
    root_stream
        .write_all(b"GET / HTTP/1.1\r\nHost: www.example.com\r\n\r\n")
        .await
        .unwrap();
    let n = root_stream.read(&mut response).await.unwrap();
    assert!(String::from_utf8_lossy(&response[..n]).contains("ROOT"));

    client_task.abort();
    server_task.abort();
    api_task.abort();
    root_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_group_load_balances_between_members() {
    let _guard = acquire_test_lock().await;
    let local_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_a_port = local_a.local_addr().unwrap().port();
    let task_a = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_a.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                let _ = stream.read(&mut buf).await.unwrap();
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nA")
                    .await
                    .unwrap();
            });
        }
    });

    let local_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_b_port = local_b.local_addr().unwrap().port();
    let task_b = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_b.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                let _ = stream.read(&mut buf).await.unwrap();
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nB")
                    .await
                    .unwrap();
            });
        }
    });

    let bind_port = unused_port();
    let vhost_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: vhost_port,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let make_client_cfg = |name: &str, local_port| ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: name.to_string(),
            proxy_type: ProxyType::Http,
            local_ip: "127.0.0.1".to_string(),
            local_port,
            remote_port: 0,
            group: Some("web-group".to_string()),
            group_key: Some("web-secret".to_string()),
            custom_domains: vec!["group.example.com".to_string()],
            locations: vec!["/".to_string()],
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_a_task = tokio::spawn(client::run(make_client_cfg("web-a", local_a_port)));
    let client_b_task = tokio::spawn(client::run(make_client_cfg("web-b", local_b_port)));
    sleep(Duration::from_millis(500)).await;

    let mut bodies = Vec::new();
    for _ in 0..4 {
        let response = http_request(
            format!("127.0.0.1:{vhost_port}"),
            b"GET / HTTP/1.1\r\nHost: group.example.com\r\n\r\n",
        )
        .await;
        bodies.push(response);
    }
    assert!(bodies.iter().any(|body| body.ends_with('A')), "{bodies:?}");
    assert!(bodies.iter().any(|body| body.ends_with('B')), "{bodies:?}");

    client_a_task.abort();
    client_b_task.abort();
    server_task.abort();
    task_a.abort();
    task_b.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcpmux_proxy_forwards_connect_tunnel() {
    let _guard = acquire_test_lock().await;
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = echo.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    stream.write_all(&buf[..n]).await.unwrap();
                }
            });
        }
    });

    let bind_port = unused_port();
    let vhost_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: vhost_port,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "mux-echo".to_string(),
            proxy_type: ProxyType::Tcpmux,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port: 0,
            group: None,
            group_key: None,
            custom_domains: vec!["mux.example.com".to_string()],
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));
    sleep(Duration::from_millis(300)).await;

    let mut stream = connect_with_retry(format!("127.0.0.1:{vhost_port}")).await;
    stream
        .write_all(b"CONNECT mux.example.com:443 HTTP/1.1\r\nHost: mux.example.com:443\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    let mut byte = [0_u8; 1];
    while !response.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).await.unwrap();
        response.push(byte[0]);
    }
    assert!(String::from_utf8_lossy(&response).contains("200 Connection Established"));

    stream.write_all(b"hello mux").await.unwrap();
    let mut got = vec![0_u8; "hello mux".len()];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(got, b"hello mux");

    client_task.abort();
    server_task.abort();
    echo_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcpmux_group_load_balances_between_members() {
    let _guard = acquire_test_lock().await;
    let local_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_a_port = local_a.local_addr().unwrap().port();
    let task_a = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_a.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 32];
                let _ = stream.read(&mut buf).await.unwrap();
                stream.write_all(b"A").await.unwrap();
            });
        }
    });
    let local_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_b_port = local_b.local_addr().unwrap().port();
    let task_b = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_b.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 32];
                let _ = stream.read(&mut buf).await.unwrap();
                stream.write_all(b"B").await.unwrap();
            });
        }
    });

    let bind_port = unused_port();
    let vhost_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: vhost_port,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let make_client_cfg = |name: &str, local_port| ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: name.to_string(),
            proxy_type: ProxyType::Tcpmux,
            local_ip: "127.0.0.1".to_string(),
            local_port,
            remote_port: 0,
            group: Some("mux-group".to_string()),
            group_key: Some("mux-secret".to_string()),
            custom_domains: vec!["mux-group.example.com".to_string()],
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_a_task = tokio::spawn(client::run(make_client_cfg("mux-a", local_a_port)));
    let client_b_task = tokio::spawn(client::run(make_client_cfg("mux-b", local_b_port)));
    sleep(Duration::from_millis(500)).await;

    let mut got = Vec::new();
    for _ in 0..4 {
        let mut stream = connect_with_retry(format!("127.0.0.1:{vhost_port}")).await;
        stream
            .write_all(
                b"CONNECT mux-group.example.com:443 HTTP/1.1\r\nHost: mux-group.example.com:443\r\n\r\n",
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        let mut byte = [0_u8; 1];
        while !response.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.unwrap();
            response.push(byte[0]);
        }
        assert!(String::from_utf8_lossy(&response).contains("200 Connection Established"));
        stream.write_all(b"x").await.unwrap();
        stream.read_exact(&mut byte).await.unwrap();
        got.push(byte[0]);
    }
    assert!(got.contains(&b'A'), "{got:?}");
    assert!(got.contains(&b'B'), "{got:?}");

    let groups = http_request(
        format!("127.0.0.1:{dashboard_port}"),
        b"GET /api/groups HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(groups.contains("\"protocol\":\"tcpmux\""));
    assert!(groups.contains("\"group\":\"mux-group\""));
    assert!(groups.contains("\"member_count\":2"));

    client_a_task.abort();
    client_b_task.abort();
    server_task.abort();
    task_a.abort();
    task_b.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn https_proxy_routes_by_sni() {
    let _guard = acquire_test_lock().await;
    let local_tls = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_tls_port = local_tls.local_addr().unwrap().port();
    let tls_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_tls.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                let _ = stream.read(&mut buf).await.unwrap();
                stream.write_all(b"tls-ok").await.unwrap();
            });
        }
    });

    let bind_port = unused_port();
    let vhost_https_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "secure-web".to_string(),
            proxy_type: ProxyType::Https,
            local_ip: "127.0.0.1".to_string(),
            local_port: local_tls_port,
            remote_port: 0,
            group: None,
            group_key: None,
            custom_domains: vec!["*.example.com".to_string()],
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));
    sleep(Duration::from_millis(300)).await;

    let mut stream = connect_with_retry(format!("127.0.0.1:{vhost_https_port}")).await;
    stream
        .write_all(&tls_client_hello_for_sni("secure.example.com"))
        .await
        .unwrap();

    let mut got = [0_u8; 6];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, b"tls-ok");

    client_task.abort();
    server_task.abort();
    tls_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn https_group_load_balances_between_members() {
    let _guard = acquire_test_lock().await;
    let local_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_a_port = local_a.local_addr().unwrap().port();
    let task_a = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_a.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                let _ = stream.read(&mut buf).await.unwrap();
                stream.write_all(b"A").await.unwrap();
            });
        }
    });
    let local_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_b_port = local_b.local_addr().unwrap().port();
    let task_b = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_b.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                let _ = stream.read(&mut buf).await.unwrap();
                stream.write_all(b"B").await.unwrap();
            });
        }
    });

    let bind_port = unused_port();
    let vhost_https_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let make_client_cfg = |name: &str, local_port| ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: name.to_string(),
            proxy_type: ProxyType::Https,
            local_ip: "127.0.0.1".to_string(),
            local_port,
            remote_port: 0,
            group: Some("secure-group".to_string()),
            group_key: Some("secure-secret".to_string()),
            custom_domains: vec!["secure-group.example.com".to_string()],
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_a_task = tokio::spawn(client::run(make_client_cfg("secure-a", local_a_port)));
    let client_b_task = tokio::spawn(client::run(make_client_cfg("secure-b", local_b_port)));
    sleep(Duration::from_millis(500)).await;

    let mut got = Vec::new();
    for _ in 0..4 {
        let mut stream = connect_with_retry(format!("127.0.0.1:{vhost_https_port}")).await;
        stream
            .write_all(&tls_client_hello_for_sni("secure-group.example.com"))
            .await
            .unwrap();
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await.unwrap();
        got.push(byte[0]);
    }
    assert!(got.contains(&b'A'), "{got:?}");
    assert!(got.contains(&b'B'), "{got:?}");

    client_a_task.abort();
    client_b_task.abort();
    server_task.abort();
    task_a.abort();
    task_b.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn https_proxy_falls_back_to_star_domain_without_sni() {
    let _guard = acquire_test_lock().await;
    let local_tls = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_tls_port = local_tls.local_addr().unwrap().port();
    let tls_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_tls.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                let _ = stream.read(&mut buf).await.unwrap();
                stream.write_all(b"fallback-ok").await.unwrap();
            });
        }
    });

    let bind_port = unused_port();
    let vhost_https_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "fallback-secure-web".to_string(),
            proxy_type: ProxyType::Https,
            local_ip: "127.0.0.1".to_string(),
            local_port: local_tls_port,
            remote_port: 0,
            group: None,
            group_key: None,
            custom_domains: vec!["*".to_string()],
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));
    sleep(Duration::from_millis(300)).await;

    let mut stream = connect_with_retry(format!("127.0.0.1:{vhost_https_port}")).await;
    stream
        .write_all(&tls_client_hello_without_sni())
        .await
        .unwrap();

    let mut got = [0_u8; 11];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, b"fallback-ok");

    client_task.abort();
    server_task.abort();
    tls_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udp_proxy_forwards_packets() {
    let _guard = acquire_test_lock().await;
    let udp_echo = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_echo_port = udp_echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        let mut buf = [0_u8; 1024];
        loop {
            let (n, peer) = udp_echo.recv_from(&mut buf).await.unwrap();
            udp_echo.send_to(&buf[..n], peer).await.unwrap();
        }
    });

    let bind_port = unused_port();
    let remote_port = unused_udp_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "udp-echo".to_string(),
            proxy_type: ProxyType::Udp,
            local_ip: "127.0.0.1".to_string(),
            local_port: udp_echo_port,
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));
    sleep(Duration::from_millis(300)).await;

    let visitor = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    visitor
        .send_to(b"hello udp", format!("127.0.0.1:{remote_port}"))
        .await
        .unwrap();

    let mut buf = [0_u8; 1024];
    let (n, _) =
        recv_udp_ignoring_connection_reset(&visitor, &mut buf, Duration::from_secs(5)).await;
    assert_eq!(&buf[..n], b"hello udp");

    client_task.abort();
    server_task.abort();
    echo_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udp_group_load_balances_between_members() {
    let _guard = acquire_test_lock().await;
    let local_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let local_a_port = local_a.local_addr().unwrap().port();
    let task_a = tokio::spawn(async move {
        let mut buf = [0_u8; 1024];
        loop {
            let (_, peer) = local_a.recv_from(&mut buf).await.unwrap();
            local_a.send_to(b"A", peer).await.unwrap();
        }
    });
    let local_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let local_b_port = local_b.local_addr().unwrap().port();
    let task_b = tokio::spawn(async move {
        let mut buf = [0_u8; 1024];
        loop {
            let (_, peer) = local_b.recv_from(&mut buf).await.unwrap();
            local_b.send_to(b"B", peer).await.unwrap();
        }
    });

    let bind_port = unused_port();
    let remote_port = unused_udp_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let make_client_cfg = |name: &str, local_port| ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: name.to_string(),
            proxy_type: ProxyType::Udp,
            local_ip: "127.0.0.1".to_string(),
            local_port,
            remote_port,
            group: Some("dns-group".to_string()),
            group_key: Some("group-secret".to_string()),
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_a_task = tokio::spawn(client::run(make_client_cfg("dns-a", local_a_port)));
    let client_b_task = tokio::spawn(client::run(make_client_cfg("dns-b", local_b_port)));
    sleep(Duration::from_millis(500)).await;

    let visitor = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut idx = 0;
    while Instant::now() < deadline
        && !(got.contains(&b"A".to_vec()) && got.contains(&b"B".to_vec()))
    {
        visitor
            .send_to(
                format!("query-{idx}").as_bytes(),
                format!("127.0.0.1:{remote_port}"),
            )
            .await
            .unwrap();
        idx += 1;
        let mut buf = [0_u8; 16];
        if let Ok(Ok((n, _))) =
            tokio::time::timeout(Duration::from_millis(500), visitor.recv_from(&mut buf)).await
        {
            got.push(buf[..n].to_vec());
        }
    }

    assert!(got.contains(&b"A".to_vec()), "{got:?}");
    assert!(got.contains(&b"B".to_vec()), "{got:?}");

    let groups = http_request(
        format!("127.0.0.1:{dashboard_port}"),
        b"GET /api/groups HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(groups.contains("\"protocol\":\"udp\""));
    assert!(groups.contains("\"group\":\"dns-group\""));
    assert!(groups.contains("\"member_count\":2"));

    let closed = http_request(
        format!("127.0.0.1:{dashboard_port}"),
        b"POST /api/groups/udp/dns-group/close HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer secret\r\n\r\n",
    )
    .await;
    assert!(closed.contains("200 OK"));
    assert!(closed.contains("\"closed_count\":2"));
    assert!(closed.contains("dns-a"));
    assert!(closed.contains("dns-b"));

    let groups = http_request(
        format!("127.0.0.1:{dashboard_port}"),
        b"GET /api/groups HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(!groups.contains("\"group\":\"dns-group\""));

    client_a_task.abort();
    client_b_task.abort();
    server_task.abort();
    task_a.abort();
    task_b.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udp_proxy_batches_visitor_packets_to_client() {
    let _guard = acquire_test_lock().await;
    let bind_port = unused_port();
    let remote_port = unused_udp_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let mut control = connect_with_retry(format!("127.0.0.1:{bind_port}")).await;
    write_msg(
        &mut control,
        &Message::Login {
            version: "test".to_string(),
            hostname: "test".to_string(),
            os: "test".to_string(),
            arch: "test".to_string(),
            run_id: String::new(),
            token: Some("secret".to_string()),
            pool_count: 0,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control).await.unwrap() {
        Message::LoginResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected login response: {other:?}"),
    }

    write_msg(
        &mut control,
        &Message::NewProxy {
            proxy_name: "udp-batch".to_string(),
            proxy_type: "udp".to_string(),
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control).await.unwrap() {
        Message::NewProxyResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected proxy response: {other:?}"),
    }

    let visitor = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for idx in 0..4 {
        visitor
            .send_to(
                format!("batch-{idx}").as_bytes(),
                format!("127.0.0.1:{remote_port}"),
            )
            .await
            .unwrap();
    }

    let mut batched = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let msg = tokio::time::timeout(Duration::from_secs(1), read_msg(&mut control))
            .await
            .unwrap()
            .unwrap();
        match msg {
            Message::UdpPacketBatch { packets } => {
                batched = Some(packets);
                break;
            }
            Message::UdpPacket { .. } => {}
            other => panic!("unexpected control message: {other:?}"),
        }
    }

    let packets = batched.expect("server did not send a UDP packet batch");
    assert!(packets.len() >= 2, "{packets:?}");
    assert!(packets
        .iter()
        .all(|packet| packet.proxy_name == "udp-batch"));

    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udp_group_batches_visitor_packets_to_each_member() {
    let _guard = acquire_test_lock().await;
    let bind_port = unused_port();
    let remote_port = unused_udp_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let mut control_a = connect_with_retry(format!("127.0.0.1:{bind_port}")).await;
    write_msg(
        &mut control_a,
        &Message::Login {
            version: "test".to_string(),
            hostname: "test-a".to_string(),
            os: "test".to_string(),
            arch: "test".to_string(),
            run_id: "udp-group-a-run".to_string(),
            token: Some("secret".to_string()),
            pool_count: 0,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control_a).await.unwrap() {
        Message::LoginResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected login response: {other:?}"),
    }
    write_msg(
        &mut control_a,
        &Message::NewProxy {
            proxy_name: "udp-group-a".to_string(),
            proxy_type: "udp".to_string(),
            remote_port,
            group: Some("udp-batch-group".to_string()),
            group_key: Some("secret-group".to_string()),
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control_a).await.unwrap() {
        Message::NewProxyResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected proxy response: {other:?}"),
    }

    let mut control_b = connect_with_retry(format!("127.0.0.1:{bind_port}")).await;
    write_msg(
        &mut control_b,
        &Message::Login {
            version: "test".to_string(),
            hostname: "test-b".to_string(),
            os: "test".to_string(),
            arch: "test".to_string(),
            run_id: "udp-group-b-run".to_string(),
            token: Some("secret".to_string()),
            pool_count: 0,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control_b).await.unwrap() {
        Message::LoginResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected login response: {other:?}"),
    }
    write_msg(
        &mut control_b,
        &Message::NewProxy {
            proxy_name: "udp-group-b".to_string(),
            proxy_type: "udp".to_string(),
            remote_port,
            group: Some("udp-batch-group".to_string()),
            group_key: Some("secret-group".to_string()),
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control_b).await.unwrap() {
        Message::NewProxyResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected proxy response: {other:?}"),
    }

    let visitor = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for idx in 0..8 {
        visitor
            .send_to(
                format!("group-batch-{idx}").as_bytes(),
                format!("127.0.0.1:{remote_port}"),
            )
            .await
            .unwrap();
    }

    let mut batch_a = None;
    let mut batch_b = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (batch_a.is_none() || batch_b.is_none()) {
        if batch_a.is_none() {
            if let Ok(Ok(msg)) =
                tokio::time::timeout(Duration::from_millis(500), read_msg(&mut control_a)).await
            {
                match msg {
                    Message::UdpPacketBatch { packets } => batch_a = Some(packets),
                    Message::UdpPacket { .. } => {}
                    other => panic!("unexpected control-a message: {other:?}"),
                }
            }
        }
        if batch_b.is_none() {
            if let Ok(Ok(msg)) =
                tokio::time::timeout(Duration::from_millis(500), read_msg(&mut control_b)).await
            {
                match msg {
                    Message::UdpPacketBatch { packets } => batch_b = Some(packets),
                    Message::UdpPacket { .. } => {}
                    other => panic!("unexpected control-b message: {other:?}"),
                }
            }
        }
    }

    let packets_a = batch_a.expect("server did not batch packets for udp-group-a");
    let packets_b = batch_b.expect("server did not batch packets for udp-group-b");
    assert!(packets_a.len() >= 2, "{packets_a:?}");
    assert!(packets_b.len() >= 2, "{packets_b:?}");
    assert!(packets_a
        .iter()
        .all(|packet| packet.proxy_name == "udp-group-a"));
    assert!(packets_b
        .iter()
        .all(|packet| packet.proxy_name == "udp-group-b"));

    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udp_proxy_batches_local_responses_to_server() {
    let _guard = acquire_test_lock().await;
    let udp_service = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_service_port = udp_service.local_addr().unwrap().port();
    let service_task = tokio::spawn(async move {
        let mut buf = [0_u8; 1024];
        let (_, peer) = udp_service.recv_from(&mut buf).await.unwrap();
        for idx in 0..4 {
            udp_service
                .send_to(format!("response-{idx}").as_bytes(), peer)
                .await
                .unwrap();
        }
    });

    let control = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bind_port = control.local_addr().unwrap().port();
    let server_task = tokio::spawn(async move {
        let (mut stream, _) = control.accept().await.unwrap();
        match read_msg(&mut stream).await.unwrap() {
            Message::Login { .. } => {}
            other => panic!("unexpected login message: {other:?}"),
        }
        write_msg(
            &mut stream,
            &Message::LoginResp {
                version: "test".to_string(),
                run_id: "run-udp-batch".to_string(),
                error: String::new(),
            },
        )
        .await
        .unwrap();
        match read_msg(&mut stream).await.unwrap() {
            Message::NewProxy { proxy_name, .. } if proxy_name == "udp-response-batch" => {}
            other => panic!("unexpected new proxy message: {other:?}"),
        }
        write_msg(
            &mut stream,
            &Message::NewProxyResp {
                proxy_name: "udp-response-batch".to_string(),
                remote_addr: "127.0.0.1:0".to_string(),
                error: String::new(),
            },
        )
        .await
        .unwrap();
        write_msg(
            &mut stream,
            &Message::UdpPacket {
                proxy_name: "udp-response-batch".to_string(),
                content: b"start".to_vec(),
                visitor_addr: "127.0.0.1:54321".to_string(),
            },
        )
        .await
        .unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let msg = tokio::time::timeout(Duration::from_secs(1), read_msg(&mut stream))
                .await
                .unwrap()
                .unwrap();
            match msg {
                Message::UdpPacketBatch { packets } => return packets,
                Message::UdpPacket { .. } => {}
                other => panic!("unexpected control message: {other:?}"),
            }
        }
        panic!("client did not send UDP response batch");
    });

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig { token: None },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "udp-response-batch".to_string(),
            proxy_type: ProxyType::Udp,
            local_ip: "127.0.0.1".to_string(),
            local_port: udp_service_port,
            remote_port: 0,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));

    let packets = server_task.await.unwrap();
    assert!(packets.len() >= 2, "{packets:?}");
    assert!(packets
        .iter()
        .all(|packet| packet.proxy_name == "udp-response-batch"));
    assert!(packets
        .iter()
        .all(|packet| packet.visitor_addr == "127.0.0.1:54321"));

    client_task.abort();
    service_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_udp_batch_reuses_local_session() {
    let _guard = acquire_test_lock().await;
    let udp_service = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_service_port = udp_service.local_addr().unwrap().port();
    let (service_tx, service_rx) = oneshot::channel();
    let service_task = tokio::spawn(async move {
        let mut got = Vec::new();
        let mut buf = [0_u8; 1024];
        for _ in 0..3 {
            let (n, _) = udp_service.recv_from(&mut buf).await.unwrap();
            got.push(String::from_utf8_lossy(&buf[..n]).to_string());
        }
        let _ = service_tx.send(got);
    });

    let plugin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plugin_port = plugin.local_addr().unwrap().port();
    let (plugin_tx, plugin_rx) = oneshot::channel();
    let plugin_task = tokio::spawn(async move {
        let (mut stream, _) = plugin.accept().await.unwrap();
        let mut buf = vec![0_u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let _ = plugin_tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });

    let control = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bind_port = control.local_addr().unwrap().port();
    let server_task = tokio::spawn(async move {
        let (mut stream, _) = control.accept().await.unwrap();
        match read_msg(&mut stream).await.unwrap() {
            Message::Login { .. } => {}
            other => panic!("unexpected login message: {other:?}"),
        }
        write_msg(
            &mut stream,
            &Message::LoginResp {
                version: "test".to_string(),
                run_id: "run-client-udp-batch".to_string(),
                error: String::new(),
            },
        )
        .await
        .unwrap();
        match read_msg(&mut stream).await.unwrap() {
            Message::NewProxy { proxy_name, .. } if proxy_name == "udp-client-batch" => {}
            other => panic!("unexpected new proxy message: {other:?}"),
        }
        write_msg(
            &mut stream,
            &Message::NewProxyResp {
                proxy_name: "udp-client-batch".to_string(),
                remote_addr: "127.0.0.1:0".to_string(),
                error: String::new(),
            },
        )
        .await
        .unwrap();
        write_msg(
            &mut stream,
            &Message::UdpPacketBatch {
                packets: vec![
                    UdpPacketFrame {
                        proxy_name: "udp-client-batch".to_string(),
                        content: b"one".to_vec(),
                        visitor_addr: "127.0.0.1:54321".to_string(),
                    },
                    UdpPacketFrame {
                        proxy_name: "udp-client-batch".to_string(),
                        content: b"two".to_vec(),
                        visitor_addr: "127.0.0.1:54321".to_string(),
                    },
                    UdpPacketFrame {
                        proxy_name: "udp-client-batch".to_string(),
                        content: b"three".to_vec(),
                        visitor_addr: "127.0.0.1:54321".to_string(),
                    },
                ],
            },
        )
        .await
        .unwrap();
        sleep(Duration::from_secs(5)).await;
    });

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig { token: None },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: ClientPluginConfig {
            local_connect_url: Some(format!("http://127.0.0.1:{plugin_port}/local_connect")),
        },
        proxies: vec![ProxyConfig {
            name: "udp-client-batch".to_string(),
            proxy_type: ProxyType::Udp,
            local_ip: "127.0.0.1".to_string(),
            local_port: udp_service_port,
            remote_port: 0,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));

    let got = tokio::time::timeout(Duration::from_secs(5), service_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, vec!["one", "two", "three"]);
    let plugin_request = tokio::time::timeout(Duration::from_secs(5), plugin_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(plugin_request.contains("POST /local_connect HTTP/1.1"));
    assert!(plugin_request.contains("\"proxy_name\":\"udp-client-batch\""));

    client_task.abort();
    server_task.abort();
    service_task.abort();
    plugin_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_udp_batch_reuses_visitor_destination() {
    let _guard = acquire_test_lock().await;
    let bind_port = unused_port();
    let remote_port = unused_udp_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let mut control = connect_with_retry(format!("127.0.0.1:{bind_port}")).await;
    write_msg(
        &mut control,
        &Message::Login {
            version: "test".to_string(),
            hostname: "test".to_string(),
            os: "test".to_string(),
            arch: "test".to_string(),
            run_id: String::new(),
            token: Some("secret".to_string()),
            pool_count: 0,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control).await.unwrap() {
        Message::LoginResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected login response: {other:?}"),
    }
    write_msg(
        &mut control,
        &Message::NewProxy {
            proxy_name: "udp-server-batch".to_string(),
            proxy_type: "udp".to_string(),
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
        },
    )
    .await
    .unwrap();
    match read_msg(&mut control).await.unwrap() {
        Message::NewProxyResp { error, .. } if error.is_empty() => {}
        other => panic!("unexpected proxy response: {other:?}"),
    }

    let visitor = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let visitor_addr = visitor.local_addr().unwrap().to_string();
    write_msg(
        &mut control,
        &Message::UdpPacketBatch {
            packets: vec![
                UdpPacketFrame {
                    proxy_name: "udp-server-batch".to_string(),
                    content: b"alpha".to_vec(),
                    visitor_addr: visitor_addr.clone(),
                },
                UdpPacketFrame {
                    proxy_name: "udp-server-batch".to_string(),
                    content: b"beta".to_vec(),
                    visitor_addr,
                },
            ],
        },
    )
    .await
    .unwrap();

    let mut got = Vec::new();
    let mut buf = [0_u8; 1024];
    for _ in 0..2 {
        let (n, _) =
            recv_udp_ignoring_connection_reset(&visitor, &mut buf, Duration::from_secs(5)).await;
        got.push(String::from_utf8_lossy(&buf[..n]).to_string());
    }
    assert_eq!(got, vec!["alpha", "beta"]);

    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sudp_visitor_forwards_packets_directly_when_peer_reachable() {
    let _guard = acquire_test_lock().await;
    let udp_echo = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_echo_port = udp_echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        let mut buf = [0_u8; 1024];
        loop {
            let (n, peer) = udp_echo.recv_from(&mut buf).await.unwrap();
            udp_echo.send_to(&buf[..n], peer).await.unwrap();
        }
    });

    let bind_port = unused_port();
    let visitor_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let owner_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "private-udp".to_string(),
            proxy_type: ProxyType::Sudp,
            local_ip: "127.0.0.1".to_string(),
            local_port: udp_echo_port,
            remote_port: 0,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: Some("private".to_string()),
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let owner_task = tokio::spawn(client::run(owner_cfg));
    sleep(Duration::from_millis(200)).await;

    let visitor_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: Vec::new(),
        visitors: vec![VisitorConfig {
            name: "local-private-udp".to_string(),
            visitor_type: ProxyType::Sudp,
            server_name: "private-udp".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: visitor_port,
            sk: Some("private".to_string()),
        }],
    };
    let visitor_task = tokio::spawn(client::run(visitor_cfg));
    sleep(Duration::from_millis(300)).await;

    let user = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    user.send_to(b"hello sudp", format!("127.0.0.1:{visitor_port}"))
        .await
        .unwrap();
    let mut buf = [0_u8; 1024];
    let (n, _) = recv_udp_ignoring_connection_reset(&user, &mut buf, Duration::from_secs(5)).await;
    assert_eq!(&buf[..n], b"hello sudp");
    sleep(Duration::from_millis(400)).await;

    let metrics = http_request(
        format!("127.0.0.1:{dashboard_port}"),
        b"GET /api/metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(metrics.contains("\"bytes_up_total\":0"));
    assert!(metrics.contains("\"bytes_down_total\":0"));

    visitor_task.abort();
    owner_task.abort();
    server_task.abort();
    echo_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sudp_group_visitor_forwards_packets_directly_when_peer_reachable() {
    let _guard = acquire_test_lock().await;
    let udp_echo = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_echo_port = udp_echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        let mut buf = [0_u8; 1024];
        loop {
            let (n, peer) = udp_echo.recv_from(&mut buf).await.unwrap();
            udp_echo.send_to(&buf[..n], peer).await.unwrap();
        }
    });

    let bind_port = unused_port();
    let visitor_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let owner_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "private-udp-a".to_string(),
            proxy_type: ProxyType::Sudp,
            local_ip: "127.0.0.1".to_string(),
            local_port: udp_echo_port,
            remote_port: 0,
            group: Some("private-udp-group".to_string()),
            group_key: Some("group-secret".to_string()),
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: Some("private".to_string()),
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let owner_task = tokio::spawn(client::run(owner_cfg));
    sleep(Duration::from_millis(200)).await;

    let visitor_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: Vec::new(),
        visitors: vec![VisitorConfig {
            name: "local-private-udp-group".to_string(),
            visitor_type: ProxyType::Sudp,
            server_name: "private-udp-group".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: visitor_port,
            sk: Some("private".to_string()),
        }],
    };
    let visitor_task = tokio::spawn(client::run(visitor_cfg));
    sleep(Duration::from_millis(300)).await;

    let user = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    user.send_to(b"hello sudp group", format!("127.0.0.1:{visitor_port}"))
        .await
        .unwrap();
    let mut buf = [0_u8; 1024];
    let (n, _) = recv_udp_ignoring_connection_reset(&user, &mut buf, Duration::from_secs(5)).await;
    assert_eq!(&buf[..n], b"hello sudp group");
    sleep(Duration::from_millis(400)).await;

    let metrics = http_request(
        format!("127.0.0.1:{dashboard_port}"),
        b"GET /api/metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(metrics.contains("\"bytes_up_total\":0"));
    assert!(metrics.contains("\"bytes_down_total\":0"));

    visitor_task.abort();
    owner_task.abort();
    server_task.abort();
    echo_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sudp_visitor_waits_for_delayed_owner_nat_notification() {
    let _guard = acquire_test_lock().await;
    let udp_echo = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_echo_port = udp_echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        let mut buf = [0_u8; 1024];
        loop {
            let (n, peer) = udp_echo.recv_from(&mut buf).await.unwrap();
            udp_echo.send_to(&buf[..n], peer).await.unwrap();
        }
    });

    let bind_port = unused_port();
    let visitor_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let visitor_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: Vec::new(),
        visitors: vec![VisitorConfig {
            name: "local-delayed-sudp".to_string(),
            visitor_type: ProxyType::Sudp,
            server_name: "delayed-sudp".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: visitor_port,
            sk: Some("private".to_string()),
        }],
    };
    let visitor_task = tokio::spawn(client::run(visitor_cfg));
    sleep(Duration::from_millis(150)).await;

    let user = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    user.send_to(b"hello delayed sudp", format!("127.0.0.1:{visitor_port}"))
        .await
        .unwrap();

    sleep(Duration::from_millis(120)).await;
    let owner_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "delayed-sudp".to_string(),
            proxy_type: ProxyType::Sudp,
            local_ip: "127.0.0.1".to_string(),
            local_port: udp_echo_port,
            remote_port: 0,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: Some("private".to_string()),
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let owner_task = tokio::spawn(client::run(owner_cfg));

    let mut buf = [0_u8; 1024];
    let (n, _) = recv_udp_ignoring_connection_reset(&user, &mut buf, Duration::from_secs(5)).await;
    assert_eq!(&buf[..n], b"hello delayed sudp");
    sleep(Duration::from_millis(400)).await;

    let metrics = http_request(
        format!("127.0.0.1:{dashboard_port}"),
        b"GET /api/metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(metrics.contains("\"bytes_up_total\":0"));
    assert!(metrics.contains("\"bytes_down_total\":0"));

    owner_task.abort();
    visitor_task.abort();
    server_task.abort();
    echo_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_check_closes_unhealthy_tcp_proxy() {
    let _guard = acquire_test_lock().await;
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = echo.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    stream.write_all(&buf[..n]).await.unwrap();
                }
            });
        }
    });

    let bind_port = unused_port();
    let remote_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "checked-echo".to_string(),
            proxy_type: ProxyType::Tcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: Some(HealthCheckConfig {
                check_type: HealthCheckType::Tcp,
                interval_seconds: 1,
                timeout_seconds: 1,
                max_failed: 1,
            }),
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));

    let mut stream = connect_with_retry(format!("127.0.0.1:{remote_port}")).await;
    stream.write_all(b"healthy").await.unwrap();
    let mut got = vec![0_u8; "healthy".len()];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(got, b"healthy");

    echo_task.abort();
    wait_until_connect_fails(format!("127.0.0.1:{remote_port}")).await;

    client_task.abort();
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_health_check_registers_proxy_after_healthy_response() {
    let _guard = acquire_test_lock().await;
    let local_http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_http_port = local_http.local_addr().unwrap().port();
    let http_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = local_http.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                let _ = stream.read(&mut buf).await.unwrap();
                stream
                    .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                    .await
                    .unwrap();
            });
        }
    });

    let bind_port = unused_port();
    let remote_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "checked-http".to_string(),
            proxy_type: ProxyType::Tcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: local_http_port,
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: Some(HealthCheckConfig {
                check_type: HealthCheckType::Http,
                interval_seconds: 1,
                timeout_seconds: 1,
                max_failed: 1,
            }),
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));

    let mut stream = connect_with_retry(format!("127.0.0.1:{remote_port}")).await;
    stream
        .write_all(b"GET /through-proxy HTTP/1.1\r\nHost: local\r\n\r\n")
        .await
        .unwrap();
    let mut response = vec![0_u8; 128];
    let n = stream.read(&mut response).await.unwrap();
    assert!(String::from_utf8_lossy(&response[..n]).contains("204 No Content"));

    let dashboard_addr = format!("127.0.0.1:{dashboard_port}");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let proxies = http_request(
            dashboard_addr.clone(),
            b"GET /api/proxies HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
        if proxies.contains("\"name\":\"checked-http\"")
            && proxies.contains("\"health\"")
            && proxies.contains("\"healthy\":true")
            && proxies.contains("\"type\":\"http\"")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "dashboard did not report health status"
        );
        sleep(Duration::from_millis(50)).await;
    }

    client_task.abort();
    server_task.abort();
    http_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dashboard_returns_status_json() {
    let _guard = acquire_test_lock().await;
    let bind_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let mut stream = connect_with_retry(format!("127.0.0.1:{dashboard_port}")).await;
    stream
        .write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut buf = vec![0_u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(response.contains("200 OK"));
    assert!(response.contains("\"version\""));
    assert!(response.contains("\"proxies\""));

    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dashboard_api_lists_metrics_and_closes_proxy() {
    let _guard = acquire_test_lock().await;
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = echo.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    stream.write_all(&buf[..n]).await.unwrap();
                }
            });
        }
    });

    let bind_port = unused_port();
    let remote_port = unused_port();
    let dashboard_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port,
        allow_ports: Vec::new(),
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        plugins: ServerPluginConfig::default(),
        transport: TransportConfig::default(),
    };
    let server_task = tokio::spawn(server::run(server_cfg));
    sleep(Duration::from_millis(100)).await;

    let client_cfg = ClientConfig {
        server_addr: "127.0.0.1".to_string(),
        server_port: bind_port,
        auth: AuthConfig {
            token: Some("secret".to_string()),
        },
        pool_count: 0,
        transport: TransportConfig::default(),
        plugins: Default::default(),
        proxies: vec![ProxyConfig {
            name: "admin-echo".to_string(),
            proxy_type: ProxyType::Tcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port,
            group: None,
            group_key: None,
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        }],
        visitors: Vec::new(),
    };
    let client_task = tokio::spawn(client::run(client_cfg));

    let mut visitor = connect_with_retry(format!("127.0.0.1:{remote_port}")).await;
    visitor.write_all(b"count-me").await.unwrap();
    let mut got = vec![0_u8; "count-me".len()];
    visitor.read_exact(&mut got).await.unwrap();

    let dashboard_addr = format!("127.0.0.1:{dashboard_port}");
    let proxies = http_request(
        dashboard_addr.clone(),
        b"GET /api/proxies HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(proxies.contains("200 OK"));
    assert!(proxies.contains("admin-echo"));
    assert!(proxies.contains("started_unix_secs"));

    let clients = http_request(
        dashboard_addr.clone(),
        b"GET /api/clients HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(clients.contains("200 OK"));
    assert!(clients.contains("work_pool_queued"));
    assert!(clients.contains("work_pool_replenishing"));

    let metrics = http_request(
        dashboard_addr.clone(),
        b"GET /api/metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(metrics.contains("200 OK"));
    assert!(metrics.contains("bytes_up_total"));

    let reset_metrics = http_request(
        dashboard_addr.clone(),
        b"POST /api/metrics/reset HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer secret\r\n\r\n",
    )
    .await;
    assert!(reset_metrics.contains("200 OK"));
    assert!(reset_metrics.contains("\"visitor_connections_total\":0"));
    assert!(reset_metrics.contains("\"bytes_up_total\":0"));
    assert!(reset_metrics.contains("\"bytes_down_total\":0"));

    let unauthorized = http_request(
        dashboard_addr.clone(),
        b"POST /api/proxies/admin-echo/close HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(unauthorized.contains("401 Unauthorized"));

    let closed = http_request(
        dashboard_addr,
        b"POST /api/proxies/admin-echo/close HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer secret\r\n\r\n",
    )
    .await;
    assert!(closed.contains("200 OK"));
    assert!(closed.contains("\"closed\":true"));
    wait_until_connect_fails(format!("127.0.0.1:{remote_port}")).await;

    client_task.abort();
    server_task.abort();
    echo_task.abort();
}
