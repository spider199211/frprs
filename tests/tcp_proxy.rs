use frprs::{
    client,
    config::{
        AuthConfig, ClientConfig, HealthCheckConfig, HealthCheckType, ProxyConfig, ProxyType,
        ServerConfig, ServerPluginConfig, TransportConfig, TransportProtocol, VisitorConfig,
    },
    server,
};
use std::{net::TcpListener as StdTcpListener, sync::OnceLock};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{Mutex, MutexGuard},
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

async fn connect_with_retry(addr: String) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(5);
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
    let transport = TransportConfig { protocol };
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
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
        proxies: vec![ProxyConfig {
            name: "echo".to_string(),
            proxy_type: ProxyType::Tcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port,
            custom_domains: Vec::new(),
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
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
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
        proxies: vec![ProxyConfig {
            name: "echo".to_string(),
            proxy_type: ProxyType::Tcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port,
            custom_domains: Vec::new(),
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
async fn tcp_proxy_forwards_bytes_over_kcp_transport() {
    assert_tcp_proxy_forwards_with_transport(TransportProtocol::Kcp).await;
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
        proxies: vec![ProxyConfig {
            name: "private-echo".to_string(),
            proxy_type: ProxyType::Stcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port: 0,
            custom_domains: Vec::new(),
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
        proxies: vec![ProxyConfig {
            name: "web".to_string(),
            proxy_type: ProxyType::Http,
            local_ip: "127.0.0.1".to_string(),
            local_port: local_http_port,
            remote_port: 0,
            custom_domains: vec!["www.example.com".to_string()],
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
        proxies: vec![ProxyConfig {
            name: "secure-web".to_string(),
            proxy_type: ProxyType::Https,
            local_ip: "127.0.0.1".to_string(),
            local_port: local_tls_port,
            remote_port: 0,
            custom_domains: vec!["secure.example.com".to_string()],
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
    let remote_port = unused_port();
    let server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".to_string(),
        bind_port,
        proxy_bind_addr: "127.0.0.1".to_string(),
        vhost_http_port: 0,
        vhost_https_port: 0,
        dashboard_addr: "127.0.0.1".to_string(),
        dashboard_port: 0,
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
        proxies: vec![ProxyConfig {
            name: "udp-echo".to_string(),
            proxy_type: ProxyType::Udp,
            local_ip: "127.0.0.1".to_string(),
            local_port: udp_echo_port,
            remote_port,
            custom_domains: Vec::new(),
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
    let (n, _) = tokio::time::timeout(Duration::from_secs(5), visitor.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], b"hello udp");

    client_task.abort();
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
        proxies: vec![ProxyConfig {
            name: "checked-echo".to_string(),
            proxy_type: ProxyType::Tcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: echo_port,
            remote_port,
            custom_domains: Vec::new(),
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
