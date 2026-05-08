use crate::{
    config::{
        ClientConfig, HealthCheckType, ProxyConfig, ProxyType, TransportProtocol, VisitorConfig,
    },
    protocol::{read_msg, write_msg, Message},
    transports::{self, BoxStream},
};
use anyhow::{anyhow, bail, Context, Result};
use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    io::{self, WriteHalf},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::Mutex,
    time,
};
use tracing::{debug, error, info, warn};

#[derive(Clone)]
struct ClientState {
    cfg: Arc<ClientConfig>,
    run_id: String,
    proxies: Arc<HashMap<String, ProxyConfig>>,
    writer: Arc<Mutex<WriteHalf<BoxStream>>>,
}

impl ClientState {
    async fn send(&self, msg: &Message) -> Result<()> {
        let mut writer = self.writer.lock().await;
        write_msg(&mut *writer, msg).await
    }
}

pub async fn run(cfg: ClientConfig) -> Result<()> {
    let cfg = Arc::new(cfg);
    loop {
        match run_session(cfg.clone()).await {
            Ok(()) => warn!("frpc session ended; reconnecting soon"),
            Err(err) => warn!("frpc session failed: {err:#}; reconnecting soon"),
        }
        time::sleep(Duration::from_secs(2)).await;
    }
}

pub async fn run_from_file(path: PathBuf) -> Result<()> {
    let mut last_modified = config_modified(&path)?;

    loop {
        let cfg = Arc::new(ClientConfig::load(&path)?);
        let session = tokio::spawn(run_session(cfg));

        loop {
            time::sleep(Duration::from_secs(2)).await;
            let modified = config_modified(&path)?;
            if modified > last_modified {
                info!("config changed, reloading frpc session");
                last_modified = modified;
                session.abort();
                let _ = session.await;
                break;
            }

            if session.is_finished() {
                match session.await {
                    Ok(Ok(())) => warn!("frpc session ended; reconnecting soon"),
                    Ok(Err(err)) => warn!("frpc session failed: {err:#}; reconnecting soon"),
                    Err(err) if err.is_cancelled() => {}
                    Err(err) => warn!("frpc session task failed: {err:#}; reconnecting soon"),
                }
                break;
            }
        }

        time::sleep(Duration::from_secs(2)).await;
    }
}

async fn run_session(cfg: Arc<ClientConfig>) -> Result<()> {
    if cfg.proxies.is_empty() && cfg.visitors.is_empty() {
        bail!("no proxies or visitors configured");
    }

    let server_addr = cfg.server_addr();
    let mut stream = connect_control(&cfg)
        .await
        .with_context(|| format!("connect frps at {server_addr}"))?;
    info!("connected to frps at {server_addr}");

    let login = Message::Login {
        version: env!("CARGO_PKG_VERSION").to_string(),
        hostname: env::var("COMPUTERNAME")
            .or_else(|_| env::var("HOSTNAME"))
            .unwrap_or_default(),
        os: env::consts::OS.to_string(),
        arch: env::consts::ARCH.to_string(),
        run_id: String::new(),
        token: cfg.auth.token.clone(),
        pool_count: cfg.pool_count,
    };
    write_msg(&mut stream, &login).await?;

    let run_id = match read_msg(&mut stream).await? {
        Message::LoginResp { run_id, error, .. } if error.is_empty() => run_id,
        Message::LoginResp { error, .. } => bail!("login rejected by server: {error}"),
        other => bail!("unexpected login response: {other:?}"),
    };
    info!("login accepted run_id={run_id}");

    let (mut reader, writer) = tokio::io::split(stream);
    let proxies = Arc::new(
        cfg.proxies
            .iter()
            .cloned()
            .map(|proxy| (proxy.name.clone(), proxy))
            .collect::<HashMap<_, _>>(),
    );
    let state = ClientState {
        cfg,
        run_id,
        proxies,
        writer: Arc::new(Mutex::new(writer)),
    };

    for proxy in state.proxies.values().cloned() {
        if proxy.health_check.is_some() {
            let health_state = state.clone();
            tokio::spawn(async move {
                if let Err(err) = run_health_check(health_state, proxy).await {
                    debug!("health check loop stopped: {err:#}");
                }
            });
        } else {
            register_proxy(&state, &proxy).await?;
        }
    }

    for visitor in state.cfg.visitors.iter().cloned() {
        let visitor_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = run_visitor_listener(visitor_state, visitor).await {
                warn!("visitor listener stopped: {err:#}");
            }
        });
    }

    for _ in 0..state.cfg.pool_count {
        let pool_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = open_work_conn(pool_state).await {
                debug!("pooled work connection ended: {err:#}");
            }
        });
    }

    let ping_state = state.clone();
    tokio::spawn(async move {
        loop {
            time::sleep(Duration::from_secs(30)).await;
            if let Err(err) = ping_state
                .send(&Message::Ping {
                    token: ping_state.cfg.auth.token.clone(),
                })
                .await
            {
                debug!("stop ping loop: {err:#}");
                break;
            }
        }
    });

    loop {
        match read_msg(&mut reader).await? {
            Message::NewProxyResp {
                proxy_name,
                remote_addr,
                error,
            } if error.is_empty() => {
                info!("proxy {proxy_name} started on {remote_addr}");
            }
            Message::NewProxyResp {
                proxy_name, error, ..
            } => {
                error!("proxy {proxy_name} failed: {error}");
            }
            Message::ReqWorkConn => {
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(err) = open_work_conn(state).await {
                        warn!("work connection failed: {err:#}");
                    }
                });
            }
            Message::UdpPacket {
                proxy_name,
                content,
                visitor_addr,
            } => {
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(err) =
                        handle_udp_packet(state, proxy_name, content, visitor_addr).await
                    {
                        warn!("udp packet handling failed: {err:#}");
                    }
                });
            }
            Message::Pong { error } if error.is_empty() => {
                debug!("server pong");
            }
            Message::Pong { error } => {
                bail!("server rejected ping: {error}");
            }
            other => {
                debug!("ignored server message: {other:?}");
            }
        }
    }
}

async fn register_proxy(state: &ClientState, proxy: &ProxyConfig) -> Result<()> {
    state
        .send(&Message::NewProxy {
            proxy_name: proxy.name.clone(),
            proxy_type: proxy.proxy_type.as_str().to_string(),
            remote_port: proxy.remote_port,
            custom_domains: proxy.custom_domains.clone(),
            bandwidth_limit: proxy.bandwidth_limit.clone(),
            sk: proxy.sk.clone(),
        })
        .await
}

async fn connect_control(cfg: &ClientConfig) -> Result<BoxStream> {
    match cfg.transport.protocol {
        TransportProtocol::Tcp => {
            let server_addr = cfg.server_addr();
            let stream = TcpStream::connect(&server_addr)
                .await
                .with_context(|| format!("connect tcp frps at {server_addr}"))?;
            Ok(Box::new(stream))
        }
        TransportProtocol::Kcp => {
            let server_addr = resolve_server_addr(cfg).await?;
            let stream = transports::kcp::connect(server_addr).await?;
            Ok(Box::new(stream))
        }
        TransportProtocol::Quic => {
            let server_addr = resolve_server_addr(cfg).await?;
            let stream = transports::quic::connect_stream_insecure(server_addr).await?;
            Ok(Box::new(stream))
        }
    }
}

async fn resolve_server_addr(cfg: &ClientConfig) -> Result<std::net::SocketAddr> {
    let server_addr = cfg.server_addr();
    let mut addrs = tokio::net::lookup_host(&server_addr)
        .await
        .with_context(|| format!("resolve server address {server_addr}"))?;
    addrs
        .next()
        .ok_or_else(|| anyhow!("server address {server_addr} did not resolve"))
}

async fn close_proxy(state: &ClientState, proxy_name: &str) -> Result<()> {
    state
        .send(&Message::CloseProxy {
            proxy_name: proxy_name.to_string(),
        })
        .await
}

async fn run_visitor_listener(state: ClientState, visitor: VisitorConfig) -> Result<()> {
    match visitor.visitor_type {
        ProxyType::Stcp => {}
        ProxyType::Sudp | ProxyType::Xtcp => {
            bail!(
                "visitor {} type {} is recognized but not implemented yet",
                visitor.name,
                visitor.visitor_type.as_str()
            );
        }
        other => {
            bail!(
                "visitor {} uses invalid visitor type {}",
                visitor.name,
                other.as_str()
            );
        }
    }

    let bind_addr = format!("{}:{}", visitor.bind_addr, visitor.bind_port);
    let listener = TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("listen visitor {} on {bind_addr}", visitor.name))?;
    let local_addr = listener
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or(bind_addr);
    info!(
        "visitor {} listening on {} for serverName={}",
        visitor.name, local_addr, visitor.server_name
    );

    loop {
        let (inbound, peer) = listener
            .accept()
            .await
            .with_context(|| format!("accept visitor {}", visitor.name))?;
        let state = state.clone();
        let visitor = visitor.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_stcp_visitor(state, visitor, inbound).await {
                warn!("visitor connection {peer} failed: {err:#}");
            }
        });
    }
}

async fn handle_stcp_visitor(
    state: ClientState,
    visitor: VisitorConfig,
    mut inbound: TcpStream,
) -> Result<()> {
    let server_addr = state.cfg.server_addr();
    let mut stream = connect_control(&state.cfg)
        .await
        .with_context(|| format!("connect frps visitor control at {server_addr}"))?;
    write_msg(
        &mut stream,
        &Message::NewVisitorConn {
            proxy_name: visitor.server_name.clone(),
            sk: visitor.sk.clone(),
            token: state.cfg.auth.token.clone(),
        },
    )
    .await?;

    match read_msg(&mut stream).await? {
        Message::NewVisitorConnResp { error, .. } if error.is_empty() => {}
        Message::NewVisitorConnResp { error, .. } => {
            bail!("server rejected visitor {}: {error}", visitor.name);
        }
        other => bail!("unexpected visitor response: {other:?}"),
    }

    io::copy_bidirectional(&mut inbound, &mut stream)
        .await
        .with_context(|| format!("copy stcp visitor bytes for {}", visitor.name))?;
    Ok(())
}

fn config_modified(path: &Path) -> Result<SystemTime> {
    fs::metadata(path)
        .with_context(|| format!("read metadata for config {}", path.display()))?
        .modified()
        .with_context(|| format!("read modified time for config {}", path.display()))
}

async fn run_health_check(state: ClientState, proxy: ProxyConfig) -> Result<()> {
    let health = proxy
        .health_check
        .clone()
        .ok_or_else(|| anyhow!("missing health check config"))?;
    let mut active = false;
    let mut failed = 0_u32;

    loop {
        let healthy = check_proxy_health(&proxy, health.check_type, health.timeout_seconds).await;

        if healthy {
            failed = 0;
            if !active {
                info!("health check passed for {}, registering proxy", proxy.name);
                register_proxy(&state, &proxy).await?;
                active = true;
            }
        } else {
            failed = failed.saturating_add(1);
            warn!(
                "health check failed for {} ({failed}/{})",
                proxy.name, health.max_failed
            );
            if active && failed >= health.max_failed {
                warn!("health check closing unhealthy proxy {}", proxy.name);
                close_proxy(&state, &proxy.name).await?;
                active = false;
            }
        }

        time::sleep(Duration::from_secs(health.interval_seconds.max(1))).await;
    }
}

async fn check_proxy_health(
    proxy: &ProxyConfig,
    check_type: HealthCheckType,
    timeout_seconds: u64,
) -> bool {
    match check_type {
        HealthCheckType::Tcp => {
            let addr = format!("{}:{}", proxy.local_ip, proxy.local_port);
            matches!(
                time::timeout(
                    Duration::from_secs(timeout_seconds.max(1)),
                    TcpStream::connect(addr)
                )
                .await,
                Ok(Ok(_))
            )
        }
    }
}

async fn open_work_conn(state: ClientState) -> Result<()> {
    let server_addr = state.cfg.server_addr();
    let mut stream = connect_control(&state.cfg)
        .await
        .with_context(|| format!("open work connection to {server_addr}"))?;
    write_msg(
        &mut stream,
        &Message::NewWorkConn {
            run_id: state.run_id.clone(),
            token: state.cfg.auth.token.clone(),
        },
    )
    .await?;

    let start = read_msg(&mut stream).await?;
    let proxy_name = match start {
        Message::StartWorkConn {
            proxy_name, error, ..
        } if error.is_empty() => proxy_name,
        Message::StartWorkConn { error, .. } => bail!("server rejected work connection: {error}"),
        other => bail!("unexpected work connection response: {other:?}"),
    };

    let proxy = state
        .proxies
        .get(&proxy_name)
        .ok_or_else(|| anyhow!("unknown proxy {proxy_name}"))?;
    let local_addr = format!("{}:{}", proxy.local_ip, proxy.local_port);
    let mut local = TcpStream::connect(&local_addr)
        .await
        .with_context(|| format!("connect local service for proxy {proxy_name} at {local_addr}"))?;

    io::copy_bidirectional(&mut stream, &mut local)
        .await
        .with_context(|| format!("copy bytes for proxy {proxy_name}"))?;
    Ok(())
}

async fn handle_udp_packet(
    state: ClientState,
    proxy_name: String,
    content: Vec<u8>,
    visitor_addr: String,
) -> Result<()> {
    let proxy = state
        .proxies
        .get(&proxy_name)
        .ok_or_else(|| anyhow!("unknown udp proxy {proxy_name}"))?;
    let local_addr = format!("{}:{}", proxy.local_ip, proxy.local_port);
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .context("bind local udp relay socket")?;
    socket
        .send_to(&content, &local_addr)
        .await
        .with_context(|| format!("send udp packet to local service {local_addr}"))?;

    let mut buf = vec![0_u8; 64 * 1024];
    let (n, _) = time::timeout(Duration::from_secs(5), socket.recv_from(&mut buf))
        .await
        .context("wait local udp response timed out")?
        .context("read local udp response")?;

    state
        .send(&Message::UdpPacket {
            proxy_name,
            content: buf[..n].to_vec(),
            visitor_addr,
        })
        .await
}
