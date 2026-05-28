use crate::{
    config::{
        ClientConfig, HealthCheckType, ProxyConfig, ProxyType, TransportProtocol, VisitorConfig,
    },
    protocol::{read_msg, write_msg, Message, UdpPacketFrame},
    transports::{self, BoxStream},
};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::json;
use std::{
    collections::HashMap,
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, WriteHalf},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{oneshot, Mutex},
    time,
};
use tracing::{debug, error, info, warn};

#[derive(Clone)]
struct ClientState {
    cfg: Arc<ClientConfig>,
    run_id: String,
    proxies: Arc<HashMap<String, ProxyConfig>>,
    udp_sessions: Arc<Mutex<HashMap<(String, String), Arc<UdpSocket>>>>,
    sudp_visitor_sessions: Arc<Mutex<HashMap<String, SudpVisitorSession>>>,
    nat_hole_waiters: Arc<Mutex<HashMap<String, oneshot::Sender<NatHoleResponse>>>>,
    writer: Arc<Mutex<WriteHalf<BoxStream>>>,
}

#[derive(Clone)]
struct SudpVisitorSession {
    socket: Arc<UdpSocket>,
    peer: SocketAddr,
}

#[derive(Debug)]
struct NatHoleResponse {
    peer_observed_addr: String,
    peer_local_addrs: Vec<String>,
    waiting: bool,
    error: String,
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
        udp_sessions: Arc::new(Mutex::new(HashMap::new())),
        sudp_visitor_sessions: Arc::new(Mutex::new(HashMap::new())),
        nat_hole_waiters: Arc::new(Mutex::new(HashMap::new())),
        writer: Arc::new(Mutex::new(writer)),
    };

    for proxy in state.proxies.values().cloned() {
        if proxy.proxy_type == ProxyType::Xtcp {
            start_xtcp_direct_owner_listener(state.clone(), proxy.clone()).await?;
        }
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

    spawn_work_conns(state.clone(), state.cfg.pool_count, "pooled");

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
            Message::ReqWorkConn { count } => {
                spawn_work_conns(state.clone(), count, "requested");
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
            Message::UdpPacketBatch { packets } => {
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_udp_packet_batch(state, packets).await {
                        warn!("udp packet batch handling failed: {err:#}");
                    }
                });
            }
            Message::SudpPacket {
                proxy_name,
                session_id,
                content,
                from_visitor,
                ..
            } => {
                let state = state.clone();
                tokio::spawn(async move {
                    let result = if from_visitor {
                        handle_sudp_owner_packet(state, proxy_name, session_id, content).await
                    } else {
                        handle_sudp_visitor_response(state, session_id, content).await
                    };
                    if let Err(err) = result {
                        warn!("sudp packet handling failed: {err:#}");
                    }
                });
            }
            Message::Pong { error } if error.is_empty() => {
                debug!("server pong");
            }
            Message::Pong { error } => {
                bail!("server rejected ping: {error}");
            }
            Message::NatHoleResp {
                transaction_id,
                role,
                observed_addr: _,
                peer_observed_addr,
                peer_local_addrs,
                waiting,
                error,
                ..
            } => {
                let key = nat_hole_waiter_key(&transaction_id, &role);
                if let Some(waiter) = state.nat_hole_waiters.lock().await.remove(&key) {
                    let _ = waiter.send(NatHoleResponse {
                        peer_observed_addr,
                        peer_local_addrs,
                        waiting,
                        error,
                    });
                } else {
                    debug!(
                        "ignored nat hole response for transaction {transaction_id} role {role}"
                    );
                }
            }
            other => {
                debug!("ignored server message: {other:?}");
            }
        }
    }
}

fn spawn_work_conns(state: ClientState, count: usize, label: &'static str) {
    for _ in 0..count {
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = open_work_conn(state).await {
                warn!("{label} work connection failed: {err:#}");
            }
        });
    }
}

async fn register_proxy(state: &ClientState, proxy: &ProxyConfig) -> Result<()> {
    state
        .send(&Message::NewProxy {
            proxy_name: proxy.name.clone(),
            proxy_type: proxy.proxy_type.as_str().to_string(),
            remote_port: proxy.remote_port,
            group: proxy.group.clone(),
            group_key: proxy.group_key.clone(),
            custom_domains: proxy.custom_domains.clone(),
            locations: proxy.locations.clone(),
            host_header_rewrite: proxy.host_header_rewrite.clone(),
            request_headers: proxy.request_headers.set.clone(),
            http_user: proxy.http_user.clone(),
            http_password: proxy.http_password.clone(),
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
            configure_tcp_stream(&stream);
            Ok(Box::new(stream))
        }
        TransportProtocol::Tls => {
            let server_addr = resolve_server_addr(cfg).await?;
            let stream = transports::tls::connect_insecure(server_addr).await?;
            Ok(Box::new(stream))
        }
        TransportProtocol::Websocket => {
            let server_addr = resolve_server_addr(cfg).await?;
            let stream = transports::websocket::connect(server_addr).await?;
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

fn nat_hole_waiter_key(transaction_id: &str, role: &str) -> String {
    format!("{transaction_id}|{role}")
}

async fn request_nat_hole(
    state: &ClientState,
    transaction_id: &str,
    proxy_name: &str,
    role: &str,
    local_addrs: Vec<String>,
) -> Result<NatHoleResponse> {
    let (tx, rx) = oneshot::channel();
    let key = nat_hole_waiter_key(transaction_id, role);
    state.nat_hole_waiters.lock().await.insert(key.clone(), tx);
    let send_result = state
        .send(&Message::NatHoleRegister {
            transaction_id: transaction_id.to_string(),
            proxy_name: proxy_name.to_string(),
            role: role.to_string(),
            local_addrs,
        })
        .await;
    if let Err(err) = send_result {
        state.nat_hole_waiters.lock().await.remove(&key);
        return Err(err);
    }

    let resp = match time::timeout(Duration::from_secs(5), rx).await {
        Ok(resp) => resp.context("nat hole response channel closed")?,
        Err(err) => {
            state.nat_hole_waiters.lock().await.remove(&key);
            return Err(err).context("wait for nat hole response timed out");
        }
    };
    if !resp.error.is_empty() {
        bail!("nat hole register failed: {}", resp.error);
    }
    Ok(resp)
}

async fn announce_nat_hole(
    state: &ClientState,
    transaction_id: &str,
    proxy_name: &str,
    role: &str,
    local_addrs: Vec<String>,
) -> Result<()> {
    state
        .send(&Message::NatHoleRegister {
            transaction_id: transaction_id.to_string(),
            proxy_name: proxy_name.to_string(),
            role: role.to_string(),
            local_addrs,
        })
        .await
}

async fn run_visitor_listener(state: ClientState, visitor: VisitorConfig) -> Result<()> {
    match visitor.visitor_type {
        ProxyType::Stcp | ProxyType::Xtcp => {}
        ProxyType::Sudp => {
            return run_sudp_visitor_listener(state, visitor).await;
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
        configure_tcp_stream(&inbound);
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
    if visitor.visitor_type == ProxyType::Xtcp
        && try_xtcp_direct_visitor(&state, &visitor, &mut inbound).await?
    {
        return Ok(());
    }

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

async fn start_xtcp_direct_owner_listener(state: ClientState, proxy: ProxyConfig) -> Result<()> {
    let bind_ip = proxy.local_ip.as_str();
    let listener = TcpListener::bind(format!("{bind_ip}:0"))
        .await
        .with_context(|| format!("listen xtcp direct owner {}", proxy.name))?;
    let local_addr = listener
        .local_addr()
        .context("read xtcp direct owner addr")?;
    let advertised_addr = local_addr.to_string();

    announce_nat_hole(
        &state,
        &proxy.name,
        &proxy.name,
        "server",
        vec![advertised_addr.clone()],
    )
    .await?;
    if let Some(group) = proxy.group.as_deref() {
        announce_nat_hole(
            &state,
            group,
            group,
            "server",
            vec![advertised_addr.clone()],
        )
        .await?;
    }

    info!(
        "xtcp direct owner {} listening on {}",
        proxy.name, advertised_addr
    );
    tokio::spawn(async move {
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(err) => {
                    debug!("xtcp direct owner {} accept failed: {err:#}", proxy.name);
                    break;
                }
            };
            configure_tcp_stream(&stream);
            let proxy = proxy.clone();
            tokio::spawn(async move {
                if let Err(err) = handle_xtcp_direct_owner_conn(proxy, stream).await {
                    debug!("xtcp direct owner connection {peer} failed: {err:#}");
                }
            });
        }
    });
    Ok(())
}

async fn handle_xtcp_direct_owner_conn(proxy: ProxyConfig, mut stream: TcpStream) -> Result<()> {
    let requested_proxy = match read_msg(&mut stream).await? {
        Message::NewVisitorConn { proxy_name, sk, .. } => {
            let allowed_name = proxy_name == proxy.name
                || proxy
                    .group
                    .as_deref()
                    .map(|group| group == proxy_name)
                    .unwrap_or(false);
            if !allowed_name {
                write_msg(
                    &mut stream,
                    &Message::NewVisitorConnResp {
                        proxy_name,
                        error: "xtcp proxy is not registered on direct peer".to_string(),
                    },
                )
                .await?;
                return Ok(());
            }
            if proxy.sk != sk {
                write_msg(
                    &mut stream,
                    &Message::NewVisitorConnResp {
                        proxy_name,
                        error: "xtcp secret key mismatch".to_string(),
                    },
                )
                .await?;
                return Ok(());
            }
            proxy_name
        }
        other => bail!("unexpected xtcp direct handshake: {other:?}"),
    };

    write_msg(
        &mut stream,
        &Message::NewVisitorConnResp {
            proxy_name: requested_proxy.clone(),
            error: String::new(),
        },
    )
    .await?;
    let local_addr = format!("{}:{}", proxy.local_ip, proxy.local_port);
    let mut local = TcpStream::connect(&local_addr)
        .await
        .with_context(|| format!("connect local xtcp service at {local_addr}"))?;
    configure_tcp_stream(&local);
    io::copy_bidirectional(&mut stream, &mut local)
        .await
        .with_context(|| format!("copy xtcp direct bytes for {requested_proxy}"))?;
    Ok(())
}

async fn try_xtcp_direct_visitor(
    state: &ClientState,
    visitor: &VisitorConfig,
    inbound: &mut TcpStream,
) -> Result<bool> {
    let resp = match request_nat_hole(
        state,
        &visitor.server_name,
        &visitor.server_name,
        "visitor",
        Vec::new(),
    )
    .await
    {
        Ok(resp) => resp,
        Err(err) => {
            debug!(
                "xtcp direct lookup for {} failed: {err:#}",
                visitor.server_name
            );
            return Ok(false);
        }
    };
    if resp.waiting {
        debug!("xtcp direct lookup for {} is waiting", visitor.server_name);
        return Ok(false);
    }

    let candidates = xtcp_direct_candidates(&resp);
    for candidate in candidates {
        let Ok(addr) = candidate.parse::<SocketAddr>() else {
            debug!("ignore invalid xtcp direct candidate {candidate}");
            continue;
        };
        let connect = time::timeout(Duration::from_secs(2), TcpStream::connect(addr)).await;
        let mut peer = match connect {
            Ok(Ok(stream)) => stream,
            Ok(Err(err)) => {
                debug!("xtcp direct connect {candidate} failed: {err:#}");
                continue;
            }
            Err(_) => {
                debug!("xtcp direct connect {candidate} timed out");
                continue;
            }
        };
        configure_tcp_stream(&peer);
        write_msg(
            &mut peer,
            &Message::NewVisitorConn {
                proxy_name: visitor.server_name.clone(),
                sk: visitor.sk.clone(),
                token: None,
            },
        )
        .await?;
        match read_msg(&mut peer).await? {
            Message::NewVisitorConnResp { error, .. } if error.is_empty() => {
                info!(
                    "xtcp visitor {} connected directly to {}",
                    visitor.name, candidate
                );
                io::copy_bidirectional(inbound, &mut peer)
                    .await
                    .with_context(|| {
                        format!("copy xtcp direct visitor bytes for {}", visitor.name)
                    })?;
                return Ok(true);
            }
            Message::NewVisitorConnResp { error, .. } => {
                debug!("xtcp direct peer {candidate} rejected visitor: {error}");
            }
            other => {
                debug!("unexpected xtcp direct response from {candidate}: {other:?}");
            }
        }
    }

    Ok(false)
}

fn xtcp_direct_candidates(resp: &NatHoleResponse) -> Vec<String> {
    let mut out = Vec::new();
    for addr in &resp.peer_local_addrs {
        if !out.contains(addr) {
            out.push(addr.clone());
        }
    }
    if !resp.peer_observed_addr.is_empty() && !out.contains(&resp.peer_observed_addr) {
        out.push(resp.peer_observed_addr.clone());
    }
    out
}

async fn run_sudp_visitor_listener(state: ClientState, visitor: VisitorConfig) -> Result<()> {
    let bind_addr = format!("{}:{}", visitor.bind_addr, visitor.bind_port);
    let socket = Arc::new(
        UdpSocket::bind(&bind_addr)
            .await
            .with_context(|| format!("listen sudp visitor {} on {bind_addr}", visitor.name))?,
    );
    let local_addr = socket
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or(bind_addr);
    info!(
        "sudp visitor {} listening on {} for serverName={}",
        visitor.name, local_addr, visitor.server_name
    );

    let mut buf = vec![0_u8; 64 * 1024];
    loop {
        let (n, peer) = socket
            .recv_from(&mut buf)
            .await
            .with_context(|| format!("recv sudp visitor {}", visitor.name))?;
        let session_id = format!("{}|{peer}", visitor.name);
        state.sudp_visitor_sessions.lock().await.insert(
            session_id.clone(),
            SudpVisitorSession {
                socket: socket.clone(),
                peer,
            },
        );
        state
            .send(&Message::SudpPacket {
                proxy_name: visitor.server_name.clone(),
                session_id,
                content: buf[..n].to_vec(),
                sk: visitor.sk.clone(),
                from_visitor: true,
            })
            .await?;
    }
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
        let check_type = health_check_type_name(health.check_type).to_string();
        let detail = if healthy {
            "ok".to_string()
        } else {
            format!(
                "failed to reach {}:{} within {}s",
                proxy.local_ip,
                proxy.local_port,
                health.timeout_seconds.max(1)
            )
        };

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

        if active {
            state
                .send(&Message::HealthStatus {
                    proxy_name: proxy.name.clone(),
                    healthy,
                    check_type,
                    detail,
                    checked_unix_secs: unix_now(),
                })
                .await?;
        }

        time::sleep(Duration::from_secs(health.interval_seconds.max(1))).await;
    }
}

fn health_check_type_name(check_type: HealthCheckType) -> &'static str {
    match check_type {
        HealthCheckType::Tcp => "tcp",
        HealthCheckType::Http => "http",
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
        HealthCheckType::Http => check_http_health(proxy, timeout_seconds).await,
    }
}

async fn check_http_health(proxy: &ProxyConfig, timeout_seconds: u64) -> bool {
    let addr = format!("{}:{}", proxy.local_ip, proxy.local_port);
    let timeout = Duration::from_secs(timeout_seconds.max(1));
    let mut stream = match time::timeout(timeout, TcpStream::connect(&addr)).await {
        Ok(Ok(stream)) => stream,
        _ => return false,
    };
    let request =
        format!("GET / HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    if !matches!(
        time::timeout(timeout, stream.write_all(request.as_bytes())).await,
        Ok(Ok(()))
    ) {
        return false;
    }

    let mut buf = [0_u8; 64];
    let n = match time::timeout(timeout, stream.read(&mut buf)).await {
        Ok(Ok(n)) => n,
        _ => return false,
    };
    let status = std::str::from_utf8(&buf[..n]).unwrap_or_default();
    status.starts_with("HTTP/1.1 2")
        || status.starts_with("HTTP/1.0 2")
        || status.starts_with("HTTP/1.1 3")
        || status.starts_with("HTTP/1.0 3")
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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
    let (proxy_name, src_addr, dst_addr) = match start {
        Message::StartWorkConn {
            proxy_name,
            src_addr,
            dst_addr,
            error,
        } if error.is_empty() => (proxy_name, src_addr, dst_addr),
        Message::StartWorkConn { error, .. } => bail!("server rejected work connection: {error}"),
        other => bail!("unexpected work connection response: {other:?}"),
    };

    let proxy = state
        .proxies
        .get(&proxy_name)
        .ok_or_else(|| anyhow!("unknown proxy {proxy_name}"))?;
    let local_addr = format!("{}:{}", proxy.local_ip, proxy.local_port);
    call_client_plugin_hook(
        state.cfg.plugins.local_connect_url.as_deref(),
        json!({
            "op": "LocalConnect",
            "proxy_name": proxy_name,
            "proxy_type": proxy.proxy_type.as_str(),
            "local_addr": local_addr,
            "src_addr": src_addr,
            "dst_addr": dst_addr,
        }),
    )
    .await?;
    let mut local = TcpStream::connect(&local_addr)
        .await
        .with_context(|| format!("connect local service for proxy {proxy_name} at {local_addr}"))?;
    configure_tcp_stream(&local);

    io::copy_bidirectional(&mut stream, &mut local)
        .await
        .with_context(|| format!("copy bytes for proxy {proxy_name}"))?;
    Ok(())
}

fn configure_tcp_stream(stream: &TcpStream) {
    if let Err(err) = stream.set_nodelay(true) {
        debug!("set TCP_NODELAY failed: {err:#}");
    }
}

async fn call_client_plugin_hook(url: Option<&str>, payload: serde_json::Value) -> Result<()> {
    let Some(url) = url else {
        return Ok(());
    };
    let target = parse_http_url(url)?;
    let body = payload.to_string();
    let mut stream = time::timeout(
        Duration::from_secs(5),
        TcpStream::connect(format!("{}:{}", target.host, target.port)),
    )
    .await
    .context("connect client plugin hook timed out")?
    .with_context(|| format!("connect client plugin hook {url}"))?;
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        target.path,
        target.host,
        body.len(),
        body
    );
    stream
        .write_all(request.as_bytes())
        .await
        .with_context(|| format!("write client plugin hook request {url}"))?;
    let mut response = vec![0_u8; 128];
    let n = time::timeout(Duration::from_secs(5), stream.read(&mut response))
        .await
        .context("read client plugin hook response timed out")?
        .with_context(|| format!("read client plugin hook response {url}"))?;
    let status = std::str::from_utf8(&response[..n]).unwrap_or_default();
    let ok = status.starts_with("HTTP/1.1 2") || status.starts_with("HTTP/1.0 2");
    if !ok {
        bail!("client plugin hook {url} rejected request: {status:?}");
    }
    Ok(())
}

struct ParsedHttpUrl {
    host: String,
    port: u16,
    path: String,
}

fn parse_http_url(url: &str) -> Result<ParsedHttpUrl> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| anyhow!("only http:// client plugin hooks are supported"))?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.parse::<u16>()?),
        None => (authority.to_string(), 80),
    };
    if host.is_empty() {
        bail!("client plugin hook URL has empty host");
    }
    Ok(ParsedHttpUrl {
        host,
        port,
        path: format!("/{path}"),
    })
}

async fn handle_udp_packet(
    state: ClientState,
    proxy_name: String,
    content: Vec<u8>,
    visitor_addr: String,
) -> Result<()> {
    let (socket, local_addr) = udp_session_for_visitor(&state, &proxy_name, &visitor_addr).await?;
    socket
        .send_to(&content, &local_addr)
        .await
        .with_context(|| format!("send udp packet to local service {local_addr}"))?;
    Ok(())
}

async fn udp_session_for_visitor(
    state: &ClientState,
    proxy_name: &str,
    visitor_addr: &str,
) -> Result<(Arc<UdpSocket>, String)> {
    let proxy = state
        .proxies
        .get(proxy_name)
        .ok_or_else(|| anyhow!("unknown udp proxy {proxy_name}"))?;
    let local_addr = format!("{}:{}", proxy.local_ip, proxy.local_port);
    let key = (proxy_name.to_string(), visitor_addr.to_string());
    if let Some(socket) = {
        let sessions = state.udp_sessions.lock().await;
        sessions.get(&key).cloned()
    } {
        return Ok((socket, local_addr));
    }

    call_client_plugin_hook(
        state.cfg.plugins.local_connect_url.as_deref(),
        json!({
            "op": "LocalConnect",
            "proxy_name": proxy_name,
            "proxy_type": proxy.proxy_type.as_str(),
            "local_addr": local_addr.clone(),
            "src_addr": visitor_addr,
            "dst_addr": "",
        }),
    )
    .await?;
    let socket = Arc::new(
        UdpSocket::bind("0.0.0.0:0")
            .await
            .context("bind local udp relay socket")?,
    );
    let socket = {
        let mut sessions = state.udp_sessions.lock().await;
        if let Some(socket) = sessions.get(&key) {
            socket.clone()
        } else {
            sessions.insert(key.clone(), socket.clone());
            spawn_udp_response_loop((*state).clone(), key, socket.clone());
            socket
        }
    };
    Ok((socket, local_addr))
}

async fn handle_udp_packet_batch(state: ClientState, packets: Vec<UdpPacketFrame>) -> Result<()> {
    let mut sessions: HashMap<(String, String), (Arc<UdpSocket>, String)> = HashMap::new();
    for UdpPacketFrame {
        proxy_name,
        content,
        visitor_addr,
    } in packets
    {
        let key = (proxy_name, visitor_addr);
        let (socket, local_addr) = if let Some(session) = sessions.get(&key) {
            (session.0.clone(), session.1.clone())
        } else {
            let session = udp_session_for_visitor(&state, &key.0, &key.1).await?;
            sessions.insert(key.clone(), (session.0.clone(), session.1.clone()));
            session
        };
        socket
            .send_to(&content, &local_addr)
            .await
            .with_context(|| format!("send udp packet to local service {local_addr}"))?;
    }
    Ok(())
}

async fn handle_sudp_owner_packet(
    state: ClientState,
    proxy_name: String,
    session_id: String,
    content: Vec<u8>,
) -> Result<()> {
    let proxy = state
        .proxies
        .get(&proxy_name)
        .ok_or_else(|| anyhow!("unknown sudp proxy {proxy_name}"))?;
    let local_addr = format!("{}:{}", proxy.local_ip, proxy.local_port);
    let key = (proxy_name.clone(), session_id.clone());
    let socket = {
        let mut sessions = state.udp_sessions.lock().await;
        if let Some(socket) = sessions.get(&key) {
            socket.clone()
        } else {
            call_client_plugin_hook(
                state.cfg.plugins.local_connect_url.as_deref(),
                json!({
                    "op": "LocalConnect",
                    "proxy_name": proxy_name.clone(),
                    "proxy_type": proxy.proxy_type.as_str(),
                    "local_addr": local_addr.clone(),
                    "src_addr": session_id.clone(),
                    "dst_addr": "",
                }),
            )
            .await?;
            let socket = Arc::new(
                UdpSocket::bind("0.0.0.0:0")
                    .await
                    .context("bind local sudp relay socket")?,
            );
            sessions.insert(key.clone(), socket.clone());
            spawn_sudp_owner_response_loop(state.clone(), key, socket.clone());
            socket
        }
    };
    socket
        .send_to(&content, &local_addr)
        .await
        .with_context(|| format!("send sudp packet to local service {local_addr}"))?;
    Ok(())
}

fn spawn_sudp_owner_response_loop(
    state: ClientState,
    key: (String, String),
    socket: Arc<UdpSocket>,
) {
    tokio::spawn(async move {
        let mut buf = vec![0_u8; 64 * 1024];
        loop {
            let received = time::timeout(Duration::from_secs(60), socket.recv_from(&mut buf)).await;
            let n = match received {
                Ok(Ok((n, _))) => n,
                Ok(Err(err)) => {
                    debug!("sudp owner recv failed: {err:#}");
                    break;
                }
                Err(_) => break,
            };
            if let Err(err) = state
                .send(&Message::SudpPacket {
                    proxy_name: key.0.clone(),
                    session_id: key.1.clone(),
                    content: buf[..n].to_vec(),
                    sk: None,
                    from_visitor: false,
                })
                .await
            {
                debug!("sudp owner response send failed: {err:#}");
                break;
            }
        }

        let mut sessions = state.udp_sessions.lock().await;
        if sessions
            .get(&key)
            .map(|current| Arc::ptr_eq(current, &socket))
            .unwrap_or(false)
        {
            sessions.remove(&key);
        }
    });
}

async fn handle_sudp_visitor_response(
    state: ClientState,
    session_id: String,
    content: Vec<u8>,
) -> Result<()> {
    let session = {
        let sessions = state.sudp_visitor_sessions.lock().await;
        sessions
            .get(&session_id)
            .cloned()
            .ok_or_else(|| anyhow!("unknown sudp visitor session {session_id}"))?
    };
    session
        .socket
        .send_to(&content, session.peer)
        .await
        .context("send sudp response to visitor peer")?;
    Ok(())
}

fn spawn_udp_response_loop(state: ClientState, key: (String, String), socket: Arc<UdpSocket>) {
    tokio::spawn(async move {
        let mut buf = vec![0_u8; 64 * 1024];
        loop {
            let received = time::timeout(Duration::from_secs(60), socket.recv_from(&mut buf)).await;
            let n = match received {
                Ok(Ok((n, _))) => n,
                Ok(Err(err)) => {
                    debug!("udp session recv failed: {err:#}");
                    break;
                }
                Err(_) => break,
            };
            let mut packets = vec![UdpPacketFrame {
                proxy_name: key.0.clone(),
                content: buf[..n].to_vec(),
                visitor_addr: key.1.clone(),
            }];
            while packets.len() < 32 {
                match time::timeout(Duration::from_millis(2), socket.recv_from(&mut buf)).await {
                    Ok(Ok((n, _))) => packets.push(UdpPacketFrame {
                        proxy_name: key.0.clone(),
                        content: buf[..n].to_vec(),
                        visitor_addr: key.1.clone(),
                    }),
                    Ok(Err(err)) => {
                        debug!("udp session recv failed: {err:#}");
                        break;
                    }
                    Err(_) => break,
                }
            }

            let msg = if packets.len() == 1 {
                let packet = packets.pop().expect("batch has one packet");
                Message::UdpPacket {
                    proxy_name: packet.proxy_name,
                    content: packet.content,
                    visitor_addr: packet.visitor_addr,
                }
            } else {
                Message::UdpPacketBatch { packets }
            };
            if let Err(err) = state.send(&msg).await {
                debug!("udp session response send failed: {err:#}");
                break;
            }
        }

        let mut sessions = state.udp_sessions.lock().await;
        if sessions
            .get(&key)
            .map(|current| Arc::ptr_eq(current, &socket))
            .unwrap_or(false)
        {
            sessions.remove(&key);
        }
    });
}
