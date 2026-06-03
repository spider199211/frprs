use crate::{
    config::{
        ClientConfig, HealthCheckType, ProxyConfig, ProxyType, TransportProtocol, VisitorConfig,
    },
    protocol::{read_msg, write_msg, Message, UdpPacketFrame},
    transports::{self, BoxStream},
};
use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    env, fs,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, WriteHalf},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{mpsc, oneshot, Mutex},
    time,
};
use tracing::{debug, error, info, warn};

const DIRECT_SUDP_MAGIC: &str = "frprs-sudp-direct-v1";
const NAT_HOLE_CACHED_RESPONSE_TTL: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct ClientState {
    cfg: Arc<ClientConfig>,
    run_id: String,
    proxies: Arc<HashMap<String, ProxyConfig>>,
    udp_sessions: Arc<Mutex<HashMap<(String, String), Arc<UdpSocket>>>>,
    sudp_visitor_sessions: Arc<Mutex<HashMap<String, SudpVisitorSession>>>,
    sudp_direct_owner_sockets: Arc<Mutex<HashMap<String, Arc<UdpSocket>>>>,
    sudp_direct_visitor_sockets: Arc<Mutex<HashMap<String, Arc<UdpSocket>>>>,
    sudp_direct_visitor_peers: Arc<Mutex<HashMap<String, SocketAddr>>>,
    sudp_direct_pending: Arc<Mutex<HashMap<String, VecDeque<DirectSudpFrame>>>>,
    sudp_direct_confirmed: Arc<Mutex<HashSet<String>>>,
    sudp_direct_probe_sessions: Arc<Mutex<HashSet<String>>>,
    nat_hole_waiters: Arc<Mutex<HashMap<String, oneshot::Sender<NatHoleResponse>>>>,
    nat_hole_cached: Arc<Mutex<HashMap<String, NatHoleCachedResponse>>>,
    writer: Arc<Mutex<WriteHalf<BoxStream>>>,
    quic_session: Option<Arc<transports::quic::QuicClientSession>>,
}

#[derive(Clone)]
struct SudpVisitorSession {
    socket: Arc<UdpSocket>,
    peer: SocketAddr,
}

#[derive(Debug, Clone)]
struct NatHoleResponse {
    peer_observed_addr: String,
    peer_local_addrs: Vec<String>,
    waiting: bool,
    error: String,
}

#[derive(Clone)]
struct NatHoleCachedResponse {
    resp: NatHoleResponse,
    cached_at: Instant,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DirectSudpFrame {
    magic: String,
    proxy_name: String,
    session_id: String,
    content: Vec<u8>,
    sk: Option<String>,
    from_visitor: bool,
    ack: bool,
    #[serde(default)]
    probe: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DirectCandidateSource {
    Local,
    Observed,
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
    let (mut stream, quic_session) = connect_session_control(&cfg)
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
        sudp_direct_owner_sockets: Arc::new(Mutex::new(HashMap::new())),
        sudp_direct_visitor_sockets: Arc::new(Mutex::new(HashMap::new())),
        sudp_direct_visitor_peers: Arc::new(Mutex::new(HashMap::new())),
        sudp_direct_pending: Arc::new(Mutex::new(HashMap::new())),
        sudp_direct_confirmed: Arc::new(Mutex::new(HashSet::new())),
        sudp_direct_probe_sessions: Arc::new(Mutex::new(HashSet::new())),
        nat_hole_waiters: Arc::new(Mutex::new(HashMap::new())),
        nat_hole_cached: Arc::new(Mutex::new(HashMap::new())),
        writer: Arc::new(Mutex::new(writer)),
        quic_session,
    };

    for proxy in state.proxies.values().cloned() {
        if proxy.proxy_type == ProxyType::Xtcp {
            start_xtcp_direct_owner_listener(state.clone(), proxy.clone()).await?;
        } else if proxy.proxy_type == ProxyType::Sudp {
            start_sudp_direct_owner_listener(state.clone(), proxy.clone()).await?;
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
                proxy_name,
                role,
                observed_addr: _,
                peer_observed_addr,
                peer_local_addrs,
                waiting,
                error,
                ..
            } => {
                handle_nat_hole_response(
                    &state,
                    transaction_id,
                    proxy_name,
                    role,
                    NatHoleResponse {
                        peer_observed_addr,
                        peer_local_addrs,
                        waiting,
                        error,
                    },
                )
                .await;
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

async fn connect_session_control(
    cfg: &ClientConfig,
) -> Result<(BoxStream, Option<Arc<transports::quic::QuicClientSession>>)> {
    if cfg.transport.protocol == TransportProtocol::Quic {
        let server_addr = resolve_server_addr(cfg).await?;
        let session =
            Arc::new(transports::quic::QuicClientSession::connect_insecure(server_addr).await?);
        let stream = session.open_stream().await?;
        return Ok((Box::new(stream), Some(session)));
    }

    Ok((connect_control(cfg).await?, None))
}

async fn open_control_stream(state: &ClientState) -> Result<BoxStream> {
    if let Some(session) = &state.quic_session {
        return Ok(Box::new(session.open_stream().await?));
    }

    connect_control(&state.cfg).await
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

async fn handle_nat_hole_response(
    state: &ClientState,
    transaction_id: String,
    proxy_name: String,
    role: String,
    resp: NatHoleResponse,
) {
    let key = nat_hole_waiter_key(&transaction_id, &role);
    if let Some(waiter) = state.nat_hole_waiters.lock().await.remove(&key) {
        let _ = waiter.send(resp);
        return;
    }
    if resp.waiting || !resp.error.is_empty() {
        debug!("ignored nat hole response for transaction {transaction_id} role {role}");
        return;
    }
    if role.eq_ignore_ascii_case("server") {
        if let Err(err) =
            punch_sudp_owner_candidates(state, &proxy_name, &transaction_id, &resp).await
        {
            debug!("sudp owner punch for {proxy_name}/{transaction_id} failed: {err:#}");
        }
    }
    state.nat_hole_cached.lock().await.insert(
        key,
        NatHoleCachedResponse {
            resp,
            cached_at: Instant::now(),
        },
    );
}

async fn take_cached_nat_hole_response(state: &ClientState, key: &str) -> Option<NatHoleResponse> {
    let mut cached = state.nat_hole_cached.lock().await;
    let cached_resp = cached.remove(key)?;
    (cached_resp.cached_at.elapsed() <= NAT_HOLE_CACHED_RESPONSE_TTL).then_some(cached_resp.resp)
}

async fn request_nat_hole(
    state: &ClientState,
    transaction_id: &str,
    proxy_name: &str,
    role: &str,
    local_addrs: Vec<String>,
) -> Result<NatHoleResponse> {
    let key = nat_hole_waiter_key(transaction_id, role);
    if let Some(resp) = take_cached_nat_hole_response(state, &key).await {
        if !resp.error.is_empty() {
            bail!("nat hole register failed: {}", resp.error);
        }
        return Ok(resp);
    }

    let (tx, rx) = oneshot::channel();
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

async fn request_nat_hole_with_grace(
    state: &ClientState,
    transaction_id: &str,
    proxy_name: &str,
    role: &str,
    local_addrs: Vec<String>,
    grace: Duration,
) -> Result<NatHoleResponse> {
    let resp = request_nat_hole(state, transaction_id, proxy_name, role, local_addrs).await?;
    if !resp.waiting || grace.is_zero() {
        return Ok(resp);
    }

    let key = nat_hole_waiter_key(transaction_id, role);
    if let Some(resp) = take_cached_nat_hole_response(state, &key).await {
        if !resp.error.is_empty() {
            bail!("nat hole register failed: {}", resp.error);
        }
        return Ok(resp);
    }

    let (tx, rx) = oneshot::channel();
    state.nat_hole_waiters.lock().await.insert(key.clone(), tx);
    match time::timeout(grace, rx).await {
        Ok(Ok(resp)) => {
            if !resp.error.is_empty() {
                bail!("nat hole register failed: {}", resp.error);
            }
            Ok(resp)
        }
        Ok(Err(_)) => {
            state.nat_hole_waiters.lock().await.remove(&key);
            Ok(resp)
        }
        Err(_) => {
            state.nat_hole_waiters.lock().await.remove(&key);
            Ok(resp)
        }
    }
}

async fn punch_sudp_owner_candidates(
    state: &ClientState,
    proxy_name: &str,
    transaction_id: &str,
    resp: &NatHoleResponse,
) -> Result<()> {
    let Some(proxy) = sudp_direct_owner_proxy(state, proxy_name) else {
        return Ok(());
    };
    let Some(socket) = state
        .sudp_direct_owner_sockets
        .lock()
        .await
        .get(&proxy.name)
        .cloned()
    else {
        return Ok(());
    };
    let frame = DirectSudpFrame {
        magic: DIRECT_SUDP_MAGIC.to_string(),
        proxy_name: proxy_name.to_string(),
        session_id: transaction_id.to_string(),
        content: Vec::new(),
        sk: None,
        from_visitor: false,
        ack: true,
        probe: true,
    };
    let payload = serde_json::to_vec(&frame).context("encode sudp owner punch probe")?;
    let mut candidates = Vec::new();
    for candidate in direct_candidates(resp) {
        let Ok(addr) = candidate.parse::<SocketAddr>() else {
            debug!("ignore invalid sudp owner punch candidate {candidate}");
            continue;
        };
        candidates.push(addr);
    }
    send_sudp_owner_punch_payload(&socket, &payload, &candidates).await;
    schedule_sudp_owner_punch_retries(proxy_name.to_string(), socket, payload, candidates);
    Ok(())
}

async fn send_sudp_owner_punch_payload(
    socket: &UdpSocket,
    payload: &[u8],
    candidates: &[SocketAddr],
) {
    for addr in candidates {
        if let Err(err) = socket.send_to(payload, addr).await {
            debug!("sudp owner punch to {addr} failed: {err:#}");
        }
    }
}

fn schedule_sudp_owner_punch_retries(
    proxy_name: String,
    socket: Arc<UdpSocket>,
    payload: Vec<u8>,
    candidates: Vec<SocketAddr>,
) {
    if candidates.is_empty() {
        return;
    }
    tokio::spawn(async move {
        for delay in [
            Duration::from_millis(120),
            Duration::from_millis(260),
            Duration::from_millis(520),
        ] {
            time::sleep(delay).await;
            send_sudp_owner_punch_payload(&socket, &payload, &candidates).await;
            debug!(
                "sudp owner {proxy_name} retried punch to {} candidates",
                candidates.len()
            );
        }
    });
}

fn sudp_direct_owner_proxy(state: &ClientState, name_or_group: &str) -> Option<ProxyConfig> {
    state
        .proxies
        .get(name_or_group)
        .filter(|proxy| proxy.proxy_type == ProxyType::Sudp)
        .cloned()
        .or_else(|| {
            state
                .proxies
                .values()
                .find(|proxy| {
                    proxy.proxy_type == ProxyType::Sudp
                        && proxy.group.as_deref() == Some(name_or_group)
                })
                .cloned()
        })
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
    let mut stream = open_control_stream(&state)
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
    let resp = match request_nat_hole_with_grace(
        state,
        &visitor.server_name,
        &visitor.server_name,
        "visitor",
        Vec::new(),
        Duration::from_millis(500),
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

    if let Some((mut peer, candidate)) =
        connect_xtcp_direct_peer(visitor, direct_candidates(&resp)).await?
    {
        info!(
            "xtcp visitor {} connected directly to {}",
            visitor.name, candidate
        );
        io::copy_bidirectional(inbound, &mut peer)
            .await
            .with_context(|| format!("copy xtcp direct visitor bytes for {}", visitor.name))?;
        return Ok(true);
    }

    Ok(false)
}

async fn connect_xtcp_direct_peer(
    visitor: &VisitorConfig,
    candidates: Vec<String>,
) -> Result<Option<(TcpStream, String)>> {
    if candidates.is_empty() {
        return Ok(None);
    }

    let (tx, mut rx) = mpsc::channel(candidates.len());
    for candidate in candidates {
        let tx = tx.clone();
        let visitor = visitor.clone();
        tokio::spawn(async move {
            let result = time::timeout(
                Duration::from_secs(2),
                connect_xtcp_direct_candidate(&visitor, &candidate),
            )
            .await
            .unwrap_or_else(|_| Err(anyhow!("xtcp direct candidate timed out")));
            let _ = tx.send((candidate, result)).await;
        });
    }
    drop(tx);

    while let Some((candidate, result)) = rx.recv().await {
        match result {
            Ok(peer) => return Ok(Some((peer, candidate))),
            Err(err) => debug!("xtcp direct candidate {candidate} failed: {err:#}"),
        }
    }
    Ok(None)
}

async fn connect_xtcp_direct_candidate(
    visitor: &VisitorConfig,
    candidate: &str,
) -> Result<TcpStream> {
    let addr = candidate
        .parse::<SocketAddr>()
        .with_context(|| format!("parse xtcp direct candidate {candidate}"))?;
    let mut peer = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connect xtcp direct candidate {candidate}"))?;
    configure_tcp_stream(&peer);
    write_msg(
        &mut peer,
        &Message::NewVisitorConn {
            proxy_name: visitor.server_name.clone(),
            sk: visitor.sk.clone(),
            token: None,
        },
    )
    .await
    .with_context(|| format!("write xtcp direct handshake to {candidate}"))?;

    match read_msg(&mut peer)
        .await
        .with_context(|| format!("read xtcp direct handshake from {candidate}"))?
    {
        Message::NewVisitorConnResp { error, .. } if error.is_empty() => Ok(peer),
        Message::NewVisitorConnResp { error, .. } => {
            bail!("xtcp direct peer rejected visitor: {error}")
        }
        other => bail!("unexpected xtcp direct response: {other:?}"),
    }
}

fn direct_candidates(resp: &NatHoleResponse) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut candidates = Vec::new();
    for (source, candidate) in resp
        .peer_local_addrs
        .iter()
        .map(|addr| (DirectCandidateSource::Local, addr))
        .chain(std::iter::once((
            DirectCandidateSource::Observed,
            &resp.peer_observed_addr,
        )))
    {
        if candidate.is_empty() {
            continue;
        }
        let Ok(addr) = candidate.parse::<SocketAddr>() else {
            debug!("ignore invalid direct candidate {candidate}");
            continue;
        };
        if seen.insert(addr) {
            candidates.push((direct_candidate_rank(source, &addr), candidate.clone()));
        }
    }
    candidates.sort_by_key(|(rank, _)| *rank);
    candidates
        .into_iter()
        .map(|(_, candidate)| candidate)
        .collect()
}

fn direct_candidate_rank(source: DirectCandidateSource, addr: &SocketAddr) -> (u8, u8) {
    (source as u8, direct_candidate_addr_rank(addr.ip()))
}

fn direct_candidate_addr_rank(ip: IpAddr) -> u8 {
    match ip {
        IpAddr::V4(ip) if is_public_ipv4(ip) => 0,
        IpAddr::V6(ip) if is_public_ipv6(ip) => 0,
        IpAddr::V4(ip) if ip.is_private() => 1,
        IpAddr::V6(ip) if ip.is_unique_local() => 1,
        IpAddr::V4(ip) if ip.is_link_local() => 2,
        IpAddr::V6(ip) if ip.is_unicast_link_local() => 2,
        IpAddr::V4(ip) if ip.is_loopback() => 3,
        IpAddr::V6(ip) if ip.is_loopback() => 3,
        IpAddr::V4(ip) if ip.is_unspecified() || ip.is_multicast() => 5,
        IpAddr::V6(ip) if ip.is_unspecified() || ip.is_multicast() => 5,
        _ => 4,
    }
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_unspecified()
        || ip.is_multicast())
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    !(ip.is_loopback()
        || ip.is_unique_local()
        || ip.is_unicast_link_local()
        || ip.is_unspecified()
        || ip.is_multicast())
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
        if try_sudp_direct_visitor(&state, &visitor, &session_id, &buf[..n]).await? {
            if !state
                .sudp_direct_confirmed
                .lock()
                .await
                .contains(&session_id)
            {
                schedule_sudp_relay_fallback(
                    state.clone(),
                    visitor.server_name.clone(),
                    session_id,
                    buf[..n].to_vec(),
                    visitor.sk.clone(),
                );
            }
            continue;
        }
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

async fn start_sudp_direct_owner_listener(state: ClientState, proxy: ProxyConfig) -> Result<()> {
    let socket = Arc::new(
        UdpSocket::bind(format!("{}:0", proxy.local_ip))
            .await
            .with_context(|| format!("listen sudp direct owner {}", proxy.name))?,
    );
    let advertised_addr = socket
        .local_addr()
        .context("read sudp direct owner addr")?
        .to_string();
    state
        .sudp_direct_owner_sockets
        .lock()
        .await
        .insert(proxy.name.clone(), socket.clone());
    announce_nat_hole(
        &state,
        &proxy.name,
        &proxy.name,
        "server",
        vec![advertised_addr.clone()],
    )
    .await?;
    if let Some(group_name) = proxy.group.as_deref().filter(|group| !group.is_empty()) {
        announce_nat_hole(
            &state,
            group_name,
            group_name,
            "server",
            vec![advertised_addr.clone()],
        )
        .await?;
    }
    info!(
        "sudp direct owner {} listening on {}",
        proxy.name, advertised_addr
    );

    let sessions = Arc::new(Mutex::new(HashMap::<String, Arc<UdpSocket>>::new()));
    tokio::spawn(async move {
        let mut buf = vec![0_u8; 64 * 1024];
        loop {
            let (n, peer) = match socket.recv_from(&mut buf).await {
                Ok(received) => received,
                Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => {
                    debug!("sudp direct owner ignored udp reset: {err:#}");
                    continue;
                }
                Err(err) => {
                    debug!("sudp direct owner {} recv failed: {err:#}", proxy.name);
                    break;
                }
            };
            let frame = match parse_sudp_direct_frame(&buf[..n]) {
                Some(frame) if frame.from_visitor && !frame.ack => frame,
                _ => continue,
            };
            if frame.proxy_name != proxy.name
                && proxy.group.as_deref() != Some(frame.proxy_name.as_str())
            {
                debug!(
                    "ignore sudp direct frame for {} on owner {}",
                    frame.proxy_name, proxy.name
                );
                continue;
            }
            if frame.sk != proxy.sk {
                debug!(
                    "ignore sudp direct frame with mismatched sk for {}",
                    proxy.name
                );
                continue;
            }

            let ack = DirectSudpFrame {
                magic: DIRECT_SUDP_MAGIC.to_string(),
                proxy_name: frame.proxy_name.clone(),
                session_id: frame.session_id.clone(),
                content: Vec::new(),
                sk: None,
                from_visitor: false,
                ack: true,
                probe: false,
            };
            if let Ok(payload) = serde_json::to_vec(&ack) {
                let _ = socket.send_to(&payload, peer).await;
            }

            if frame.probe {
                continue;
            }

            let local = match sudp_direct_owner_session(
                &proxy,
                &frame.session_id,
                socket.clone(),
                sessions.clone(),
                peer,
            )
            .await
            {
                Ok(socket) => socket,
                Err(err) => {
                    debug!("sudp direct session {} failed: {err:#}", frame.session_id);
                    continue;
                }
            };
            let local_addr = format!("{}:{}", proxy.local_ip, proxy.local_port);
            if let Err(err) = local.send_to(&frame.content, &local_addr).await {
                debug!("sudp direct send to local {local_addr} failed: {err:#}");
            }
        }
    });
    Ok(())
}

async fn sudp_direct_owner_session(
    proxy: &ProxyConfig,
    session_id: &str,
    direct_socket: Arc<UdpSocket>,
    sessions: Arc<Mutex<HashMap<String, Arc<UdpSocket>>>>,
    peer: SocketAddr,
) -> Result<Arc<UdpSocket>> {
    if let Some(socket) = sessions.lock().await.get(session_id).cloned() {
        return Ok(socket);
    }

    let socket = Arc::new(
        UdpSocket::bind("0.0.0.0:0")
            .await
            .context("bind sudp direct local relay socket")?,
    );
    let socket = {
        let mut sessions_guard = sessions.lock().await;
        if let Some(existing) = sessions_guard.get(session_id) {
            existing.clone()
        } else {
            sessions_guard.insert(session_id.to_string(), socket.clone());
            spawn_sudp_direct_owner_response_loop(
                proxy.name.clone(),
                session_id.to_string(),
                socket.clone(),
                direct_socket,
                sessions.clone(),
                peer,
            );
            socket
        }
    };
    Ok(socket)
}

fn spawn_sudp_direct_owner_response_loop(
    proxy_name: String,
    session_id: String,
    local_socket: Arc<UdpSocket>,
    direct_socket: Arc<UdpSocket>,
    sessions: Arc<Mutex<HashMap<String, Arc<UdpSocket>>>>,
    peer: SocketAddr,
) {
    tokio::spawn(async move {
        let mut buf = vec![0_u8; 64 * 1024];
        loop {
            let received =
                time::timeout(Duration::from_secs(60), local_socket.recv_from(&mut buf)).await;
            let n = match received {
                Ok(Ok((n, _))) => n,
                Ok(Err(err)) => {
                    debug!("sudp direct local recv failed: {err:#}");
                    break;
                }
                Err(_) => break,
            };
            let frame = DirectSudpFrame {
                magic: DIRECT_SUDP_MAGIC.to_string(),
                proxy_name: proxy_name.clone(),
                session_id: session_id.clone(),
                content: buf[..n].to_vec(),
                sk: None,
                from_visitor: false,
                ack: false,
                probe: false,
            };
            let payload = match serde_json::to_vec(&frame) {
                Ok(payload) => payload,
                Err(err) => {
                    debug!("encode sudp direct response failed: {err:#}");
                    break;
                }
            };
            if let Err(err) = direct_socket.send_to(&payload, peer).await {
                debug!("send sudp direct response failed: {err:#}");
                break;
            }
        }

        let mut sessions = sessions.lock().await;
        if sessions
            .get(&session_id)
            .map(|current| Arc::ptr_eq(current, &local_socket))
            .unwrap_or(false)
        {
            sessions.remove(&session_id);
        }
    });
}

async fn try_sudp_direct_visitor(
    state: &ClientState,
    visitor: &VisitorConfig,
    session_id: &str,
    content: &[u8],
) -> Result<bool> {
    let direct_socket = sudp_direct_visitor_socket(state, session_id, &visitor.bind_addr).await?;
    if let Some(peer) = state
        .sudp_direct_visitor_peers
        .lock()
        .await
        .get(session_id)
        .copied()
    {
        match send_sudp_direct_payload(&direct_socket, visitor, session_id, content, peer).await {
            Ok(()) => {
                state
                    .sudp_direct_confirmed
                    .lock()
                    .await
                    .insert(session_id.to_string());
                return Ok(true);
            }
            Err(err) => {
                debug!("sudp direct send to selected peer {peer} failed: {err:#}");
                state
                    .sudp_direct_visitor_peers
                    .lock()
                    .await
                    .remove(session_id);
                state.sudp_direct_confirmed.lock().await.remove(session_id);
            }
        }
    }

    let local_addr = direct_socket
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_default();
    let resp = match request_nat_hole_with_grace(
        state,
        &visitor.server_name,
        &visitor.server_name,
        "visitor",
        vec![local_addr],
        Duration::from_millis(500),
    )
    .await
    {
        Ok(resp) => resp,
        Err(err) => {
            debug!(
                "sudp direct lookup for {} failed: {err:#}",
                visitor.server_name
            );
            return Ok(false);
        }
    };
    if resp.waiting {
        return Ok(false);
    }

    state
        .sudp_direct_pending
        .lock()
        .await
        .entry(session_id.to_string())
        .or_default()
        .push_back(sudp_direct_visitor_frame(
            visitor,
            session_id,
            content.to_vec(),
            false,
        ));

    let mut candidates = Vec::new();
    for candidate in direct_candidates(&resp) {
        let Ok(addr) = candidate.parse::<SocketAddr>() else {
            debug!("ignore invalid sudp direct candidate {candidate}");
            continue;
        };
        candidates.push(addr);
    }
    let sent = send_sudp_direct_probe(&direct_socket, visitor, session_id, &candidates).await?;
    if !sent {
        remove_sudp_pending_payload(state, session_id, content).await;
    } else {
        schedule_sudp_direct_probe_retries(
            state.clone(),
            visitor.clone(),
            session_id.to_string(),
            candidates,
        );
    }
    Ok(sent)
}

async fn send_sudp_direct_probe(
    socket: &UdpSocket,
    visitor: &VisitorConfig,
    session_id: &str,
    candidates: &[SocketAddr],
) -> Result<bool> {
    let frame = sudp_direct_visitor_frame(visitor, session_id, Vec::new(), true);
    let payload = serde_json::to_vec(&frame).context("encode sudp direct probe")?;
    let mut sent = false;
    for addr in candidates {
        match socket.send_to(&payload, addr).await {
            Ok(_) => {
                debug!(
                    "sudp visitor {} sent direct probe to {}",
                    visitor.name, addr
                );
                sent = true;
            }
            Err(err) => {
                debug!("sudp direct probe to {addr} failed: {err:#}");
            }
        }
    }
    Ok(sent)
}

fn schedule_sudp_direct_probe_retries(
    state: ClientState,
    visitor: VisitorConfig,
    session_id: String,
    candidates: Vec<SocketAddr>,
) {
    if candidates.is_empty() {
        return;
    }
    tokio::spawn(async move {
        {
            let mut sessions = state.sudp_direct_probe_sessions.lock().await;
            if !sessions.insert(session_id.clone()) {
                return;
            }
        }

        for delay in [
            Duration::from_millis(120),
            Duration::from_millis(260),
            Duration::from_millis(520),
        ] {
            time::sleep(delay).await;
            if state
                .sudp_direct_confirmed
                .lock()
                .await
                .contains(&session_id)
                || state
                    .sudp_direct_visitor_peers
                    .lock()
                    .await
                    .contains_key(&session_id)
                || !state
                    .sudp_direct_pending
                    .lock()
                    .await
                    .contains_key(&session_id)
            {
                break;
            }

            let socket = {
                state
                    .sudp_direct_visitor_sockets
                    .lock()
                    .await
                    .get(&session_id)
                    .cloned()
            };
            let Some(socket) = socket else {
                break;
            };
            if let Err(err) =
                send_sudp_direct_probe(&socket, &visitor, &session_id, &candidates).await
            {
                debug!("sudp direct probe retry failed: {err:#}");
            }
        }

        state
            .sudp_direct_probe_sessions
            .lock()
            .await
            .remove(&session_id);
    });
}

fn sudp_direct_visitor_frame(
    visitor: &VisitorConfig,
    session_id: &str,
    content: Vec<u8>,
    probe: bool,
) -> DirectSudpFrame {
    DirectSudpFrame {
        magic: DIRECT_SUDP_MAGIC.to_string(),
        proxy_name: visitor.server_name.clone(),
        session_id: session_id.to_string(),
        content,
        sk: visitor.sk.clone(),
        from_visitor: true,
        ack: false,
        probe,
    }
}

async fn send_sudp_direct_payload(
    socket: &UdpSocket,
    visitor: &VisitorConfig,
    session_id: &str,
    content: &[u8],
    peer: SocketAddr,
) -> Result<()> {
    let frame = sudp_direct_visitor_frame(visitor, session_id, content.to_vec(), false);
    let payload = serde_json::to_vec(&frame).context("encode sudp direct packet")?;
    socket
        .send_to(&payload, peer)
        .await
        .with_context(|| format!("send sudp direct packet to {peer}"))?;
    Ok(())
}

async fn remove_sudp_pending_payload(state: &ClientState, session_id: &str, content: &[u8]) {
    let mut pending = state.sudp_direct_pending.lock().await;
    if let Some(queue) = pending.get_mut(session_id) {
        if let Some(index) = queue
            .iter()
            .position(|queued| !queued.probe && queued.content.as_slice() == content)
        {
            queue.remove(index);
        }
        if queue.is_empty() {
            pending.remove(session_id);
        }
    }
}

async fn sudp_direct_visitor_socket(
    state: &ClientState,
    session_id: &str,
    bind_addr: &str,
) -> Result<Arc<UdpSocket>> {
    if let Some(socket) = state
        .sudp_direct_visitor_sockets
        .lock()
        .await
        .get(session_id)
        .cloned()
    {
        return Ok(socket);
    }

    let socket = Arc::new(
        UdpSocket::bind(format!("{bind_addr}:0"))
            .await
            .context("bind sudp direct visitor socket")?,
    );
    let socket = {
        let mut sockets = state.sudp_direct_visitor_sockets.lock().await;
        if let Some(existing) = sockets.get(session_id) {
            existing.clone()
        } else {
            sockets.insert(session_id.to_string(), socket.clone());
            spawn_sudp_direct_visitor_response_loop(
                state.clone(),
                session_id.to_string(),
                socket.clone(),
            );
            socket
        }
    };
    Ok(socket)
}

fn spawn_sudp_direct_visitor_response_loop(
    state: ClientState,
    session_id: String,
    socket: Arc<UdpSocket>,
) {
    tokio::spawn(async move {
        let mut buf = vec![0_u8; 64 * 1024];
        loop {
            let received = time::timeout(Duration::from_secs(60), socket.recv_from(&mut buf)).await;
            let (n, peer) = match received {
                Ok(Ok(received)) => received,
                Ok(Err(err)) if err.kind() == std::io::ErrorKind::ConnectionReset => {
                    debug!("sudp direct visitor ignored udp reset: {err:#}");
                    continue;
                }
                Ok(Err(err)) => {
                    debug!("sudp direct visitor recv failed: {err:#}");
                    break;
                }
                Err(_) => break,
            };
            let Some((response_session_id, content)) = parse_sudp_direct_response(&buf[..n]) else {
                continue;
            };
            if response_session_id != session_id {
                continue;
            }
            state
                .sudp_direct_visitor_peers
                .lock()
                .await
                .insert(session_id.clone(), peer);
            if let Err(err) = flush_sudp_direct_pending(&state, &session_id, &socket, peer).await {
                debug!("sudp direct pending flush failed: {err:#}");
                continue;
            }
            state
                .sudp_direct_confirmed
                .lock()
                .await
                .insert(session_id.clone());
            if let Some(content) = content {
                if let Err(err) =
                    handle_sudp_visitor_response(state.clone(), session_id.clone(), content).await
                {
                    debug!("sudp direct visitor response failed: {err:#}");
                    break;
                }
            }
        }

        let mut sockets = state.sudp_direct_visitor_sockets.lock().await;
        if sockets
            .get(&session_id)
            .map(|current| Arc::ptr_eq(current, &socket))
            .unwrap_or(false)
        {
            sockets.remove(&session_id);
        }
        state
            .sudp_direct_visitor_peers
            .lock()
            .await
            .remove(&session_id);
        state.sudp_direct_pending.lock().await.remove(&session_id);
        state.sudp_direct_confirmed.lock().await.remove(&session_id);
        state
            .sudp_direct_probe_sessions
            .lock()
            .await
            .remove(&session_id);
    });
}

async fn flush_sudp_direct_pending(
    state: &ClientState,
    session_id: &str,
    socket: &UdpSocket,
    peer: SocketAddr,
) -> Result<()> {
    let mut frames = Vec::new();
    {
        let mut pending = state.sudp_direct_pending.lock().await;
        if let Some(queue) = pending.get_mut(session_id) {
            while let Some(frame) = queue.pop_front() {
                frames.push(frame);
            }
            if queue.is_empty() {
                pending.remove(session_id);
            }
        }
    }

    for frame in frames {
        let payload = serde_json::to_vec(&frame).context("encode sudp pending direct packet")?;
        socket
            .send_to(&payload, peer)
            .await
            .with_context(|| format!("send sudp pending direct packet to {peer}"))?;
    }
    Ok(())
}

fn schedule_sudp_relay_fallback(
    state: ClientState,
    proxy_name: String,
    session_id: String,
    content: Vec<u8>,
    sk: Option<String>,
) {
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(750)).await;
        if state
            .sudp_direct_confirmed
            .lock()
            .await
            .contains(&session_id)
        {
            return;
        }
        if let Err(err) = state
            .send(&Message::SudpPacket {
                proxy_name,
                session_id,
                content,
                sk,
                from_visitor: true,
            })
            .await
        {
            debug!("sudp relay fallback send failed: {err:#}");
        }
    });
}

fn parse_sudp_direct_response(data: &[u8]) -> Option<(String, Option<Vec<u8>>)> {
    let frame = parse_sudp_direct_frame(data)?;
    if frame.from_visitor {
        return None;
    }
    let content = if frame.ack { None } else { Some(frame.content) };
    Some((frame.session_id, content))
}

fn parse_sudp_direct_frame(data: &[u8]) -> Option<DirectSudpFrame> {
    let frame = serde_json::from_slice::<DirectSudpFrame>(data).ok()?;
    (frame.magic == DIRECT_SUDP_MAGIC).then_some(frame)
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
    let mut stream = open_control_stream(&state)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthConfig, ClientPluginConfig, TransportConfig};
    use tokio::io::duplex;

    fn test_state() -> (ClientState, tokio::io::DuplexStream) {
        let (client, server) = duplex(4096);
        let stream: BoxStream = Box::new(client);
        let (_reader, writer) = tokio::io::split(stream);
        let state = ClientState {
            cfg: Arc::new(ClientConfig {
                server_addr: "127.0.0.1".to_string(),
                server_port: 7000,
                auth: AuthConfig { token: None },
                pool_count: 0,
                transport: TransportConfig::default(),
                plugins: ClientPluginConfig::default(),
                proxies: Vec::new(),
                visitors: Vec::new(),
            }),
            run_id: "test-run".to_string(),
            proxies: Arc::new(HashMap::new()),
            udp_sessions: Arc::new(Mutex::new(HashMap::new())),
            sudp_visitor_sessions: Arc::new(Mutex::new(HashMap::new())),
            sudp_direct_owner_sockets: Arc::new(Mutex::new(HashMap::new())),
            sudp_direct_visitor_sockets: Arc::new(Mutex::new(HashMap::new())),
            sudp_direct_visitor_peers: Arc::new(Mutex::new(HashMap::new())),
            sudp_direct_pending: Arc::new(Mutex::new(HashMap::new())),
            sudp_direct_confirmed: Arc::new(Mutex::new(HashSet::new())),
            sudp_direct_probe_sessions: Arc::new(Mutex::new(HashSet::new())),
            nat_hole_waiters: Arc::new(Mutex::new(HashMap::new())),
            nat_hole_cached: Arc::new(Mutex::new(HashMap::new())),
            writer: Arc::new(Mutex::new(writer)),
            quic_session: None,
        };
        (state, server)
    }

    async fn spawn_xtcp_direct_test_peer(delay: Duration) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            match read_msg(&mut stream).await.unwrap() {
                Message::NewVisitorConn { proxy_name, .. } => {
                    time::sleep(delay).await;
                    write_msg(
                        &mut stream,
                        &Message::NewVisitorConnResp {
                            proxy_name,
                            error: String::new(),
                        },
                    )
                    .await
                    .unwrap();
                }
                other => panic!("unexpected direct handshake: {other:?}"),
            }
        });
        (addr, task)
    }

    #[tokio::test]
    async fn nat_hole_response_without_waiter_is_cached() {
        let (state, _server) = test_state();
        handle_nat_hole_response(
            &state,
            "tx-cache".to_string(),
            "xtcp-ssh".to_string(),
            "server".to_string(),
            NatHoleResponse {
                peer_observed_addr: "127.0.0.1:7002".to_string(),
                peer_local_addrs: vec!["10.0.0.20:10002".to_string()],
                waiting: false,
                error: String::new(),
            },
        )
        .await;

        let cached = state
            .nat_hole_cached
            .lock()
            .await
            .remove(&nat_hole_waiter_key("tx-cache", "server"))
            .unwrap();
        assert_eq!(cached.resp.peer_observed_addr, "127.0.0.1:7002");
        assert_eq!(cached.resp.peer_local_addrs, vec!["10.0.0.20:10002"]);
    }

    #[tokio::test]
    async fn request_nat_hole_consumes_cached_response() {
        let (state, _server) = test_state();
        state.nat_hole_cached.lock().await.insert(
            nat_hole_waiter_key("tx-cache", "visitor"),
            NatHoleCachedResponse {
                resp: NatHoleResponse {
                    peer_observed_addr: "127.0.0.1:7001".to_string(),
                    peer_local_addrs: vec!["10.0.0.10:10001".to_string()],
                    waiting: false,
                    error: String::new(),
                },
                cached_at: Instant::now(),
            },
        );

        let resp = request_nat_hole(
            &state,
            "tx-cache",
            "xtcp-ssh",
            "visitor",
            vec!["10.0.0.20:10002".to_string()],
        )
        .await
        .unwrap();

        assert_eq!(resp.peer_observed_addr, "127.0.0.1:7001");
        assert_eq!(resp.peer_local_addrs, vec!["10.0.0.10:10001"]);
        assert!(!state
            .nat_hole_cached
            .lock()
            .await
            .contains_key(&nat_hole_waiter_key("tx-cache", "visitor")));
    }

    #[tokio::test]
    async fn request_nat_hole_ignores_expired_cached_response() {
        let (state, mut server) = test_state();
        state.nat_hole_cached.lock().await.insert(
            nat_hole_waiter_key("tx-stale", "visitor"),
            NatHoleCachedResponse {
                resp: NatHoleResponse {
                    peer_observed_addr: "127.0.0.1:7001".to_string(),
                    peer_local_addrs: vec!["10.0.0.10:10001".to_string()],
                    waiting: false,
                    error: String::new(),
                },
                cached_at: Instant::now() - NAT_HOLE_CACHED_RESPONSE_TTL - Duration::from_secs(1),
            },
        );

        let request_state = state.clone();
        let request = tokio::spawn(async move {
            request_nat_hole(
                &request_state,
                "tx-stale",
                "xtcp-ssh",
                "visitor",
                vec!["10.0.0.20:10002".to_string()],
            )
            .await
        });

        match time::timeout(Duration::from_secs(1), read_msg(&mut server))
            .await
            .unwrap()
            .unwrap()
        {
            Message::NatHoleRegister {
                transaction_id,
                proxy_name,
                role,
                local_addrs,
            } => {
                assert_eq!(transaction_id, "tx-stale");
                assert_eq!(proxy_name, "xtcp-ssh");
                assert_eq!(role, "visitor");
                assert_eq!(local_addrs, vec!["10.0.0.20:10002"]);
            }
            other => panic!("unexpected nat hole register: {other:?}"),
        }

        handle_nat_hole_response(
            &state,
            "tx-stale".to_string(),
            "xtcp-ssh".to_string(),
            "visitor".to_string(),
            NatHoleResponse {
                peer_observed_addr: "127.0.0.1:7003".to_string(),
                peer_local_addrs: vec!["10.0.0.30:10003".to_string()],
                waiting: false,
                error: String::new(),
            },
        )
        .await;

        let resp = request.await.unwrap().unwrap();
        assert_eq!(resp.peer_observed_addr, "127.0.0.1:7003");
        assert_eq!(resp.peer_local_addrs, vec!["10.0.0.30:10003"]);
    }

    #[tokio::test]
    async fn request_nat_hole_with_grace_uses_async_notification_after_waiting() {
        let (state, mut server) = test_state();
        let request_state = state.clone();
        let request = tokio::spawn(async move {
            request_nat_hole_with_grace(
                &request_state,
                "xtcp-delayed",
                "xtcp-delayed",
                "visitor",
                Vec::new(),
                Duration::from_secs(1),
            )
            .await
        });

        match time::timeout(Duration::from_secs(1), read_msg(&mut server))
            .await
            .unwrap()
            .unwrap()
        {
            Message::NatHoleRegister {
                transaction_id,
                proxy_name,
                role,
                ..
            } => {
                assert_eq!(transaction_id, "xtcp-delayed");
                assert_eq!(proxy_name, "xtcp-delayed");
                assert_eq!(role, "visitor");
            }
            other => panic!("unexpected nat hole register: {other:?}"),
        }

        handle_nat_hole_response(
            &state,
            "xtcp-delayed".to_string(),
            "xtcp-delayed".to_string(),
            "visitor".to_string(),
            NatHoleResponse {
                peer_observed_addr: String::new(),
                peer_local_addrs: Vec::new(),
                waiting: true,
                error: String::new(),
            },
        )
        .await;
        handle_nat_hole_response(
            &state,
            "xtcp-delayed".to_string(),
            "xtcp-delayed".to_string(),
            "visitor".to_string(),
            NatHoleResponse {
                peer_observed_addr: "127.0.0.1:7001".to_string(),
                peer_local_addrs: vec!["10.0.0.10:10001".to_string()],
                waiting: false,
                error: String::new(),
            },
        )
        .await;

        let resp = time::timeout(Duration::from_secs(1), request)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!resp.waiting);
        assert_eq!(resp.peer_observed_addr, "127.0.0.1:7001");
        assert_eq!(resp.peer_local_addrs, vec!["10.0.0.10:10001"]);
    }

    #[tokio::test]
    async fn sudp_owner_punches_candidates_from_async_nat_notification() {
        let (mut state, _server) = test_state();
        let proxy = ProxyConfig {
            name: "sudp-echo".to_string(),
            proxy_type: ProxyType::Sudp,
            local_ip: "127.0.0.1".to_string(),
            local_port: 7001,
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
            sk: Some("secret".to_string()),
            health_check: None,
        };
        state.proxies = Arc::new(HashMap::from([(proxy.name.clone(), proxy)]));
        let owner_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        state
            .sudp_direct_owner_sockets
            .lock()
            .await
            .insert("sudp-echo".to_string(), owner_socket);
        let visitor_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let visitor_addr = visitor_socket.local_addr().unwrap().to_string();

        handle_nat_hole_response(
            &state,
            "sudp-echo".to_string(),
            "sudp-echo".to_string(),
            "server".to_string(),
            NatHoleResponse {
                peer_observed_addr: String::new(),
                peer_local_addrs: vec![visitor_addr],
                waiting: false,
                error: String::new(),
            },
        )
        .await;

        let mut buf = vec![0_u8; 1024];
        let (n, _) = time::timeout(Duration::from_secs(1), visitor_socket.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let frame = parse_sudp_direct_frame(&buf[..n]).unwrap();
        assert_eq!(frame.proxy_name, "sudp-echo");
        assert_eq!(frame.session_id, "sudp-echo");
        assert!(frame.ack);
        assert!(frame.probe);
        assert!(!frame.from_visitor);

        let (n, _) = time::timeout(Duration::from_secs(1), visitor_socket.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let retry_frame = parse_sudp_direct_frame(&buf[..n]).unwrap();
        assert_eq!(retry_frame.proxy_name, "sudp-echo");
        assert_eq!(retry_frame.session_id, "sudp-echo");
        assert!(retry_frame.ack);
        assert!(retry_frame.probe);
        assert!(!retry_frame.from_visitor);
    }

    #[tokio::test]
    async fn sudp_group_owner_punches_candidates_from_async_nat_notification() {
        let (mut state, _server) = test_state();
        let proxy = ProxyConfig {
            name: "sudp-echo-a".to_string(),
            proxy_type: ProxyType::Sudp,
            local_ip: "127.0.0.1".to_string(),
            local_port: 7001,
            remote_port: 0,
            group: Some("sudp-echo-group".to_string()),
            group_key: Some("group-secret".to_string()),
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: Some("secret".to_string()),
            health_check: None,
        };
        state.proxies = Arc::new(HashMap::from([(proxy.name.clone(), proxy)]));
        let owner_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        state
            .sudp_direct_owner_sockets
            .lock()
            .await
            .insert("sudp-echo-a".to_string(), owner_socket);
        let visitor_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let visitor_addr = visitor_socket.local_addr().unwrap().to_string();

        handle_nat_hole_response(
            &state,
            "sudp-echo-group".to_string(),
            "sudp-echo-group".to_string(),
            "server".to_string(),
            NatHoleResponse {
                peer_observed_addr: String::new(),
                peer_local_addrs: vec![visitor_addr],
                waiting: false,
                error: String::new(),
            },
        )
        .await;

        let mut buf = vec![0_u8; 1024];
        let (n, _) = time::timeout(Duration::from_secs(1), visitor_socket.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let frame = parse_sudp_direct_frame(&buf[..n]).unwrap();
        assert_eq!(frame.proxy_name, "sudp-echo-group");
        assert_eq!(frame.session_id, "sudp-echo-group");
        assert!(frame.ack);
        assert!(frame.probe);
        assert!(!frame.from_visitor);
    }

    #[tokio::test]
    async fn sudp_direct_probe_retries_until_confirmed() {
        let (state, _server) = test_state();
        let session_id = "visitor|127.0.0.1:50001".to_string();
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        state
            .sudp_direct_visitor_sockets
            .lock()
            .await
            .insert(session_id.clone(), socket);
        state.sudp_direct_pending.lock().await.insert(
            session_id.clone(),
            VecDeque::from([sudp_direct_visitor_frame(
                &VisitorConfig {
                    name: "visitor".to_string(),
                    visitor_type: ProxyType::Sudp,
                    server_name: "sudp-echo".to_string(),
                    bind_addr: "127.0.0.1".to_string(),
                    bind_port: 0,
                    sk: Some("secret".to_string()),
                },
                &session_id,
                b"hello".to_vec(),
                false,
            )]),
        );
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let visitor = VisitorConfig {
            name: "visitor".to_string(),
            visitor_type: ProxyType::Sudp,
            server_name: "sudp-echo".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };

        schedule_sudp_direct_probe_retries(
            state.clone(),
            visitor,
            session_id.clone(),
            vec![peer.local_addr().unwrap()],
        );

        let mut buf = vec![0_u8; 1024];
        for _ in 0..2 {
            let (n, _) = time::timeout(Duration::from_secs(1), peer.recv_from(&mut buf))
                .await
                .unwrap()
                .unwrap();
            let frame = parse_sudp_direct_frame(&buf[..n]).unwrap();
            assert_eq!(frame.session_id, session_id);
            assert!(frame.probe);
            assert!(frame.from_visitor);
        }

        state
            .sudp_direct_confirmed
            .lock()
            .await
            .insert(session_id.clone());
        let extra = time::timeout(Duration::from_millis(700), peer.recv_from(&mut buf)).await;
        assert!(extra.is_err(), "probe retries continued after confirmation");
    }

    #[test]
    fn direct_candidates_dedupes_and_filters_invalid_addrs() {
        let resp = NatHoleResponse {
            peer_observed_addr: "127.0.0.1:9000".to_string(),
            peer_local_addrs: vec![
                "127.0.0.1:7000".to_string(),
                "not-a-socket".to_string(),
                "127.0.0.1:7000".to_string(),
            ],
            waiting: false,
            error: String::new(),
        };

        assert_eq!(
            direct_candidates(&resp),
            vec!["127.0.0.1:7000".to_string(), "127.0.0.1:9000".to_string()]
        );
    }

    #[test]
    fn direct_candidates_prefer_reachable_local_addr_classes() {
        let resp = NatHoleResponse {
            peer_observed_addr: "8.8.4.4:9000".to_string(),
            peer_local_addrs: vec![
                "127.0.0.1:7000".to_string(),
                "10.0.0.2:7000".to_string(),
                "8.8.8.8:7000".to_string(),
            ],
            waiting: false,
            error: String::new(),
        };

        assert_eq!(
            direct_candidates(&resp),
            vec![
                "8.8.8.8:7000".to_string(),
                "10.0.0.2:7000".to_string(),
                "127.0.0.1:7000".to_string(),
                "8.8.4.4:9000".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn xtcp_direct_candidates_race_to_first_successful_handshake() {
        let (slow_addr, slow_task) = spawn_xtcp_direct_test_peer(Duration::from_millis(800)).await;
        let (fast_addr, fast_task) = spawn_xtcp_direct_test_peer(Duration::from_millis(10)).await;
        let visitor = VisitorConfig {
            name: "visitor".to_string(),
            visitor_type: ProxyType::Xtcp,
            server_name: "xtcp-echo".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };

        let start = std::time::Instant::now();
        let (_peer, candidate) =
            connect_xtcp_direct_peer(&visitor, vec![slow_addr.clone(), fast_addr.clone()])
                .await
                .unwrap()
                .unwrap();

        assert_eq!(candidate, fast_addr);
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "candidate racing waited for slow candidate"
        );
        slow_task.abort();
        fast_task.abort();
    }

    #[tokio::test]
    async fn sudp_direct_ack_flushes_pending_payload_to_selected_peer() {
        let (state, _server) = test_state();
        let session_id = "visitor|127.0.0.1:50000".to_string();
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let socket_addr = socket.local_addr().unwrap();
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let visitor = VisitorConfig {
            name: "visitor".to_string(),
            visitor_type: ProxyType::Sudp,
            server_name: "sudp-echo".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };
        state
            .sudp_direct_pending
            .lock()
            .await
            .entry(session_id.clone())
            .or_default()
            .push_back(sudp_direct_visitor_frame(
                &visitor,
                &session_id,
                b"hello sudp".to_vec(),
                false,
            ));
        spawn_sudp_direct_visitor_response_loop(state.clone(), session_id.clone(), socket);

        let ack = DirectSudpFrame {
            magic: DIRECT_SUDP_MAGIC.to_string(),
            proxy_name: "sudp-echo".to_string(),
            session_id: session_id.clone(),
            content: Vec::new(),
            sk: None,
            from_visitor: false,
            ack: true,
            probe: false,
        };
        peer.send_to(&serde_json::to_vec(&ack).unwrap(), socket_addr)
            .await
            .unwrap();

        let mut buf = vec![0_u8; 1024];
        let (n, _) = time::timeout(Duration::from_secs(1), peer.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let frame = parse_sudp_direct_frame(&buf[..n]).unwrap();
        assert_eq!(frame.session_id, session_id);
        assert_eq!(frame.content, b"hello sudp");
        assert!(!frame.probe);
        assert!(state
            .sudp_direct_confirmed
            .lock()
            .await
            .contains(&session_id));
        assert_eq!(
            state
                .sudp_direct_visitor_peers
                .lock()
                .await
                .get(&session_id)
                .copied(),
            Some(peer.local_addr().unwrap())
        );
        assert!(!state
            .sudp_direct_pending
            .lock()
            .await
            .contains_key(&session_id));
    }

    #[tokio::test]
    async fn sudp_relay_fallback_sends_when_direct_unconfirmed() {
        let (state, mut server) = test_state();
        schedule_sudp_relay_fallback(
            state,
            "private-udp".to_string(),
            "session-1".to_string(),
            b"hello".to_vec(),
            Some("secret".to_string()),
        );

        match time::timeout(Duration::from_secs(1), read_msg(&mut server))
            .await
            .unwrap()
            .unwrap()
        {
            Message::SudpPacket {
                proxy_name,
                session_id,
                content,
                sk,
                from_visitor,
            } => {
                assert_eq!(proxy_name, "private-udp");
                assert_eq!(session_id, "session-1");
                assert_eq!(content, b"hello");
                assert_eq!(sk.as_deref(), Some("secret"));
                assert!(from_visitor);
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn sudp_relay_fallback_skips_when_direct_confirmed() {
        let (state, mut server) = test_state();
        state
            .sudp_direct_confirmed
            .lock()
            .await
            .insert("session-1".to_string());
        schedule_sudp_relay_fallback(
            state,
            "private-udp".to_string(),
            "session-1".to_string(),
            b"hello".to_vec(),
            Some("secret".to_string()),
        );

        match time::timeout(Duration::from_millis(900), read_msg(&mut server)).await {
            Err(_) | Ok(Err(_)) => {}
            Ok(Ok(other)) => panic!("confirmed direct session should not fall back: {other:?}"),
        }
    }
}
