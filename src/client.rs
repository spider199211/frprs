use crate::{
    config::{
        ClientConfig, HealthCheckType, ProxyConfig, ProxyType, TransportProtocol, VisitorConfig,
    },
    protocol::{read_msg, write_msg, Message, UdpPacketFrame},
    stun,
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
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, WriteHalf},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{mpsc, oneshot, Mutex, OnceCell, OwnedSemaphorePermit, Semaphore},
    time,
};
use tracing::{debug, error, info, warn};

const DIRECT_SUDP_MAGIC: &str = "frprs-sudp-direct-v1";
const NAT_HOLE_STUN_TIMEOUT: Duration = Duration::from_millis(500);
const NAT_HOLE_STUN_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const NAT_HOLE_CACHED_RESPONSE_TTL: Duration = Duration::from_secs(30);
const NAT_HOLE_CACHED_RESPONSE_LIMIT: usize = 256;
const NAT_HOLE_WAITER_LIMIT: usize = 64;
const NAT_HOLE_WAITING_RETRY_TTL: Duration = Duration::from_secs(1);
const NAT_HOLE_OWNER_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
const DIRECT_CANDIDATE_LIMIT: usize = 8;
const XTCP_DIRECT_FAILURE_TTL: Duration = Duration::from_secs(5);
const XTCP_DIRECT_FAILURE_LIMIT: usize = 256;
const XTCP_DIRECT_PEER_TTL: Duration = Duration::from_secs(30);
const XTCP_DIRECT_PEER_LIMIT: usize = 256;
const SUDP_DIRECT_FAILURE_TTL: Duration = Duration::from_secs(5);
const SUDP_DIRECT_FAILURE_LIMIT: usize = 1024;
const SUDP_DIRECT_PEER_TTL: Duration = Duration::from_secs(30);
const SUDP_DIRECT_PEER_LIMIT: usize = 1024;
const SUDP_DIRECT_SESSION_LIMIT: usize = 1024;
const SUDP_DIRECT_PENDING_LIMIT: usize = 64;
const WORK_CONN_DIAL_LIMIT: usize = 32;
const UDP_PACKET_BATCH_LIMIT: usize = 32;
const UDP_PACKET_BATCH_WAIT: Duration = Duration::from_millis(2);

#[derive(Clone)]
struct ClientState {
    cfg: Arc<ClientConfig>,
    run_id: String,
    proxies: Arc<HashMap<String, ProxyConfig>>,
    udp_sessions: Arc<Mutex<HashMap<(String, String), Arc<UdpSocket>>>>,
    sudp_visitor_sessions: Arc<Mutex<HashMap<String, SudpVisitorSession>>>,
    sudp_direct_owner_sockets: Arc<Mutex<HashMap<String, Arc<UdpSocket>>>>,
    sudp_direct_visitor_sockets: Arc<Mutex<HashMap<String, SudpDirectVisitorEndpoint>>>,
    sudp_direct_visitor_sessions: Arc<Mutex<HashMap<String, Instant>>>,
    sudp_direct_visitor_tasks: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    sudp_direct_visitor_peers: Arc<Mutex<HashMap<String, SudpDirectPeer>>>,
    sudp_direct_pending: Arc<Mutex<HashMap<String, VecDeque<DirectSudpFrame>>>>,
    sudp_direct_confirmed: Arc<Mutex<HashSet<String>>>,
    sudp_direct_probe_sessions: Arc<Mutex<HashSet<String>>>,
    sudp_direct_failures: Arc<Mutex<HashMap<String, Instant>>>,
    nat_hole_waiters: Arc<Mutex<HashMap<String, Vec<oneshot::Sender<NatHoleResponse>>>>>,
    nat_hole_cached: Arc<Mutex<HashMap<String, NatHoleCachedResponse>>>,
    nat_hole_waiting: Arc<Mutex<HashMap<String, Instant>>>,
    stun_server_addrs: Arc<OnceCell<Vec<SocketAddr>>>,
    xtcp_direct_failures: Arc<Mutex<HashMap<String, Instant>>>,
    xtcp_direct_peers: Arc<Mutex<HashMap<String, XtcpDirectPeer>>>,
    writer: Arc<Mutex<WriteHalf<BoxStream>>>,
    quic_session: Option<Arc<transports::quic::QuicClientSession>>,
    tcp_mux_session: Option<Arc<transports::tcp_mux::MuxSession>>,
    work_conn_dial_limiter: Arc<Semaphore>,
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

#[derive(Clone, Copy)]
struct SudpDirectPeer {
    addr: SocketAddr,
    updated_at: Instant,
}

#[derive(Clone)]
struct XtcpDirectPeer {
    candidate: String,
    updated_at: Instant,
}

struct SudpDirectOwnerSession {
    local_socket: Arc<UdpSocket>,
    peer: Arc<Mutex<SocketAddr>>,
    updated_at: StdMutex<Instant>,
    task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}

/// 保存 SUDP visitor 直连 socket 及该 socket 已发现的候选地址。
#[derive(Clone)]
struct SudpDirectVisitorEndpoint {
    socket: Arc<UdpSocket>,
    candidate_addrs: Vec<String>,
}

#[derive(Clone)]
struct NatHoleAnnouncement {
    transaction_id: String,
    proxy_name: String,
    role: &'static str,
    sk: Option<String>,
    local_addrs: Vec<String>,
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
    let session = connect_session_control(&cfg)
        .await
        .with_context(|| format!("connect frps at {server_addr}"))?;
    let mut stream = session.control_stream;
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
        sudp_direct_visitor_sessions: Arc::new(Mutex::new(HashMap::new())),
        sudp_direct_visitor_tasks: Arc::new(Mutex::new(HashMap::new())),
        sudp_direct_visitor_peers: Arc::new(Mutex::new(HashMap::new())),
        sudp_direct_pending: Arc::new(Mutex::new(HashMap::new())),
        sudp_direct_confirmed: Arc::new(Mutex::new(HashSet::new())),
        sudp_direct_probe_sessions: Arc::new(Mutex::new(HashSet::new())),
        sudp_direct_failures: Arc::new(Mutex::new(HashMap::new())),
        nat_hole_waiters: Arc::new(Mutex::new(HashMap::new())),
        nat_hole_cached: Arc::new(Mutex::new(HashMap::new())),
        nat_hole_waiting: Arc::new(Mutex::new(HashMap::new())),
        stun_server_addrs: Arc::new(OnceCell::new()),
        xtcp_direct_failures: Arc::new(Mutex::new(HashMap::new())),
        xtcp_direct_peers: Arc::new(Mutex::new(HashMap::new())),
        writer: Arc::new(Mutex::new(writer)),
        quic_session: session.quic_session,
        tcp_mux_session: session.tcp_mux_session,
        work_conn_dial_limiter: Arc::new(Semaphore::new(WORK_CONN_DIAL_LIMIT)),
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
        TransportProtocol::Tcpmux => {
            bail!("tcp mux streams must be opened through the session multiplexer")
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

struct SessionControl {
    control_stream: BoxStream,
    quic_session: Option<Arc<transports::quic::QuicClientSession>>,
    tcp_mux_session: Option<Arc<transports::tcp_mux::MuxSession>>,
}

async fn connect_session_control(cfg: &ClientConfig) -> Result<SessionControl> {
    if cfg.transport.protocol == TransportProtocol::Quic {
        let server_addr = resolve_server_addr(cfg).await?;
        let session =
            Arc::new(transports::quic::QuicClientSession::connect_insecure(server_addr).await?);
        let stream = session.open_stream().await?;
        return Ok(SessionControl {
            control_stream: Box::new(stream),
            quic_session: Some(session),
            tcp_mux_session: None,
        });
    }

    if cfg.transport.protocol == TransportProtocol::Tcpmux {
        let server_addr = cfg.server_addr();
        let stream = TcpStream::connect(&server_addr)
            .await
            .with_context(|| format!("connect tcp mux frps at {server_addr}"))?;
        configure_tcp_stream(&stream);
        let session = transports::tcp_mux::MuxSession::client(stream);
        let control_stream = session.open_stream().await?;
        return Ok(SessionControl {
            control_stream,
            quic_session: None,
            tcp_mux_session: Some(session),
        });
    }

    Ok(SessionControl {
        control_stream: connect_control(cfg).await?,
        quic_session: None,
        tcp_mux_session: None,
    })
}

async fn open_control_stream(state: &ClientState) -> Result<BoxStream> {
    if let Some(session) = &state.quic_session {
        return Ok(Box::new(session.open_stream().await?));
    }
    if let Some(session) = &state.tcp_mux_session {
        return session.open_stream().await;
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

async fn direct_local_addrs(state: &ClientState, bound_addr: SocketAddr) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut addrs = Vec::new();
    push_direct_local_addr(&mut addrs, &mut seen, bound_addr);

    if bound_addr.ip().is_unspecified() {
        if let Some(ip) = routed_local_ip(&state.cfg).await {
            push_direct_local_addr(
                &mut addrs,
                &mut seen,
                SocketAddr::new(ip, bound_addr.port()),
            );
        }
    }

    addrs
}

/// 合并 SUDP 直连 socket 的本地候选与由同一 socket 探测到的公网映射候选。
async fn sudp_socket_candidate_addrs(
    state: &ClientState,
    socket: &UdpSocket,
    bound_addr: SocketAddr,
) -> Vec<String> {
    let mut addrs = direct_local_addrs(state, bound_addr).await;
    let Some(mapped_addr) = discover_sudp_mapped_addr(state, socket).await else {
        return addrs;
    };
    if !is_usable_direct_candidate(&mapped_addr)
        || addrs
            .iter()
            .any(|addr| addr.parse::<SocketAddr>().ok() == Some(mapped_addr))
    {
        return addrs;
    }

    // 公网映射优先公告，使候选排序在错误的控制连接来源端口之前尝试真实 SUDP 映射。
    addrs.insert(0, mapped_addr.to_string());
    addrs
}

/// 通过显式配置的 STUN Binding 服务发现当前 SUDP socket 的公网映射地址。
async fn discover_sudp_mapped_addr(state: &ClientState, socket: &UdpSocket) -> Option<SocketAddr> {
    let stun_server = state
        .cfg
        .transport
        .nat_hole_stun_server
        .as_deref()
        .filter(|server| !server.trim().is_empty())?;
    let local_addr = socket.local_addr().ok()?;
    let deadline = Instant::now() + NAT_HOLE_STUN_TIMEOUT;
    let remaining = deadline.checked_duration_since(Instant::now())?;
    let server_addrs = time::timeout(
        remaining,
        state.stun_server_addrs.get_or_try_init(|| async {
            tokio::net::lookup_host(stun_server)
                .await
                .map(|addrs| addrs.collect::<Vec<_>>())
        }),
    )
    .await
    .ok()?
    .ok()?;
    let server_addr = server_addrs
        .iter()
        .copied()
        .find(|addr| addr.is_ipv4() == local_addr.is_ipv4())?;
    let (request, transaction_id) = stun::new_binding_request();
    let mut buf = [0_u8; 512];
    let mut attempts = 0_u8;
    while deadline.checked_duration_since(Instant::now()).is_some() {
        attempts += 1;
        if let Err(err) = socket.send_to(&request, server_addr).await {
            debug!("send SUDP STUN request to {server_addr} failed: {err:#}");
            return None;
        }

        let receive_deadline = deadline.min(Instant::now() + NAT_HOLE_STUN_RETRY_INTERVAL);
        while let Some(remaining) = receive_deadline.checked_duration_since(Instant::now()) {
            let received = time::timeout(remaining, socket.recv_from(&mut buf)).await;
            let (len, peer) = match received {
                Ok(Ok(received)) => received,
                Ok(Err(err)) => {
                    debug!("receive SUDP STUN response failed: {err:#}");
                    return None;
                }
                Err(_) => break,
            };
            if peer != server_addr {
                continue;
            }
            if let Some(mapped_addr) = stun::parse_binding_response(&buf[..len], &transaction_id) {
                return Some(mapped_addr);
            }
        }
    }
    debug!("SUDP STUN request to {server_addr} timed out after {attempts} attempts");
    None
}

fn push_direct_local_addr(
    addrs: &mut Vec<String>,
    seen: &mut HashSet<SocketAddr>,
    addr: SocketAddr,
) {
    if is_usable_direct_candidate(&addr) && seen.insert(addr) {
        addrs.push(addr.to_string());
    }
}

async fn routed_local_ip(cfg: &ClientConfig) -> Option<IpAddr> {
    let server_addr = resolve_server_addr(cfg).await.ok()?;
    let bind_addr = if server_addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind_addr).await.ok()?;
    socket.connect(server_addr).await.ok()?;
    let ip = socket.local_addr().ok()?.ip();
    (!ip.is_unspecified()).then_some(ip)
}

async fn close_proxy(state: &ClientState, proxy_name: &str) -> Result<()> {
    state
        .send(&Message::CloseProxy {
            proxy_name: proxy_name.to_string(),
        })
        .await
}

fn nat_hole_waiter_key(transaction_id: &str, proxy_name: &str, role: &str) -> String {
    format!("{transaction_id}|{proxy_name}|{role}")
}

async fn handle_nat_hole_response(
    state: &ClientState,
    transaction_id: String,
    proxy_name: String,
    role: String,
    resp: NatHoleResponse,
) {
    let key = nat_hole_waiter_key(&transaction_id, &proxy_name, &role);
    if let Some(waiters) = state.nat_hole_waiters.lock().await.remove(&key) {
        for waiter in waiters {
            let _ = waiter.send(resp.clone());
        }
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
    cache_nat_hole_response(state, key, resp).await;
}

async fn take_cached_nat_hole_response(state: &ClientState, key: &str) -> Option<NatHoleResponse> {
    let mut cached = state.nat_hole_cached.lock().await;
    let cached_resp = cached.remove(key)?;
    (cached_resp.cached_at.elapsed() <= NAT_HOLE_CACHED_RESPONSE_TTL).then_some(cached_resp.resp)
}

async fn cache_nat_hole_response(state: &ClientState, key: String, resp: NatHoleResponse) {
    let now = Instant::now();
    let mut cached = state.nat_hole_cached.lock().await;
    cached.retain(|_, cached_resp| {
        now.duration_since(cached_resp.cached_at) <= NAT_HOLE_CACHED_RESPONSE_TTL
    });
    if cached.len() >= NAT_HOLE_CACHED_RESPONSE_LIMIT {
        if let Some(oldest_key) = cached
            .iter()
            .min_by_key(|(_, cached_resp)| cached_resp.cached_at)
            .map(|(key, _)| key.clone())
        {
            cached.remove(&oldest_key);
        }
    }
    cached.insert(
        key,
        NatHoleCachedResponse {
            resp,
            cached_at: now,
        },
    );
}

/// 测试未配置密钥时的 NAT 注册兼容路径。
#[cfg(test)]
async fn request_nat_hole(
    state: &ClientState,
    transaction_id: &str,
    proxy_name: &str,
    role: &str,
    local_addrs: Vec<String>,
) -> Result<NatHoleResponse> {
    request_nat_hole_with_secret(state, transaction_id, proxy_name, role, None, local_addrs).await
}

/// 发送携带私有代理密钥的 NAT 注册，并复用同一事务的等待与缓存状态。
async fn request_nat_hole_with_secret(
    state: &ClientState,
    transaction_id: &str,
    proxy_name: &str,
    role: &str,
    sk: Option<String>,
    local_addrs: Vec<String>,
) -> Result<NatHoleResponse> {
    let key = nat_hole_waiter_key(transaction_id, proxy_name, role);
    if let Some(resp) = take_cached_nat_hole_response(state, &key).await {
        if !resp.error.is_empty() {
            bail!("nat hole register failed: {}", resp.error);
        }
        clear_nat_hole_waiting(state, &key).await;
        return Ok(resp);
    }
    if should_skip_nat_hole_waiting(state, &key).await {
        debug!("skip nat hole register for {key} after recent waiting response");
        return Ok(NatHoleResponse {
            peer_observed_addr: String::new(),
            peer_local_addrs: Vec::new(),
            waiting: true,
            error: String::new(),
        });
    }

    let (tx, rx) = oneshot::channel();
    let should_send = add_nat_hole_waiter(state, key.clone(), tx).await?;
    if should_send {
        let send_result = state
            .send(&Message::NatHoleRegister {
                transaction_id: transaction_id.to_string(),
                proxy_name: proxy_name.to_string(),
                role: role.to_string(),
                sk,
                local_addrs,
            })
            .await;
        if let Err(err) = send_result {
            state.nat_hole_waiters.lock().await.remove(&key);
            return Err(err);
        }
    } else {
        debug!("coalesced nat hole register for {key} onto existing waiter");
    }

    let resp = match time::timeout(Duration::from_secs(5), rx).await {
        Ok(resp) => resp.context("nat hole response channel closed")?,
        Err(err) => {
            remove_closed_nat_hole_waiters(state, &key).await;
            return Err(err).context("wait for nat hole response timed out");
        }
    };
    if !resp.error.is_empty() {
        clear_nat_hole_waiting(state, &key).await;
        bail!("nat hole register failed: {}", resp.error);
    }
    if resp.waiting {
        mark_nat_hole_waiting(state, &key).await;
    } else {
        clear_nat_hole_waiting(state, &key).await;
    }
    Ok(resp)
}

/// 测试未配置密钥时带短暂异步等待的 NAT 注册兼容路径。
#[cfg(test)]
async fn request_nat_hole_with_grace(
    state: &ClientState,
    transaction_id: &str,
    proxy_name: &str,
    role: &str,
    local_addrs: Vec<String>,
    grace: Duration,
) -> Result<NatHoleResponse> {
    request_nat_hole_with_grace_secret(
        state,
        transaction_id,
        proxy_name,
        role,
        None,
        local_addrs,
        grace,
    )
    .await
}

/// 在短暂等待异步对端通知时，发送携带私有代理密钥的 NAT 注册。
async fn request_nat_hole_with_grace_secret(
    state: &ClientState,
    transaction_id: &str,
    proxy_name: &str,
    role: &str,
    sk: Option<String>,
    local_addrs: Vec<String>,
    grace: Duration,
) -> Result<NatHoleResponse> {
    let resp =
        request_nat_hole_with_secret(state, transaction_id, proxy_name, role, sk, local_addrs)
            .await?;
    if !resp.waiting || grace.is_zero() {
        return Ok(resp);
    }

    let key = nat_hole_waiter_key(transaction_id, proxy_name, role);
    if let Some(resp) = take_cached_nat_hole_response(state, &key).await {
        if !resp.error.is_empty() {
            bail!("nat hole register failed: {}", resp.error);
        }
        clear_nat_hole_waiting(state, &key).await;
        return Ok(resp);
    }

    let (tx, rx) = oneshot::channel();
    if let Err(err) = add_nat_hole_waiter(state, key.clone(), tx).await {
        debug!("skip nat hole grace waiter for {key}: {err:#}");
        return Ok(resp);
    }
    match time::timeout(grace, rx).await {
        Ok(Ok(resp)) => {
            if !resp.error.is_empty() {
                clear_nat_hole_waiting(state, &key).await;
                bail!("nat hole register failed: {}", resp.error);
            }
            if resp.waiting {
                mark_nat_hole_waiting(state, &key).await;
            } else {
                clear_nat_hole_waiting(state, &key).await;
            }
            Ok(resp)
        }
        Ok(Err(_)) => {
            remove_closed_nat_hole_waiters(state, &key).await;
            Ok(resp)
        }
        Err(_) => {
            remove_closed_nat_hole_waiters(state, &key).await;
            Ok(resp)
        }
    }
}

async fn add_nat_hole_waiter(
    state: &ClientState,
    key: String,
    tx: oneshot::Sender<NatHoleResponse>,
) -> Result<bool> {
    let mut waiters = state.nat_hole_waiters.lock().await;
    let entries = waiters.entry(key.clone()).or_default();
    if entries.len() >= NAT_HOLE_WAITER_LIMIT {
        bail!("too many nat hole waiters for {key}");
    }
    let should_send = entries.is_empty();
    entries.push(tx);
    Ok(should_send)
}

async fn remove_closed_nat_hole_waiters(state: &ClientState, key: &str) {
    let mut waiters = state.nat_hole_waiters.lock().await;
    if let Some(entries) = waiters.get_mut(key) {
        entries.retain(|tx| !tx.is_closed());
        if entries.is_empty() {
            waiters.remove(key);
        }
    }
}

async fn should_skip_nat_hole_waiting(state: &ClientState, key: &str) -> bool {
    let mut waiting = state.nat_hole_waiting.lock().await;
    let Some(waiting_at) = waiting.get(key).copied() else {
        return false;
    };
    if waiting_at.elapsed() <= NAT_HOLE_WAITING_RETRY_TTL {
        return true;
    }
    waiting.remove(key);
    false
}

async fn mark_nat_hole_waiting(state: &ClientState, key: &str) {
    state
        .nat_hole_waiting
        .lock()
        .await
        .insert(key.to_string(), Instant::now());
}

async fn clear_nat_hole_waiting(state: &ClientState, key: &str) {
    state.nat_hole_waiting.lock().await.remove(key);
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

/// 通过控制通道发送 owner NAT 候选及其私有代理密钥。
async fn announce_nat_hole(
    state: &ClientState,
    transaction_id: &str,
    proxy_name: &str,
    role: &str,
    sk: Option<String>,
    local_addrs: Vec<String>,
) -> Result<()> {
    state
        .send(&Message::NatHoleRegister {
            transaction_id: transaction_id.to_string(),
            proxy_name: proxy_name.to_string(),
            role: role.to_string(),
            sk,
            local_addrs,
        })
        .await
}

/// 发送单条预构建的 NAT owner 公告。
async fn announce_nat_hole_entry(
    state: &ClientState,
    announcement: &NatHoleAnnouncement,
) -> Result<()> {
    announce_nat_hole(
        state,
        &announcement.transaction_id,
        &announcement.proxy_name,
        announcement.role,
        announcement.sk.clone(),
        announcement.local_addrs.clone(),
    )
    .await
}

async fn announce_nat_hole_entries(
    state: &ClientState,
    announcements: &[NatHoleAnnouncement],
) -> Result<()> {
    for announcement in announcements {
        announce_nat_hole_entry(state, announcement).await?;
    }
    Ok(())
}

/// 为代理名及可选分组名构建共享同一密钥的 owner NAT 公告。
fn owner_nat_hole_announcements(
    proxy: &ProxyConfig,
    local_addrs: Vec<String>,
) -> Vec<NatHoleAnnouncement> {
    let mut announcements = vec![NatHoleAnnouncement {
        transaction_id: proxy.name.clone(),
        proxy_name: proxy.name.clone(),
        role: "server",
        sk: proxy.sk.clone(),
        local_addrs: local_addrs.clone(),
    }];
    if let Some(group_name) = proxy.group.as_deref().filter(|group| !group.is_empty()) {
        announcements.push(NatHoleAnnouncement {
            transaction_id: group_name.to_string(),
            proxy_name: group_name.to_string(),
            role: "server",
            sk: proxy.sk.clone(),
            local_addrs,
        });
    }
    announcements
}

fn spawn_nat_hole_owner_refresh(
    state: ClientState,
    proxy_name: String,
    announcements: Vec<NatHoleAnnouncement>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            time::sleep(interval).await;
            if let Err(err) = announce_nat_hole_entries(&state, &announcements).await {
                debug!("stop nat hole refresh for {proxy_name}: {err:#}");
                break;
            }
        }
    })
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
    let local_addrs = direct_local_addrs(&state, local_addr).await;
    let advertised_addr = if local_addrs.is_empty() {
        local_addr.to_string()
    } else {
        local_addrs.join(",")
    };

    let announcements = owner_nat_hole_announcements(&proxy, local_addrs);
    announce_nat_hole_entries(&state, &announcements).await?;
    let _refresh = spawn_nat_hole_owner_refresh(
        state.clone(),
        proxy.name.clone(),
        announcements,
        NAT_HOLE_OWNER_REFRESH_INTERVAL,
    );

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

    let local_addr = format!("{}:{}", proxy.local_ip, proxy.local_port);
    let mut local = match TcpStream::connect(&local_addr).await {
        Ok(local) => local,
        Err(err) => {
            write_msg(
                &mut stream,
                &Message::NewVisitorConnResp {
                    proxy_name: requested_proxy,
                    error: format!("connect local xtcp service failed: {err}"),
                },
            )
            .await?;
            return Ok(());
        }
    };
    configure_tcp_stream(&local);

    write_msg(
        &mut stream,
        &Message::NewVisitorConnResp {
            proxy_name: requested_proxy.clone(),
            error: String::new(),
        },
    )
    .await?;
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
    if should_skip_xtcp_direct(state, &visitor.server_name).await {
        debug!(
            "skip xtcp direct lookup for {} after recent candidate failure",
            visitor.server_name
        );
        return Ok(false);
    }

    if let Some(candidate) = take_fresh_xtcp_direct_peer(state, &visitor.server_name).await {
        match connect_xtcp_direct_candidate_with_timeout(visitor, &candidate).await {
            Ok(mut peer) => {
                clear_xtcp_direct_failure(state, &visitor.server_name).await;
                info!(
                    "xtcp visitor {} reused direct peer {}",
                    visitor.name, candidate
                );
                io::copy_bidirectional(inbound, &mut peer)
                    .await
                    .with_context(|| {
                        format!("copy xtcp direct visitor bytes for {}", visitor.name)
                    })?;
                return Ok(true);
            }
            Err(err) => {
                debug!("xtcp cached direct candidate {candidate} failed: {err:#}");
                clear_xtcp_direct_peer(state, &visitor.server_name).await;
            }
        }
    }

    let resp = match request_nat_hole_with_grace_secret(
        state,
        &visitor.server_name,
        &visitor.server_name,
        "visitor",
        visitor.sk.clone(),
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

    let candidates = direct_candidates(&resp);
    if let Some((mut peer, candidate)) =
        connect_xtcp_direct_peer(visitor, candidates.clone()).await?
    {
        clear_xtcp_direct_failure(state, &visitor.server_name).await;
        record_xtcp_direct_peer(state, &visitor.server_name, candidate.clone()).await;
        info!(
            "xtcp visitor {} connected directly to {}",
            visitor.name, candidate
        );
        io::copy_bidirectional(inbound, &mut peer)
            .await
            .with_context(|| format!("copy xtcp direct visitor bytes for {}", visitor.name))?;
        return Ok(true);
    }

    if !candidates.is_empty() {
        mark_xtcp_direct_failure(state, &visitor.server_name).await;
        clear_xtcp_direct_peer(state, &visitor.server_name).await;
    }

    Ok(false)
}

async fn should_skip_xtcp_direct(state: &ClientState, server_name: &str) -> bool {
    let mut failures = state.xtcp_direct_failures.lock().await;
    let Some(failed_at) = failures.get(server_name).copied() else {
        return false;
    };
    if failed_at.elapsed() <= XTCP_DIRECT_FAILURE_TTL {
        return true;
    }
    failures.remove(server_name);
    false
}

async fn mark_xtcp_direct_failure(state: &ClientState, server_name: &str) {
    let now = Instant::now();
    let mut failures = state.xtcp_direct_failures.lock().await;
    prune_instant_cache(&mut failures, XTCP_DIRECT_FAILURE_TTL, now);
    trim_oldest_instant_cache(&mut failures, XTCP_DIRECT_FAILURE_LIMIT);
    failures.insert(server_name.to_string(), now);
}

async fn clear_xtcp_direct_failure(state: &ClientState, server_name: &str) {
    state.xtcp_direct_failures.lock().await.remove(server_name);
}

async fn take_fresh_xtcp_direct_peer(state: &ClientState, server_name: &str) -> Option<String> {
    let mut peers = state.xtcp_direct_peers.lock().await;
    let peer = peers.get(server_name).cloned()?;
    if peer.updated_at.elapsed() <= XTCP_DIRECT_PEER_TTL {
        return Some(peer.candidate);
    }
    peers.remove(server_name);
    None
}

async fn record_xtcp_direct_peer(state: &ClientState, server_name: &str, candidate: String) {
    let now = Instant::now();
    let mut peers = state.xtcp_direct_peers.lock().await;
    prune_xtcp_direct_peer_cache(&mut peers, now);
    trim_oldest_xtcp_direct_peer_cache(&mut peers);
    peers.insert(
        server_name.to_string(),
        XtcpDirectPeer {
            candidate,
            updated_at: now,
        },
    );
}

async fn clear_xtcp_direct_peer(state: &ClientState, server_name: &str) {
    state.xtcp_direct_peers.lock().await.remove(server_name);
}

fn prune_instant_cache(cache: &mut HashMap<String, Instant>, ttl: Duration, now: Instant) {
    cache.retain(|_, updated_at| now.duration_since(*updated_at) <= ttl);
}

fn trim_oldest_instant_cache(cache: &mut HashMap<String, Instant>, limit: usize) {
    while cache.len() >= limit {
        let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, updated_at)| **updated_at)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        cache.remove(&oldest);
    }
}

fn prune_xtcp_direct_peer_cache(cache: &mut HashMap<String, XtcpDirectPeer>, now: Instant) {
    cache.retain(|_, peer| now.duration_since(peer.updated_at) <= XTCP_DIRECT_PEER_TTL);
}

fn trim_oldest_xtcp_direct_peer_cache(cache: &mut HashMap<String, XtcpDirectPeer>) {
    while cache.len() >= XTCP_DIRECT_PEER_LIMIT {
        let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, peer)| peer.updated_at)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        cache.remove(&oldest);
    }
}

async fn connect_xtcp_direct_peer(
    visitor: &VisitorConfig,
    candidates: Vec<String>,
) -> Result<Option<(TcpStream, String)>> {
    if candidates.is_empty() {
        return Ok(None);
    }

    let (tx, mut rx) = mpsc::channel(candidates.len());
    let mut handles = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let tx = tx.clone();
        let visitor = visitor.clone();
        let handle = tokio::spawn(async move {
            let result = connect_xtcp_direct_candidate_with_timeout(&visitor, &candidate).await;
            let _ = tx.send((candidate, result)).await;
        });
        handles.push(handle);
    }
    drop(tx);

    while let Some((candidate, result)) = rx.recv().await {
        match result {
            Ok(peer) => {
                for handle in handles {
                    handle.abort();
                }
                return Ok(Some((peer, candidate)));
            }
            Err(err) => debug!("xtcp direct candidate {candidate} failed: {err:#}"),
        }
    }
    Ok(None)
}

async fn connect_xtcp_direct_candidate_with_timeout(
    visitor: &VisitorConfig,
    candidate: &str,
) -> Result<TcpStream> {
    time::timeout(
        Duration::from_secs(2),
        connect_xtcp_direct_candidate(visitor, candidate),
    )
    .await
    .unwrap_or_else(|_| Err(anyhow!("xtcp direct candidate timed out")))
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
        Message::NewVisitorConnResp { proxy_name, error }
            if error.is_empty() && proxy_name == visitor.server_name =>
        {
            Ok(peer)
        }
        Message::NewVisitorConnResp { proxy_name, error } if error.is_empty() => {
            bail!(
                "xtcp direct peer responded for unexpected proxy {proxy_name}, expected {}",
                visitor.server_name
            )
        }
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
        if !is_usable_direct_candidate(&addr) {
            debug!("ignore unusable direct candidate {candidate}");
            continue;
        }
        if seen.insert(addr) {
            candidates.push((direct_candidate_rank(source, &addr), candidate.clone()));
        }
    }
    candidates.sort_by_key(|(rank, _)| *rank);
    candidates
        .into_iter()
        .take(DIRECT_CANDIDATE_LIMIT)
        .map(|(_, candidate)| candidate)
        .collect()
}

fn direct_candidate_rank(source: DirectCandidateSource, addr: &SocketAddr) -> (u8, u8) {
    (direct_candidate_addr_rank(addr.ip()), source as u8)
}

fn is_usable_direct_candidate(addr: &SocketAddr) -> bool {
    if addr.port() == 0 {
        return false;
    }
    match addr.ip() {
        IpAddr::V4(ip) => !(ip.is_unspecified() || ip.is_multicast() || ip.is_broadcast()),
        IpAddr::V6(ip) => !(ip.is_unspecified() || ip.is_multicast()),
    }
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
    let local_addr = socket.local_addr().context("read sudp direct owner addr")?;
    let local_addrs = sudp_socket_candidate_addrs(&state, &socket, local_addr).await;
    let advertised_addr = if local_addrs.is_empty() {
        local_addr.to_string()
    } else {
        local_addrs.join(",")
    };
    state
        .sudp_direct_owner_sockets
        .lock()
        .await
        .insert(proxy.name.clone(), socket.clone());
    let announcements = owner_nat_hole_announcements(&proxy, local_addrs);
    announce_nat_hole_entries(&state, &announcements).await?;
    let _refresh = spawn_nat_hole_owner_refresh(
        state.clone(),
        proxy.name.clone(),
        announcements,
        NAT_HOLE_OWNER_REFRESH_INTERVAL,
    );
    info!(
        "sudp direct owner {} listening on {}",
        proxy.name, advertised_addr
    );

    let sessions = Arc::new(Mutex::new(
        HashMap::<String, Arc<SudpDirectOwnerSession>>::new(),
    ));
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
                &frame.session_id,
                &frame.proxy_name,
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
    session_id: &str,
    response_proxy_name: &str,
    direct_socket: Arc<UdpSocket>,
    sessions: Arc<Mutex<HashMap<String, Arc<SudpDirectOwnerSession>>>>,
    peer: SocketAddr,
) -> Result<Arc<UdpSocket>> {
    if let Some(session) = sessions.lock().await.get(session_id).cloned() {
        *session.peer.lock().await = peer;
        *session
            .updated_at
            .lock()
            .expect("sudp direct owner session mutex poisoned") = Instant::now();
        return Ok(session.local_socket.clone());
    }

    let socket = Arc::new(
        UdpSocket::bind("0.0.0.0:0")
            .await
            .context("bind sudp direct local relay socket")?,
    );
    let session = {
        let mut sessions_guard = sessions.lock().await;
        if let Some(existing) = sessions_guard.get(session_id) {
            existing.clone()
        } else {
            enforce_sudp_direct_owner_session_limit_locked(&mut sessions_guard, session_id);
            let session = Arc::new(SudpDirectOwnerSession {
                local_socket: socket,
                peer: Arc::new(Mutex::new(peer)),
                updated_at: StdMutex::new(Instant::now()),
                task: StdMutex::new(None),
            });
            sessions_guard.insert(session_id.to_string(), session.clone());
            let task = spawn_sudp_direct_owner_response_loop(
                response_proxy_name.to_string(),
                session_id.to_string(),
                session.clone(),
                direct_socket,
                sessions.clone(),
            );
            *session
                .task
                .lock()
                .expect("sudp direct owner task mutex poisoned") = Some(task);
            session
        }
    };
    *session.peer.lock().await = peer;
    *session
        .updated_at
        .lock()
        .expect("sudp direct owner session mutex poisoned") = Instant::now();
    Ok(session.local_socket.clone())
}

fn enforce_sudp_direct_owner_session_limit_locked(
    sessions: &mut HashMap<String, Arc<SudpDirectOwnerSession>>,
    keep_session_id: &str,
) {
    while sessions.len() >= SUDP_DIRECT_SESSION_LIMIT {
        let Some(oldest) = sessions
            .iter()
            .filter(|(session_id, _)| session_id.as_str() != keep_session_id)
            .min_by_key(|(_, session)| {
                *session
                    .updated_at
                    .lock()
                    .expect("sudp direct owner session mutex poisoned")
            })
            .map(|(session_id, _)| session_id.clone())
        else {
            break;
        };
        remove_sudp_direct_owner_session_locked(sessions, &oldest);
    }
}

fn remove_sudp_direct_owner_session_locked(
    sessions: &mut HashMap<String, Arc<SudpDirectOwnerSession>>,
    session_id: &str,
) {
    if let Some(session) = sessions.remove(session_id) {
        if let Some(task) = session
            .task
            .lock()
            .expect("sudp direct owner task mutex poisoned")
            .take()
        {
            task.abort();
        }
    }
}

fn spawn_sudp_direct_owner_response_loop(
    proxy_name: String,
    session_id: String,
    session: Arc<SudpDirectOwnerSession>,
    direct_socket: Arc<UdpSocket>,
    sessions: Arc<Mutex<HashMap<String, Arc<SudpDirectOwnerSession>>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf = vec![0_u8; 64 * 1024];
        loop {
            let received = time::timeout(
                Duration::from_secs(60),
                session.local_socket.recv_from(&mut buf),
            )
            .await;
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
            let peer = *session.peer.lock().await;
            if let Err(err) = direct_socket.send_to(&payload, peer).await {
                debug!("send sudp direct response failed: {err:#}");
                break;
            }
        }

        let mut sessions = sessions.lock().await;
        if sessions
            .get(&session_id)
            .map(|current| Arc::ptr_eq(current, &session))
            .unwrap_or(false)
        {
            sessions.remove(&session_id);
        }
    })
}

async fn try_sudp_direct_visitor(
    state: &ClientState,
    visitor: &VisitorConfig,
    session_id: &str,
    content: &[u8],
) -> Result<bool> {
    if should_skip_sudp_direct(state, session_id).await {
        debug!("skip sudp direct lookup for {session_id} after recent fallback");
        return Ok(false);
    }

    let direct_endpoint = sudp_direct_visitor_socket(state, visitor, session_id).await?;
    let direct_socket = direct_endpoint.socket;
    if let Some(peer) = take_fresh_sudp_direct_peer(state, session_id).await {
        match send_sudp_direct_payload(&direct_socket, visitor, session_id, content, peer).await {
            Ok(()) => {
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

    let resp = match request_nat_hole_with_grace_secret(
        state,
        &visitor.server_name,
        &visitor.server_name,
        "visitor",
        visitor.sk.clone(),
        direct_endpoint.candidate_addrs,
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

    let mut candidates = Vec::new();
    for candidate in direct_candidates(&resp) {
        let Ok(addr) = candidate.parse::<SocketAddr>() else {
            debug!("ignore invalid sudp direct candidate {candidate}");
            continue;
        };
        candidates.push(addr);
    }
    if candidates.is_empty() {
        return Ok(false);
    }

    push_sudp_direct_pending(state, visitor, session_id, content).await;
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

async fn should_skip_sudp_direct(state: &ClientState, session_id: &str) -> bool {
    let mut failures = state.sudp_direct_failures.lock().await;
    let Some(failed_at) = failures.get(session_id).copied() else {
        return false;
    };
    if failed_at.elapsed() <= SUDP_DIRECT_FAILURE_TTL {
        return true;
    }
    failures.remove(session_id);
    false
}

async fn mark_sudp_direct_failure(state: &ClientState, session_id: &str) {
    let now = Instant::now();
    let mut failures = state.sudp_direct_failures.lock().await;
    prune_instant_cache(&mut failures, SUDP_DIRECT_FAILURE_TTL, now);
    trim_oldest_instant_cache(&mut failures, SUDP_DIRECT_FAILURE_LIMIT);
    failures.insert(session_id.to_string(), now);
}

async fn clear_sudp_direct_failure(state: &ClientState, session_id: &str) {
    state.sudp_direct_failures.lock().await.remove(session_id);
}

async fn take_fresh_sudp_direct_peer(state: &ClientState, session_id: &str) -> Option<SocketAddr> {
    let mut peers = state.sudp_direct_visitor_peers.lock().await;
    let peer = peers.get(session_id).copied()?;
    if peer.updated_at.elapsed() <= SUDP_DIRECT_PEER_TTL {
        return Some(peer.addr);
    }
    peers.remove(session_id);
    drop(peers);
    state.sudp_direct_confirmed.lock().await.remove(session_id);
    None
}

async fn record_sudp_direct_peer(state: &ClientState, session_id: &str, addr: SocketAddr) {
    let now = Instant::now();
    let removed = {
        let mut peers = state.sudp_direct_visitor_peers.lock().await;
        let mut removed = prune_sudp_direct_peer_cache(&mut peers, now);
        removed.extend(trim_oldest_sudp_direct_peer_cache(&mut peers));
        peers.insert(
            session_id.to_string(),
            SudpDirectPeer {
                addr,
                updated_at: now,
            },
        );
        removed
    };
    if !removed.is_empty() {
        let mut confirmed = state.sudp_direct_confirmed.lock().await;
        for session_id in removed {
            confirmed.remove(&session_id);
        }
    }
}

fn prune_sudp_direct_peer_cache(
    cache: &mut HashMap<String, SudpDirectPeer>,
    now: Instant,
) -> Vec<String> {
    let expired = cache
        .iter()
        .filter(|(_, peer)| now.duration_since(peer.updated_at) > SUDP_DIRECT_PEER_TTL)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in &expired {
        cache.remove(key);
    }
    expired
}

fn trim_oldest_sudp_direct_peer_cache(cache: &mut HashMap<String, SudpDirectPeer>) -> Vec<String> {
    let mut removed = Vec::new();
    while cache.len() >= SUDP_DIRECT_PEER_LIMIT {
        let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, peer)| peer.updated_at)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        cache.remove(&oldest);
        removed.push(oldest);
    }
    removed
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
                || should_skip_sudp_direct(&state, &session_id).await
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
            let Some(endpoint) = socket else {
                break;
            };
            if let Err(err) =
                send_sudp_direct_probe(&endpoint.socket, &visitor, &session_id, &candidates).await
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

async fn push_sudp_direct_pending(
    state: &ClientState,
    visitor: &VisitorConfig,
    session_id: &str,
    content: &[u8],
) {
    let mut pending = state.sudp_direct_pending.lock().await;
    let queue = pending.entry(session_id.to_string()).or_default();
    while queue.len() >= SUDP_DIRECT_PENDING_LIMIT {
        queue.pop_front();
    }
    queue.push_back(sudp_direct_visitor_frame(
        visitor,
        session_id,
        content.to_vec(),
        false,
    ));
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

async fn touch_sudp_direct_visitor_session(state: &ClientState, session_id: &str) {
    state
        .sudp_direct_visitor_sessions
        .lock()
        .await
        .insert(session_id.to_string(), Instant::now());
}

async fn enforce_sudp_direct_visitor_session_limit(state: &ClientState, keep_session_id: &str) {
    loop {
        let evicted = {
            let mut sessions = state.sudp_direct_visitor_sessions.lock().await;
            if sessions.len() <= SUDP_DIRECT_SESSION_LIMIT {
                return;
            }
            let oldest = sessions
                .iter()
                .filter(|(session_id, _)| session_id.as_str() != keep_session_id)
                .min_by_key(|(_, updated_at)| **updated_at)
                .map(|(session_id, _)| session_id.clone());
            if let Some(oldest) = &oldest {
                sessions.remove(oldest);
            }
            oldest
        };
        let Some(evicted) = evicted else {
            return;
        };
        remove_sudp_direct_visitor_session_state(state, &evicted, true).await;
    }
}

async fn remove_sudp_direct_visitor_session_state(
    state: &ClientState,
    session_id: &str,
    abort_task: bool,
) {
    if let Some(task) = state
        .sudp_direct_visitor_tasks
        .lock()
        .await
        .remove(session_id)
    {
        if abort_task {
            task.abort();
        }
    }
    state
        .sudp_direct_visitor_sessions
        .lock()
        .await
        .remove(session_id);
    state
        .sudp_direct_visitor_sockets
        .lock()
        .await
        .remove(session_id);
    state
        .sudp_direct_visitor_peers
        .lock()
        .await
        .remove(session_id);
    state.sudp_direct_pending.lock().await.remove(session_id);
    state.sudp_direct_confirmed.lock().await.remove(session_id);
    state
        .sudp_direct_probe_sessions
        .lock()
        .await
        .remove(session_id);
    state.sudp_direct_failures.lock().await.remove(session_id);
}

async fn sudp_direct_visitor_socket(
    state: &ClientState,
    visitor: &VisitorConfig,
    session_id: &str,
) -> Result<SudpDirectVisitorEndpoint> {
    if let Some(endpoint) = state
        .sudp_direct_visitor_sockets
        .lock()
        .await
        .get(session_id)
        .cloned()
    {
        touch_sudp_direct_visitor_session(state, session_id).await;
        ensure_sudp_direct_visitor_task(
            state,
            session_id,
            &visitor.server_name,
            endpoint.socket.clone(),
        )
        .await;
        enforce_sudp_direct_visitor_session_limit(state, session_id).await;
        return Ok(endpoint);
    }

    let socket = Arc::new(
        UdpSocket::bind(format!("{}:0", visitor.bind_addr))
            .await
            .context("bind sudp direct visitor socket")?,
    );
    let local_addr = socket
        .local_addr()
        .context("read sudp direct visitor socket address")?;
    // 必须在启动常驻接收任务前完成 STUN，避免两个 recv_from 竞争同一响应。
    let candidate_addrs = sudp_socket_candidate_addrs(state, &socket, local_addr).await;
    let endpoint = SudpDirectVisitorEndpoint {
        socket,
        candidate_addrs,
    };
    let endpoint = {
        let mut sockets = state.sudp_direct_visitor_sockets.lock().await;
        if let Some(existing) = sockets.get(session_id) {
            existing.clone()
        } else {
            sockets.insert(session_id.to_string(), endpoint.clone());
            endpoint
        }
    };
    touch_sudp_direct_visitor_session(state, session_id).await;
    ensure_sudp_direct_visitor_task(
        state,
        session_id,
        &visitor.server_name,
        endpoint.socket.clone(),
    )
    .await;
    enforce_sudp_direct_visitor_session_limit(state, session_id).await;
    Ok(endpoint)
}

async fn ensure_sudp_direct_visitor_task(
    state: &ClientState,
    session_id: &str,
    expected_proxy_name: &str,
    socket: Arc<UdpSocket>,
) {
    let mut tasks = state.sudp_direct_visitor_tasks.lock().await;
    if tasks.contains_key(session_id) {
        return;
    }
    let task = spawn_sudp_direct_visitor_response_loop(
        state.clone(),
        session_id.to_string(),
        expected_proxy_name.to_string(),
        socket,
    );
    tasks.insert(session_id.to_string(), task);
}

fn spawn_sudp_direct_visitor_response_loop(
    state: ClientState,
    session_id: String,
    expected_proxy_name: String,
    socket: Arc<UdpSocket>,
) -> tokio::task::JoinHandle<()> {
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
            let Some((proxy_name, response_session_id, content, ack)) =
                parse_sudp_direct_response(&buf[..n])
            else {
                continue;
            };
            if response_session_id != session_id || proxy_name != expected_proxy_name {
                continue;
            }
            touch_sudp_direct_visitor_session(&state, &session_id).await;
            record_sudp_direct_peer(&state, &session_id, peer).await;
            if let Err(err) = flush_sudp_direct_pending(&state, &session_id, &socket, peer).await {
                debug!("sudp direct pending flush failed: {err:#}");
                continue;
            }
            if !ack {
                state
                    .sudp_direct_confirmed
                    .lock()
                    .await
                    .insert(session_id.clone());
                clear_sudp_direct_failure(&state, &session_id).await;
            }
            if let Some(content) = content {
                if let Err(err) =
                    handle_sudp_visitor_response(state.clone(), session_id.clone(), content).await
                {
                    debug!("sudp direct visitor response failed: {err:#}");
                    break;
                }
            }
        }

        let owns_session = state
            .sudp_direct_visitor_sockets
            .lock()
            .await
            .get(&session_id)
            .map(|current| Arc::ptr_eq(&current.socket, &socket))
            .unwrap_or(false);
        if owns_session {
            remove_sudp_direct_visitor_session_state(&state, &session_id, false).await;
        }
    })
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
        mark_sudp_direct_failure(&state, &session_id).await;
        if let Err(err) = state
            .send(&Message::SudpPacket {
                proxy_name,
                session_id: session_id.clone(),
                content,
                sk,
                from_visitor: true,
            })
            .await
        {
            debug!("sudp relay fallback send failed: {err:#}");
        } else {
            state.sudp_direct_pending.lock().await.remove(&session_id);
        }
    });
}

fn parse_sudp_direct_response(data: &[u8]) -> Option<(String, String, Option<Vec<u8>>, bool)> {
    let frame = parse_sudp_direct_frame(data)?;
    if frame.from_visitor || frame.sk.is_some() {
        return None;
    }
    let content = if frame.ack { None } else { Some(frame.content) };
    Some((frame.proxy_name, frame.session_id, content, frame.ack))
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
    let dial_permit = acquire_work_conn_dial_permit(&state).await?;
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
    drop(dial_permit);

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

async fn acquire_work_conn_dial_permit(state: &ClientState) -> Result<OwnedSemaphorePermit> {
    state
        .work_conn_dial_limiter
        .clone()
        .acquire_owned()
        .await
        .context("work connection dial limiter closed")
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
    let mut sessions: HashMap<(String, String), (Arc<UdpSocket>, String)> =
        HashMap::with_capacity(packets.len());
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
            let mut packets = Vec::with_capacity(UDP_PACKET_BATCH_LIMIT);
            packets.push(UdpPacketFrame {
                proxy_name: key.0.clone(),
                content: buf[..n].to_vec(),
                visitor_addr: key.1.clone(),
            });
            while packets.len() < UDP_PACKET_BATCH_LIMIT {
                match time::timeout(UDP_PACKET_BATCH_WAIT, socket.recv_from(&mut buf)).await {
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

    type XtcpHangingPeer = (String, oneshot::Receiver<()>, tokio::task::JoinHandle<()>);

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
            sudp_direct_visitor_sessions: Arc::new(Mutex::new(HashMap::new())),
            sudp_direct_visitor_tasks: Arc::new(Mutex::new(HashMap::new())),
            sudp_direct_visitor_peers: Arc::new(Mutex::new(HashMap::new())),
            sudp_direct_pending: Arc::new(Mutex::new(HashMap::new())),
            sudp_direct_confirmed: Arc::new(Mutex::new(HashSet::new())),
            sudp_direct_probe_sessions: Arc::new(Mutex::new(HashSet::new())),
            sudp_direct_failures: Arc::new(Mutex::new(HashMap::new())),
            nat_hole_waiters: Arc::new(Mutex::new(HashMap::new())),
            nat_hole_cached: Arc::new(Mutex::new(HashMap::new())),
            nat_hole_waiting: Arc::new(Mutex::new(HashMap::new())),
            stun_server_addrs: Arc::new(OnceCell::new()),
            xtcp_direct_failures: Arc::new(Mutex::new(HashMap::new())),
            xtcp_direct_peers: Arc::new(Mutex::new(HashMap::new())),
            writer: Arc::new(Mutex::new(writer)),
            quic_session: None,
            tcp_mux_session: None,
            work_conn_dial_limiter: Arc::new(Semaphore::new(WORK_CONN_DIAL_LIMIT)),
        };
        (state, server)
    }

    #[tokio::test]
    async fn direct_local_addrs_replace_unspecified_bind_with_routed_ip() {
        let (state, _server) = test_state();
        let addrs = direct_local_addrs(&state, "0.0.0.0:34567".parse().unwrap()).await;

        assert!(!addrs.iter().any(|addr| addr.starts_with("0.0.0.0:")));
        assert!(
            addrs.iter().any(|addr| addr == "127.0.0.1:34567"),
            "expected routed loopback candidate, got {addrs:?}"
        );
    }

    /// 启动仅处理一次 Binding Request 的测试 STUN 服务。
    async fn spawn_test_stun_server(
        mapped_addr: SocketAddr,
        dropped_requests: usize,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let stun_server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stun_addr = stun_server.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let mut buf = [0_u8; 512];
            for request_index in 0..=dropped_requests {
                let (len, peer) = stun_server.recv_from(&mut buf).await.unwrap();
                if request_index == dropped_requests {
                    let response = stun::binding_response(&buf[..len], mapped_addr).unwrap();
                    stun_server.send_to(&response, peer).await.unwrap();
                }
            }
        });
        (stun_addr, server_task)
    }

    /// 验证 SUDP owner 会通过直连 socket 获取 STUN 映射，并将其置于本地候选之前。
    #[tokio::test]
    async fn sudp_owner_candidates_prefer_stun_mapped_addr() {
        let mapped_addr: SocketAddr = "198.51.100.8:45678".parse().unwrap();
        let (stun_addr, server_task) = spawn_test_stun_server(mapped_addr, 0).await;

        let (mut state, _server) = test_state();
        let mut cfg = (*state.cfg).clone();
        cfg.transport.nat_hole_stun_server = Some(stun_addr.to_string());
        state.cfg = Arc::new(cfg);
        let owner_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let owner_local_addr = owner_socket.local_addr().unwrap();

        let candidates = sudp_socket_candidate_addrs(&state, &owner_socket, owner_local_addr).await;

        assert_eq!(
            candidates.first().map(String::as_str),
            Some("198.51.100.8:45678")
        );
        assert!(candidates.contains(&owner_local_addr.to_string()));
        assert_eq!(
            state.stun_server_addrs.get().map(Vec::as_slice),
            Some([stun_addr].as_slice())
        );
        server_task.await.unwrap();
    }

    /// 验证首个 Binding Request 丢失后仍会在总超时内重传并取得映射地址。
    #[tokio::test]
    async fn sudp_stun_retries_dropped_binding_request() {
        let mapped_addr: SocketAddr = "198.51.100.10:45680".parse().unwrap();
        let (stun_addr, server_task) = spawn_test_stun_server(mapped_addr, 1).await;
        let (mut state, _server) = test_state();
        let mut cfg = (*state.cfg).clone();
        cfg.transport.nat_hole_stun_server = Some(stun_addr.to_string());
        state.cfg = Arc::new(cfg);
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let discovered = discover_sudp_mapped_addr(&state, &socket).await;

        assert_eq!(discovered, Some(mapped_addr));
        server_task.await.unwrap();
    }

    /// 验证 SUDP visitor 在启动接收任务前缓存同一 socket 的 STUN 映射候选。
    #[tokio::test]
    async fn sudp_visitor_endpoint_caches_stun_mapped_addr() {
        let mapped_addr: SocketAddr = "198.51.100.9:45679".parse().unwrap();
        let (stun_addr, server_task) = spawn_test_stun_server(mapped_addr, 0).await;
        let (mut state, _server) = test_state();
        let mut cfg = (*state.cfg).clone();
        cfg.transport.nat_hole_stun_server = Some(stun_addr.to_string());
        state.cfg = Arc::new(cfg);
        let visitor = VisitorConfig {
            name: "visitor".to_string(),
            visitor_type: ProxyType::Sudp,
            server_name: "sudp-echo".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };
        let session_id = "visitor|127.0.0.1:50000";

        let endpoint = sudp_direct_visitor_socket(&state, &visitor, session_id)
            .await
            .unwrap();

        assert_eq!(
            endpoint.candidate_addrs.first().map(String::as_str),
            Some("198.51.100.9:45679")
        );
        assert!(endpoint
            .candidate_addrs
            .contains(&endpoint.socket.local_addr().unwrap().to_string()));
        server_task.await.unwrap();
        remove_sudp_direct_visitor_session_state(&state, session_id, true).await;
    }

    #[test]
    fn owner_nat_hole_announcements_include_all_local_candidates_for_group() {
        let proxy = ProxyConfig {
            name: "sudp-a".to_string(),
            proxy_type: ProxyType::Sudp,
            local_ip: "127.0.0.1".to_string(),
            local_port: 9000,
            remote_port: 0,
            group: Some("sudp-group".to_string()),
            group_key: Some("secret".to_string()),
            custom_domains: Vec::new(),
            locations: Vec::new(),
            host_header_rewrite: None,
            request_headers: Default::default(),
            http_user: None,
            http_password: None,
            bandwidth_limit: None,
            sk: None,
            health_check: None,
        };
        let local_addrs = vec![
            "127.0.0.1:10001".to_string(),
            "192.168.1.10:10001".to_string(),
        ];

        let announcements = owner_nat_hole_announcements(&proxy, local_addrs.clone());

        assert_eq!(announcements.len(), 2);
        assert_eq!(announcements[0].transaction_id, "sudp-a");
        assert_eq!(announcements[0].local_addrs, local_addrs);
        assert_eq!(announcements[1].transaction_id, "sudp-group");
        assert_eq!(announcements[1].local_addrs, announcements[0].local_addrs);
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
    async fn work_connection_dial_permits_are_bounded() {
        let (state, _server) = test_state();
        let mut permits = Vec::with_capacity(WORK_CONN_DIAL_LIMIT);

        for _ in 0..WORK_CONN_DIAL_LIMIT {
            permits.push(acquire_work_conn_dial_permit(&state).await.unwrap());
        }

        assert_eq!(state.work_conn_dial_limiter.available_permits(), 0);
        let blocked = time::timeout(
            Duration::from_millis(100),
            acquire_work_conn_dial_permit(&state),
        )
        .await;
        assert!(blocked.is_err(), "extra dial permit was not bounded");

        drop(permits.pop());
        let permit = time::timeout(
            Duration::from_secs(1),
            acquire_work_conn_dial_permit(&state),
        )
        .await
        .unwrap()
        .unwrap();
        drop(permit);
    }

    async fn spawn_hanging_xtcp_direct_test_peer() -> XtcpHangingPeer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (closed_tx, closed_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            match read_msg(&mut stream).await.unwrap() {
                Message::NewVisitorConn { .. } => {
                    let mut buf = [0_u8; 1];
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => {
                            let _ = closed_tx.send(());
                        }
                        Ok(_) => {}
                    }
                }
                other => panic!("unexpected direct handshake: {other:?}"),
            }
        });
        (addr, closed_rx, task)
    }

    async fn spawn_xtcp_echo_direct_test_peer(
        max_conns: usize,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            for _ in 0..max_conns {
                let (mut stream, _) = listener.accept().await.unwrap();
                match read_msg(&mut stream).await.unwrap() {
                    Message::NewVisitorConn { proxy_name, .. } => {
                        write_msg(
                            &mut stream,
                            &Message::NewVisitorConnResp {
                                proxy_name,
                                error: String::new(),
                            },
                        )
                        .await
                        .unwrap();

                        let mut buf = [0_u8; 1024];
                        loop {
                            let n = stream.read(&mut buf).await.unwrap();
                            if n == 0 {
                                break;
                            }
                            stream.write_all(&buf[..n]).await.unwrap();
                        }
                    }
                    other => panic!("unexpected direct handshake: {other:?}"),
                }
            }
        });
        (addr, task)
    }

    async fn assert_xtcp_direct_echo(
        state: ClientState,
        visitor: VisitorConfig,
        payload: &'static [u8],
    ) -> bool {
        let (mut client, mut inbound) = tcp_pair().await;
        let direct =
            tokio::spawn(
                async move { try_xtcp_direct_visitor(&state, &visitor, &mut inbound).await },
            );

        client.write_all(payload).await.unwrap();
        let mut echoed = vec![0_u8; payload.len()];
        client.read_exact(&mut echoed).await.unwrap();
        client.shutdown().await.unwrap();

        assert_eq!(echoed, payload);
        time::timeout(Duration::from_secs(1), direct)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    }

    async fn spawn_rejecting_xtcp_direct_test_peer() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            match read_msg(&mut stream).await.unwrap() {
                Message::NewVisitorConn { proxy_name, .. } => {
                    write_msg(
                        &mut stream,
                        &Message::NewVisitorConnResp {
                            proxy_name,
                            error: "direct peer unavailable".to_string(),
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

    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(TcpStream::connect(addr));
        let (server, _) = listener.accept().await.unwrap();
        (client.await.unwrap().unwrap(), server)
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
            .remove(&nat_hole_waiter_key("tx-cache", "xtcp-ssh", "server"))
            .unwrap();
        assert_eq!(cached.resp.peer_observed_addr, "127.0.0.1:7002");
        assert_eq!(cached.resp.peer_local_addrs, vec!["10.0.0.20:10002"]);
    }

    #[tokio::test]
    async fn request_nat_hole_consumes_cached_response() {
        let (state, _server) = test_state();
        state.nat_hole_cached.lock().await.insert(
            nat_hole_waiter_key("tx-cache", "xtcp-ssh", "visitor"),
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
            .contains_key(&nat_hole_waiter_key("tx-cache", "xtcp-ssh", "visitor")));
    }

    #[tokio::test]
    async fn nat_hole_response_wrong_proxy_does_not_satisfy_waiter() {
        let (state, mut server) = test_state();
        let request_state = state.clone();
        let request = tokio::spawn(async move {
            request_nat_hole(
                &request_state,
                "tx-proxy-bound",
                "xtcp-right",
                "visitor",
                Vec::new(),
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
                assert_eq!(transaction_id, "tx-proxy-bound");
                assert_eq!(proxy_name, "xtcp-right");
                assert_eq!(role, "visitor");
            }
            other => panic!("unexpected nat hole register: {other:?}"),
        }

        let right_key = nat_hole_waiter_key("tx-proxy-bound", "xtcp-right", "visitor");
        let wrong_key = nat_hole_waiter_key("tx-proxy-bound", "xtcp-wrong", "visitor");
        handle_nat_hole_response(
            &state,
            "tx-proxy-bound".to_string(),
            "xtcp-wrong".to_string(),
            "visitor".to_string(),
            NatHoleResponse {
                peer_observed_addr: "127.0.0.1:7002".to_string(),
                peer_local_addrs: vec!["10.0.0.20:10002".to_string()],
                waiting: false,
                error: String::new(),
            },
        )
        .await;

        assert!(state.nat_hole_waiters.lock().await.contains_key(&right_key));
        assert!(state.nat_hole_cached.lock().await.contains_key(&wrong_key));

        handle_nat_hole_response(
            &state,
            "tx-proxy-bound".to_string(),
            "xtcp-right".to_string(),
            "visitor".to_string(),
            NatHoleResponse {
                peer_observed_addr: "127.0.0.1:7001".to_string(),
                peer_local_addrs: vec!["10.0.0.10:10001".to_string()],
                waiting: false,
                error: String::new(),
            },
        )
        .await;

        let resp = request.await.unwrap().unwrap();
        assert_eq!(resp.peer_observed_addr, "127.0.0.1:7001");
        assert_eq!(resp.peer_local_addrs, vec!["10.0.0.10:10001"]);
        assert!(!state.nat_hole_waiters.lock().await.contains_key(&right_key));
    }

    #[tokio::test]
    async fn nat_hole_cached_responses_are_bounded_and_prune_expired_entries() {
        let (state, _server) = test_state();
        {
            let mut cached = state.nat_hole_cached.lock().await;
            cached.insert(
                nat_hole_waiter_key("tx-expired", "xtcp-ssh", "visitor"),
                NatHoleCachedResponse {
                    resp: NatHoleResponse {
                        peer_observed_addr: "127.0.0.1:7000".to_string(),
                        peer_local_addrs: Vec::new(),
                        waiting: false,
                        error: String::new(),
                    },
                    cached_at: Instant::now()
                        - NAT_HOLE_CACHED_RESPONSE_TTL
                        - Duration::from_secs(1),
                },
            );
        }

        for idx in 0..=NAT_HOLE_CACHED_RESPONSE_LIMIT {
            cache_nat_hole_response(
                &state,
                nat_hole_waiter_key(&format!("tx-{idx}"), "xtcp-ssh", "visitor"),
                NatHoleResponse {
                    peer_observed_addr: format!("127.0.0.1:{}", 7000 + idx),
                    peer_local_addrs: Vec::new(),
                    waiting: false,
                    error: String::new(),
                },
            )
            .await;
        }

        let cached = state.nat_hole_cached.lock().await;
        assert_eq!(cached.len(), NAT_HOLE_CACHED_RESPONSE_LIMIT);
        assert!(!cached.contains_key(&nat_hole_waiter_key("tx-expired", "xtcp-ssh", "visitor")));
        assert!(!cached.contains_key(&nat_hole_waiter_key("tx-0", "xtcp-ssh", "visitor")));
        assert!(cached.contains_key(&nat_hole_waiter_key(
            &format!("tx-{NAT_HOLE_CACHED_RESPONSE_LIMIT}"),
            "xtcp-ssh",
            "visitor"
        )));
    }

    #[tokio::test]
    async fn request_nat_hole_ignores_expired_cached_response() {
        let (state, mut server) = test_state();
        state.nat_hole_cached.lock().await.insert(
            nat_hole_waiter_key("tx-stale", "xtcp-ssh", "visitor"),
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
                sk,
                local_addrs,
            } => {
                assert_eq!(transaction_id, "tx-stale");
                assert_eq!(proxy_name, "xtcp-ssh");
                assert_eq!(role, "visitor");
                assert_eq!(sk, None);
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
    async fn concurrent_nat_hole_waiters_share_async_notification() {
        let (state, mut server) = test_state();
        let first_state = state.clone();
        let first = tokio::spawn(async move {
            request_nat_hole_with_grace(
                &first_state,
                "xtcp-concurrent",
                "xtcp-concurrent",
                "visitor",
                Vec::new(),
                Duration::from_secs(1),
            )
            .await
        });
        let second_state = state.clone();
        let second = tokio::spawn(async move {
            request_nat_hole_with_grace(
                &second_state,
                "xtcp-concurrent",
                "xtcp-concurrent",
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
                assert_eq!(transaction_id, "xtcp-concurrent");
                assert_eq!(proxy_name, "xtcp-concurrent");
                assert_eq!(role, "visitor");
            }
            other => panic!("unexpected nat hole register: {other:?}"),
        }

        let key = nat_hole_waiter_key("xtcp-concurrent", "xtcp-concurrent", "visitor");
        time::timeout(Duration::from_secs(1), async {
            loop {
                let waiter_count = state
                    .nat_hole_waiters
                    .lock()
                    .await
                    .get(&key)
                    .map(Vec::len)
                    .unwrap_or(0);
                if waiter_count == 2 {
                    break;
                }
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("concurrent requests were not coalesced onto one register");
        match time::timeout(Duration::from_millis(200), read_msg(&mut server)).await {
            Err(_) | Ok(Err(_)) => {}
            Ok(Ok(other)) => {
                panic!("coalesced nat hole requests sent duplicate register: {other:?}")
            }
        }

        handle_nat_hole_response(
            &state,
            "xtcp-concurrent".to_string(),
            "xtcp-concurrent".to_string(),
            "visitor".to_string(),
            NatHoleResponse {
                peer_observed_addr: String::new(),
                peer_local_addrs: Vec::new(),
                waiting: true,
                error: String::new(),
            },
        )
        .await;

        time::timeout(Duration::from_secs(1), async {
            loop {
                let waiter_count = state
                    .nat_hole_waiters
                    .lock()
                    .await
                    .get(&key)
                    .map(Vec::len)
                    .unwrap_or(0);
                if waiter_count == 2 {
                    break;
                }
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("concurrent requests did not both enter grace wait");

        handle_nat_hole_response(
            &state,
            "xtcp-concurrent".to_string(),
            "xtcp-concurrent".to_string(),
            "visitor".to_string(),
            NatHoleResponse {
                peer_observed_addr: "127.0.0.1:7001".to_string(),
                peer_local_addrs: vec!["10.0.0.10:10001".to_string()],
                waiting: false,
                error: String::new(),
            },
        )
        .await;

        for response in [
            first.await.unwrap().unwrap(),
            second.await.unwrap().unwrap(),
        ] {
            assert!(!response.waiting);
            assert_eq!(response.peer_observed_addr, "127.0.0.1:7001");
            assert_eq!(response.peer_local_addrs, vec!["10.0.0.10:10001"]);
        }
        assert!(!state.nat_hole_waiters.lock().await.contains_key(&key));
    }

    #[tokio::test]
    async fn nat_hole_waiters_are_bounded_per_key() {
        let (state, _server) = test_state();
        let key = nat_hole_waiter_key("xtcp-bounded", "xtcp-bounded", "visitor");
        let mut receivers = Vec::new();

        for idx in 0..NAT_HOLE_WAITER_LIMIT {
            let (tx, rx) = oneshot::channel();
            receivers.push(rx);
            let should_send = add_nat_hole_waiter(&state, key.clone(), tx).await.unwrap();
            assert_eq!(should_send, idx == 0);
        }

        let (tx, rx) = oneshot::channel();
        receivers.push(rx);
        let err = add_nat_hole_waiter(&state, key.clone(), tx)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("too many nat hole waiters"));
        assert_eq!(
            state.nat_hole_waiters.lock().await.get(&key).map(Vec::len),
            Some(NAT_HOLE_WAITER_LIMIT)
        );
    }

    #[tokio::test]
    async fn request_nat_hole_recent_waiting_skips_duplicate_register() {
        let (state, mut server) = test_state();
        let request_state = state.clone();
        let request = tokio::spawn(async move {
            request_nat_hole(
                &request_state,
                "xtcp-waiting",
                "xtcp-waiting",
                "visitor",
                Vec::new(),
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
                assert_eq!(transaction_id, "xtcp-waiting");
                assert_eq!(proxy_name, "xtcp-waiting");
                assert_eq!(role, "visitor");
            }
            other => panic!("unexpected nat hole register: {other:?}"),
        }

        handle_nat_hole_response(
            &state,
            "xtcp-waiting".to_string(),
            "xtcp-waiting".to_string(),
            "visitor".to_string(),
            NatHoleResponse {
                peer_observed_addr: String::new(),
                peer_local_addrs: Vec::new(),
                waiting: true,
                error: String::new(),
            },
        )
        .await;

        let resp = request.await.unwrap().unwrap();
        assert!(resp.waiting);
        assert!(should_skip_nat_hole_waiting(&state, "xtcp-waiting|xtcp-waiting|visitor").await);

        let resp = request_nat_hole(
            &state,
            "xtcp-waiting",
            "xtcp-waiting",
            "visitor",
            Vec::new(),
        )
        .await
        .unwrap();
        assert!(resp.waiting);
        match time::timeout(Duration::from_millis(200), read_msg(&mut server)).await {
            Err(_) | Ok(Err(_)) => {}
            Ok(Ok(other)) => panic!("recent waiting still sent NAT register: {other:?}"),
        }
    }

    #[tokio::test]
    async fn request_nat_hole_cached_match_bypasses_recent_waiting() {
        let (state, mut server) = test_state();
        let key = nat_hole_waiter_key("xtcp-waiting", "xtcp-waiting", "visitor");
        mark_nat_hole_waiting(&state, &key).await;
        state.nat_hole_cached.lock().await.insert(
            key.clone(),
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
            "xtcp-waiting",
            "xtcp-waiting",
            "visitor",
            Vec::new(),
        )
        .await
        .unwrap();

        assert!(!resp.waiting);
        assert_eq!(resp.peer_observed_addr, "127.0.0.1:7001");
        assert_eq!(resp.peer_local_addrs, vec!["10.0.0.10:10001"]);
        assert!(!state.nat_hole_waiting.lock().await.contains_key(&key));
        match time::timeout(Duration::from_millis(200), read_msg(&mut server)).await {
            Err(_) | Ok(Err(_)) => {}
            Ok(Ok(other)) => panic!("cached match still sent NAT register: {other:?}"),
        }
    }

    #[tokio::test]
    async fn nat_hole_owner_refresh_reannounces_proxy_and_group() {
        let (state, mut server) = test_state();
        let announcements = vec![
            NatHoleAnnouncement {
                transaction_id: "xtcp-echo".to_string(),
                proxy_name: "xtcp-echo".to_string(),
                role: "server",
                sk: Some("secret".to_string()),
                local_addrs: vec!["127.0.0.1:10001".to_string()],
            },
            NatHoleAnnouncement {
                transaction_id: "xtcp-group".to_string(),
                proxy_name: "xtcp-group".to_string(),
                role: "server",
                sk: Some("secret".to_string()),
                local_addrs: vec!["127.0.0.1:10001".to_string()],
            },
        ];

        let refresh = spawn_nat_hole_owner_refresh(
            state,
            "xtcp-echo".to_string(),
            announcements,
            Duration::from_millis(10),
        );

        for expected in ["xtcp-echo", "xtcp-group"] {
            match time::timeout(Duration::from_secs(1), read_msg(&mut server))
                .await
                .unwrap()
                .unwrap()
            {
                Message::NatHoleRegister {
                    transaction_id,
                    proxy_name,
                    role,
                    sk,
                    local_addrs,
                } => {
                    assert_eq!(transaction_id, expected);
                    assert_eq!(proxy_name, expected);
                    assert_eq!(role, "server");
                    assert_eq!(sk.as_deref(), Some("secret"));
                    assert_eq!(local_addrs, vec!["127.0.0.1:10001"]);
                }
                other => panic!("unexpected nat hole refresh: {other:?}"),
            }
        }

        refresh.abort();
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
    async fn sudp_owner_session_responses_use_latest_peer() {
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
        let direct_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let sessions = Arc::new(Mutex::new(
            HashMap::<String, Arc<SudpDirectOwnerSession>>::new(),
        ));
        let session_id = "local-sudp|127.0.0.1:50000";
        let peer_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let local_a = sudp_direct_owner_session(
            session_id,
            &proxy.name,
            direct_socket.clone(),
            sessions.clone(),
            peer_a.local_addr().unwrap(),
        )
        .await
        .unwrap();
        let local_b = sudp_direct_owner_session(
            session_id,
            &proxy.name,
            direct_socket,
            sessions,
            peer_b.local_addr().unwrap(),
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(&local_a, &local_b));

        let local_service = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let local_target = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            local_b.local_addr().unwrap().port(),
        );
        local_service.send_to(b"pong", local_target).await.unwrap();

        let mut buf = vec![0_u8; 1024];
        let (n, _) = time::timeout(Duration::from_secs(1), peer_b.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let frame = parse_sudp_direct_frame(&buf[..n]).unwrap();
        assert_eq!(frame.proxy_name, "sudp-echo");
        assert_eq!(frame.session_id, session_id);
        assert_eq!(frame.content, b"pong");
        assert!(!frame.from_visitor);

        let stale = time::timeout(Duration::from_millis(100), peer_a.recv_from(&mut buf)).await;
        assert!(stale.is_err(), "response was sent to stale peer");
    }

    #[tokio::test]
    async fn sudp_direct_owner_sessions_are_bounded_and_evict_oldest() {
        let direct_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let sessions = Arc::new(Mutex::new(
            HashMap::<String, Arc<SudpDirectOwnerSession>>::new(),
        ));

        for idx in 0..=SUDP_DIRECT_SESSION_LIMIT {
            let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            sudp_direct_owner_session(
                &format!("owner-session-{idx}"),
                "sudp-echo",
                direct_socket.clone(),
                sessions.clone(),
                peer.local_addr().unwrap(),
            )
            .await
            .unwrap();
        }

        let sessions = sessions.lock().await;
        assert_eq!(sessions.len(), SUDP_DIRECT_SESSION_LIMIT);
        assert!(!sessions.contains_key("owner-session-0"));
        assert!(sessions.contains_key(&format!("owner-session-{SUDP_DIRECT_SESSION_LIMIT}")));
    }

    #[tokio::test]
    async fn sudp_direct_probe_retries_until_confirmed() {
        let (state, _server) = test_state();
        let session_id = "visitor|127.0.0.1:50001".to_string();
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        state.sudp_direct_visitor_sockets.lock().await.insert(
            session_id.clone(),
            SudpDirectVisitorEndpoint {
                socket,
                candidate_addrs: Vec::new(),
            },
        );
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

    #[tokio::test]
    async fn sudp_direct_probe_retries_stop_after_recent_failure() {
        let (state, _server) = test_state();
        let session_id = "visitor|127.0.0.1:50002".to_string();
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        state.sudp_direct_visitor_sockets.lock().await.insert(
            session_id.clone(),
            SudpDirectVisitorEndpoint {
                socket,
                candidate_addrs: Vec::new(),
            },
        );
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
        mark_sudp_direct_failure(&state, &session_id).await;
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
        let retry = time::timeout(Duration::from_millis(700), peer.recv_from(&mut buf)).await;
        assert!(
            retry.is_err(),
            "probe retry ignored recent failure cooldown"
        );
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if !state
                .sudp_direct_probe_sessions
                .lock()
                .await
                .contains(&session_id)
            {
                return;
            }
            time::sleep(Duration::from_millis(20)).await;
        }

        panic!("probe retry session was not cleaned up after recent failure");
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
    fn direct_candidates_filters_unusable_addrs() {
        let resp = NatHoleResponse {
            peer_observed_addr: "0.0.0.0:9000".to_string(),
            peer_local_addrs: vec![
                "0.0.0.0:7000".to_string(),
                "127.0.0.1:0".to_string(),
                "224.0.0.1:7000".to_string(),
                "255.255.255.255:7000".to_string(),
                "[::]:7000".to_string(),
                "[ff02::1]:7000".to_string(),
                "127.0.0.1:7000".to_string(),
            ],
            waiting: false,
            error: String::new(),
        };

        assert_eq!(direct_candidates(&resp), vec!["127.0.0.1:7000"]);
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
                "8.8.4.4:9000".to_string(),
                "10.0.0.2:7000".to_string(),
                "127.0.0.1:7000".to_string(),
            ]
        );
    }

    #[test]
    fn direct_candidates_prefer_observed_public_addr_over_private_local_addr() {
        let resp = NatHoleResponse {
            peer_observed_addr: "8.8.4.4:9000".to_string(),
            peer_local_addrs: vec!["10.0.0.2:7000".to_string()],
            waiting: false,
            error: String::new(),
        };

        assert_eq!(
            direct_candidates(&resp),
            vec!["8.8.4.4:9000".to_string(), "10.0.0.2:7000".to_string()]
        );
    }

    #[test]
    fn direct_candidates_are_bounded_to_highest_ranked_addrs() {
        let resp = NatHoleResponse {
            peer_observed_addr: "8.8.4.4:9000".to_string(),
            peer_local_addrs: vec![
                "127.0.0.1:7000".to_string(),
                "127.0.0.1:7001".to_string(),
                "127.0.0.1:7002".to_string(),
                "127.0.0.1:7003".to_string(),
                "127.0.0.1:7004".to_string(),
                "127.0.0.1:7005".to_string(),
                "127.0.0.1:7006".to_string(),
                "127.0.0.1:7007".to_string(),
                "10.0.0.2:7000".to_string(),
                "10.0.0.3:7000".to_string(),
                "198.51.100.10:7000".to_string(),
            ],
            waiting: false,
            error: String::new(),
        };

        let candidates = direct_candidates(&resp);

        assert_eq!(candidates.len(), DIRECT_CANDIDATE_LIMIT);
        assert_eq!(candidates[0], "8.8.4.4:9000");
        assert!(candidates.contains(&"10.0.0.2:7000".to_string()));
        assert!(candidates.contains(&"10.0.0.3:7000".to_string()));
        assert_eq!(
            candidates
                .iter()
                .filter(|candidate| candidate.starts_with("127.0.0.1:"))
                .count(),
            DIRECT_CANDIDATE_LIMIT - 3
        );
        assert!(!candidates.contains(&"198.51.100.10:7000".to_string()));
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
    async fn xtcp_direct_candidate_race_aborts_slower_candidates() {
        let (slow_addr, slow_closed, slow_task) = spawn_hanging_xtcp_direct_test_peer().await;
        let (fast_addr, fast_task) = spawn_xtcp_direct_test_peer(Duration::from_millis(200)).await;
        let visitor = VisitorConfig {
            name: "visitor".to_string(),
            visitor_type: ProxyType::Xtcp,
            server_name: "xtcp-echo".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };

        let (_peer, candidate) =
            connect_xtcp_direct_peer(&visitor, vec![slow_addr.clone(), fast_addr.clone()])
                .await
                .unwrap()
                .unwrap();

        assert_eq!(candidate, fast_addr);
        time::timeout(Duration::from_millis(500), slow_closed)
            .await
            .expect("slow candidate was not aborted after fast candidate won")
            .unwrap();
        slow_task.abort();
        fast_task.abort();
    }

    #[tokio::test]
    async fn xtcp_direct_candidate_rejects_wrong_response_proxy() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            match read_msg(&mut stream).await.unwrap() {
                Message::NewVisitorConn { .. } => {
                    write_msg(
                        &mut stream,
                        &Message::NewVisitorConnResp {
                            proxy_name: "other-xtcp".to_string(),
                            error: String::new(),
                        },
                    )
                    .await
                    .unwrap();
                }
                other => panic!("unexpected direct handshake: {other:?}"),
            }
        });
        let visitor = VisitorConfig {
            name: "visitor".to_string(),
            visitor_type: ProxyType::Xtcp,
            server_name: "xtcp-echo".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };

        let err = connect_xtcp_direct_candidate(&visitor, &addr)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("unexpected proxy other-xtcp"),
            "unexpected error: {err:#}"
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn xtcp_direct_cached_success_skips_nat_lookup() {
        let (state, mut server) = test_state();
        let (direct_addr, direct_task) = spawn_xtcp_echo_direct_test_peer(2).await;
        state.nat_hole_cached.lock().await.insert(
            nat_hole_waiter_key("cached-xtcp", "cached-xtcp", "visitor"),
            NatHoleCachedResponse {
                resp: NatHoleResponse {
                    peer_observed_addr: String::new(),
                    peer_local_addrs: vec![direct_addr.clone()],
                    waiting: false,
                    error: String::new(),
                },
                cached_at: Instant::now(),
            },
        );
        let visitor = VisitorConfig {
            name: "local-cached-xtcp".to_string(),
            visitor_type: ProxyType::Xtcp,
            server_name: "cached-xtcp".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };

        assert!(
            assert_xtcp_direct_echo(state.clone(), visitor.clone(), b"first direct").await,
            "first direct connection did not use NAT-provided candidate"
        );
        assert_eq!(
            take_fresh_xtcp_direct_peer(&state, "cached-xtcp").await,
            Some(direct_addr)
        );
        assert!(
            assert_xtcp_direct_echo(state.clone(), visitor, b"second direct").await,
            "second direct connection did not use cached candidate"
        );

        let register = time::timeout(Duration::from_millis(200), read_msg(&mut server)).await;
        assert!(
            register.is_err(),
            "cached XTCP direct peer still performed NAT lookup"
        );
        direct_task.await.unwrap();
    }

    #[tokio::test]
    async fn xtcp_direct_failure_marks_recent_skip_cache() {
        let (state, _server) = test_state();
        let (bad_addr, bad_task) = spawn_rejecting_xtcp_direct_test_peer().await;
        state.nat_hole_cached.lock().await.insert(
            nat_hole_waiter_key("bad-xtcp", "bad-xtcp", "visitor"),
            NatHoleCachedResponse {
                resp: NatHoleResponse {
                    peer_observed_addr: String::new(),
                    peer_local_addrs: vec![bad_addr],
                    waiting: false,
                    error: String::new(),
                },
                cached_at: Instant::now(),
            },
        );
        let visitor = VisitorConfig {
            name: "local-bad-xtcp".to_string(),
            visitor_type: ProxyType::Xtcp,
            server_name: "bad-xtcp".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };
        let (_client, mut inbound) = tcp_pair().await;

        let used_direct = try_xtcp_direct_visitor(&state, &visitor, &mut inbound)
            .await
            .unwrap();

        assert!(!used_direct);
        assert!(should_skip_xtcp_direct(&state, "bad-xtcp").await);
        bad_task.abort();
    }

    #[tokio::test]
    async fn xtcp_direct_failures_are_bounded_and_prune_expired_entries() {
        let (state, _server) = test_state();
        state.xtcp_direct_failures.lock().await.insert(
            "expired-xtcp".to_string(),
            Instant::now() - XTCP_DIRECT_FAILURE_TTL - Duration::from_secs(1),
        );

        for idx in 0..=XTCP_DIRECT_FAILURE_LIMIT {
            mark_xtcp_direct_failure(&state, &format!("xtcp-fail-{idx}")).await;
        }

        let failures = state.xtcp_direct_failures.lock().await;
        assert_eq!(failures.len(), XTCP_DIRECT_FAILURE_LIMIT);
        assert!(!failures.contains_key("expired-xtcp"));
        assert!(!failures.contains_key("xtcp-fail-0"));
        assert!(failures.contains_key(&format!("xtcp-fail-{XTCP_DIRECT_FAILURE_LIMIT}")));
    }

    #[tokio::test]
    async fn xtcp_direct_peers_are_bounded_and_prune_expired_entries() {
        let (state, _server) = test_state();
        state.xtcp_direct_peers.lock().await.insert(
            "expired-xtcp-peer".to_string(),
            XtcpDirectPeer {
                candidate: "127.0.0.1:10000".to_string(),
                updated_at: Instant::now() - XTCP_DIRECT_PEER_TTL - Duration::from_secs(1),
            },
        );

        for idx in 0..=XTCP_DIRECT_PEER_LIMIT {
            record_xtcp_direct_peer(
                &state,
                &format!("xtcp-peer-{idx}"),
                format!("127.0.0.1:{}", 20000 + idx),
            )
            .await;
        }

        let peers = state.xtcp_direct_peers.lock().await;
        assert_eq!(peers.len(), XTCP_DIRECT_PEER_LIMIT);
        assert!(!peers.contains_key("expired-xtcp-peer"));
        assert!(!peers.contains_key("xtcp-peer-0"));
        assert!(peers.contains_key(&format!("xtcp-peer-{XTCP_DIRECT_PEER_LIMIT}")));
    }

    #[tokio::test]
    async fn xtcp_direct_recent_failure_skips_nat_lookup() {
        let (state, mut server) = test_state();
        mark_xtcp_direct_failure(&state, "cached-bad-xtcp").await;
        let visitor = VisitorConfig {
            name: "local-cached-bad-xtcp".to_string(),
            visitor_type: ProxyType::Xtcp,
            server_name: "cached-bad-xtcp".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };
        let (_client, mut inbound) = tcp_pair().await;

        let used_direct = try_xtcp_direct_visitor(&state, &visitor, &mut inbound)
            .await
            .unwrap();
        let register = time::timeout(Duration::from_millis(200), read_msg(&mut server)).await;

        assert!(!used_direct);
        assert!(
            register.is_err(),
            "recent failure still performed NAT lookup"
        );
    }

    #[tokio::test]
    async fn xtcp_direct_expired_failure_retries_nat_lookup() {
        let (state, mut server) = test_state();
        state.xtcp_direct_failures.lock().await.insert(
            "expired-bad-xtcp".to_string(),
            Instant::now() - XTCP_DIRECT_FAILURE_TTL - Duration::from_millis(1),
        );
        let visitor = VisitorConfig {
            name: "local-expired-bad-xtcp".to_string(),
            visitor_type: ProxyType::Xtcp,
            server_name: "expired-bad-xtcp".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };
        let (_client, mut inbound) = tcp_pair().await;
        let request_state = state.clone();
        let request = tokio::spawn(async move {
            try_xtcp_direct_visitor(&request_state, &visitor, &mut inbound).await
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
                sk,
                ..
            } => {
                assert_eq!(transaction_id, "expired-bad-xtcp");
                assert_eq!(proxy_name, "expired-bad-xtcp");
                assert_eq!(role, "visitor");
                assert_eq!(sk.as_deref(), Some("secret"));
            }
            other => panic!("unexpected nat hole register: {other:?}"),
        }

        handle_nat_hole_response(
            &state,
            "expired-bad-xtcp".to_string(),
            "expired-bad-xtcp".to_string(),
            "visitor".to_string(),
            NatHoleResponse {
                peer_observed_addr: String::new(),
                peer_local_addrs: Vec::new(),
                waiting: true,
                error: String::new(),
            },
        )
        .await;

        assert!(!request.await.unwrap().unwrap());
        assert!(!state
            .xtcp_direct_failures
            .lock()
            .await
            .contains_key("expired-bad-xtcp"));
    }

    #[tokio::test]
    async fn xtcp_direct_owner_rejects_unregistered_proxy_name() {
        let proxy = ProxyConfig {
            name: "xtcp-echo".to_string(),
            proxy_type: ProxyType::Xtcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: 1,
            remote_port: 0,
            group: Some("xtcp-group".to_string()),
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
        let (mut visitor, owner) = tcp_pair().await;
        let owner_task = tokio::spawn(handle_xtcp_direct_owner_conn(proxy, owner));

        write_msg(
            &mut visitor,
            &Message::NewVisitorConn {
                proxy_name: "other-xtcp".to_string(),
                sk: Some("secret".to_string()),
                token: None,
            },
        )
        .await
        .unwrap();

        match read_msg(&mut visitor).await.unwrap() {
            Message::NewVisitorConnResp { error, .. } => {
                assert!(error.contains("not registered"));
            }
            other => panic!("unexpected direct response: {other:?}"),
        }
        owner_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn xtcp_direct_owner_rejects_secret_mismatch() {
        let proxy = ProxyConfig {
            name: "xtcp-echo".to_string(),
            proxy_type: ProxyType::Xtcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: 1,
            remote_port: 0,
            group: Some("xtcp-group".to_string()),
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
        let (mut visitor, owner) = tcp_pair().await;
        let owner_task = tokio::spawn(handle_xtcp_direct_owner_conn(proxy, owner));

        write_msg(
            &mut visitor,
            &Message::NewVisitorConn {
                proxy_name: "xtcp-group".to_string(),
                sk: Some("wrong-secret".to_string()),
                token: None,
            },
        )
        .await
        .unwrap();

        match read_msg(&mut visitor).await.unwrap() {
            Message::NewVisitorConnResp { error, .. } => {
                assert!(error.contains("secret key mismatch"));
            }
            other => panic!("unexpected direct response: {other:?}"),
        }
        owner_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn xtcp_direct_owner_rejects_when_local_service_unavailable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let missing_port = listener.local_addr().unwrap().port();
        drop(listener);
        let proxy = ProxyConfig {
            name: "xtcp-missing-local".to_string(),
            proxy_type: ProxyType::Xtcp,
            local_ip: "127.0.0.1".to_string(),
            local_port: missing_port,
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
        let (mut visitor, owner) = tcp_pair().await;
        let owner_task = tokio::spawn(handle_xtcp_direct_owner_conn(proxy, owner));

        write_msg(
            &mut visitor,
            &Message::NewVisitorConn {
                proxy_name: "xtcp-missing-local".to_string(),
                sk: Some("secret".to_string()),
                token: None,
            },
        )
        .await
        .unwrap();

        match read_msg(&mut visitor).await.unwrap() {
            Message::NewVisitorConnResp { error, .. } => {
                assert!(error.contains("connect local xtcp service failed"));
            }
            other => panic!("unexpected direct response: {other:?}"),
        }
        owner_task.await.unwrap().unwrap();
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
        spawn_sudp_direct_visitor_response_loop(
            state.clone(),
            session_id.clone(),
            visitor.server_name.clone(),
            socket,
        );

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
        assert!(!state
            .sudp_direct_confirmed
            .lock()
            .await
            .contains(&session_id));
        let direct_peer = state
            .sudp_direct_visitor_peers
            .lock()
            .await
            .get(&session_id)
            .copied()
            .unwrap();
        assert_eq!(direct_peer.addr, peer.local_addr().unwrap());
        assert!(!state
            .sudp_direct_pending
            .lock()
            .await
            .contains_key(&session_id));
    }

    #[tokio::test]
    async fn sudp_direct_response_with_wrong_proxy_is_ignored() {
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
        spawn_sudp_direct_visitor_response_loop(
            state.clone(),
            session_id.clone(),
            visitor.server_name.clone(),
            socket,
        );

        let wrong_proxy_ack = DirectSudpFrame {
            magic: DIRECT_SUDP_MAGIC.to_string(),
            proxy_name: "other-sudp".to_string(),
            session_id: session_id.clone(),
            content: Vec::new(),
            sk: None,
            from_visitor: false,
            ack: true,
            probe: false,
        };
        peer.send_to(&serde_json::to_vec(&wrong_proxy_ack).unwrap(), socket_addr)
            .await
            .unwrap();

        assert!(
            time::timeout(
                Duration::from_millis(200),
                peer.recv_from(&mut [0_u8; 1024])
            )
            .await
            .is_err(),
            "wrong-proxy ack flushed pending payload"
        );
        assert!(!state
            .sudp_direct_confirmed
            .lock()
            .await
            .contains(&session_id));
        assert!(!state
            .sudp_direct_visitor_peers
            .lock()
            .await
            .contains_key(&session_id));
        assert!(state
            .sudp_direct_pending
            .lock()
            .await
            .contains_key(&session_id));
    }

    #[tokio::test]
    async fn sudp_direct_visitor_origin_response_is_ignored() {
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
        spawn_sudp_direct_visitor_response_loop(
            state.clone(),
            session_id.clone(),
            visitor.server_name.clone(),
            socket,
        );

        let reflected_ack = DirectSudpFrame {
            magic: DIRECT_SUDP_MAGIC.to_string(),
            proxy_name: "sudp-echo".to_string(),
            session_id: session_id.clone(),
            content: Vec::new(),
            sk: Some("secret".to_string()),
            from_visitor: true,
            ack: true,
            probe: false,
        };
        peer.send_to(&serde_json::to_vec(&reflected_ack).unwrap(), socket_addr)
            .await
            .unwrap();

        assert!(
            time::timeout(
                Duration::from_millis(200),
                peer.recv_from(&mut [0_u8; 1024])
            )
            .await
            .is_err(),
            "visitor-origin ack flushed pending payload"
        );
        assert!(!state
            .sudp_direct_visitor_peers
            .lock()
            .await
            .contains_key(&session_id));
        assert!(state
            .sudp_direct_pending
            .lock()
            .await
            .contains_key(&session_id));
    }

    #[tokio::test]
    async fn sudp_direct_response_with_secret_field_is_ignored() {
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
        spawn_sudp_direct_visitor_response_loop(
            state.clone(),
            session_id.clone(),
            visitor.server_name.clone(),
            socket,
        );

        let ack_with_secret = DirectSudpFrame {
            magic: DIRECT_SUDP_MAGIC.to_string(),
            proxy_name: "sudp-echo".to_string(),
            session_id: session_id.clone(),
            content: Vec::new(),
            sk: Some("secret".to_string()),
            from_visitor: false,
            ack: true,
            probe: false,
        };
        peer.send_to(&serde_json::to_vec(&ack_with_secret).unwrap(), socket_addr)
            .await
            .unwrap();

        assert!(
            time::timeout(
                Duration::from_millis(200),
                peer.recv_from(&mut [0_u8; 1024])
            )
            .await
            .is_err(),
            "secret-bearing ack flushed pending payload"
        );
        assert!(!state
            .sudp_direct_visitor_peers
            .lock()
            .await
            .contains_key(&session_id));
        assert!(state
            .sudp_direct_pending
            .lock()
            .await
            .contains_key(&session_id));
    }

    #[tokio::test]
    async fn sudp_direct_data_response_confirms_selected_peer() {
        let (state, _server) = test_state();
        let session_id = "visitor|127.0.0.1:50000".to_string();
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let socket_addr = socket.local_addr().unwrap();
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let visitor_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        state.sudp_visitor_sessions.lock().await.insert(
            session_id.clone(),
            SudpVisitorSession {
                socket: visitor_socket,
                peer: peer.local_addr().unwrap(),
            },
        );
        mark_sudp_direct_failure(&state, &session_id).await;
        spawn_sudp_direct_visitor_response_loop(
            state.clone(),
            session_id.clone(),
            "sudp-echo".to_string(),
            socket,
        );

        let response = DirectSudpFrame {
            magic: DIRECT_SUDP_MAGIC.to_string(),
            proxy_name: "sudp-echo".to_string(),
            session_id: session_id.clone(),
            content: b"pong".to_vec(),
            sk: None,
            from_visitor: false,
            ack: false,
            probe: false,
        };
        peer.send_to(&serde_json::to_vec(&response).unwrap(), socket_addr)
            .await
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if state
                .sudp_direct_confirmed
                .lock()
                .await
                .contains(&session_id)
            {
                assert!(!state
                    .sudp_direct_failures
                    .lock()
                    .await
                    .contains_key(&session_id));
                return;
            }
            time::sleep(Duration::from_millis(20)).await;
        }

        panic!("sudp direct data response did not confirm peer");
    }

    #[tokio::test]
    async fn sudp_direct_pending_payloads_are_bounded_per_session() {
        let (state, _server) = test_state();
        let session_id = "visitor|127.0.0.1:50000";
        let visitor = VisitorConfig {
            name: "visitor".to_string(),
            visitor_type: ProxyType::Sudp,
            server_name: "sudp-echo".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };

        for idx in 0..(SUDP_DIRECT_PENDING_LIMIT + 6) {
            push_sudp_direct_pending(
                &state,
                &visitor,
                session_id,
                format!("payload-{idx}").as_bytes(),
            )
            .await;
        }

        let pending = state.sudp_direct_pending.lock().await;
        let queue = pending.get(session_id).expect("pending queue should exist");
        assert_eq!(queue.len(), SUDP_DIRECT_PENDING_LIMIT);
        assert_eq!(queue.front().unwrap().content, b"payload-6");
        assert_eq!(
            queue.back().unwrap().content,
            format!("payload-{}", SUDP_DIRECT_PENDING_LIMIT + 5).as_bytes()
        );
    }

    #[tokio::test]
    async fn sudp_relay_fallback_still_sends_after_direct_ack_without_response() {
        let (state, mut server) = test_state();
        let session_id = "visitor|127.0.0.1:50000".to_string();
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let socket_addr = socket.local_addr().unwrap();
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        spawn_sudp_direct_visitor_response_loop(
            state.clone(),
            session_id.clone(),
            "sudp-echo".to_string(),
            socket,
        );

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

        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if state
                .sudp_direct_visitor_peers
                .lock()
                .await
                .contains_key(&session_id)
            {
                break;
            }
            time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!state
            .sudp_direct_confirmed
            .lock()
            .await
            .contains(&session_id));

        schedule_sudp_relay_fallback(
            state,
            "sudp-echo".to_string(),
            session_id.clone(),
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
                session_id: got_session_id,
                content,
                sk,
                from_visitor,
            } => {
                assert_eq!(proxy_name, "sudp-echo");
                assert_eq!(got_session_id, session_id);
                assert_eq!(content, b"hello");
                assert_eq!(sk.as_deref(), Some("secret"));
                assert!(from_visitor);
            }
            other => panic!("unexpected fallback message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn sudp_direct_stale_peer_triggers_nat_lookup() {
        let (state, mut server) = test_state();
        let session_id = "local-sudp|127.0.0.1:50000".to_string();
        let stale_peer = "127.0.0.1:40000".parse().unwrap();
        state.sudp_direct_visitor_peers.lock().await.insert(
            session_id.clone(),
            SudpDirectPeer {
                addr: stale_peer,
                updated_at: Instant::now() - SUDP_DIRECT_PEER_TTL - Duration::from_secs(1),
            },
        );
        state
            .sudp_direct_confirmed
            .lock()
            .await
            .insert(session_id.clone());
        let visitor = VisitorConfig {
            name: "local-sudp".to_string(),
            visitor_type: ProxyType::Sudp,
            server_name: "sudp-echo".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };

        let request_state = state.clone();
        let request_session_id = session_id.clone();
        let request = tokio::spawn(async move {
            try_sudp_direct_visitor(&request_state, &visitor, &request_session_id, b"hello").await
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
                sk,
                local_addrs,
            } => {
                assert_eq!(transaction_id, "sudp-echo");
                assert_eq!(proxy_name, "sudp-echo");
                assert_eq!(role, "visitor");
                assert_eq!(sk.as_deref(), Some("secret"));
                assert_eq!(local_addrs.len(), 1);
            }
            other => panic!("unexpected nat hole register: {other:?}"),
        }

        handle_nat_hole_response(
            &state,
            "sudp-echo".to_string(),
            "sudp-echo".to_string(),
            "visitor".to_string(),
            NatHoleResponse {
                peer_observed_addr: String::new(),
                peer_local_addrs: Vec::new(),
                waiting: true,
                error: String::new(),
            },
        )
        .await;

        let used_direct = request.await.unwrap().unwrap();
        assert!(!used_direct);
        assert!(!state
            .sudp_direct_visitor_peers
            .lock()
            .await
            .contains_key(&session_id));
        assert!(!state
            .sudp_direct_confirmed
            .lock()
            .await
            .contains(&session_id));
    }

    #[tokio::test]
    async fn sudp_direct_recent_failure_skips_nat_lookup() {
        let (state, mut server) = test_state();
        let session_id = "local-sudp|127.0.0.1:50000".to_string();
        mark_sudp_direct_failure(&state, &session_id).await;
        let visitor = VisitorConfig {
            name: "local-sudp".to_string(),
            visitor_type: ProxyType::Sudp,
            server_name: "sudp-echo".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };

        let used_direct = try_sudp_direct_visitor(&state, &visitor, &session_id, b"hello")
            .await
            .unwrap();

        assert!(!used_direct);
        assert!(should_skip_sudp_direct(&state, &session_id).await);
        match time::timeout(Duration::from_millis(200), read_msg(&mut server)).await {
            Err(_) | Ok(Err(_)) => {}
            Ok(Ok(other)) => panic!("recent sudp failure still performed NAT lookup: {other:?}"),
        }
    }

    #[tokio::test]
    async fn sudp_direct_failures_are_bounded_and_prune_expired_entries() {
        let (state, _server) = test_state();
        state.sudp_direct_failures.lock().await.insert(
            "expired-sudp".to_string(),
            Instant::now() - SUDP_DIRECT_FAILURE_TTL - Duration::from_secs(1),
        );

        for idx in 0..=SUDP_DIRECT_FAILURE_LIMIT {
            mark_sudp_direct_failure(&state, &format!("sudp-fail-{idx}")).await;
        }

        let failures = state.sudp_direct_failures.lock().await;
        assert_eq!(failures.len(), SUDP_DIRECT_FAILURE_LIMIT);
        assert!(!failures.contains_key("expired-sudp"));
        assert!(!failures.contains_key("sudp-fail-0"));
        assert!(failures.contains_key(&format!("sudp-fail-{SUDP_DIRECT_FAILURE_LIMIT}")));
    }

    #[tokio::test]
    async fn sudp_direct_peers_are_bounded_and_prune_expired_entries() {
        let (state, _server) = test_state();
        state.sudp_direct_visitor_peers.lock().await.insert(
            "expired-sudp-peer".to_string(),
            SudpDirectPeer {
                addr: "127.0.0.1:10000".parse().unwrap(),
                updated_at: Instant::now() - SUDP_DIRECT_PEER_TTL - Duration::from_secs(1),
            },
        );
        state
            .sudp_direct_confirmed
            .lock()
            .await
            .insert("expired-sudp-peer".to_string());

        for idx in 0..=SUDP_DIRECT_PEER_LIMIT {
            let session_id = format!("sudp-peer-{idx}");
            record_sudp_direct_peer(
                &state,
                &session_id,
                format!("127.0.0.1:{}", 30000 + idx).parse().unwrap(),
            )
            .await;
            if idx == 0 {
                state.sudp_direct_confirmed.lock().await.insert(session_id);
            }
        }

        let peers = state.sudp_direct_visitor_peers.lock().await;
        assert_eq!(peers.len(), SUDP_DIRECT_PEER_LIMIT);
        assert!(!peers.contains_key("expired-sudp-peer"));
        assert!(!peers.contains_key("sudp-peer-0"));
        assert!(peers.contains_key(&format!("sudp-peer-{SUDP_DIRECT_PEER_LIMIT}")));
        drop(peers);

        let confirmed = state.sudp_direct_confirmed.lock().await;
        assert!(!confirmed.contains("expired-sudp-peer"));
        assert!(!confirmed.contains("sudp-peer-0"));
    }

    #[tokio::test]
    async fn sudp_direct_visitor_sessions_are_bounded_and_evict_oldest_state() {
        let (state, _server) = test_state();
        let visitor = VisitorConfig {
            name: "local-sudp".to_string(),
            visitor_type: ProxyType::Sudp,
            server_name: "sudp-echo".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };
        let first_session = "sudp-session-0".to_string();

        for idx in 0..=SUDP_DIRECT_SESSION_LIMIT {
            let session_id = format!("sudp-session-{idx}");
            sudp_direct_visitor_socket(&state, &visitor, &session_id)
                .await
                .unwrap();
            if idx == 0 {
                state
                    .sudp_direct_visitor_sessions
                    .lock()
                    .await
                    .insert(session_id.clone(), Instant::now() - Duration::from_secs(1));
                state
                    .sudp_direct_pending
                    .lock()
                    .await
                    .insert(session_id.clone(), VecDeque::new());
                state
                    .sudp_direct_confirmed
                    .lock()
                    .await
                    .insert(session_id.clone());
                state
                    .sudp_direct_probe_sessions
                    .lock()
                    .await
                    .insert(session_id.clone());
                mark_sudp_direct_failure(&state, &session_id).await;
                record_sudp_direct_peer(&state, &session_id, "127.0.0.1:10000".parse().unwrap())
                    .await;
            }
        }

        assert_eq!(
            state.sudp_direct_visitor_sessions.lock().await.len(),
            SUDP_DIRECT_SESSION_LIMIT
        );
        assert_eq!(
            state.sudp_direct_visitor_sockets.lock().await.len(),
            SUDP_DIRECT_SESSION_LIMIT
        );
        assert_eq!(
            state.sudp_direct_visitor_tasks.lock().await.len(),
            SUDP_DIRECT_SESSION_LIMIT
        );
        assert!(!state
            .sudp_direct_visitor_sessions
            .lock()
            .await
            .contains_key(&first_session));
        assert!(!state
            .sudp_direct_visitor_sockets
            .lock()
            .await
            .contains_key(&first_session));
        assert!(!state
            .sudp_direct_visitor_peers
            .lock()
            .await
            .contains_key(&first_session));
        assert!(!state
            .sudp_direct_pending
            .lock()
            .await
            .contains_key(&first_session));
        assert!(!state
            .sudp_direct_confirmed
            .lock()
            .await
            .contains(&first_session));
        assert!(!state
            .sudp_direct_probe_sessions
            .lock()
            .await
            .contains(&first_session));
        assert!(!state
            .sudp_direct_failures
            .lock()
            .await
            .contains_key(&first_session));
        assert!(state
            .sudp_direct_visitor_sessions
            .lock()
            .await
            .contains_key(&format!("sudp-session-{SUDP_DIRECT_SESSION_LIMIT}")));

        let remaining = state
            .sudp_direct_visitor_sessions
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for session_id in remaining {
            remove_sudp_direct_visitor_session_state(&state, &session_id, true).await;
        }
    }

    #[tokio::test]
    async fn sudp_direct_expired_failure_retries_nat_lookup() {
        let (state, mut server) = test_state();
        let session_id = "local-sudp|127.0.0.1:50000".to_string();
        state.sudp_direct_failures.lock().await.insert(
            session_id.clone(),
            Instant::now() - SUDP_DIRECT_FAILURE_TTL - Duration::from_millis(1),
        );
        let visitor = VisitorConfig {
            name: "local-sudp".to_string(),
            visitor_type: ProxyType::Sudp,
            server_name: "sudp-echo".to_string(),
            bind_addr: "127.0.0.1".to_string(),
            bind_port: 0,
            sk: Some("secret".to_string()),
        };

        let request_state = state.clone();
        let request_session_id = session_id.clone();
        let request = tokio::spawn(async move {
            try_sudp_direct_visitor(&request_state, &visitor, &request_session_id, b"hello").await
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
                sk,
                local_addrs,
            } => {
                assert_eq!(transaction_id, "sudp-echo");
                assert_eq!(proxy_name, "sudp-echo");
                assert_eq!(role, "visitor");
                assert_eq!(sk.as_deref(), Some("secret"));
                assert_eq!(local_addrs.len(), 1);
            }
            other => panic!("unexpected nat hole register: {other:?}"),
        }

        handle_nat_hole_response(
            &state,
            "sudp-echo".to_string(),
            "sudp-echo".to_string(),
            "visitor".to_string(),
            NatHoleResponse {
                peer_observed_addr: String::new(),
                peer_local_addrs: Vec::new(),
                waiting: true,
                error: String::new(),
            },
        )
        .await;

        let used_direct = request.await.unwrap().unwrap();
        assert!(!used_direct);
        assert!(!state
            .sudp_direct_failures
            .lock()
            .await
            .contains_key(&session_id));
    }

    #[tokio::test]
    async fn sudp_relay_fallback_sends_when_direct_unconfirmed() {
        let (state, mut server) = test_state();
        let session_id = "session-1".to_string();
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let socket_addr = socket.local_addr().unwrap();
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        state
            .sudp_direct_pending
            .lock()
            .await
            .entry(session_id.clone())
            .or_default()
            .push_back(sudp_direct_visitor_frame(
                &VisitorConfig {
                    name: "visitor".to_string(),
                    visitor_type: ProxyType::Sudp,
                    server_name: "private-udp".to_string(),
                    bind_addr: "127.0.0.1".to_string(),
                    bind_port: 0,
                    sk: Some("secret".to_string()),
                },
                &session_id,
                b"hello".to_vec(),
                false,
            ));
        spawn_sudp_direct_visitor_response_loop(
            state.clone(),
            session_id.clone(),
            "private-udp".to_string(),
            socket,
        );
        schedule_sudp_relay_fallback(
            state.clone(),
            "private-udp".to_string(),
            session_id.clone(),
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
                session_id: got_session_id,
                content,
                sk,
                from_visitor,
            } => {
                assert_eq!(proxy_name, "private-udp");
                assert_eq!(got_session_id, session_id);
                assert_eq!(content, b"hello");
                assert_eq!(sk.as_deref(), Some("secret"));
                assert!(from_visitor);
            }
            other => panic!("unexpected message: {other:?}"),
        }
        assert!(should_skip_sudp_direct(&state, &session_id).await);
        assert!(!state
            .sudp_direct_pending
            .lock()
            .await
            .contains_key(&session_id));

        let ack = DirectSudpFrame {
            magic: DIRECT_SUDP_MAGIC.to_string(),
            proxy_name: "private-udp".to_string(),
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
        let late_direct = time::timeout(Duration::from_millis(300), peer.recv_from(&mut buf)).await;
        assert!(
            late_direct.is_err(),
            "late direct ack flushed payload already sent by relay fallback"
        );
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
