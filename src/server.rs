use crate::{
    config::{ServerConfig, TransportProtocol},
    nathole::{NatHoleCandidate, NatHoleController, NatHoleOutcome, NatHolePeer, NatHoleRole},
    protocol::{read_msg, write_msg, Message, UdpPacketFrame},
    transports::{self, BoxStream},
};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::json;
use std::{
    collections::{HashMap, VecDeque},
    net::{SocketAddr, ToSocketAddrs},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, WriteHalf},
    net::{TcpListener, TcpSocket, TcpStream, UdpSocket},
    sync::{mpsc, Mutex, Notify},
    task::JoinHandle,
    time,
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
struct ServerState {
    cfg: Arc<ServerConfig>,
    controls: Arc<Mutex<HashMap<String, Arc<Control>>>>,
    http_routes: Arc<Mutex<HashMap<String, Vec<ProxyRoute>>>>,
    http_route_next: Arc<Mutex<HashMap<String, usize>>>,
    tcpmux_routes: Arc<Mutex<HashMap<String, Vec<ProxyRoute>>>>,
    tcpmux_route_next: Arc<Mutex<HashMap<String, usize>>>,
    https_routes: Arc<Mutex<HashMap<String, Vec<ProxyRoute>>>>,
    https_route_next: Arc<Mutex<HashMap<String, usize>>>,
    stcp_routes: Arc<Mutex<HashMap<String, StcpRoute>>>,
    stcp_groups: Arc<Mutex<HashMap<String, StcpGroup>>>,
    xtcp_routes: Arc<Mutex<HashMap<String, StcpRoute>>>,
    xtcp_groups: Arc<Mutex<HashMap<String, StcpGroup>>>,
    sudp_routes: Arc<Mutex<HashMap<String, SudpRoute>>>,
    sudp_groups: Arc<Mutex<HashMap<String, SudpGroup>>>,
    sudp_sessions: Arc<Mutex<HashMap<(String, String), Arc<Control>>>>,
    tcp_groups: Arc<Mutex<HashMap<String, TcpGroup>>>,
    udp_groups: Arc<Mutex<HashMap<String, UdpGroup>>>,
    nathole: Arc<NatHoleController>,
    udp_relays: Arc<Mutex<HashMap<String, Arc<UdpSocket>>>>,
    datagram_user_sessions: Arc<Mutex<HashMap<(String, String), Instant>>>,
    proxy_entries: Arc<Mutex<HashMap<String, ProxyEntry>>>,
    allow_ports: Arc<Mutex<Vec<String>>>,
    metrics: Arc<ServerMetrics>,
}

struct Control {
    run_id: String,
    peer_addr: SocketAddr,
    pool_count: usize,
    writer: Mutex<WriteHalf<BoxStream>>,
    work_pool: WorkPool,
}

struct WorkPool {
    queue: Mutex<VecDeque<BoxStream>>,
    notify: Notify,
    queued: AtomicUsize,
    pending: AtomicUsize,
    waiters: AtomicUsize,
    replenishing: AtomicBool,
}

#[derive(Clone)]
struct ProxyRoute {
    control: Arc<Control>,
    proxy_name: String,
    bandwidth_limit: Option<u64>,
    group: Option<String>,
    group_key: Option<String>,
    locations: Vec<String>,
    host_header_rewrite: Option<String>,
    request_headers: HashMap<String, String>,
    http_user: Option<String>,
    http_password: Option<String>,
}

#[derive(Clone)]
struct StcpRoute {
    control: Arc<Control>,
    proxy_name: String,
    sk: Option<String>,
    bandwidth_limit: Option<u64>,
}

struct StcpGroup {
    group_name: String,
    group_key: Option<String>,
    sk: Option<String>,
    routes: Vec<StcpRoute>,
    next: usize,
}

#[derive(Clone)]
struct SudpRoute {
    control: Arc<Control>,
    proxy_name: String,
    sk: Option<String>,
}

struct SudpGroup {
    group_name: String,
    group_key: Option<String>,
    sk: Option<String>,
    routes: Vec<SudpRoute>,
    next: usize,
}

#[derive(Clone)]
struct TcpGroupRoute {
    control: Arc<Control>,
    proxy_name: String,
    bandwidth_limit: Option<u64>,
}

struct TcpGroup {
    group_name: String,
    group_key: Option<String>,
    remote_port: u16,
    remote_addr: String,
    routes: Vec<TcpGroupRoute>,
    next: usize,
    task: JoinHandle<()>,
}

#[derive(Clone)]
struct UdpGroupRoute {
    control: Arc<Control>,
    proxy_name: String,
}

struct QueuedUdpPacket {
    control: Arc<Control>,
    packet: UdpPacketFrame,
}

struct UdpGroup {
    group_name: String,
    group_key: Option<String>,
    remote_port: u16,
    remote_addr: String,
    socket: Arc<UdpSocket>,
    routes: Vec<UdpGroupRoute>,
    next: usize,
    task: JoinHandle<()>,
}

struct ProxyEntry {
    run_id: String,
    proxy_type: String,
    group: Option<String>,
    tcp_group_map_key: Option<String>,
    udp_group_map_key: Option<String>,
    domains: Vec<String>,
    remote_addr: String,
    bandwidth_limit: Option<u64>,
    health: Option<ProxyHealthStatus>,
    started_unix_secs: u64,
    task: Option<JoinHandle<()>>,
}

#[derive(Clone)]
struct ProxyHealthStatus {
    healthy: bool,
    check_type: String,
    detail: String,
    checked_unix_secs: u64,
    updated_unix_secs: u64,
}

struct ServerMetrics {
    started_unix_secs: u64,
    visitor_connections_total: AtomicU64,
    bytes_up_total: AtomicU64,
    bytes_down_total: AtomicU64,
}

const DATAGRAM_USER_SESSION_TTL: Duration = Duration::from_secs(60);

impl Control {
    async fn send(&self, msg: &Message) -> Result<()> {
        let mut writer = self.writer.lock().await;
        write_msg(&mut *writer, msg).await
    }
}

impl WorkPool {
    fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            queued: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
            waiters: AtomicUsize::new(0),
            replenishing: AtomicBool::new(false),
        }
    }

    async fn push(&self, stream: BoxStream) {
        {
            let mut queue = self.queue.lock().await;
            queue.push_back(stream);
        }
        self.queued.fetch_add(1, Ordering::Release);
        let _ = self
            .pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                Some(value.saturating_sub(1))
            });
        self.notify.notify_one();
    }

    async fn pop(&self) -> Option<BoxStream> {
        let mut queue = self.queue.lock().await;
        let stream = queue.pop_front();
        if stream.is_some() {
            self.queued.fetch_sub(1, Ordering::AcqRel);
        }
        stream
    }

    fn reserve_missing(&self, target: usize) -> usize {
        let mut missing = 0;
        let _ = self
            .pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                let available = self.queued.load(Ordering::Acquire).saturating_add(pending);
                if available >= target {
                    return None;
                }
                missing = target - available;
                Some(pending.saturating_add(missing))
            });
        missing
    }

    fn available(&self) -> usize {
        self.queued
            .load(Ordering::Acquire)
            .saturating_add(self.pending.load(Ordering::Acquire))
    }

    fn release_reserved(&self, count: usize) {
        if count == 0 {
            return;
        }
        let _ = self
            .pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                Some(value.saturating_sub(count))
            });
    }
}

pub async fn run(cfg: ServerConfig) -> Result<()> {
    let allow_ports = cfg.allow_ports.clone();
    let state = ServerState {
        cfg: Arc::new(cfg),
        controls: Arc::new(Mutex::new(HashMap::new())),
        http_routes: Arc::new(Mutex::new(HashMap::new())),
        http_route_next: Arc::new(Mutex::new(HashMap::new())),
        tcpmux_routes: Arc::new(Mutex::new(HashMap::new())),
        tcpmux_route_next: Arc::new(Mutex::new(HashMap::new())),
        https_routes: Arc::new(Mutex::new(HashMap::new())),
        https_route_next: Arc::new(Mutex::new(HashMap::new())),
        stcp_routes: Arc::new(Mutex::new(HashMap::new())),
        stcp_groups: Arc::new(Mutex::new(HashMap::new())),
        xtcp_routes: Arc::new(Mutex::new(HashMap::new())),
        xtcp_groups: Arc::new(Mutex::new(HashMap::new())),
        sudp_routes: Arc::new(Mutex::new(HashMap::new())),
        sudp_groups: Arc::new(Mutex::new(HashMap::new())),
        sudp_sessions: Arc::new(Mutex::new(HashMap::new())),
        tcp_groups: Arc::new(Mutex::new(HashMap::new())),
        udp_groups: Arc::new(Mutex::new(HashMap::new())),
        nathole: Arc::new(NatHoleController::default()),
        udp_relays: Arc::new(Mutex::new(HashMap::new())),
        datagram_user_sessions: Arc::new(Mutex::new(HashMap::new())),
        proxy_entries: Arc::new(Mutex::new(HashMap::new())),
        allow_ports: Arc::new(Mutex::new(allow_ports)),
        metrics: Arc::new(ServerMetrics {
            started_unix_secs: unix_now(),
            visitor_connections_total: AtomicU64::new(0),
            bytes_up_total: AtomicU64::new(0),
            bytes_down_total: AtomicU64::new(0),
        }),
    };

    let protocol = state.cfg.transport.protocol;
    if state.cfg.dashboard_port > 0 {
        let dashboard_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = run_dashboard(dashboard_state).await {
                error!("dashboard listener stopped: {err:#}");
            }
        });
    }
    if state.cfg.vhost_http_port > 0 {
        let http_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = run_http_vhost(http_state).await {
                error!("http vhost listener stopped: {err:#}");
            }
        });
    }
    if state.cfg.vhost_https_port > 0 {
        let https_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = run_https_vhost(https_state).await {
                error!("https vhost listener stopped: {err:#}");
            }
        });
    }

    match protocol {
        TransportProtocol::Tcp => run_tcp_control_listener(state).await,
        TransportProtocol::Tcpmux => run_tcp_mux_control_listener(state).await,
        TransportProtocol::Tls => run_tls_control_listener(state).await,
        TransportProtocol::Websocket => run_websocket_control_listener(state).await,
        TransportProtocol::Kcp => run_kcp_control_listener(state).await,
        TransportProtocol::Quic => run_quic_control_listener(state).await,
    }
}

async fn run_tcp_control_listener(state: ServerState) -> Result<()> {
    let listener = bind_tcp_listener(&state.cfg.control_addr())
        .await
        .with_context(|| format!("listen frps control on {}", state.cfg.control_addr()))?;
    let addr = listener
        .local_addr()
        .context("read frps listener address")?;
    info!("frps tcp control listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await.context("accept frps connection")?;
        configure_tcp_stream(&stream);
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(state, Box::new(stream), peer).await {
                debug!("connection {peer} closed: {err:#}");
            }
        });
    }
}

async fn run_tcp_mux_control_listener(state: ServerState) -> Result<()> {
    let listener = bind_tcp_listener(&state.cfg.control_addr())
        .await
        .with_context(|| {
            format!(
                "listen frps tcp mux control on {}",
                state.cfg.control_addr()
            )
        })?;
    let addr = listener
        .local_addr()
        .context("read frps tcp mux listener address")?;
    info!("frps tcp mux control listening on {addr}");

    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .context("accept frps tcp mux connection")?;
        configure_tcp_stream(&stream);
        let mux = transports::tcp_mux::MuxSession::server(stream);
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                let stream = match mux.accept_stream().await {
                    Ok(stream) => stream,
                    Err(err) => {
                        debug!("tcp mux connection {peer} stream accept stopped: {err:#}");
                        break;
                    }
                };
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(state, stream, peer).await {
                        debug!("tcp mux stream {peer} closed: {err:#}");
                    }
                });
            }
        });
    }
}

async fn run_tls_control_listener(state: ServerState) -> Result<()> {
    let listener = bind_tcp_listener(&state.cfg.control_addr())
        .await
        .with_context(|| format!("listen frps tls control on {}", state.cfg.control_addr()))?;
    let addr = listener
        .local_addr()
        .context("read frps tls listener address")?;
    info!("frps tls control listening on {addr}");

    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .context("accept frps tls tcp connection")?;
        configure_tcp_stream(&stream);
        let state = state.clone();
        tokio::spawn(async move {
            let stream = match transports::tls::accept(stream).await {
                Ok(stream) => stream,
                Err(err) => {
                    debug!("tls handshake from {peer} failed: {err:#}");
                    return;
                }
            };
            if let Err(err) = handle_connection(state, Box::new(stream), peer).await {
                debug!("tls connection {peer} closed: {err:#}");
            }
        });
    }
}

async fn run_websocket_control_listener(state: ServerState) -> Result<()> {
    let listener = bind_tcp_listener(&state.cfg.control_addr())
        .await
        .with_context(|| {
            format!(
                "listen frps websocket control on {}",
                state.cfg.control_addr()
            )
        })?;
    let addr = listener
        .local_addr()
        .context("read frps websocket listener address")?;
    info!("frps websocket control listening on {addr}");

    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .context("accept frps websocket tcp connection")?;
        configure_tcp_stream(&stream);
        let state = state.clone();
        tokio::spawn(async move {
            let stream = match transports::websocket::accept(stream).await {
                Ok(stream) => stream,
                Err(err) => {
                    debug!("websocket upgrade from {peer} failed: {err:#}");
                    return;
                }
            };
            if let Err(err) = handle_connection(state, Box::new(stream), peer).await {
                debug!("websocket connection {peer} closed: {err:#}");
            }
        });
    }
}

async fn bind_tcp_listener(addr: &str) -> Result<TcpListener> {
    let socket_addr = addr
        .to_socket_addrs()
        .with_context(|| format!("resolve listen address {addr}"))?
        .next()
        .ok_or_else(|| anyhow!("listen address {addr} did not resolve"))?;
    let socket = if socket_addr.is_ipv6() {
        TcpSocket::new_v6().context("create ipv6 tcp socket")?
    } else {
        TcpSocket::new_v4().context("create ipv4 tcp socket")?
    };
    socket
        .set_reuseaddr(true)
        .context("set tcp listener reuseaddr")?;
    socket
        .bind(socket_addr)
        .with_context(|| format!("bind tcp listener on {socket_addr}"))?;
    socket
        .listen(4096)
        .with_context(|| format!("listen tcp on {socket_addr}"))
}

fn configure_tcp_stream(stream: &TcpStream) {
    if let Err(err) = stream.set_nodelay(true) {
        debug!("set TCP_NODELAY failed: {err:#}");
    }
}

async fn run_kcp_control_listener(state: ServerState) -> Result<()> {
    let addr = state
        .cfg
        .control_addr()
        .parse::<SocketAddr>()
        .with_context(|| format!("parse frps kcp bind addr {}", state.cfg.control_addr()))?;
    let mut listener = transports::kcp::bind(addr).await?;
    let local_addr = listener.local_addr().context("read kcp listener address")?;
    info!("frps kcp control listening on {local_addr}");

    loop {
        let (stream, peer) = listener.accept().await.context("accept kcp connection")?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(state, Box::new(stream), peer).await {
                debug!("kcp connection {peer} closed: {err:#}");
            }
        });
    }
}

async fn run_quic_control_listener(state: ServerState) -> Result<()> {
    let addr = state
        .cfg
        .control_addr()
        .parse::<SocketAddr>()
        .with_context(|| format!("parse frps quic bind addr {}", state.cfg.control_addr()))?;
    let endpoint = transports::quic::QuicServerEndpoint::bind(addr)?;
    let local_addr = endpoint.local_addr()?;
    info!("frps quic control listening on {local_addr}");

    loop {
        let connection = endpoint.accept_connection().await?;
        let peer = connection.remote_addr();
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                let stream = match connection.accept_stream().await {
                    Ok(stream) => stream,
                    Err(err) => {
                        debug!("quic connection {peer} stream accept stopped: {err:#}");
                        break;
                    }
                };
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(state, Box::new(stream), peer).await {
                        debug!("quic stream {peer} closed: {err:#}");
                    }
                });
            }
        });
    }
}

async fn handle_connection(
    state: ServerState,
    mut stream: BoxStream,
    peer: SocketAddr,
) -> Result<()> {
    let first = read_msg(&mut stream).await?;
    match first {
        Message::Login {
            version,
            hostname,
            os,
            arch,
            run_id,
            token,
            pool_count,
        } => {
            verify_token(&state.cfg, token.as_deref())?;
            call_plugin_hook(
                state.cfg.plugins.login_url.as_deref(),
                json!({
                    "op": "Login",
                    "version": version,
                    "hostname": hostname,
                    "os": os,
                    "arch": arch,
                    "run_id": run_id,
                    "peer": peer.to_string(),
                    "pool_count": pool_count,
                }),
            )
            .await?;
            let run_id = if run_id.is_empty() {
                Uuid::new_v4().to_string()
            } else {
                run_id
            };
            info!("client login run_id={run_id} version={version} host={hostname} os={os} arch={arch} peer={peer}");
            register_control(state, stream, run_id, pool_count, peer).await
        }
        Message::NewWorkConn { run_id, token } => {
            verify_token(&state.cfg, token.as_deref())?;
            register_work_conn(state, run_id, stream).await
        }
        Message::NewVisitorConn {
            proxy_name,
            sk,
            token,
        } => {
            verify_token(&state.cfg, token.as_deref())?;
            handle_stcp_visitor_conn(state, stream, peer, proxy_name, sk).await
        }
        other => bail!("unexpected first message from {peer}: {other:?}"),
    }
}

async fn register_control(
    state: ServerState,
    stream: BoxStream,
    run_id: String,
    pool_count: usize,
    peer: SocketAddr,
) -> Result<()> {
    let (mut reader, writer) = tokio::io::split(stream);
    let control = Arc::new(Control {
        run_id: run_id.clone(),
        peer_addr: peer,
        pool_count,
        writer: Mutex::new(writer),
        work_pool: WorkPool::new(),
    });

    {
        let mut controls = state.controls.lock().await;
        controls.insert(run_id.clone(), control.clone());
    }

    control
        .send(&Message::LoginResp {
            version: env!("CARGO_PKG_VERSION").to_string(),
            run_id: run_id.clone(),
            error: String::new(),
        })
        .await?;

    loop {
        let msg = read_msg(&mut reader).await;
        match msg {
            Ok(Message::NewProxy {
                proxy_name,
                proxy_type,
                remote_port,
                group,
                group_key,
                custom_domains,
                locations,
                host_header_rewrite,
                request_headers,
                http_user,
                http_password,
                bandwidth_limit,
                sk,
            }) => {
                let resp = match start_proxy(
                    state.clone(),
                    control.clone(),
                    proxy_name.clone(),
                    proxy_type.clone(),
                    remote_port,
                    group,
                    group_key,
                    custom_domains,
                    locations,
                    host_header_rewrite,
                    request_headers,
                    http_user,
                    http_password,
                    bandwidth_limit,
                    sk,
                )
                .await
                {
                    Ok(remote_addr) => Message::NewProxyResp {
                        proxy_name,
                        remote_addr,
                        error: String::new(),
                    },
                    Err(err) => Message::NewProxyResp {
                        proxy_name,
                        remote_addr: String::new(),
                        error: err.to_string(),
                    },
                };
                control.send(&resp).await?;
            }
            Ok(Message::Ping { token }) => {
                let error = verify_token(&state.cfg, token.as_deref())
                    .err()
                    .map(|err| err.to_string())
                    .unwrap_or_default();
                control.send(&Message::Pong { error }).await?;
            }
            Ok(Message::UdpPacket {
                proxy_name,
                content,
                visitor_addr,
            }) => {
                if let Err(err) =
                    send_udp_packet_to_visitor(state.clone(), &proxy_name, &content, &visitor_addr)
                        .await
                {
                    warn!("send udp packet for proxy {proxy_name} failed: {err:#}");
                }
            }
            Ok(Message::UdpPacketBatch { packets }) => {
                send_udp_packet_batch_to_visitors(state.clone(), packets).await;
            }
            Ok(Message::SudpPacket {
                proxy_name,
                session_id,
                content,
                sk,
                from_visitor,
            }) => {
                if let Err(err) = handle_sudp_packet(
                    state.clone(),
                    control.clone(),
                    proxy_name,
                    session_id,
                    content,
                    sk,
                    from_visitor,
                )
                .await
                {
                    warn!("sudp packet handling failed: {err:#}");
                }
            }
            Ok(Message::NatHoleRegister {
                transaction_id,
                proxy_name,
                role,
                local_addrs,
            }) => {
                let resp = handle_nat_hole_register(
                    state.clone(),
                    control.clone(),
                    peer,
                    transaction_id,
                    proxy_name,
                    role,
                    local_addrs,
                )
                .await;
                control.send(&resp).await?;
            }
            Ok(Message::CloseProxy { proxy_name }) => {
                if close_proxy(state.clone(), &proxy_name).await? {
                    info!("proxy {proxy_name} closed");
                } else {
                    debug!("close_proxy ignored for unknown proxy {proxy_name}");
                }
            }
            Ok(Message::HealthStatus {
                proxy_name,
                healthy,
                check_type,
                detail,
                checked_unix_secs,
            }) => {
                update_proxy_health(
                    state.clone(),
                    &proxy_name,
                    healthy,
                    check_type,
                    detail,
                    checked_unix_secs,
                )
                .await;
            }
            Ok(other) => {
                warn!("ignored unsupported control message: {other:?}");
            }
            Err(err) => {
                info!("control {run_id} disconnected: {err:#}");
                state.controls.lock().await.remove(&run_id);
                let removed_nat_peers = state.nathole.remove_run_id(&run_id);
                if removed_nat_peers > 0 {
                    debug!("removed {removed_nat_peers} nat hole peers for run_id {run_id}");
                }
                close_proxies_for_run_id(state.clone(), &run_id).await?;
                return Ok(());
            }
        }
    }
}

async fn register_work_conn(state: ServerState, run_id: String, stream: BoxStream) -> Result<()> {
    let control = {
        let controls = state.controls.lock().await;
        controls.get(&run_id).cloned()
    }
    .ok_or_else(|| anyhow!("no active control for run_id {run_id}"))?;

    control.work_pool.push(stream).await;
    Ok(())
}

async fn handle_nat_hole_register(
    state: ServerState,
    control: Arc<Control>,
    peer: SocketAddr,
    transaction_id: String,
    proxy_name: String,
    role: String,
    local_addrs: Vec<String>,
) -> Message {
    let parsed = parse_nat_hole_role(&role)
        .and_then(|role| parse_socket_addrs(&local_addrs).map(|local_addrs| (role, local_addrs)));
    let (parsed_role, parsed_local_addrs) = match parsed {
        Ok(parsed) => parsed,
        Err(err) => {
            return Message::NatHoleResp {
                transaction_id,
                proxy_name,
                role,
                observed_addr: peer.to_string(),
                peer_observed_addr: String::new(),
                peer_local_addrs: Vec::new(),
                waiting: false,
                error: err.to_string(),
            };
        }
    };

    let current_peer = NatHolePeer {
        transaction_id: transaction_id.clone(),
        proxy_name: proxy_name.clone(),
        run_id: control.run_id.clone(),
        role: parsed_role,
        observed_addr: peer,
        local_addrs: parsed_local_addrs,
    };
    let registered = state.nathole.register(current_peer.clone());

    match registered {
        Ok(NatHoleOutcome::Waiting) => Message::NatHoleResp {
            transaction_id,
            proxy_name,
            role,
            observed_addr: peer.to_string(),
            peer_observed_addr: String::new(),
            peer_local_addrs: Vec::new(),
            waiting: true,
            error: String::new(),
        },
        Ok(NatHoleOutcome::Matched(candidate)) => {
            notify_nat_hole_peer(state, current_peer.clone(), &candidate).await;
            Message::NatHoleResp {
                transaction_id,
                proxy_name,
                role,
                observed_addr: peer.to_string(),
                peer_observed_addr: candidate.observed_addr.to_string(),
                peer_local_addrs: candidate
                    .local_addrs
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
                waiting: false,
                error: String::new(),
            }
        }
        Err(err) => Message::NatHoleResp {
            transaction_id,
            proxy_name,
            role,
            observed_addr: peer.to_string(),
            peer_observed_addr: String::new(),
            peer_local_addrs: Vec::new(),
            waiting: false,
            error: err.to_string(),
        },
    }
}

async fn notify_nat_hole_peer(
    state: ServerState,
    current_peer: NatHolePeer,
    candidate: &NatHoleCandidate,
) {
    if candidate.peer_run_id == current_peer.run_id {
        return;
    }
    let peer_control = {
        let controls = state.controls.lock().await;
        controls.get(&candidate.peer_run_id).cloned()
    };
    let Some(peer_control) = peer_control else {
        debug!(
            "nat hole peer control {} is no longer connected",
            candidate.peer_run_id
        );
        return;
    };
    let msg = Message::NatHoleResp {
        transaction_id: current_peer.transaction_id.clone(),
        proxy_name: current_peer.proxy_name.clone(),
        role: nat_hole_role_name(candidate.peer_role).to_string(),
        observed_addr: candidate.observed_addr.to_string(),
        peer_observed_addr: current_peer.observed_addr.to_string(),
        peer_local_addrs: current_peer
            .local_addrs
            .iter()
            .map(ToString::to_string)
            .collect(),
        waiting: false,
        error: String::new(),
    };
    if let Err(err) = peer_control.send(&msg).await {
        debug!(
            "notify nat hole peer {} for transaction {} failed: {err:#}",
            candidate.peer_run_id, current_peer.transaction_id
        );
    }
}

async fn handle_stcp_visitor_conn(
    state: ServerState,
    mut stream: BoxStream,
    peer: SocketAddr,
    proxy_name: String,
    sk: Option<String>,
) -> Result<()> {
    let direct_route = {
        let routes = state.stcp_routes.lock().await;
        routes.get(&proxy_name).cloned()
    };
    let route = match direct_route {
        Some(route) => Some(route),
        None => {
            let direct_xtcp_route = {
                let routes = state.xtcp_routes.lock().await;
                routes.get(&proxy_name).cloned()
            };
            match direct_xtcp_route {
                Some(route) => Some(route),
                None => {
                    match next_private_tcp_group_route(state.clone(), "stcp", &proxy_name).await {
                        Some(route) => Some(route),
                        None => {
                            next_private_tcp_group_route(state.clone(), "xtcp", &proxy_name).await
                        }
                    }
                }
            }
        }
    };

    let Some(route) = route else {
        write_msg(
            &mut stream,
            &Message::NewVisitorConnResp {
                proxy_name,
                error: "stcp proxy is not registered".to_string(),
            },
        )
        .await?;
        return Ok(());
    };

    if route.sk != sk {
        write_msg(
            &mut stream,
            &Message::NewVisitorConnResp {
                proxy_name,
                error: "stcp secret key mismatch".to_string(),
            },
        )
        .await?;
        return Ok(());
    }

    write_msg(
        &mut stream,
        &Message::NewVisitorConnResp {
            proxy_name: route.proxy_name.clone(),
            error: String::new(),
        },
    )
    .await?;

    state
        .metrics
        .visitor_connections_total
        .fetch_add(1, Ordering::Relaxed);
    let (up, down) = bridge_stcp_visitor(route, stream, peer).await?;
    state
        .metrics
        .bytes_up_total
        .fetch_add(up, Ordering::Relaxed);
    state
        .metrics
        .bytes_down_total
        .fetch_add(down, Ordering::Relaxed);
    Ok(())
}

async fn start_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    proxy_type: String,
    remote_port: u16,
    group: Option<String>,
    group_key: Option<String>,
    custom_domains: Vec<String>,
    locations: Vec<String>,
    host_header_rewrite: Option<String>,
    request_headers: HashMap<String, String>,
    http_user: Option<String>,
    http_password: Option<String>,
    bandwidth_limit: Option<String>,
    sk: Option<String>,
) -> Result<String> {
    close_proxy(state.clone(), &proxy_name).await?;
    call_plugin_hook(
        state.cfg.plugins.new_proxy_url.as_deref(),
        json!({
            "op": "NewProxy",
            "run_id": control.run_id,
            "proxy_name": proxy_name,
            "proxy_type": proxy_type,
            "remote_port": remote_port,
            "group": group,
            "custom_domains": custom_domains,
            "locations": locations,
            "host_header_rewrite": host_header_rewrite,
            "request_headers": request_headers,
            "http_user": http_user,
            "bandwidth_limit": bandwidth_limit,
            "sk": sk,
        }),
    )
    .await?;
    let bandwidth_limit = bandwidth_limit
        .as_deref()
        .map(parse_bandwidth_limit)
        .transpose()
        .with_context(|| format!("parse bandwidthLimit for proxy {proxy_name}"))?;

    match proxy_type.as_str() {
        "tcp" => {
            if let Some(group) = group {
                start_tcp_group_proxy(
                    state,
                    control,
                    proxy_name,
                    remote_port,
                    group,
                    group_key,
                    bandwidth_limit,
                )
                .await
            } else {
                start_tcp_proxy(state, control, proxy_name, remote_port, bandwidth_limit).await
            }
        }
        "udp" => {
            if let Some(group) = group {
                start_udp_group_proxy(state, control, proxy_name, remote_port, group, group_key)
                    .await
            } else {
                start_udp_proxy(state, control, proxy_name, remote_port).await
            }
        }
        "http" => {
            start_http_proxy(
                state,
                control,
                proxy_name,
                custom_domains,
                group,
                group_key,
                locations,
                host_header_rewrite,
                request_headers,
                http_user,
                http_password,
                bandwidth_limit,
            )
            .await
        }
        "https" => {
            start_https_proxy(
                state,
                control,
                proxy_name,
                custom_domains,
                group,
                group_key,
                bandwidth_limit,
            )
            .await
        }
        "tcpmux" => {
            start_tcpmux_proxy(
                state,
                control,
                proxy_name,
                custom_domains,
                group,
                group_key,
                bandwidth_limit,
            )
            .await
        }
        "stcp" => {
            start_stcp_proxy(
                state,
                control,
                proxy_name,
                group,
                group_key,
                sk,
                bandwidth_limit,
            )
            .await
        }
        "sudp" => {
            start_sudp_proxy(
                state,
                control,
                proxy_name,
                group,
                group_key,
                sk,
                bandwidth_limit,
            )
            .await
        }
        "xtcp" => {
            start_xtcp_proxy(
                state,
                control,
                proxy_name,
                group,
                group_key,
                sk,
                bandwidth_limit,
            )
            .await
        }
        _ => bail!("proxy type {proxy_type} is not implemented"),
    }
}

fn ensure_remote_port_allowed(
    allow_ports: &[String],
    proxy_name: &str,
    remote_port: u16,
) -> Result<()> {
    if remote_port == 0 || remote_port_allowed(allow_ports, remote_port)? {
        return Ok(());
    }

    bail!("remotePort {remote_port} for proxy {proxy_name} is not allowed by allowPorts")
}

async fn ensure_state_remote_port_allowed(
    state: &ServerState,
    proxy_name: &str,
    remote_port: u16,
) -> Result<()> {
    let allow_ports = state.allow_ports.lock().await;
    ensure_remote_port_allowed(&allow_ports, proxy_name, remote_port)
}

async fn start_tcp_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    remote_port: u16,
    bandwidth_limit: Option<u64>,
) -> Result<String> {
    ensure_state_remote_port_allowed(&state, &proxy_name, remote_port).await?;
    let bind_addr = format!("{}:{}", state.cfg.proxy_bind_addr, remote_port);
    let listener = bind_tcp_listener(&bind_addr)
        .await
        .with_context(|| format!("listen tcp proxy {proxy_name} on {bind_addr}"))?;
    let remote_addr = listener
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or(bind_addr);
    info!("tcp proxy {proxy_name} listening on {remote_addr}");

    let run_id = control.run_id.clone();
    let task_proxy_name = proxy_name.clone();
    let metrics = state.metrics.clone();
    let new_user_conn_url = state.cfg.plugins.new_user_conn_url.clone();
    let task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((visitor, visitor_addr)) => {
                    configure_tcp_stream(&visitor);
                    let control = control.clone();
                    let proxy_name = task_proxy_name.clone();
                    let metrics = metrics.clone();
                    let new_user_conn_url = new_user_conn_url.clone();
                    metrics
                        .visitor_connections_total
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(async move {
                        if let Err(err) = handle_tcp_visitor(
                            control,
                            metrics,
                            proxy_name.clone(),
                            visitor,
                            visitor_addr,
                            bandwidth_limit,
                            new_user_conn_url,
                        )
                        .await
                        {
                            warn!("tcp proxy {proxy_name} visitor {visitor_addr} failed: {err:#}");
                        }
                    });
                }
                Err(err) => {
                    error!("tcp proxy listener failed: {err:#}");
                    break;
                }
            }
        }
    });
    state.proxy_entries.lock().await.insert(
        proxy_name,
        ProxyEntry {
            run_id,
            proxy_type: "tcp".to_string(),
            group: None,
            tcp_group_map_key: None,
            udp_group_map_key: None,
            domains: Vec::new(),
            remote_addr: remote_addr.clone(),
            bandwidth_limit,
            health: None,
            started_unix_secs: unix_now(),
            task: Some(task),
        },
    );

    Ok(remote_addr)
}

async fn start_tcp_group_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    remote_port: u16,
    group: String,
    group_key: Option<String>,
    bandwidth_limit: Option<u64>,
) -> Result<String> {
    ensure_state_remote_port_allowed(&state, &proxy_name, remote_port).await?;
    if group.trim().is_empty() {
        bail!("tcp proxy {proxy_name} has empty group");
    }

    let map_key = group_map_key(remote_port, &group);
    let route = TcpGroupRoute {
        control: control.clone(),
        proxy_name: proxy_name.clone(),
        bandwidth_limit,
    };

    let remote_addr = {
        let mut groups = state.tcp_groups.lock().await;
        if let Some(existing) = groups.get_mut(&map_key) {
            if existing.group_key != group_key {
                bail!("tcp group {group} remotePort {remote_port} groupKey mismatch");
            }
            if existing
                .routes
                .iter()
                .any(|route| route.proxy_name == proxy_name)
            {
                bail!("tcp group {group} already contains proxy {proxy_name}");
            }
            existing.routes.push(route);
            existing.remote_addr.clone()
        } else {
            let bind_addr = format!("{}:{}", state.cfg.proxy_bind_addr, remote_port);
            let listener = bind_tcp_listener(&bind_addr)
                .await
                .with_context(|| format!("listen tcp group {group} on {bind_addr}"))?;
            let remote_addr = listener
                .local_addr()
                .map(|addr| addr.to_string())
                .unwrap_or(bind_addr);
            let task_state = state.clone();
            let task_key = map_key.clone();
            let task_group = group.clone();
            let metrics = state.metrics.clone();
            let new_user_conn_url = state.cfg.plugins.new_user_conn_url.clone();
            let task = tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((visitor, visitor_addr)) => {
                            configure_tcp_stream(&visitor);
                            let route = match next_tcp_group_route(task_state.clone(), &task_key)
                                .await
                            {
                                Some(route) => route,
                                None => {
                                    debug!(
                                        "tcp group {task_group} has no available route for {visitor_addr}"
                                    );
                                    continue;
                                }
                            };
                            let metrics = metrics.clone();
                            let new_user_conn_url = new_user_conn_url.clone();
                            metrics
                                .visitor_connections_total
                                .fetch_add(1, Ordering::Relaxed);
                            tokio::spawn(async move {
                                if let Err(err) = handle_tcp_visitor(
                                    route.control,
                                    metrics,
                                    route.proxy_name.clone(),
                                    visitor,
                                    visitor_addr,
                                    route.bandwidth_limit,
                                    new_user_conn_url,
                                )
                                .await
                                {
                                    warn!(
                                        "tcp group route {} visitor {visitor_addr} failed: {err:#}",
                                        route.proxy_name
                                    );
                                }
                            });
                        }
                        Err(err) => {
                            error!("tcp group listener {task_group} failed: {err:#}");
                            break;
                        }
                    }
                }
            });

            info!("tcp group {group} listening on {remote_addr}");
            groups.insert(
                map_key.clone(),
                TcpGroup {
                    group_name: group.clone(),
                    group_key: group_key.clone(),
                    remote_port,
                    remote_addr: remote_addr.clone(),
                    routes: vec![route],
                    next: 0,
                    task,
                },
            );
            remote_addr
        }
    };

    state.proxy_entries.lock().await.insert(
        proxy_name,
        ProxyEntry {
            run_id: control.run_id.clone(),
            proxy_type: "tcp".to_string(),
            group: Some(group),
            tcp_group_map_key: Some(map_key),
            udp_group_map_key: None,
            domains: Vec::new(),
            remote_addr: remote_addr.clone(),
            bandwidth_limit,
            health: None,
            started_unix_secs: unix_now(),
            task: None,
        },
    );

    Ok(remote_addr)
}

async fn next_tcp_group_route(state: ServerState, map_key: &str) -> Option<TcpGroupRoute> {
    let mut groups = state.tcp_groups.lock().await;
    let group = groups.get_mut(map_key)?;
    if group.routes.is_empty() {
        return None;
    }
    let index = group.next % group.routes.len();
    group.next = group.next.wrapping_add(1);
    group.routes.get(index).cloned()
}

fn spawn_udp_packet_batcher(
    name: String,
    mut packet_rx: mpsc::Receiver<QueuedUdpPacket>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(first) = packet_rx.recv().await {
            let mut queued = Vec::with_capacity(32);
            queued.push(first);
            time::sleep(Duration::from_millis(2)).await;
            while queued.len() < 32 {
                let Ok(packet) = packet_rx.try_recv() else {
                    break;
                };
                queued.push(packet);
            }

            let mut by_control: HashMap<String, (Arc<Control>, Vec<UdpPacketFrame>)> =
                HashMap::new();
            for queued_packet in queued {
                let entry = by_control
                    .entry(queued_packet.control.run_id.clone())
                    .or_insert_with(|| (queued_packet.control.clone(), Vec::new()));
                entry.1.push(queued_packet.packet);
            }

            for (_, (control, mut packets)) in by_control {
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

                if let Err(err) = control.send(&msg).await {
                    warn!("{name} failed to forward packet batch to client: {err:#}");
                }
            }
        }
    })
}

async fn start_udp_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    remote_port: u16,
) -> Result<String> {
    ensure_state_remote_port_allowed(&state, &proxy_name, remote_port).await?;
    let bind_addr = format!("{}:{}", state.cfg.proxy_bind_addr, remote_port);
    let socket = Arc::new(
        UdpSocket::bind(&bind_addr)
            .await
            .with_context(|| format!("listen udp proxy {proxy_name} on {bind_addr}"))?,
    );
    let remote_addr = socket
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or(bind_addr);

    state
        .udp_relays
        .lock()
        .await
        .insert(proxy_name.clone(), socket.clone());
    info!("udp proxy {proxy_name} listening on {remote_addr}");

    let run_id = control.run_id.clone();
    let task_proxy_name = proxy_name.clone();
    let metrics = state.metrics.clone();
    let (packet_tx, packet_rx) = mpsc::channel::<QueuedUdpPacket>(128);
    spawn_udp_packet_batcher(format!("udp proxy {proxy_name}"), packet_rx);
    let task_state = state.clone();
    let task_control = control.clone();
    let task_remote_addr = remote_addr.clone();
    let task = tokio::spawn(async move {
        let mut buf = vec![0_u8; 64 * 1024];
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((n, visitor_addr)) => {
                    let visitor_addr = visitor_addr.to_string();
                    if let Err(err) = call_datagram_new_user_conn_hook(
                        &task_state,
                        &task_control,
                        "udp",
                        &task_proxy_name,
                        &visitor_addr,
                        &task_remote_addr,
                    )
                    .await
                    {
                        warn!("udp proxy {task_proxy_name} new user hook rejected {visitor_addr}: {err:#}");
                        continue;
                    }
                    metrics
                        .visitor_connections_total
                        .fetch_add(1, Ordering::Relaxed);
                    metrics
                        .bytes_up_total
                        .fetch_add(n as u64, Ordering::Relaxed);
                    let packet = UdpPacketFrame {
                        proxy_name: task_proxy_name.clone(),
                        content: buf[..n].to_vec(),
                        visitor_addr,
                    };
                    if let Err(err) = packet_tx
                        .send(QueuedUdpPacket {
                            control: task_control.clone(),
                            packet,
                        })
                        .await
                    {
                        warn!("udp proxy {task_proxy_name} failed to queue packet batch: {err:#}");
                        break;
                    }
                }
                Err(err) => {
                    warn!("udp proxy {task_proxy_name} recv failed: {err:#}");
                    break;
                }
            }
        }
    });
    state.proxy_entries.lock().await.insert(
        proxy_name,
        ProxyEntry {
            run_id,
            proxy_type: "udp".to_string(),
            group: None,
            tcp_group_map_key: None,
            udp_group_map_key: None,
            domains: Vec::new(),
            remote_addr: remote_addr.clone(),
            bandwidth_limit: None,
            health: None,
            started_unix_secs: unix_now(),
            task: Some(task),
        },
    );

    Ok(remote_addr)
}

async fn start_udp_group_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    remote_port: u16,
    group: String,
    group_key: Option<String>,
) -> Result<String> {
    ensure_state_remote_port_allowed(&state, &proxy_name, remote_port).await?;
    if group.trim().is_empty() {
        bail!("udp proxy {proxy_name} has empty group");
    }

    let map_key = group_map_key(remote_port, &group);
    let route = UdpGroupRoute {
        control: control.clone(),
        proxy_name: proxy_name.clone(),
    };

    let remote_addr = {
        let mut groups = state.udp_groups.lock().await;
        if let Some(existing) = groups.get_mut(&map_key) {
            if existing.group_key != group_key {
                bail!("udp group {group} remotePort {remote_port} groupKey mismatch");
            }
            if existing
                .routes
                .iter()
                .any(|route| route.proxy_name == proxy_name)
            {
                bail!("udp group {group} already contains proxy {proxy_name}");
            }
            state
                .udp_relays
                .lock()
                .await
                .insert(proxy_name.clone(), existing.socket.clone());
            existing.routes.push(route);
            existing.remote_addr.clone()
        } else {
            let bind_addr = format!("{}:{}", state.cfg.proxy_bind_addr, remote_port);
            let socket = Arc::new(
                UdpSocket::bind(&bind_addr)
                    .await
                    .with_context(|| format!("listen udp group {group} on {bind_addr}"))?,
            );
            let remote_addr = socket
                .local_addr()
                .map(|addr| addr.to_string())
                .unwrap_or(bind_addr);
            state
                .udp_relays
                .lock()
                .await
                .insert(proxy_name.clone(), socket.clone());

            let task_state = state.clone();
            let task_key = map_key.clone();
            let task_group = group.clone();
            let task_socket = socket.clone();
            let task_remote_addr = remote_addr.clone();
            let metrics = state.metrics.clone();
            let (packet_tx, packet_rx) = mpsc::channel::<QueuedUdpPacket>(128);
            spawn_udp_packet_batcher(format!("udp group {group}"), packet_rx);
            let task = tokio::spawn(async move {
                let mut buf = vec![0_u8; 64 * 1024];
                loop {
                    match task_socket.recv_from(&mut buf).await {
                        Ok((n, visitor_addr)) => {
                            metrics
                                .visitor_connections_total
                                .fetch_add(1, Ordering::Relaxed);
                            metrics
                                .bytes_up_total
                                .fetch_add(n as u64, Ordering::Relaxed);
                            let route = match next_udp_group_route(task_state.clone(), &task_key)
                                .await
                            {
                                Some(route) => route,
                                None => {
                                    debug!(
                                        "udp group {task_group} has no available route for {visitor_addr}"
                                    );
                                    continue;
                                }
                            };
                            let visitor_addr = visitor_addr.to_string();
                            if let Err(err) = call_datagram_new_user_conn_hook(
                                &task_state,
                                &route.control,
                                "udp",
                                &route.proxy_name,
                                &visitor_addr,
                                &task_remote_addr,
                            )
                            .await
                            {
                                warn!(
                                    "udp group route {} new user hook rejected {visitor_addr}: {err:#}",
                                    route.proxy_name
                                );
                                continue;
                            }
                            let packet = UdpPacketFrame {
                                proxy_name: route.proxy_name.clone(),
                                content: buf[..n].to_vec(),
                                visitor_addr,
                            };
                            if let Err(err) = packet_tx
                                .send(QueuedUdpPacket {
                                    control: route.control.clone(),
                                    packet,
                                })
                                .await
                            {
                                warn!(
                                    "udp group route {} failed to queue packet batch: {err:#}",
                                    route.proxy_name
                                );
                                break;
                            }
                        }
                        Err(err) => {
                            warn!("udp group listener {task_group} failed: {err:#}");
                            break;
                        }
                    }
                }
            });

            info!("udp group {group} listening on {remote_addr}");
            groups.insert(
                map_key.clone(),
                UdpGroup {
                    group_name: group.clone(),
                    group_key: group_key.clone(),
                    remote_port,
                    remote_addr: remote_addr.clone(),
                    socket,
                    routes: vec![route],
                    next: 0,
                    task,
                },
            );
            remote_addr
        }
    };

    state.proxy_entries.lock().await.insert(
        proxy_name,
        ProxyEntry {
            run_id: control.run_id.clone(),
            proxy_type: "udp".to_string(),
            group: Some(group),
            tcp_group_map_key: None,
            udp_group_map_key: Some(map_key),
            domains: Vec::new(),
            remote_addr: remote_addr.clone(),
            bandwidth_limit: None,
            health: None,
            started_unix_secs: unix_now(),
            task: None,
        },
    );

    Ok(remote_addr)
}

async fn next_udp_group_route(state: ServerState, map_key: &str) -> Option<UdpGroupRoute> {
    let mut groups = state.udp_groups.lock().await;
    let group = groups.get_mut(map_key)?;
    if group.routes.is_empty() {
        return None;
    }
    let index = group.next % group.routes.len();
    group.next = group.next.wrapping_add(1);
    group.routes.get(index).cloned()
}

async fn call_datagram_new_user_conn_hook(
    state: &ServerState,
    control: &Arc<Control>,
    proxy_type: &str,
    proxy_name: &str,
    user_addr: &str,
    remote_addr: &str,
) -> Result<()> {
    if state.cfg.plugins.new_user_conn_url.is_none() {
        return Ok(());
    }

    let now = Instant::now();
    let proxy_key = format!("{proxy_type}:{proxy_name}");
    let session_key = (proxy_key, user_addr.to_string());
    let should_call = {
        let mut sessions = state.datagram_user_sessions.lock().await;
        sessions
            .retain(|_, updated_at| now.duration_since(*updated_at) <= DATAGRAM_USER_SESSION_TTL);
        let should_call = !sessions.contains_key(&session_key);
        sessions.insert(session_key.clone(), now);
        should_call
    };
    if !should_call {
        return Ok(());
    }

    let result = call_plugin_hook(
        state.cfg.plugins.new_user_conn_url.as_deref(),
        json!({
            "op": "NewUserConn",
            "proxy_name": proxy_name,
            "proxy_type": proxy_type,
            "run_id": control.run_id.clone(),
            "user_addr": user_addr,
            "remote_addr": remote_addr,
        }),
    )
    .await;
    if result.is_err() {
        state
            .datagram_user_sessions
            .lock()
            .await
            .remove(&session_key);
    }
    result
}

async fn send_udp_packet_to_visitor(
    state: ServerState,
    proxy_name: &str,
    content: &[u8],
    visitor_addr: &str,
) -> Result<()> {
    let (socket, visitor_addr) =
        resolve_udp_visitor_destination(&state, proxy_name, visitor_addr).await?;
    socket
        .send_to(content, visitor_addr)
        .await
        .context("send udp response to visitor")?;
    state
        .metrics
        .bytes_down_total
        .fetch_add(content.len() as u64, Ordering::Relaxed);
    Ok(())
}

async fn send_udp_packet_batch_to_visitors(state: ServerState, packets: Vec<UdpPacketFrame>) {
    let mut destinations: HashMap<(String, String), (Arc<UdpSocket>, SocketAddr)> = HashMap::new();
    let mut bytes_down = 0_u64;

    for packet in packets {
        let key = (packet.proxy_name, packet.visitor_addr);
        let destination = if let Some(destination) = destinations.get(&key) {
            Some((destination.0.clone(), destination.1))
        } else {
            match resolve_udp_visitor_destination(&state, &key.0, &key.1).await {
                Ok(destination) => {
                    destinations.insert(key.clone(), (destination.0.clone(), destination.1));
                    Some(destination)
                }
                Err(err) => {
                    warn!(
                        "send udp packet batch item for proxy {} failed: {err:#}",
                        key.0
                    );
                    None
                }
            }
        };
        let Some((socket, visitor_addr)) = destination else {
            continue;
        };
        if let Err(err) = socket.send_to(&packet.content, visitor_addr).await {
            warn!(
                "send udp packet batch item for proxy {} failed: {err:#}",
                key.0
            );
            continue;
        }
        bytes_down += packet.content.len() as u64;
    }

    if bytes_down > 0 {
        state
            .metrics
            .bytes_down_total
            .fetch_add(bytes_down, Ordering::Relaxed);
    }
}

async fn resolve_udp_visitor_destination(
    state: &ServerState,
    proxy_name: &str,
    visitor_addr: &str,
) -> Result<(Arc<UdpSocket>, SocketAddr)> {
    let socket = {
        let relays = state.udp_relays.lock().await;
        relays
            .get(proxy_name)
            .cloned()
            .ok_or_else(|| anyhow!("unknown udp proxy {proxy_name}"))?
    };
    let visitor_addr = visitor_addr
        .parse::<SocketAddr>()
        .with_context(|| format!("parse udp visitor address {visitor_addr}"))?;
    Ok((socket, visitor_addr))
}

async fn handle_sudp_packet(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    session_id: String,
    content: Vec<u8>,
    sk: Option<String>,
    from_visitor: bool,
) -> Result<()> {
    if from_visitor {
        let content_len = content.len() as u64;
        let route = {
            let routes = state.sudp_routes.lock().await;
            routes.get(&proxy_name).cloned()
        };
        let route = match route {
            Some(route) => route,
            None => next_sudp_group_route(state.clone(), &proxy_name)
                .await
                .ok_or_else(|| anyhow!("unknown sudp proxy {proxy_name}"))?,
        };
        if route.sk != sk {
            bail!("sudp secret key mismatch for proxy {proxy_name}");
        }
        let route_proxy_name = route.proxy_name.clone();
        let remote_addr = format!("sudp://{proxy_name}");
        call_datagram_new_user_conn_hook(
            &state,
            &route.control,
            "sudp",
            &route_proxy_name,
            &session_id,
            &remote_addr,
        )
        .await?;
        state
            .sudp_sessions
            .lock()
            .await
            .insert((route_proxy_name.clone(), session_id.clone()), control);
        route
            .control
            .send(&Message::SudpPacket {
                proxy_name: route_proxy_name,
                session_id,
                content,
                sk: None,
                from_visitor: true,
            })
            .await?;
        state
            .metrics
            .bytes_up_total
            .fetch_add(content_len, Ordering::Relaxed);
    } else {
        let content_len = content.len() as u64;
        let visitor_control = {
            let sessions = state.sudp_sessions.lock().await;
            sessions
                .get(&(proxy_name.clone(), session_id.clone()))
                .cloned()
                .ok_or_else(|| anyhow!("unknown sudp session {proxy_name}/{session_id}"))?
        };
        visitor_control
            .send(&Message::SudpPacket {
                proxy_name,
                session_id,
                content,
                sk: None,
                from_visitor: false,
            })
            .await?;
        state
            .metrics
            .bytes_down_total
            .fetch_add(content_len, Ordering::Relaxed);
    }
    Ok(())
}

async fn start_http_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    custom_domains: Vec<String>,
    group: Option<String>,
    group_key: Option<String>,
    locations: Vec<String>,
    host_header_rewrite: Option<String>,
    request_headers: HashMap<String, String>,
    http_user: Option<String>,
    http_password: Option<String>,
    bandwidth_limit: Option<u64>,
) -> Result<String> {
    if state.cfg.vhost_http_port == 0 {
        bail!("vhostHTTPPort must be configured on frps for http proxies");
    }
    if custom_domains.is_empty() {
        bail!("http proxy {proxy_name} needs customDomains");
    }

    let route = ProxyRoute {
        control: control.clone(),
        proxy_name: proxy_name.clone(),
        bandwidth_limit,
        group: group.clone(),
        group_key: group_key.clone(),
        locations: normalize_locations(&locations),
        host_header_rewrite,
        request_headers,
        http_user,
        http_password,
    };
    let mut routes = state.http_routes.lock().await;
    for domain in &custom_domains {
        let domain_routes = routes.entry(normalize_host(domain)).or_default();
        ensure_http_group_key_matches(domain_routes, &route)?;
        domain_routes.push(route.clone());
    }

    let first = &custom_domains[0];
    let remote_addr = format!("http://{}:{}", first, state.cfg.vhost_http_port);
    info!("http proxy {proxy_name} registered for domains {custom_domains:?}");
    state.proxy_entries.lock().await.insert(
        proxy_name,
        ProxyEntry {
            run_id: control.run_id.clone(),
            proxy_type: "http".to_string(),
            group,
            tcp_group_map_key: None,
            udp_group_map_key: None,
            domains: custom_domains,
            remote_addr: remote_addr.clone(),
            bandwidth_limit,
            health: None,
            started_unix_secs: unix_now(),
            task: None,
        },
    );
    Ok(remote_addr)
}

async fn start_https_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    custom_domains: Vec<String>,
    group: Option<String>,
    group_key: Option<String>,
    bandwidth_limit: Option<u64>,
) -> Result<String> {
    if state.cfg.vhost_https_port == 0 {
        bail!("vhostHTTPSPort must be configured on frps for https proxies");
    }
    if custom_domains.is_empty() {
        bail!("https proxy {proxy_name} needs customDomains");
    }

    let route = ProxyRoute {
        control: control.clone(),
        proxy_name: proxy_name.clone(),
        bandwidth_limit,
        group: group.clone(),
        group_key: group_key.clone(),
        locations: Vec::new(),
        host_header_rewrite: None,
        request_headers: HashMap::new(),
        http_user: None,
        http_password: None,
    };
    let mut routes = state.https_routes.lock().await;
    for domain in &custom_domains {
        let domain_routes = routes.entry(normalize_host(domain)).or_default();
        ensure_http_group_key_matches(domain_routes, &route)?;
        domain_routes.push(route.clone());
    }

    let first = &custom_domains[0];
    let remote_addr = format!("https://{}:{}", first, state.cfg.vhost_https_port);
    info!("https proxy {proxy_name} registered for domains {custom_domains:?}");
    state.proxy_entries.lock().await.insert(
        proxy_name,
        ProxyEntry {
            run_id: control.run_id.clone(),
            proxy_type: "https".to_string(),
            group,
            tcp_group_map_key: None,
            udp_group_map_key: None,
            domains: custom_domains,
            remote_addr: remote_addr.clone(),
            bandwidth_limit,
            health: None,
            started_unix_secs: unix_now(),
            task: None,
        },
    );
    Ok(remote_addr)
}

async fn start_tcpmux_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    custom_domains: Vec<String>,
    group: Option<String>,
    group_key: Option<String>,
    bandwidth_limit: Option<u64>,
) -> Result<String> {
    if state.cfg.vhost_http_port == 0 {
        bail!("vhostHTTPPort must be configured on frps for tcpmux proxies");
    }
    if custom_domains.is_empty() {
        bail!("tcpmux proxy {proxy_name} needs customDomains");
    }

    let route = ProxyRoute {
        control: control.clone(),
        proxy_name: proxy_name.clone(),
        bandwidth_limit,
        group: group.clone(),
        group_key: group_key.clone(),
        locations: Vec::new(),
        host_header_rewrite: None,
        request_headers: HashMap::new(),
        http_user: None,
        http_password: None,
    };
    let mut routes = state.tcpmux_routes.lock().await;
    for domain in &custom_domains {
        let domain_routes = routes.entry(normalize_host(domain)).or_default();
        ensure_http_group_key_matches(domain_routes, &route)?;
        domain_routes.push(route.clone());
    }

    let first = &custom_domains[0];
    let remote_addr = format!("tcpmux://{}:{}", first, state.cfg.vhost_http_port);
    info!("tcpmux proxy {proxy_name} registered for domains {custom_domains:?}");
    state.proxy_entries.lock().await.insert(
        proxy_name,
        ProxyEntry {
            run_id: control.run_id.clone(),
            proxy_type: "tcpmux".to_string(),
            group,
            tcp_group_map_key: None,
            udp_group_map_key: None,
            domains: custom_domains,
            remote_addr: remote_addr.clone(),
            bandwidth_limit,
            health: None,
            started_unix_secs: unix_now(),
            task: None,
        },
    );
    Ok(remote_addr)
}

async fn start_stcp_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    group: Option<String>,
    group_key: Option<String>,
    sk: Option<String>,
    bandwidth_limit: Option<u64>,
) -> Result<String> {
    let route = StcpRoute {
        control: control.clone(),
        proxy_name: proxy_name.clone(),
        sk: sk.clone(),
        bandwidth_limit,
    };
    let mut entry_group = group.clone();

    let remote_addr = if let Some(group_name) = group.clone() {
        if group_name.trim().is_empty() {
            bail!("stcp proxy {proxy_name} has empty group");
        }
        let group_name = group_name.trim().to_string();
        entry_group = Some(group_name.clone());
        let mut groups = state.stcp_groups.lock().await;
        if let Some(existing) = groups.get_mut(&group_name) {
            if existing.group_key != group_key {
                bail!("stcp group {group_name} groupKey mismatch");
            }
            if existing.sk != sk {
                bail!("stcp group {group_name} sk mismatch");
            }
            if existing
                .routes
                .iter()
                .any(|route| route.proxy_name == proxy_name)
            {
                bail!("stcp group {group_name} already contains proxy {proxy_name}");
            }
            existing.routes.push(route);
        } else {
            groups.insert(
                group_name.clone(),
                StcpGroup {
                    group_name: group_name.clone(),
                    group_key,
                    sk,
                    routes: vec![route],
                    next: 0,
                },
            );
        }
        info!("stcp proxy {proxy_name} registered in group {group_name}");
        format!("stcp://{group_name}")
    } else {
        let remote_addr = format!("stcp://{proxy_name}");
        state
            .stcp_routes
            .lock()
            .await
            .insert(proxy_name.clone(), route);
        info!("stcp proxy {proxy_name} registered");
        remote_addr
    };

    state.proxy_entries.lock().await.insert(
        proxy_name,
        ProxyEntry {
            run_id: control.run_id.clone(),
            proxy_type: "stcp".to_string(),
            group: entry_group,
            tcp_group_map_key: None,
            udp_group_map_key: None,
            domains: Vec::new(),
            remote_addr: remote_addr.clone(),
            bandwidth_limit,
            health: None,
            started_unix_secs: unix_now(),
            task: None,
        },
    );
    Ok(remote_addr)
}

async fn start_xtcp_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    group: Option<String>,
    group_key: Option<String>,
    sk: Option<String>,
    bandwidth_limit: Option<u64>,
) -> Result<String> {
    let route = StcpRoute {
        control: control.clone(),
        proxy_name: proxy_name.clone(),
        sk: sk.clone(),
        bandwidth_limit,
    };
    let mut entry_group = group.clone();

    let remote_addr = if let Some(group_name) = group.clone() {
        if group_name.trim().is_empty() {
            bail!("xtcp proxy {proxy_name} has empty group");
        }
        let group_name = group_name.trim().to_string();
        entry_group = Some(group_name.clone());
        let mut groups = state.xtcp_groups.lock().await;
        if let Some(existing) = groups.get_mut(&group_name) {
            if existing.group_key != group_key {
                bail!("xtcp group {group_name} groupKey mismatch");
            }
            if existing.sk != sk {
                bail!("xtcp group {group_name} sk mismatch");
            }
            if existing
                .routes
                .iter()
                .any(|route| route.proxy_name == proxy_name)
            {
                bail!("xtcp group {group_name} already contains proxy {proxy_name}");
            }
            existing.routes.push(route);
        } else {
            groups.insert(
                group_name.clone(),
                StcpGroup {
                    group_name: group_name.clone(),
                    group_key,
                    sk,
                    routes: vec![route],
                    next: 0,
                },
            );
        }
        info!("xtcp proxy {proxy_name} registered in relay group {group_name}");
        format!("xtcp://{group_name}")
    } else {
        let remote_addr = format!("xtcp://{proxy_name}");
        state
            .xtcp_routes
            .lock()
            .await
            .insert(proxy_name.clone(), route);
        info!("xtcp proxy {proxy_name} registered with server-relayed fallback");
        remote_addr
    };

    state.proxy_entries.lock().await.insert(
        proxy_name,
        ProxyEntry {
            run_id: control.run_id.clone(),
            proxy_type: "xtcp".to_string(),
            group: entry_group,
            tcp_group_map_key: None,
            udp_group_map_key: None,
            domains: Vec::new(),
            remote_addr: remote_addr.clone(),
            bandwidth_limit,
            health: None,
            started_unix_secs: unix_now(),
            task: None,
        },
    );
    Ok(remote_addr)
}

async fn next_stcp_group_route(state: ServerState, group_name: &str) -> Option<StcpRoute> {
    let mut groups = state.stcp_groups.lock().await;
    let group = groups.get_mut(group_name)?;
    if group.routes.is_empty() {
        return None;
    }
    let index = group.next % group.routes.len();
    group.next = group.next.wrapping_add(1);
    group.routes.get(index).cloned()
}

async fn next_private_tcp_group_route(
    state: ServerState,
    protocol: &str,
    group_name: &str,
) -> Option<StcpRoute> {
    match protocol {
        "stcp" => next_stcp_group_route(state, group_name).await,
        "xtcp" => {
            let mut groups = state.xtcp_groups.lock().await;
            let group = groups.get_mut(group_name)?;
            if group.routes.is_empty() {
                return None;
            }
            let index = group.next % group.routes.len();
            group.next = group.next.wrapping_add(1);
            group.routes.get(index).cloned()
        }
        _ => None,
    }
}

async fn start_sudp_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    group: Option<String>,
    group_key: Option<String>,
    sk: Option<String>,
    bandwidth_limit: Option<u64>,
) -> Result<String> {
    let route = SudpRoute {
        control: control.clone(),
        proxy_name: proxy_name.clone(),
        sk: sk.clone(),
    };
    let mut entry_group = group.clone();

    let remote_addr = if let Some(group_name) = group.clone() {
        if group_name.trim().is_empty() {
            bail!("sudp proxy {proxy_name} has empty group");
        }
        let group_name = group_name.trim().to_string();
        entry_group = Some(group_name.clone());
        let mut groups = state.sudp_groups.lock().await;
        if let Some(existing) = groups.get_mut(&group_name) {
            if existing.group_key != group_key {
                bail!("sudp group {group_name} groupKey mismatch");
            }
            if existing.sk != sk {
                bail!("sudp group {group_name} sk mismatch");
            }
            if existing
                .routes
                .iter()
                .any(|route| route.proxy_name == proxy_name)
            {
                bail!("sudp group {group_name} already contains proxy {proxy_name}");
            }
            existing.routes.push(route);
        } else {
            groups.insert(
                group_name.clone(),
                SudpGroup {
                    group_name: group_name.clone(),
                    group_key,
                    sk,
                    routes: vec![route],
                    next: 0,
                },
            );
        }
        info!("sudp proxy {proxy_name} registered in group {group_name}");
        format!("sudp://{group_name}")
    } else {
        let remote_addr = format!("sudp://{proxy_name}");
        state
            .sudp_routes
            .lock()
            .await
            .insert(proxy_name.clone(), route);
        info!("sudp proxy {proxy_name} registered");
        remote_addr
    };

    state.proxy_entries.lock().await.insert(
        proxy_name,
        ProxyEntry {
            run_id: control.run_id.clone(),
            proxy_type: "sudp".to_string(),
            group: entry_group,
            tcp_group_map_key: None,
            udp_group_map_key: None,
            domains: Vec::new(),
            remote_addr: remote_addr.clone(),
            bandwidth_limit,
            health: None,
            started_unix_secs: unix_now(),
            task: None,
        },
    );
    Ok(remote_addr)
}

async fn next_sudp_group_route(state: ServerState, group_name: &str) -> Option<SudpRoute> {
    let mut groups = state.sudp_groups.lock().await;
    let group = groups.get_mut(group_name)?;
    if group.routes.is_empty() {
        return None;
    }
    let index = group.next % group.routes.len();
    group.next = group.next.wrapping_add(1);
    group.routes.get(index).cloned()
}

async fn handle_tcp_visitor(
    control: Arc<Control>,
    metrics: Arc<ServerMetrics>,
    proxy_name: String,
    mut visitor: TcpStream,
    visitor_addr: SocketAddr,
    bandwidth_limit: Option<u64>,
    new_user_conn_url: Option<String>,
) -> Result<()> {
    call_plugin_hook(
        new_user_conn_url.as_deref(),
        json!({
            "op": "NewUserConn",
            "proxy_name": proxy_name.clone(),
            "proxy_type": "tcp",
            "run_id": control.run_id.clone(),
            "user_addr": visitor_addr.to_string(),
            "remote_addr": visitor.local_addr().map(|addr| addr.to_string()).unwrap_or_default(),
        }),
    )
    .await?;
    let mut work = acquire_work_conn(&control).await?;

    write_msg(
        &mut work,
        &Message::StartWorkConn {
            proxy_name: proxy_name.clone(),
            src_addr: visitor_addr.to_string(),
            dst_addr: String::new(),
            error: String::new(),
        },
    )
    .await?;

    let (up, down) = copy_bidirectional_limited(&mut visitor, &mut work, bandwidth_limit)
        .await
        .with_context(|| {
            format!(
                "proxy bytes for {proxy_name} via control {}",
                control.run_id
            )
        })?;
    metrics.bytes_up_total.fetch_add(up, Ordering::Relaxed);
    metrics.bytes_down_total.fetch_add(down, Ordering::Relaxed);
    Ok(())
}

async fn bridge_stcp_visitor(
    route: StcpRoute,
    mut visitor: BoxStream,
    visitor_addr: SocketAddr,
) -> Result<(u64, u64)> {
    let mut work = acquire_work_conn(&route.control).await?;

    write_msg(
        &mut work,
        &Message::StartWorkConn {
            proxy_name: route.proxy_name.clone(),
            src_addr: visitor_addr.to_string(),
            dst_addr: "stcp".to_string(),
            error: String::new(),
        },
    )
    .await?;

    copy_bidirectional_limited(&mut visitor, &mut work, route.bandwidth_limit)
        .await
        .with_context(|| {
            format!(
                "proxy stcp bytes for {} via control {}",
                route.proxy_name, route.control.run_id
            )
        })
}

async fn run_dashboard(state: ServerState) -> Result<()> {
    let addr = format!("{}:{}", state.cfg.dashboard_addr, state.cfg.dashboard_port);
    let listener = bind_tcp_listener(&addr)
        .await
        .with_context(|| format!("listen dashboard on {addr}"))?;
    let local_addr = listener.local_addr().context("read dashboard address")?;
    info!("dashboard listening on {local_addr}");

    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .context("accept dashboard visitor")?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_dashboard_request(state, stream).await {
                debug!("dashboard visitor {peer} failed: {err:#}");
            }
        });
    }
}

async fn handle_dashboard_request(state: ServerState, mut stream: TcpStream) -> Result<()> {
    let prefix = read_http_prefix(&mut stream).await?;
    let request_line = std::str::from_utf8(&prefix)
        .ok()
        .and_then(|text| text.lines().next())
        .unwrap_or_default();
    let method = request_line.split_whitespace().next().unwrap_or("GET");
    let path = request_line.split_whitespace().nth(1).unwrap_or("/");

    match (method, path) {
        ("GET", "/api/status") => {
            let body = dashboard_status_json(&state).await?.to_string();
            write_http_response(&mut stream, "200 OK", "application/json", body.as_bytes()).await?;
            return Ok(());
        }
        ("GET", "/api/clients") => {
            let body = client_list_json(&state).await?.to_string();
            write_http_response(&mut stream, "200 OK", "application/json", body.as_bytes()).await?;
            return Ok(());
        }
        ("GET", "/api/proxies") => {
            let body = proxy_list_json(&state).await?.to_string();
            write_http_response(&mut stream, "200 OK", "application/json", body.as_bytes()).await?;
            return Ok(());
        }
        ("GET", "/api/groups") => {
            let body = tcp_group_list_json(&state).await?.to_string();
            write_http_response(&mut stream, "200 OK", "application/json", body.as_bytes()).await?;
            return Ok(());
        }
        ("GET", "/api/metrics") => {
            let body = metrics_json(&state).to_string();
            write_http_response(&mut stream, "200 OK", "application/json", body.as_bytes()).await?;
            return Ok(());
        }
        ("POST", "/api/metrics/reset") => {
            if !dashboard_admin_authorized(&state, &prefix) {
                write_http_response(
                    &mut stream,
                    "401 Unauthorized",
                    "application/json",
                    br#"{"error":"unauthorized"}"#,
                )
                .await?;
                return Ok(());
            }
            reset_metrics(&state);
            let body = metrics_json(&state).to_string();
            write_http_response(&mut stream, "200 OK", "application/json", body.as_bytes()).await?;
            return Ok(());
        }
        ("GET", "/api/config/allow_ports") => {
            let body = allow_ports_json(&state).await.to_string();
            write_http_response(&mut stream, "200 OK", "application/json", body.as_bytes()).await?;
            return Ok(());
        }
        ("POST", "/api/config/allow_ports") | ("PUT", "/api/config/allow_ports") => {
            if !dashboard_admin_authorized(&state, &prefix) {
                write_http_response(
                    &mut stream,
                    "401 Unauthorized",
                    "application/json",
                    br#"{"error":"unauthorized"}"#,
                )
                .await?;
                return Ok(());
            }

            let body = read_http_body(&mut stream, &prefix).await?;
            let allow_ports = parse_allow_ports_update(&body)?;
            validate_allow_ports(&allow_ports)?;
            *state.allow_ports.lock().await = allow_ports.clone();
            let body = json!({
                "allow_ports": allow_ports,
            })
            .to_string();
            write_http_response(&mut stream, "200 OK", "application/json", body.as_bytes()).await?;
            return Ok(());
        }
        ("POST", _) | ("DELETE", _) => {
            if let Some(run_id) = admin_close_client_run_id(method, path) {
                if !dashboard_admin_authorized(&state, &prefix) {
                    write_http_response(
                        &mut stream,
                        "401 Unauthorized",
                        "application/json",
                        br#"{"error":"unauthorized"}"#,
                    )
                    .await?;
                    return Ok(());
                }

                let closed = close_client_proxies(state.clone(), &run_id).await?;
                let status = if closed.is_empty() {
                    "404 Not Found"
                } else {
                    "200 OK"
                };
                let body = json!({
                    "run_id": run_id,
                    "closed_count": closed.len(),
                    "closed": closed,
                })
                .to_string();
                write_http_response(&mut stream, status, "application/json", body.as_bytes())
                    .await?;
                return Ok(());
            }

            if let Some((protocol, group)) = admin_close_group_target(method, path) {
                if !dashboard_admin_authorized(&state, &prefix) {
                    write_http_response(
                        &mut stream,
                        "401 Unauthorized",
                        "application/json",
                        br#"{"error":"unauthorized"}"#,
                    )
                    .await?;
                    return Ok(());
                }

                let closed = close_proxy_group(state.clone(), &protocol, &group).await?;
                let status = if closed.is_empty() {
                    "404 Not Found"
                } else {
                    "200 OK"
                };
                let body = json!({
                    "protocol": protocol,
                    "group": group,
                    "closed_count": closed.len(),
                    "closed": closed,
                })
                .to_string();
                write_http_response(&mut stream, status, "application/json", body.as_bytes())
                    .await?;
                return Ok(());
            }

            if let Some(proxy_name) = admin_close_proxy_name(method, path) {
                if !dashboard_admin_authorized(&state, &prefix) {
                    write_http_response(
                        &mut stream,
                        "401 Unauthorized",
                        "application/json",
                        br#"{"error":"unauthorized"}"#,
                    )
                    .await?;
                    return Ok(());
                }

                let closed = close_proxy(state.clone(), &proxy_name).await?;
                let status = if closed { "200 OK" } else { "404 Not Found" };
                let body = json!({
                    "proxy_name": proxy_name,
                    "closed": closed,
                })
                .to_string();
                write_http_response(&mut stream, status, "application/json", body.as_bytes())
                    .await?;
                return Ok(());
            }
        }
        _ => {}
    }

    let status = dashboard_status_json(&state).await?;
    let body = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>frprs</title></head>\
         <body><h1>frprs dashboard</h1><pre>{}</pre></body></html>",
        serde_json::to_string_pretty(&status)?
    );
    write_http_response(
        &mut stream,
        "200 OK",
        "text/html; charset=utf-8",
        body.as_bytes(),
    )
    .await
}

async fn dashboard_status_json(state: &ServerState) -> Result<serde_json::Value> {
    let client_count = state.controls.lock().await.len();
    let proxy_count = state.proxy_entries.lock().await.len();
    let clients = client_list_json(state).await?;
    let proxies = proxy_list_json(state).await?;
    let groups = tcp_group_list_json(state).await?;
    let metrics = metrics_json(state);

    Ok(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "started_unix_secs": state.metrics.started_unix_secs,
        "client_count": client_count,
        "proxy_count": proxy_count,
        "clients": clients,
        "proxies": proxies,
        "groups": groups,
        "metrics": metrics,
    }))
}

async fn client_list_json(state: &ServerState) -> Result<serde_json::Value> {
    let controls = state.controls.lock().await;
    let clients = controls
        .values()
        .map(|control| {
            json!({
                "run_id": control.run_id,
                "peer_addr": control.peer_addr.to_string(),
                "pool_count": control.pool_count,
                "work_pool_queued": control.work_pool.queued.load(Ordering::Relaxed),
                "work_pool_pending": control.work_pool.pending.load(Ordering::Relaxed),
                "work_pool_waiters": control.work_pool.waiters.load(Ordering::Relaxed),
                "work_pool_replenishing": control.work_pool.replenishing.load(Ordering::Relaxed),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!(clients))
}

async fn proxy_list_json(state: &ServerState) -> Result<serde_json::Value> {
    let proxies = state.proxy_entries.lock().await;
    let proxy_list = proxies
        .iter()
        .map(|(name, entry)| {
            let health = entry.health.as_ref().map(|health| {
                json!({
                    "healthy": health.healthy,
                    "type": health.check_type,
                    "detail": health.detail,
                    "checked_unix_secs": health.checked_unix_secs,
                    "updated_unix_secs": health.updated_unix_secs,
                })
            });
            json!({
                "name": name,
                "run_id": entry.run_id,
                "type": entry.proxy_type,
                "group": entry.group,
                "domains": entry.domains,
                "remote_addr": entry.remote_addr,
                "bandwidth_limit_bytes_per_second": entry.bandwidth_limit,
                "health": health,
                "started_unix_secs": entry.started_unix_secs,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!(proxy_list))
}

async fn tcp_group_list_json(state: &ServerState) -> Result<serde_json::Value> {
    let tcp_groups = state.tcp_groups.lock().await;
    let mut group_list = tcp_groups
        .values()
        .map(|group| {
            json!({
                "protocol": "tcp",
                "group": group.group_name,
                "remote_port": group.remote_port,
                "remote_addr": group.remote_addr,
                "member_count": group.routes.len(),
                "members": group.routes.iter().map(|route| route.proxy_name.clone()).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    drop(tcp_groups);

    let udp_groups = state.udp_groups.lock().await;
    group_list.extend(udp_groups.values().map(|group| {
        json!({
            "protocol": "udp",
            "group": group.group_name,
            "remote_port": group.remote_port,
            "remote_addr": group.remote_addr,
            "member_count": group.routes.len(),
            "members": group.routes.iter().map(|route| route.proxy_name.clone()).collect::<Vec<_>>(),
        })
    }));
    drop(udp_groups);

    let stcp_groups = state.stcp_groups.lock().await;
    group_list.extend(stcp_groups.values().map(|group| {
        json!({
            "protocol": "stcp",
            "group": group.group_name,
            "remote_port": null,
            "remote_addr": format!("stcp://{}", group.group_name),
            "member_count": group.routes.len(),
            "members": group.routes.iter().map(|route| route.proxy_name.clone()).collect::<Vec<_>>(),
        })
    }));
    drop(stcp_groups);

    let xtcp_groups = state.xtcp_groups.lock().await;
    group_list.extend(xtcp_groups.values().map(|group| {
        json!({
            "protocol": "xtcp",
            "group": group.group_name,
            "remote_port": null,
            "remote_addr": format!("xtcp://{}", group.group_name),
            "member_count": group.routes.len(),
            "members": group.routes.iter().map(|route| route.proxy_name.clone()).collect::<Vec<_>>(),
        })
    }));
    drop(xtcp_groups);

    let sudp_groups = state.sudp_groups.lock().await;
    group_list.extend(sudp_groups.values().map(|group| {
        json!({
            "protocol": "sudp",
            "group": group.group_name,
            "remote_port": null,
            "remote_addr": format!("sudp://{}", group.group_name),
            "member_count": group.routes.len(),
            "members": group.routes.iter().map(|route| route.proxy_name.clone()).collect::<Vec<_>>(),
        })
    }));
    drop(sudp_groups);

    let proxies = state.proxy_entries.lock().await;
    let mut vhost_groups: HashMap<(String, String), (String, Vec<String>)> = HashMap::new();
    for (name, entry) in proxies.iter() {
        if !matches!(entry.proxy_type.as_str(), "http" | "https" | "tcpmux") {
            continue;
        }
        let Some(group) = entry.group.clone() else {
            continue;
        };
        let (remote_addr, members) = vhost_groups
            .entry((entry.proxy_type.clone(), group))
            .or_insert_with(|| (entry.remote_addr.clone(), Vec::new()));
        if remote_addr.is_empty() {
            *remote_addr = entry.remote_addr.clone();
        }
        members.push(name.clone());
    }
    drop(proxies);

    group_list.extend(vhost_groups.into_iter().map(
        |((protocol, group), (remote_addr, members))| {
            json!({
                "protocol": protocol,
                "group": group,
                "remote_port": null,
                "remote_addr": remote_addr,
                "member_count": members.len(),
                "members": members,
            })
        },
    ));
    Ok(json!(group_list))
}

fn metrics_json(state: &ServerState) -> serde_json::Value {
    json!({
        "visitor_connections_total": state.metrics.visitor_connections_total.load(Ordering::Relaxed),
        "bytes_up_total": state.metrics.bytes_up_total.load(Ordering::Relaxed),
        "bytes_down_total": state.metrics.bytes_down_total.load(Ordering::Relaxed),
    })
}

fn reset_metrics(state: &ServerState) {
    state
        .metrics
        .visitor_connections_total
        .store(0, Ordering::Relaxed);
    state.metrics.bytes_up_total.store(0, Ordering::Relaxed);
    state.metrics.bytes_down_total.store(0, Ordering::Relaxed);
}

async fn allow_ports_json(state: &ServerState) -> serde_json::Value {
    let allow_ports = state.allow_ports.lock().await.clone();
    json!({
        "allow_ports": allow_ports,
    })
}

fn admin_close_proxy_name(method: &str, path: &str) -> Option<String> {
    let rest = path.strip_prefix("/api/proxies/")?;
    match method {
        "DELETE" => Some(rest.trim_matches('/').to_string()).filter(|name| !name.is_empty()),
        "POST" => rest
            .strip_suffix("/close")
            .map(|name| name.trim_matches('/').to_string())
            .filter(|name| !name.is_empty()),
        _ => None,
    }
}

fn admin_close_client_run_id(method: &str, path: &str) -> Option<String> {
    let rest = path.strip_prefix("/api/clients/")?;
    match method {
        "DELETE" => Some(rest.trim_matches('/').to_string()).filter(|run_id| !run_id.is_empty()),
        "POST" => rest
            .strip_suffix("/close")
            .map(|run_id| run_id.trim_matches('/').to_string())
            .filter(|run_id| !run_id.is_empty()),
        _ => None,
    }
}

fn admin_close_group_target(method: &str, path: &str) -> Option<(String, String)> {
    let rest = path.strip_prefix("/api/groups/")?;
    let mut parts = rest.trim_matches('/').split('/');
    let protocol = parts.next()?.to_ascii_lowercase();
    let group = parts.next()?.to_string();
    if protocol.is_empty() || group.is_empty() {
        return None;
    }

    match method {
        "DELETE" if parts.next().is_none() => Some((protocol, group)),
        "POST" if parts.next() == Some("close") && parts.next().is_none() => {
            Some((protocol, group))
        }
        _ => None,
    }
}

fn dashboard_admin_authorized(state: &ServerState, prefix: &[u8]) -> bool {
    let Some(expected) = state.cfg.auth.token.as_deref() else {
        return true;
    };

    parse_http_header(prefix, "authorization")
        .and_then(|value| value.strip_prefix("Bearer ").map(str::to_string))
        .as_deref()
        == Some(expected)
        || parse_http_header(prefix, "x-frprs-token").as_deref() == Some(expected)
}

async fn run_http_vhost(state: ServerState) -> Result<()> {
    let addr = format!(
        "{}:{}",
        state.cfg.proxy_bind_addr, state.cfg.vhost_http_port
    );
    let listener = bind_tcp_listener(&addr)
        .await
        .with_context(|| format!("listen http vhost on {addr}"))?;
    let local_addr = listener.local_addr().context("read http vhost address")?;
    info!("http vhost listening on {local_addr}");

    loop {
        let (stream, peer) = listener.accept().await.context("accept http visitor")?;
        configure_tcp_stream(&stream);
        state
            .metrics
            .visitor_connections_total
            .fetch_add(1, Ordering::Relaxed);
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_http_visitor(state, stream, peer).await {
                warn!("http visitor {peer} failed: {err:#}");
            }
        });
    }
}

async fn run_https_vhost(state: ServerState) -> Result<()> {
    let addr = format!(
        "{}:{}",
        state.cfg.proxy_bind_addr, state.cfg.vhost_https_port
    );
    let listener = bind_tcp_listener(&addr)
        .await
        .with_context(|| format!("listen https vhost on {addr}"))?;
    let local_addr = listener.local_addr().context("read https vhost address")?;
    info!("https vhost listening on {local_addr}");

    loop {
        let (stream, peer) = listener.accept().await.context("accept https visitor")?;
        configure_tcp_stream(&stream);
        state
            .metrics
            .visitor_connections_total
            .fetch_add(1, Ordering::Relaxed);
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_https_visitor(state, stream, peer).await {
                warn!("https visitor {peer} failed: {err:#}");
            }
        });
    }
}

async fn handle_http_visitor(
    state: ServerState,
    mut visitor: TcpStream,
    visitor_addr: SocketAddr,
) -> Result<()> {
    let prefix = read_http_prefix(&mut visitor).await?;
    let method = parse_http_method(&prefix).unwrap_or_default();
    let host = parse_http_host(&prefix).ok_or_else(|| anyhow!("missing Host header"))?;
    let path = parse_http_path(&prefix).unwrap_or_else(|| "/".to_string());
    if method.eq_ignore_ascii_case("CONNECT") {
        return handle_tcpmux_connect(state, visitor, visitor_addr, host, path).await;
    }

    let route = select_http_route(state.clone(), &host, &path)
        .await
        .ok_or_else(|| anyhow!("no http proxy registered for host {host} path {path}"))?;

    if !http_basic_auth_matches(&prefix, &route) {
        write_http_unauthorized(&mut visitor).await?;
        return Ok(());
    }

    call_plugin_hook(
        state.cfg.plugins.new_user_conn_url.as_deref(),
        json!({
            "op": "NewUserConn",
            "proxy_name": route.proxy_name.clone(),
            "proxy_type": "http",
            "run_id": route.control.run_id.clone(),
            "user_addr": visitor_addr.to_string(),
            "remote_addr": visitor.local_addr().map(|addr| addr.to_string()).unwrap_or_default(),
            "dst_addr": host.clone(),
            "path": path,
        }),
    )
    .await?;

    let prefix = rewrite_http_prefix(&prefix, &route, visitor_addr)?;

    let mut work = acquire_work_conn(&route.control).await?;

    write_msg(
        &mut work,
        &Message::StartWorkConn {
            proxy_name: route.proxy_name.clone(),
            src_addr: visitor_addr.to_string(),
            dst_addr: host,
            error: String::new(),
        },
    )
    .await?;
    work.write_all(&prefix)
        .await
        .context("write buffered http request to work connection")?;

    let (up, down) = copy_bidirectional_limited(&mut visitor, &mut work, route.bandwidth_limit)
        .await
        .with_context(|| format!("proxy http bytes for {}", route.proxy_name))?;
    state
        .metrics
        .bytes_up_total
        .fetch_add(up, Ordering::Relaxed);
    state
        .metrics
        .bytes_down_total
        .fetch_add(down, Ordering::Relaxed);
    Ok(())
}

async fn handle_tcpmux_connect(
    state: ServerState,
    mut visitor: TcpStream,
    visitor_addr: SocketAddr,
    host: String,
    target: String,
) -> Result<()> {
    let route = select_tcpmux_route(state.clone(), &target)
        .await
        .ok_or_else(|| anyhow!("no tcpmux proxy registered for target {target}"))?;

    call_plugin_hook(
        state.cfg.plugins.new_user_conn_url.as_deref(),
        json!({
            "op": "NewUserConn",
            "proxy_name": route.proxy_name.clone(),
            "proxy_type": "tcpmux",
            "run_id": route.control.run_id.clone(),
            "user_addr": visitor_addr.to_string(),
            "remote_addr": visitor.local_addr().map(|addr| addr.to_string()).unwrap_or_default(),
            "dst_addr": target,
        }),
    )
    .await?;

    visitor
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await
        .context("write tcpmux connect response")?;
    let mut work = acquire_work_conn(&route.control).await?;
    write_msg(
        &mut work,
        &Message::StartWorkConn {
            proxy_name: route.proxy_name.clone(),
            src_addr: visitor_addr.to_string(),
            dst_addr: host,
            error: String::new(),
        },
    )
    .await?;

    let (up, down) = copy_bidirectional_limited(&mut visitor, &mut work, route.bandwidth_limit)
        .await
        .with_context(|| format!("proxy tcpmux bytes for {}", route.proxy_name))?;
    state
        .metrics
        .bytes_up_total
        .fetch_add(up, Ordering::Relaxed);
    state
        .metrics
        .bytes_down_total
        .fetch_add(down, Ordering::Relaxed);
    Ok(())
}

async fn handle_https_visitor(
    state: ServerState,
    mut visitor: TcpStream,
    visitor_addr: SocketAddr,
) -> Result<()> {
    let prefix = read_tls_client_hello_prefix(&mut visitor).await?;
    let sni = parse_tls_sni(&prefix);
    let route = select_https_route(state.clone(), sni.as_deref())
        .await
        .ok_or_else(|| {
            anyhow!(
                "no https proxy registered for SNI {}",
                sni.as_deref().unwrap_or("<none>")
            )
        })?;
    let dst_addr = sni.unwrap_or_default();

    call_plugin_hook(
        state.cfg.plugins.new_user_conn_url.as_deref(),
        json!({
            "op": "NewUserConn",
            "proxy_name": route.proxy_name.clone(),
            "proxy_type": "https",
            "run_id": route.control.run_id.clone(),
            "user_addr": visitor_addr.to_string(),
            "remote_addr": visitor.local_addr().map(|addr| addr.to_string()).unwrap_or_default(),
            "dst_addr": dst_addr.clone(),
        }),
    )
    .await?;

    let mut work = acquire_work_conn(&route.control).await?;

    write_msg(
        &mut work,
        &Message::StartWorkConn {
            proxy_name: route.proxy_name.clone(),
            src_addr: visitor_addr.to_string(),
            dst_addr,
            error: String::new(),
        },
    )
    .await?;
    work.write_all(&prefix)
        .await
        .context("write buffered tls client hello to work connection")?;

    let (up, down) = copy_bidirectional_limited(&mut visitor, &mut work, route.bandwidth_limit)
        .await
        .with_context(|| format!("proxy https bytes for {}", route.proxy_name))?;
    state
        .metrics
        .bytes_up_total
        .fetch_add(up, Ordering::Relaxed);
    state
        .metrics
        .bytes_down_total
        .fetch_add(down, Ordering::Relaxed);
    Ok(())
}

async fn acquire_work_conn(control: &Arc<Control>) -> Result<BoxStream> {
    if let Some(work) = control.work_pool.pop().await {
        schedule_replenish_work_pool(control.clone());
        return Ok(work);
    }

    let waiters = control.work_pool.waiters.fetch_add(1, Ordering::AcqRel) + 1;
    let target = waiters.max(1);
    if let Err(err) = request_work_conns(control, target).await {
        control.work_pool.waiters.fetch_sub(1, Ordering::AcqRel);
        return Err(err);
    }
    let wait_for_work = async {
        loop {
            if let Some(work) = control.work_pool.pop().await {
                schedule_replenish_work_pool(control.clone());
                return Ok(work);
            }
            control.work_pool.notify.notified().await;
        }
    };

    let result = time::timeout(Duration::from_secs(10), wait_for_work).await;
    control.work_pool.waiters.fetch_sub(1, Ordering::AcqRel);
    match result {
        Ok(result) => result,
        Err(err) => Err(err).context("wait for client work connection timed out"),
    }
}

fn schedule_replenish_work_pool(control: Arc<Control>) {
    if control.pool_count == 0 {
        return;
    }
    if control
        .work_pool
        .replenishing
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }

    tokio::spawn(async move {
        loop {
            if let Err(err) = replenish_work_pool(&control).await {
                debug!(
                    "replenish work pool for control {} failed: {err:#}",
                    control.run_id
                );
            }

            control
                .work_pool
                .replenishing
                .store(false, Ordering::Release);

            if control.work_pool.available() >= control.pool_count {
                break;
            }
            if control
                .work_pool
                .replenishing
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                break;
            }
        }
    });
}

async fn replenish_work_pool(control: &Arc<Control>) -> Result<()> {
    if control.pool_count == 0 {
        return Ok(());
    }
    request_work_conns(control, control.pool_count).await
}

async fn request_work_conns(control: &Arc<Control>, target_available: usize) -> Result<()> {
    let count = control.work_pool.reserve_missing(target_available);
    if count == 0 {
        return Ok(());
    }
    let msg = Message::ReqWorkConn { count };
    if let Err(err) = control.send(&msg).await {
        control.work_pool.release_reserved(count);
        return Err(err);
    }
    Ok(())
}

async fn copy_bidirectional_limited<L, R>(
    left: &mut L,
    right: &mut R,
    bytes_per_second: Option<u64>,
) -> Result<(u64, u64)>
where
    L: AsyncRead + AsyncWrite + Unpin,
    R: AsyncRead + AsyncWrite + Unpin,
{
    if bytes_per_second.is_none() {
        return io::copy_bidirectional(left, right)
            .await
            .context("copy relay streams");
    }

    let (mut left_read, mut left_write) = tokio::io::split(left);
    let (mut right_read, mut right_write) = tokio::io::split(right);
    let left_to_right = copy_limited(&mut left_read, &mut right_write, bytes_per_second);
    let right_to_left = copy_limited(&mut right_read, &mut left_write, bytes_per_second);
    let (up, down) = tokio::try_join!(left_to_right, right_to_left)?;
    Ok((up, down))
}

async fn copy_limited<R, W>(
    reader: &mut R,
    writer: &mut W,
    bytes_per_second: Option<u64>,
) -> Result<u64>
where
    R: AsyncRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin + ?Sized,
{
    let mut buf = vec![0_u8; 16 * 1024];
    let mut total = 0_u64;
    loop {
        let n = reader.read(&mut buf).await.context("read relay stream")?;
        if n == 0 {
            writer.shutdown().await.context("shutdown relay writer")?;
            return Ok(total);
        }
        writer
            .write_all(&buf[..n])
            .await
            .context("write relay stream")?;
        total += n as u64;
        if let Some(limit) = bytes_per_second {
            if limit > 0 {
                let micros = ((n as u128) * 1_000_000 / (limit as u128)) as u64;
                if micros > 0 {
                    time::sleep(Duration::from_micros(micros)).await;
                }
            }
        }
    }
}

async fn close_proxy(state: ServerState, proxy_name: &str) -> Result<bool> {
    let entry = state.proxy_entries.lock().await.remove(proxy_name);
    let Some(entry) = entry else {
        return Ok(false);
    };
    let hook_payload = json!({
        "op": "CloseProxy",
        "event": "close_proxy",
        "proxy_name": proxy_name,
        "proxy_type": entry.proxy_type.clone(),
        "run_id": entry.run_id.clone(),
        "group": entry.group.clone(),
        "domains": entry.domains.clone(),
        "remote_addr": entry.remote_addr.clone(),
        "started_unix_secs": entry.started_unix_secs,
        "closed_unix_secs": unix_now(),
    });

    if let Some(task) = entry.task {
        task.abort();
    }
    {
        let udp_key = format!("udp:{proxy_name}");
        let sudp_key = format!("sudp:{proxy_name}");
        state
            .datagram_user_sessions
            .lock()
            .await
            .retain(|(proxy_key, _), _| proxy_key != &udp_key && proxy_key != &sudp_key);
    }

    match entry.proxy_type.as_str() {
        "tcp" => {
            if let Some(map_key) = entry.tcp_group_map_key {
                let mut groups = state.tcp_groups.lock().await;
                if let Some(group) = groups.get_mut(&map_key) {
                    group.routes.retain(|route| route.proxy_name != proxy_name);
                    if group.routes.is_empty() {
                        let group = groups.remove(&map_key);
                        if let Some(group) = group {
                            group.task.abort();
                        }
                    } else if group.next >= group.routes.len() {
                        group.next = 0;
                    }
                }
            }
        }
        "udp" => {
            if let Some(map_key) = entry.udp_group_map_key {
                let mut groups = state.udp_groups.lock().await;
                if let Some(group) = groups.get_mut(&map_key) {
                    group.routes.retain(|route| route.proxy_name != proxy_name);
                    if group.routes.is_empty() {
                        let group = groups.remove(&map_key);
                        if let Some(group) = group {
                            group.task.abort();
                        }
                    } else if group.next >= group.routes.len() {
                        group.next = 0;
                    }
                }
            }
            state.udp_relays.lock().await.remove(proxy_name);
        }
        "http" => {
            let mut routes = state.http_routes.lock().await;
            for domain in entry.domains {
                let key = normalize_host(&domain);
                if let Some(domain_routes) = routes.get_mut(&key) {
                    domain_routes.retain(|route| route.proxy_name != proxy_name);
                    if domain_routes.is_empty() {
                        routes.remove(&key);
                    }
                }
            }
        }
        "https" => {
            let mut routes = state.https_routes.lock().await;
            for domain in entry.domains {
                let key = normalize_host(&domain);
                if let Some(domain_routes) = routes.get_mut(&key) {
                    domain_routes.retain(|route| route.proxy_name != proxy_name);
                    if domain_routes.is_empty() {
                        routes.remove(&key);
                    }
                }
            }
        }
        "tcpmux" => {
            let mut routes = state.tcpmux_routes.lock().await;
            for domain in entry.domains {
                let key = normalize_host(&domain);
                if let Some(domain_routes) = routes.get_mut(&key) {
                    domain_routes.retain(|route| route.proxy_name != proxy_name);
                    if domain_routes.is_empty() {
                        routes.remove(&key);
                    }
                }
            }
        }
        "stcp" => {
            if let Some(group_name) = entry.group {
                let mut groups = state.stcp_groups.lock().await;
                if let Some(group) = groups.get_mut(&group_name) {
                    group.routes.retain(|route| route.proxy_name != proxy_name);
                    if group.routes.is_empty() {
                        groups.remove(&group_name);
                    } else if group.next >= group.routes.len() {
                        group.next = 0;
                    }
                }
            }
            state.stcp_routes.lock().await.remove(proxy_name);
        }
        "xtcp" => {
            if let Some(group_name) = entry.group {
                let mut groups = state.xtcp_groups.lock().await;
                if let Some(group) = groups.get_mut(&group_name) {
                    group.routes.retain(|route| route.proxy_name != proxy_name);
                    if group.routes.is_empty() {
                        groups.remove(&group_name);
                    } else if group.next >= group.routes.len() {
                        group.next = 0;
                    }
                }
            }
            state.xtcp_routes.lock().await.remove(proxy_name);
        }
        "sudp" => {
            if let Some(group_name) = entry.group {
                let mut groups = state.sudp_groups.lock().await;
                if let Some(group) = groups.get_mut(&group_name) {
                    group.routes.retain(|route| route.proxy_name != proxy_name);
                    if group.routes.is_empty() {
                        groups.remove(&group_name);
                    } else if group.next >= group.routes.len() {
                        group.next = 0;
                    }
                }
            }
            state.sudp_routes.lock().await.remove(proxy_name);
            state
                .sudp_sessions
                .lock()
                .await
                .retain(|(name, _), _| name != proxy_name);
        }
        _ => {}
    }

    if let Err(err) =
        call_plugin_hook(state.cfg.plugins.close_proxy_url.as_deref(), hook_payload).await
    {
        warn!("close proxy plugin hook failed for {proxy_name}: {err:#}");
    }

    Ok(true)
}

async fn close_proxy_group(state: ServerState, protocol: &str, group: &str) -> Result<Vec<String>> {
    let names = {
        let entries = state.proxy_entries.lock().await;
        entries
            .iter()
            .filter(|(_, entry)| {
                entry.proxy_type == protocol && entry.group.as_deref() == Some(group)
            })
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>()
    };

    let mut closed = Vec::new();
    for name in names {
        if close_proxy(state.clone(), &name).await? {
            closed.push(name);
        }
    }

    Ok(closed)
}

async fn close_client_proxies(state: ServerState, run_id: &str) -> Result<Vec<String>> {
    let names = {
        let entries = state.proxy_entries.lock().await;
        entries
            .iter()
            .filter(|(_, entry)| entry.run_id == run_id)
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>()
    };

    let mut closed = Vec::new();
    for name in names {
        if close_proxy(state.clone(), &name).await? {
            closed.push(name);
        }
    }

    Ok(closed)
}

async fn update_proxy_health(
    state: ServerState,
    proxy_name: &str,
    healthy: bool,
    check_type: String,
    detail: String,
    checked_unix_secs: u64,
) {
    let mut entries = state.proxy_entries.lock().await;
    let Some(entry) = entries.get_mut(proxy_name) else {
        debug!("health status ignored for unknown proxy {proxy_name}");
        return;
    };

    entry.health = Some(ProxyHealthStatus {
        healthy,
        check_type,
        detail,
        checked_unix_secs,
        updated_unix_secs: unix_now(),
    });
}

async fn close_proxies_for_run_id(state: ServerState, run_id: &str) -> Result<()> {
    let names = {
        let entries = state.proxy_entries.lock().await;
        entries
            .iter()
            .filter(|(_, entry)| entry.run_id == run_id)
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>()
    };

    for name in names {
        close_proxy(state.clone(), &name).await?;
    }

    Ok(())
}

async fn read_http_prefix(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(4096);
    let mut buf = [0_u8; 1024];

    loop {
        let n = time::timeout(Duration::from_secs(5), stream.read(&mut buf))
            .await
            .context("read http header timed out")?
            .context("read http header")?;
        if n == 0 {
            bail!("visitor closed before sending http header");
        }
        out.extend_from_slice(&buf[..n]);

        if out.windows(4).any(|window| window == b"\r\n\r\n") {
            return Ok(out);
        }
        if out.len() > 64 * 1024 {
            bail!("http header exceeds 64 KiB");
        }
    }
}

fn parse_http_host(prefix: &[u8]) -> Option<String> {
    parse_http_header(prefix, "host")
}

fn parse_http_method(prefix: &[u8]) -> Option<String> {
    let header_end = http_header_end(prefix).unwrap_or(prefix.len());
    let text = std::str::from_utf8(&prefix[..header_end]).ok()?;
    text.lines()
        .next()?
        .split_whitespace()
        .next()
        .map(|method| method.to_string())
}

fn parse_http_header(prefix: &[u8], header_name: &str) -> Option<String> {
    let header_end = http_header_end(prefix).unwrap_or(prefix.len());
    let text = std::str::from_utf8(&prefix[..header_end]).ok()?;
    for line in text.lines() {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case(header_name) {
                return Some(value.trim().to_string());
            }
        }
    }
    None
}

fn parse_http_path(prefix: &[u8]) -> Option<String> {
    let header_end = http_header_end(prefix).unwrap_or(prefix.len());
    let text = std::str::from_utf8(&prefix[..header_end]).ok()?;
    text.lines()
        .next()?
        .split_whitespace()
        .nth(1)
        .map(|path| path.to_string())
}

async fn read_http_body(stream: &mut TcpStream, prefix: &[u8]) -> Result<Vec<u8>> {
    let content_length = parse_http_header(prefix, "content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let header_end = http_header_end(prefix)
        .map(|index| index + 4)
        .ok_or_else(|| anyhow!("http header terminator missing"))?;
    let mut body = prefix.get(header_end..).unwrap_or_default().to_vec();
    if body.len() > content_length {
        body.truncate(content_length);
    }
    while body.len() < content_length {
        let mut buf = vec![0_u8; content_length - body.len()];
        let n = stream
            .read(&mut buf)
            .await
            .context("read http request body")?;
        if n == 0 {
            bail!("connection closed before full http body");
        }
        body.extend_from_slice(&buf[..n]);
    }
    Ok(body)
}

#[derive(Clone)]
struct HttpRouteCandidate {
    location_len: usize,
    domain_len: usize,
    route: ProxyRoute,
}

async fn select_http_route(state: ServerState, host: &str, path: &str) -> Option<ProxyRoute> {
    let candidates = {
        let routes = state.http_routes.lock().await;
        find_http_route_candidates(&routes, host, path)
    };
    if candidates.is_empty() {
        return None;
    }

    let best_score = candidates
        .iter()
        .map(|candidate| (candidate.location_len, candidate.domain_len))
        .max()?;
    let mut best = candidates
        .into_iter()
        .filter(|candidate| (candidate.location_len, candidate.domain_len) == best_score)
        .collect::<Vec<_>>();
    let Some(group_name) = best
        .iter()
        .find_map(|candidate| candidate.route.group.clone())
    else {
        return best.into_iter().next().map(|candidate| candidate.route);
    };
    best.retain(|candidate| candidate.route.group.as_deref() == Some(group_name.as_str()));
    if best.is_empty() {
        return None;
    }

    let key = format!(
        "http:{}:{}:{}:{}",
        normalize_host(host),
        best_score.0,
        best_score.1,
        group_name
    );
    let mut next = state.http_route_next.lock().await;
    let index = next.entry(key).or_insert(0);
    let route = best[*index % best.len()].route.clone();
    *index = index.wrapping_add(1);
    Some(route)
}

async fn select_tcpmux_route(state: ServerState, target: &str) -> Option<ProxyRoute> {
    let host = normalize_host(target);
    let candidates = {
        let routes = state.tcpmux_routes.lock().await;
        find_tcpmux_route_candidates(&routes, &host)
    };
    if candidates.is_empty() {
        return None;
    }

    let best_score = candidates
        .iter()
        .map(|candidate| candidate.domain_len)
        .max()?;
    let mut best = candidates
        .into_iter()
        .filter(|candidate| candidate.domain_len == best_score)
        .collect::<Vec<_>>();
    let Some(group_name) = best
        .iter()
        .find_map(|candidate| candidate.route.group.clone())
    else {
        return best.into_iter().next().map(|candidate| candidate.route);
    };
    best.retain(|candidate| candidate.route.group.as_deref() == Some(group_name.as_str()));
    if best.is_empty() {
        return None;
    }

    let key = format!("tcpmux:{host}:{group_name}");
    let mut next = state.tcpmux_route_next.lock().await;
    let index = next.entry(key).or_insert(0);
    let route = best[*index % best.len()].route.clone();
    *index = index.wrapping_add(1);
    Some(route)
}

fn find_tcpmux_route_candidates(
    routes: &HashMap<String, Vec<ProxyRoute>>,
    host: &str,
) -> Vec<HttpsRouteCandidate> {
    let mut out = Vec::new();
    if let Some(domain_routes) = routes.get(host) {
        out.extend(
            domain_routes
                .iter()
                .cloned()
                .map(|route| HttpsRouteCandidate {
                    domain_len: host.len(),
                    route,
                }),
        );
    }
    for (domain, domain_routes) in routes {
        if let Some(suffix) = wildcard_domain_suffix(domain, host) {
            out.extend(
                domain_routes
                    .iter()
                    .cloned()
                    .map(|route| HttpsRouteCandidate {
                        domain_len: suffix.len(),
                        route,
                    }),
            );
        }
    }
    if out.is_empty() {
        if let Some(domain_routes) = routes.get("*") {
            out.extend(
                domain_routes
                    .iter()
                    .cloned()
                    .map(|route| HttpsRouteCandidate {
                        domain_len: 0,
                        route,
                    }),
            );
        }
    }
    out
}

fn find_http_route_candidates(
    routes: &HashMap<String, Vec<ProxyRoute>>,
    host: &str,
    path: &str,
) -> Vec<HttpRouteCandidate> {
    let normalized = normalize_host(host);
    let mut out = Vec::new();
    if let Some(domain_routes) = routes.get(&normalized) {
        out.extend(http_route_candidates_for_domain(
            domain_routes,
            path,
            normalized.len(),
        ));
    }
    for (domain, domain_routes) in routes {
        if let Some(suffix) = wildcard_domain_suffix(domain, &normalized) {
            out.extend(http_route_candidates_for_domain(
                domain_routes,
                path,
                suffix.len(),
            ));
        }
    }
    out
}

fn http_route_candidates_for_domain(
    routes: &[ProxyRoute],
    path: &str,
    domain_len: usize,
) -> Vec<HttpRouteCandidate> {
    routes
        .iter()
        .filter_map(|route| {
            best_location_len(&route.locations, path).map(|location_len| HttpRouteCandidate {
                location_len,
                domain_len,
                route: route.clone(),
            })
        })
        .collect()
}

fn best_location_len(locations: &[String], path: &str) -> Option<usize> {
    if locations.is_empty() {
        return Some(0);
    }

    locations
        .iter()
        .filter(|location| path_matches_location(path, location))
        .map(|location| location.len())
        .max()
}

fn path_matches_location(path: &str, location: &str) -> bool {
    if location == "/" {
        return true;
    }
    let location = location.trim_end_matches('/');
    path == location
        || path
            .strip_prefix(location)
            .map(|rest| rest.starts_with('/'))
            .unwrap_or(false)
}

fn ensure_http_group_key_matches(routes: &[ProxyRoute], route: &ProxyRoute) -> Result<()> {
    let Some(group) = route.group.as_deref() else {
        return Ok(());
    };
    for existing in routes {
        if existing.group.as_deref() == Some(group) && existing.group_key != route.group_key {
            bail!("http group {group} groupKey mismatch");
        }
    }
    Ok(())
}

#[derive(Clone)]
struct HttpsRouteCandidate {
    domain_len: usize,
    route: ProxyRoute,
}

async fn select_https_route(state: ServerState, sni: Option<&str>) -> Option<ProxyRoute> {
    let candidates = {
        let routes = state.https_routes.lock().await;
        find_https_route_candidates(&routes, sni)
    };
    if candidates.is_empty() {
        return None;
    }

    let best_score = candidates
        .iter()
        .map(|candidate| candidate.domain_len)
        .max()?;
    let mut best = candidates
        .into_iter()
        .filter(|candidate| candidate.domain_len == best_score)
        .collect::<Vec<_>>();
    let Some(group_name) = best
        .iter()
        .find_map(|candidate| candidate.route.group.clone())
    else {
        return best.into_iter().next().map(|candidate| candidate.route);
    };
    best.retain(|candidate| candidate.route.group.as_deref() == Some(group_name.as_str()));
    if best.is_empty() {
        return None;
    }

    let key = format!(
        "https:{}:{}",
        sni.map(normalize_host).unwrap_or_default(),
        group_name
    );
    let mut next = state.https_route_next.lock().await;
    let index = next.entry(key).or_insert(0);
    let route = best[*index % best.len()].route.clone();
    *index = index.wrapping_add(1);
    Some(route)
}

fn find_https_route_candidates(
    routes: &HashMap<String, Vec<ProxyRoute>>,
    sni: Option<&str>,
) -> Vec<HttpsRouteCandidate> {
    let Some(host) = sni else {
        return routes
            .get("*")
            .into_iter()
            .flat_map(|routes| {
                routes.iter().cloned().map(|route| HttpsRouteCandidate {
                    domain_len: 0,
                    route,
                })
            })
            .collect();
    };
    let normalized = normalize_host(host);
    let mut out = Vec::new();
    if let Some(domain_routes) = routes.get(&normalized) {
        out.extend(
            domain_routes
                .iter()
                .cloned()
                .map(|route| HttpsRouteCandidate {
                    domain_len: normalized.len(),
                    route,
                }),
        );
    }
    for (domain, domain_routes) in routes {
        if let Some(suffix) = wildcard_domain_suffix(domain, &normalized) {
            out.extend(
                domain_routes
                    .iter()
                    .cloned()
                    .map(|route| HttpsRouteCandidate {
                        domain_len: suffix.len(),
                        route,
                    }),
            );
        }
    }
    if out.is_empty() {
        if let Some(domain_routes) = routes.get("*") {
            out.extend(
                domain_routes
                    .iter()
                    .cloned()
                    .map(|route| HttpsRouteCandidate {
                        domain_len: 0,
                        route,
                    }),
            );
        }
    }
    out
}

fn wildcard_domain_suffix<'a>(domain: &'a str, host: &str) -> Option<&'a str> {
    let suffix = domain.strip_prefix("*.")?;
    if host != suffix
        && host
            .strip_suffix(suffix)
            .map(|prefix| prefix.ends_with('.'))
            .unwrap_or(false)
    {
        Some(suffix)
    } else {
        None
    }
}

fn normalize_locations(locations: &[String]) -> Vec<String> {
    locations
        .iter()
        .map(|location| {
            let trimmed = location.trim();
            if trimmed.is_empty() {
                "/".to_string()
            } else if trimmed.starts_with('/') {
                trimmed.to_string()
            } else {
                format!("/{trimmed}")
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthConfig, ServerPluginConfig, TransportConfig};
    use tokio::io::duplex;

    fn test_control(pool_count: usize) -> (Arc<Control>, tokio::io::DuplexStream) {
        test_control_with_run_id("test-run", pool_count)
    }

    fn test_control_with_run_id(
        run_id: &str,
        pool_count: usize,
    ) -> (Arc<Control>, tokio::io::DuplexStream) {
        let (client, server) = duplex(4096);
        let stream: BoxStream = Box::new(client);
        let (_reader, writer) = tokio::io::split(stream);
        let control = Arc::new(Control {
            run_id: run_id.to_string(),
            peer_addr: "127.0.0.1:1".parse().unwrap(),
            pool_count,
            writer: Mutex::new(writer),
            work_pool: WorkPool::new(),
        });
        (control, server)
    }

    fn test_server_state(controls: Vec<Arc<Control>>) -> ServerState {
        ServerState {
            cfg: Arc::new(ServerConfig {
                bind_addr: "127.0.0.1".to_string(),
                bind_port: 7000,
                proxy_bind_addr: "127.0.0.1".to_string(),
                vhost_http_port: 0,
                vhost_https_port: 0,
                dashboard_addr: "127.0.0.1".to_string(),
                dashboard_port: 0,
                allow_ports: Vec::new(),
                auth: AuthConfig { token: None },
                plugins: ServerPluginConfig::default(),
                transport: TransportConfig::default(),
            }),
            controls: Arc::new(Mutex::new(
                controls
                    .into_iter()
                    .map(|control| (control.run_id.clone(), control))
                    .collect(),
            )),
            http_routes: Arc::new(Mutex::new(HashMap::new())),
            http_route_next: Arc::new(Mutex::new(HashMap::new())),
            tcpmux_routes: Arc::new(Mutex::new(HashMap::new())),
            tcpmux_route_next: Arc::new(Mutex::new(HashMap::new())),
            https_routes: Arc::new(Mutex::new(HashMap::new())),
            https_route_next: Arc::new(Mutex::new(HashMap::new())),
            stcp_routes: Arc::new(Mutex::new(HashMap::new())),
            stcp_groups: Arc::new(Mutex::new(HashMap::new())),
            xtcp_routes: Arc::new(Mutex::new(HashMap::new())),
            xtcp_groups: Arc::new(Mutex::new(HashMap::new())),
            sudp_routes: Arc::new(Mutex::new(HashMap::new())),
            sudp_groups: Arc::new(Mutex::new(HashMap::new())),
            sudp_sessions: Arc::new(Mutex::new(HashMap::new())),
            tcp_groups: Arc::new(Mutex::new(HashMap::new())),
            udp_groups: Arc::new(Mutex::new(HashMap::new())),
            nathole: Arc::new(NatHoleController::default()),
            udp_relays: Arc::new(Mutex::new(HashMap::new())),
            datagram_user_sessions: Arc::new(Mutex::new(HashMap::new())),
            proxy_entries: Arc::new(Mutex::new(HashMap::new())),
            allow_ports: Arc::new(Mutex::new(Vec::new())),
            metrics: Arc::new(ServerMetrics {
                started_unix_secs: 0,
                visitor_connections_total: AtomicU64::new(0),
                bytes_up_total: AtomicU64::new(0),
                bytes_down_total: AtomicU64::new(0),
            }),
        }
    }

    #[tokio::test]
    async fn nat_hole_match_notifies_waiting_peer() {
        let (owner_control, mut owner_stream) = test_control_with_run_id("owner-run", 0);
        let (visitor_control, _visitor_stream) = test_control_with_run_id("visitor-run", 0);
        let state = test_server_state(vec![owner_control.clone(), visitor_control.clone()]);

        let owner_resp = handle_nat_hole_register(
            state.clone(),
            owner_control,
            "127.0.0.1:7001".parse().unwrap(),
            "tx-p2p".to_string(),
            "xtcp-ssh".to_string(),
            "server".to_string(),
            vec!["10.0.0.10:10001".to_string()],
        )
        .await;
        assert!(matches!(
            owner_resp,
            Message::NatHoleResp { waiting: true, .. }
        ));

        let visitor_resp = handle_nat_hole_register(
            state,
            visitor_control,
            "127.0.0.1:7002".parse().unwrap(),
            "tx-p2p".to_string(),
            "xtcp-ssh".to_string(),
            "visitor".to_string(),
            vec!["10.0.0.20:10002".to_string()],
        )
        .await;
        match visitor_resp {
            Message::NatHoleResp {
                role,
                peer_observed_addr,
                peer_local_addrs,
                waiting,
                error,
                ..
            } => {
                assert_eq!(role, "visitor");
                assert_eq!(peer_observed_addr, "127.0.0.1:7001");
                assert_eq!(peer_local_addrs, vec!["10.0.0.10:10001"]);
                assert!(!waiting);
                assert!(error.is_empty());
            }
            other => panic!("unexpected visitor response: {other:?}"),
        }

        match time::timeout(Duration::from_secs(1), read_msg(&mut owner_stream))
            .await
            .unwrap()
            .unwrap()
        {
            Message::NatHoleResp {
                role,
                observed_addr,
                peer_observed_addr,
                peer_local_addrs,
                waiting,
                error,
                ..
            } => {
                assert_eq!(role, "server");
                assert_eq!(observed_addr, "127.0.0.1:7001");
                assert_eq!(peer_observed_addr, "127.0.0.1:7002");
                assert_eq!(peer_local_addrs, vec!["10.0.0.20:10002"]);
                assert!(!waiting);
                assert!(error.is_empty());
            }
            other => panic!("unexpected owner notification: {other:?}"),
        }
    }

    #[tokio::test]
    async fn sudp_group_routes_visitor_packets_round_robin() {
        let (owner_a, mut owner_a_stream) = test_control_with_run_id("owner-a", 0);
        let (owner_b, mut owner_b_stream) = test_control_with_run_id("owner-b", 0);
        let (visitor_control, mut visitor_stream) = test_control_with_run_id("visitor-run", 0);
        let state = test_server_state(vec![
            owner_a.clone(),
            owner_b.clone(),
            visitor_control.clone(),
        ]);

        let remote_a = start_sudp_proxy(
            state.clone(),
            owner_a.clone(),
            "sudp-a".to_string(),
            Some("sudp-group".to_string()),
            Some("group-secret".to_string()),
            Some("private".to_string()),
            None,
        )
        .await
        .unwrap();
        let remote_b = start_sudp_proxy(
            state.clone(),
            owner_b.clone(),
            "sudp-b".to_string(),
            Some("sudp-group".to_string()),
            Some("group-secret".to_string()),
            Some("private".to_string()),
            None,
        )
        .await
        .unwrap();
        assert_eq!(remote_a, "sudp://sudp-group");
        assert_eq!(remote_b, "sudp://sudp-group");

        let (owner_c, _) = test_control_with_run_id("owner-c", 0);
        let mismatch = start_sudp_proxy(
            state.clone(),
            owner_c,
            "sudp-c".to_string(),
            Some("sudp-group".to_string()),
            Some("other-secret".to_string()),
            Some("private".to_string()),
            None,
        )
        .await
        .unwrap_err();
        assert!(mismatch.to_string().contains("groupKey mismatch"));

        for payload in [b"alpha".to_vec(), b"beta".to_vec()] {
            handle_sudp_packet(
                state.clone(),
                visitor_control.clone(),
                "sudp-group".to_string(),
                "session-1".to_string(),
                payload,
                Some("private".to_string()),
                true,
            )
            .await
            .unwrap();
        }

        match time::timeout(Duration::from_secs(1), read_msg(&mut owner_a_stream))
            .await
            .unwrap()
            .unwrap()
        {
            Message::SudpPacket {
                proxy_name,
                session_id,
                content,
                from_visitor,
                ..
            } => {
                assert_eq!(proxy_name, "sudp-a");
                assert_eq!(session_id, "session-1");
                assert_eq!(content, b"alpha");
                assert!(from_visitor);
            }
            other => panic!("unexpected owner-a message: {other:?}"),
        }

        match time::timeout(Duration::from_secs(1), read_msg(&mut owner_b_stream))
            .await
            .unwrap()
            .unwrap()
        {
            Message::SudpPacket {
                proxy_name,
                session_id,
                content,
                from_visitor,
                ..
            } => {
                assert_eq!(proxy_name, "sudp-b");
                assert_eq!(session_id, "session-1");
                assert_eq!(content, b"beta");
                assert!(from_visitor);
            }
            other => panic!("unexpected owner-b message: {other:?}"),
        }

        handle_sudp_packet(
            state.clone(),
            owner_a,
            "sudp-a".to_string(),
            "session-1".to_string(),
            b"pong".to_vec(),
            None,
            false,
        )
        .await
        .unwrap();
        match time::timeout(Duration::from_secs(1), read_msg(&mut visitor_stream))
            .await
            .unwrap()
            .unwrap()
        {
            Message::SudpPacket {
                proxy_name,
                session_id,
                content,
                from_visitor,
                ..
            } => {
                assert_eq!(proxy_name, "sudp-a");
                assert_eq!(session_id, "session-1");
                assert_eq!(content, b"pong");
                assert!(!from_visitor);
            }
            other => panic!("unexpected visitor message: {other:?}"),
        }

        let groups = tcp_group_list_json(&state).await.unwrap().to_string();
        assert!(groups.contains("\"protocol\":\"sudp\""));
        assert!(groups.contains("\"group\":\"sudp-group\""));
        assert!(groups.contains("\"member_count\":2"));
    }

    #[tokio::test]
    async fn work_pool_replenish_requests_are_coalesced() {
        let (control, mut server) = test_control(3);

        for _ in 0..8 {
            schedule_replenish_work_pool(control.clone());
        }

        match time::timeout(Duration::from_secs(1), read_msg(&mut server))
            .await
            .unwrap()
            .unwrap()
        {
            Message::ReqWorkConn { count } => assert_eq!(count, 3),
            other => panic!("unexpected message: {other:?}"),
        }

        let second = time::timeout(Duration::from_millis(100), read_msg(&mut server)).await;
        assert!(second.is_err(), "unexpected duplicate replenish request");
        assert_eq!(control.work_pool.pending.load(Ordering::Acquire), 3);
        assert!(!control.work_pool.replenishing.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn work_pool_waiters_request_only_immediate_demand() {
        let (control, mut server) = test_control(3);

        let first_control = control.clone();
        let first = tokio::spawn(async move { acquire_work_conn(&first_control).await });
        let second_control = control.clone();
        let second = tokio::spawn(async move { acquire_work_conn(&second_control).await });

        let mut requested = 0;
        for _ in 0..2 {
            match time::timeout(Duration::from_secs(1), read_msg(&mut server))
                .await
                .unwrap()
                .unwrap()
            {
                Message::ReqWorkConn { count } => requested += count,
                other => panic!("unexpected message: {other:?}"),
            }
        }
        assert_eq!(requested, 2);
        assert_eq!(control.work_pool.pending.load(Ordering::Acquire), 2);

        for _ in 0..2 {
            let (client, _server) = duplex(64);
            control.work_pool.push(Box::new(client)).await;
        }

        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    }
}

fn http_basic_auth_matches(prefix: &[u8], route: &ProxyRoute) -> bool {
    if route.http_user.is_none() && route.http_password.is_none() {
        return true;
    }

    let user = route.http_user.as_deref().unwrap_or_default();
    let password = route.http_password.as_deref().unwrap_or_default();
    let expected = format!(
        "Basic {}",
        base64_encode(format!("{user}:{password}").as_bytes())
    );
    parse_http_header(prefix, "authorization")
        .map(|got| got == expected)
        .unwrap_or(false)
}

fn rewrite_http_prefix(
    prefix: &[u8],
    route: &ProxyRoute,
    visitor_addr: SocketAddr,
) -> Result<Vec<u8>> {
    let Some(header_end) = http_header_end(prefix) else {
        return Ok(prefix.to_vec());
    };
    let header = std::str::from_utf8(&prefix[..header_end]).context("decode http header")?;
    let client_ip = visitor_addr.ip().to_string();
    let mut out = String::with_capacity(header.len() + 128);
    let mut lines = header.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    out.push_str(request_line);
    out.push_str("\r\n");

    let mut prior_forwarded_for = None;
    let mut wrote_host = false;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            out.push_str(line);
            out.push_str("\r\n");
            continue;
        };

        if name.eq_ignore_ascii_case("authorization")
            && (route.http_user.is_some() || route.http_password.is_some())
        {
            continue;
        }
        if name.eq_ignore_ascii_case("host") {
            if let Some(host) = &route.host_header_rewrite {
                out.push_str("Host: ");
                out.push_str(host);
                out.push_str("\r\n");
                wrote_host = true;
                continue;
            }
            wrote_host = true;
        }
        if name.eq_ignore_ascii_case("x-real-ip") {
            continue;
        }
        if name.eq_ignore_ascii_case("x-forwarded-for") {
            prior_forwarded_for = Some(value.trim().to_string());
            continue;
        }
        if route
            .request_headers
            .keys()
            .any(|key| key.eq_ignore_ascii_case(name))
        {
            continue;
        }

        out.push_str(line);
        out.push_str("\r\n");
    }

    if !wrote_host {
        if let Some(host) = &route.host_header_rewrite {
            out.push_str("Host: ");
            out.push_str(host);
            out.push_str("\r\n");
        }
    }
    for (name, value) in &route.request_headers {
        out.push_str(name);
        out.push_str(": ");
        out.push_str(value);
        out.push_str("\r\n");
    }
    out.push_str("X-Real-IP: ");
    out.push_str(&client_ip);
    out.push_str("\r\n");
    out.push_str("X-Forwarded-For: ");
    if let Some(prior) = prior_forwarded_for.filter(|value| !value.is_empty()) {
        out.push_str(&prior);
        out.push_str(", ");
    }
    out.push_str(&client_ip);
    out.push_str("\r\n\r\n");

    let mut rewritten = out.into_bytes();
    rewritten.extend_from_slice(&prefix[header_end + 4..]);
    Ok(rewritten)
}

fn http_header_end(prefix: &[u8]) -> Option<usize> {
    prefix.windows(4).position(|window| window == b"\r\n\r\n")
}

fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b2 & 0b0011_1111) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

async fn write_http_unauthorized(stream: &mut TcpStream) -> Result<()> {
    let body = b"Unauthorized";
    let header = format!(
        "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"frprs\"\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> Result<()> {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.shutdown().await?;
    Ok(())
}

fn normalize_host(host: &str) -> String {
    host.trim()
        .trim_end_matches('.')
        .split(':')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
}

fn group_map_key(remote_port: u16, group: &str) -> String {
    format!("{remote_port}:{}", group.trim())
}

fn remote_port_allowed(allow_ports: &[String], port: u16) -> Result<bool> {
    if allow_ports.is_empty() {
        return Ok(true);
    }

    for item in allow_ports {
        let (start, end) = parse_port_range(item)?;
        if (start..=end).contains(&port) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parse_allow_ports_update(body: &[u8]) -> Result<Vec<String>> {
    let value: serde_json::Value =
        serde_json::from_slice(body).context("decode allowPorts update JSON")?;
    let items = value
        .get("allow_ports")
        .or_else(|| value.get("allowPorts"))
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow!("allowPorts update needs allow_ports array"))?;
    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("allowPorts entries must be strings"))
        })
        .collect()
}

fn validate_allow_ports(allow_ports: &[String]) -> Result<()> {
    for item in allow_ports {
        parse_port_range(item)?;
    }
    Ok(())
}

fn parse_port_range(value: &str) -> Result<(u16, u16)> {
    let trimmed = value.trim();
    let (start, end) = match trimmed.split_once('-') {
        Some((start, end)) => (start.trim(), end.trim()),
        None => (trimmed, trimmed),
    };
    let start = start
        .parse::<u16>()
        .with_context(|| format!("invalid allowPorts start {start:?}"))?;
    let end = end
        .parse::<u16>()
        .with_context(|| format!("invalid allowPorts end {end:?}"))?;
    if start == 0 || end == 0 || start > end {
        bail!("invalid allowPorts range {value:?}");
    }
    Ok((start, end))
}

fn parse_bandwidth_limit(value: &str) -> Result<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("empty bandwidth limit");
    }
    let split_at = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (number, unit) = trimmed.split_at(split_at);
    let base = number
        .parse::<u64>()
        .with_context(|| format!("invalid bandwidth number {number:?}"))?;
    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" | "byte" | "bytes" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        other => bail!("unknown bandwidth unit {other:?}"),
    };
    Ok(base.saturating_mul(multiplier))
}

fn parse_nat_hole_role(value: &str) -> Result<NatHoleRole> {
    match value.trim().to_ascii_lowercase().as_str() {
        "server" | "owner" => Ok(NatHoleRole::Server),
        "visitor" => Ok(NatHoleRole::Visitor),
        other => bail!("unknown nat hole role {other:?}"),
    }
}

fn nat_hole_role_name(role: NatHoleRole) -> &'static str {
    match role {
        NatHoleRole::Server => "server",
        NatHoleRole::Visitor => "visitor",
    }
}

fn parse_socket_addrs(values: &[String]) -> Result<Vec<SocketAddr>> {
    values
        .iter()
        .map(|value| {
            value
                .parse::<SocketAddr>()
                .with_context(|| format!("parse socket address {value:?}"))
        })
        .collect()
}

async fn call_plugin_hook(url: Option<&str>, payload: serde_json::Value) -> Result<()> {
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
    .context("connect plugin hook timed out")?
    .with_context(|| format!("connect plugin hook {url}"))?;
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
        .with_context(|| format!("write plugin hook request {url}"))?;
    let mut response = vec![0_u8; 128];
    let n = time::timeout(Duration::from_secs(5), stream.read(&mut response))
        .await
        .context("read plugin hook response timed out")?
        .with_context(|| format!("read plugin hook response {url}"))?;
    let status = std::str::from_utf8(&response[..n]).unwrap_or_default();
    let ok = status.starts_with("HTTP/1.1 2") || status.starts_with("HTTP/1.0 2");
    if !ok {
        bail!("plugin hook {url} rejected request: {status:?}");
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
        .ok_or_else(|| anyhow!("only http:// plugin hooks are supported"))?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.parse::<u16>()?),
        None => (authority.to_string(), 80),
    };
    if host.is_empty() {
        bail!("plugin hook URL has empty host");
    }
    Ok(ParsedHttpUrl {
        host,
        port,
        path: format!("/{path}"),
    })
}

async fn read_tls_client_hello_prefix(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut header = [0_u8; 5];
    time::timeout(Duration::from_secs(5), stream.read_exact(&mut header))
        .await
        .context("read tls record header timed out")?
        .context("read tls record header")?;

    if header[0] != 22 {
        bail!("first TLS record is not a handshake record");
    }

    let record_len = u16::from_be_bytes([header[3], header[4]]) as usize;
    if record_len == 0 || record_len > 64 * 1024 {
        bail!("invalid TLS record length {record_len}");
    }

    let mut out = header.to_vec();
    out.resize(5 + record_len, 0);
    time::timeout(Duration::from_secs(5), stream.read_exact(&mut out[5..]))
        .await
        .context("read tls client hello timed out")?
        .context("read tls client hello")?;
    Ok(out)
}

fn parse_tls_sni(data: &[u8]) -> Option<String> {
    if data.len() < 9 || data[0] != 22 || data[5] != 1 {
        return None;
    }

    let record_end = 5 + u16::from_be_bytes([data[3], data[4]]) as usize;
    if data.len() < record_end {
        return None;
    }

    let hello_len = ((data[6] as usize) << 16) | ((data[7] as usize) << 8) | data[8] as usize;
    let hello_end = 9 + hello_len;
    if hello_end > record_end {
        return None;
    }

    let mut pos: usize = 9;
    pos = pos.checked_add(2 + 32)?;
    let session_len = *data.get(pos)? as usize;
    pos = pos.checked_add(1 + session_len)?;
    let cipher_len = read_u16(data, pos)? as usize;
    pos = pos.checked_add(2 + cipher_len)?;
    let compression_len = *data.get(pos)? as usize;
    pos = pos.checked_add(1 + compression_len)?;
    let extensions_len = read_u16(data, pos)? as usize;
    pos += 2;
    let extensions_end = pos.checked_add(extensions_len)?;
    if extensions_end > hello_end {
        return None;
    }

    while pos + 4 <= extensions_end {
        let ext_type = read_u16(data, pos)?;
        let ext_len = read_u16(data, pos + 2)? as usize;
        pos += 4;
        let ext_end = pos.checked_add(ext_len)?;
        if ext_end > extensions_end {
            return None;
        }

        if ext_type == 0 {
            let list_len = read_u16(data, pos)? as usize;
            let mut name_pos = pos + 2;
            let list_end = name_pos.checked_add(list_len)?;
            while name_pos + 3 <= list_end && list_end <= ext_end {
                let name_type = *data.get(name_pos)?;
                let name_len = read_u16(data, name_pos + 1)? as usize;
                name_pos += 3;
                let name_end = name_pos.checked_add(name_len)?;
                if name_end > list_end {
                    return None;
                }
                if name_type == 0 {
                    return std::str::from_utf8(&data[name_pos..name_end])
                        .ok()
                        .map(|s| s.to_string());
                }
                name_pos = name_end;
            }
        }

        pos = ext_end;
    }

    None
}

fn read_u16(data: &[u8], pos: usize) -> Option<u16> {
    Some(u16::from_be_bytes([*data.get(pos)?, *data.get(pos + 1)?]))
}

fn verify_token(cfg: &ServerConfig, got: Option<&str>) -> Result<()> {
    match cfg.auth.token.as_deref() {
        Some(expected) if Some(expected) != got => bail!("authentication failed"),
        _ => Ok(()),
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
