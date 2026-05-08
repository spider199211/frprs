use crate::{
    config::{ServerConfig, TransportProtocol},
    nathole::{NatHoleController, NatHoleOutcome, NatHolePeer, NatHoleRole},
    protocol::{read_msg, write_msg, Message},
    transports::{self, BoxStream},
};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::json;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, WriteHalf},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{mpsc, Mutex},
    task::JoinHandle,
    time,
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
struct ServerState {
    cfg: Arc<ServerConfig>,
    controls: Arc<Mutex<HashMap<String, Arc<Control>>>>,
    http_routes: Arc<Mutex<HashMap<String, ProxyRoute>>>,
    https_routes: Arc<Mutex<HashMap<String, ProxyRoute>>>,
    stcp_routes: Arc<Mutex<HashMap<String, StcpRoute>>>,
    nathole: Arc<NatHoleController>,
    udp_relays: Arc<Mutex<HashMap<String, Arc<UdpSocket>>>>,
    proxy_entries: Arc<Mutex<HashMap<String, ProxyEntry>>>,
    metrics: Arc<ServerMetrics>,
}

struct Control {
    run_id: String,
    pool_count: usize,
    writer: Mutex<WriteHalf<BoxStream>>,
    work_tx: mpsc::Sender<BoxStream>,
    work_rx: Mutex<mpsc::Receiver<BoxStream>>,
}

#[derive(Clone)]
struct ProxyRoute {
    control: Arc<Control>,
    proxy_name: String,
    bandwidth_limit: Option<u64>,
}

#[derive(Clone)]
struct StcpRoute {
    control: Arc<Control>,
    proxy_name: String,
    sk: Option<String>,
    bandwidth_limit: Option<u64>,
}

struct ProxyEntry {
    run_id: String,
    proxy_type: String,
    domains: Vec<String>,
    remote_addr: String,
    bandwidth_limit: Option<u64>,
    task: Option<JoinHandle<()>>,
}

struct ServerMetrics {
    started_unix_secs: u64,
    visitor_connections_total: AtomicU64,
    bytes_up_total: AtomicU64,
    bytes_down_total: AtomicU64,
}

impl Control {
    async fn send(&self, msg: &Message) -> Result<()> {
        let mut writer = self.writer.lock().await;
        write_msg(&mut *writer, msg).await
    }
}

pub async fn run(cfg: ServerConfig) -> Result<()> {
    let state = ServerState {
        cfg: Arc::new(cfg),
        controls: Arc::new(Mutex::new(HashMap::new())),
        http_routes: Arc::new(Mutex::new(HashMap::new())),
        https_routes: Arc::new(Mutex::new(HashMap::new())),
        stcp_routes: Arc::new(Mutex::new(HashMap::new())),
        nathole: Arc::new(NatHoleController::default()),
        udp_relays: Arc::new(Mutex::new(HashMap::new())),
        proxy_entries: Arc::new(Mutex::new(HashMap::new())),
        metrics: Arc::new(ServerMetrics {
            started_unix_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
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
        TransportProtocol::Kcp => run_kcp_control_listener(state).await,
        TransportProtocol::Quic => run_quic_control_listener(state).await,
    }
}

async fn run_tcp_control_listener(state: ServerState) -> Result<()> {
    let listener = TcpListener::bind(state.cfg.control_addr())
        .await
        .with_context(|| format!("listen frps control on {}", state.cfg.control_addr()))?;
    let addr = listener
        .local_addr()
        .context("read frps listener address")?;
    info!("frps tcp control listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await.context("accept frps connection")?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(state, Box::new(stream), peer).await {
                debug!("connection {peer} closed: {err:#}");
            }
        });
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
        let stream = endpoint.accept_stream().await?;
        let peer = stream.remote_addr;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(state, Box::new(stream), peer).await {
                debug!("quic connection {peer} closed: {err:#}");
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
    let (work_tx, work_rx) = mpsc::channel(256);
    let control = Arc::new(Control {
        run_id: run_id.clone(),
        pool_count,
        writer: Mutex::new(writer),
        work_tx,
        work_rx: Mutex::new(work_rx),
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
                custom_domains,
                bandwidth_limit,
                sk,
            }) => {
                let resp = match start_proxy(
                    state.clone(),
                    control.clone(),
                    proxy_name.clone(),
                    proxy_type.clone(),
                    remote_port,
                    custom_domains,
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
            Ok(other) => {
                warn!("ignored unsupported control message: {other:?}");
            }
            Err(err) => {
                info!("control {run_id} disconnected: {err:#}");
                state.controls.lock().await.remove(&run_id);
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

    control
        .work_tx
        .send(stream)
        .await
        .map_err(|_| anyhow!("control {run_id} is no longer accepting work connections"))?;
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
        .and_then(|role| parse_socket_addrs(&local_addrs).map(|local_addrs| (role, local_addrs)))
        .and_then(|(role, local_addrs)| {
            state.nathole.register(NatHolePeer {
                transaction_id: transaction_id.clone(),
                proxy_name: proxy_name.clone(),
                run_id: control.run_id.clone(),
                role,
                observed_addr: peer,
                local_addrs,
            })
        });

    match parsed {
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
        Ok(NatHoleOutcome::Matched(candidate)) => Message::NatHoleResp {
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
        },
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

async fn handle_stcp_visitor_conn(
    state: ServerState,
    mut stream: BoxStream,
    peer: SocketAddr,
    proxy_name: String,
    sk: Option<String>,
) -> Result<()> {
    let route = {
        let routes = state.stcp_routes.lock().await;
        routes.get(&proxy_name).cloned()
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
    custom_domains: Vec<String>,
    bandwidth_limit: Option<String>,
    sk: Option<String>,
) -> Result<String> {
    close_proxy(state.clone(), &proxy_name).await?;
    call_plugin_hook(
        state.cfg.plugins.new_proxy_url.as_deref(),
        json!({
            "run_id": control.run_id,
            "proxy_name": proxy_name,
            "proxy_type": proxy_type,
            "remote_port": remote_port,
            "custom_domains": custom_domains,
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
        "tcp" => start_tcp_proxy(state, control, proxy_name, remote_port, bandwidth_limit).await,
        "udp" => start_udp_proxy(state, control, proxy_name, remote_port).await,
        "http" => {
            start_http_proxy(state, control, proxy_name, custom_domains, bandwidth_limit).await
        }
        "https" => {
            start_https_proxy(state, control, proxy_name, custom_domains, bandwidth_limit).await
        }
        "stcp" => start_stcp_proxy(state, control, proxy_name, sk, bandwidth_limit).await,
        "sudp" | "xtcp" => bail!(
            "proxy type {proxy_type} is recognized but UDP visitor/P2P routing is not implemented yet"
        ),
        _ => bail!("proxy type {proxy_type} is not implemented"),
    }
}

async fn start_tcp_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    remote_port: u16,
    bandwidth_limit: Option<u64>,
) -> Result<String> {
    let bind_addr = format!("{}:{}", state.cfg.proxy_bind_addr, remote_port);
    let listener = TcpListener::bind(&bind_addr)
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
    let task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((visitor, visitor_addr)) => {
                    let control = control.clone();
                    let proxy_name = task_proxy_name.clone();
                    let metrics = metrics.clone();
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
            domains: Vec::new(),
            remote_addr: remote_addr.clone(),
            bandwidth_limit,
            task: Some(task),
        },
    );

    Ok(remote_addr)
}

async fn start_udp_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    remote_port: u16,
) -> Result<String> {
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
    let task = tokio::spawn(async move {
        let mut buf = vec![0_u8; 64 * 1024];
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((n, visitor_addr)) => {
                    metrics
                        .visitor_connections_total
                        .fetch_add(1, Ordering::Relaxed);
                    metrics
                        .bytes_up_total
                        .fetch_add(n as u64, Ordering::Relaxed);
                    let msg = Message::UdpPacket {
                        proxy_name: task_proxy_name.clone(),
                        content: buf[..n].to_vec(),
                        visitor_addr: visitor_addr.to_string(),
                    };
                    if let Err(err) = control.send(&msg).await {
                        warn!("udp proxy {task_proxy_name} failed to forward packet to client: {err:#}");
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
            domains: Vec::new(),
            remote_addr: remote_addr.clone(),
            bandwidth_limit: None,
            task: Some(task),
        },
    );

    Ok(remote_addr)
}

async fn send_udp_packet_to_visitor(
    state: ServerState,
    proxy_name: &str,
    content: &[u8],
    visitor_addr: &str,
) -> Result<()> {
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

async fn start_http_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    custom_domains: Vec<String>,
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
    };
    let mut routes = state.http_routes.lock().await;
    for domain in &custom_domains {
        routes.insert(normalize_host(domain), route.clone());
    }

    let first = &custom_domains[0];
    let remote_addr = format!("http://{}:{}", first, state.cfg.vhost_http_port);
    info!("http proxy {proxy_name} registered for domains {custom_domains:?}");
    state.proxy_entries.lock().await.insert(
        proxy_name,
        ProxyEntry {
            run_id: control.run_id.clone(),
            proxy_type: "http".to_string(),
            domains: custom_domains,
            remote_addr: remote_addr.clone(),
            bandwidth_limit,
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
    };
    let mut routes = state.https_routes.lock().await;
    for domain in &custom_domains {
        routes.insert(normalize_host(domain), route.clone());
    }

    let first = &custom_domains[0];
    let remote_addr = format!("https://{}:{}", first, state.cfg.vhost_https_port);
    info!("https proxy {proxy_name} registered for domains {custom_domains:?}");
    state.proxy_entries.lock().await.insert(
        proxy_name,
        ProxyEntry {
            run_id: control.run_id.clone(),
            proxy_type: "https".to_string(),
            domains: custom_domains,
            remote_addr: remote_addr.clone(),
            bandwidth_limit,
            task: None,
        },
    );
    Ok(remote_addr)
}

async fn start_stcp_proxy(
    state: ServerState,
    control: Arc<Control>,
    proxy_name: String,
    sk: Option<String>,
    bandwidth_limit: Option<u64>,
) -> Result<String> {
    let remote_addr = format!("stcp://{proxy_name}");
    let route = StcpRoute {
        control: control.clone(),
        proxy_name: proxy_name.clone(),
        sk,
        bandwidth_limit,
    };

    state
        .stcp_routes
        .lock()
        .await
        .insert(proxy_name.clone(), route);
    info!("stcp proxy {proxy_name} registered");

    state.proxy_entries.lock().await.insert(
        proxy_name,
        ProxyEntry {
            run_id: control.run_id.clone(),
            proxy_type: "stcp".to_string(),
            domains: Vec::new(),
            remote_addr: remote_addr.clone(),
            bandwidth_limit,
            task: None,
        },
    );
    Ok(remote_addr)
}

async fn handle_tcp_visitor(
    control: Arc<Control>,
    metrics: Arc<ServerMetrics>,
    proxy_name: String,
    mut visitor: TcpStream,
    visitor_addr: SocketAddr,
    bandwidth_limit: Option<u64>,
) -> Result<()> {
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
    let listener = TcpListener::bind(&addr)
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
    let path = request_line.split_whitespace().nth(1).unwrap_or("/");

    if path == "/api/status" {
        let body = dashboard_status_json(&state).await?.to_string();
        write_http_response(&mut stream, "200 OK", "application/json", body.as_bytes()).await?;
        return Ok(());
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
    let controls = state.controls.lock().await;
    let proxies = state.proxy_entries.lock().await;
    let proxy_list = proxies
        .iter()
        .map(|(name, entry)| {
            json!({
                "name": name,
                "run_id": entry.run_id,
                "type": entry.proxy_type,
                "domains": entry.domains,
                "remote_addr": entry.remote_addr,
                "bandwidth_limit_bytes_per_second": entry.bandwidth_limit,
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "started_unix_secs": state.metrics.started_unix_secs,
        "clients": controls.len(),
        "proxies": proxy_list,
        "metrics": {
            "visitor_connections_total": state.metrics.visitor_connections_total.load(Ordering::Relaxed),
            "bytes_up_total": state.metrics.bytes_up_total.load(Ordering::Relaxed),
            "bytes_down_total": state.metrics.bytes_down_total.load(Ordering::Relaxed),
        }
    }))
}

async fn run_http_vhost(state: ServerState) -> Result<()> {
    let addr = format!(
        "{}:{}",
        state.cfg.proxy_bind_addr, state.cfg.vhost_http_port
    );
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("listen http vhost on {addr}"))?;
    let local_addr = listener.local_addr().context("read http vhost address")?;
    info!("http vhost listening on {local_addr}");

    loop {
        let (stream, peer) = listener.accept().await.context("accept http visitor")?;
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
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("listen https vhost on {addr}"))?;
    let local_addr = listener.local_addr().context("read https vhost address")?;
    info!("https vhost listening on {local_addr}");

    loop {
        let (stream, peer) = listener.accept().await.context("accept https visitor")?;
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
    let host = parse_http_host(&prefix).ok_or_else(|| anyhow!("missing Host header"))?;
    let route = {
        let routes = state.http_routes.lock().await;
        routes.get(&normalize_host(&host)).cloned()
    }
    .ok_or_else(|| anyhow!("no http proxy registered for host {host}"))?;

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

async fn handle_https_visitor(
    state: ServerState,
    mut visitor: TcpStream,
    visitor_addr: SocketAddr,
) -> Result<()> {
    let prefix = read_tls_client_hello_prefix(&mut visitor).await?;
    let sni = parse_tls_sni(&prefix).ok_or_else(|| anyhow!("missing TLS SNI"))?;
    let route = {
        let routes = state.https_routes.lock().await;
        routes.get(&normalize_host(&sni)).cloned()
    }
    .ok_or_else(|| anyhow!("no https proxy registered for SNI {sni}"))?;

    let mut work = acquire_work_conn(&route.control).await?;

    write_msg(
        &mut work,
        &Message::StartWorkConn {
            proxy_name: route.proxy_name.clone(),
            src_addr: visitor_addr.to_string(),
            dst_addr: sni,
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
    if let Ok(work) = {
        let mut rx = control.work_rx.lock().await;
        rx.try_recv()
    } {
        if control.pool_count > 0 {
            let _ = control.send(&Message::ReqWorkConn).await;
        }
        return Ok(work);
    }

    control.send(&Message::ReqWorkConn).await?;
    let mut rx = control.work_rx.lock().await;
    time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .context("wait for client work connection timed out")?
        .ok_or_else(|| anyhow!("client control closed before work connection arrived"))
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

    if let Some(task) = entry.task {
        task.abort();
    }

    match entry.proxy_type.as_str() {
        "udp" => {
            state.udp_relays.lock().await.remove(proxy_name);
        }
        "http" => {
            let mut routes = state.http_routes.lock().await;
            for domain in entry.domains {
                routes.remove(&normalize_host(&domain));
            }
        }
        "https" => {
            let mut routes = state.https_routes.lock().await;
            for domain in entry.domains {
                routes.remove(&normalize_host(&domain));
            }
        }
        "stcp" => {
            state.stcp_routes.lock().await.remove(proxy_name);
        }
        _ => {}
    }

    Ok(true)
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
    let text = std::str::from_utf8(prefix).ok()?;
    for line in text.lines() {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("host") {
                return Some(value.trim().to_string());
            }
        }
    }
    None
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
