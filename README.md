# frp-rs

This is a Rust rewrite scaffold for `fatedier/frp`.

The current milestone implements the core reverse TCP proxy path:

- `frps` server binary
- `frpc` client binary
- frp-style TOML config loading
- token authentication
- control connection login
- dynamic TCP remote listener registration
- UDP request/response packet forwarding
- HTTP vhost routing by `Host` header and `customDomains`
- HTTPS passthrough routing by TLS SNI and `customDomains`
- TCP health checks that register healthy proxies and close unhealthy ones
- pre-opened TCP work connection pools with `poolCount`
- TCP/HTTP/HTTPS bandwidth limiting with `bandwidthLimit`
- frpc config hot reload by file modification time
- lightweight frps dashboard at `/` and `/api/status`
- basic server-side HTTP plugin hooks for login and new proxy events
- STCP private visitor protocol with `sk` authentication and local visitor listeners
- NAT hole punching controller and control-channel candidate exchange messages
- TCP, QUIC, and KCP control/work transports via `transport.protocol`
- per-visitor work connection request
- bidirectional TCP byte forwarding
- reconnecting client session loop

Implemented configuration example:

```toml
# frps.toml
bindAddr = "0.0.0.0"
bindPort = 7000
proxyBindAddr = "0.0.0.0"
vhostHTTPPort = 8080
vhostHTTPSPort = 8443
dashboardAddr = "127.0.0.1"
dashboardPort = 7500

[auth]
token = "secret"

[transport]
protocol = "tcp"
# protocol = "kcp"
# protocol = "quic"

[plugins]
loginURL = "http://127.0.0.1:9000/login"
newProxyURL = "http://127.0.0.1:9000/new_proxy"
```

```toml
# frpc.toml
serverAddr = "127.0.0.1"
serverPort = 7000
poolCount = 2

[auth]
token = "secret"

[transport]
protocol = "tcp"
# protocol = "kcp"
# protocol = "quic"

[[proxies]]
name = "ssh"
type = "tcp"
localIP = "127.0.0.1"
localPort = 22
remotePort = 6000
bandwidthLimit = "10MB"

[proxies.healthCheck]
type = "tcp"
intervalSeconds = 10
timeoutSeconds = 3
maxFailed = 3

[[proxies]]
name = "web"
type = "http"
localIP = "127.0.0.1"
localPort = 8081
customDomains = ["www.example.com"]

[[proxies]]
name = "secure-web"
type = "https"
localIP = "127.0.0.1"
localPort = 8444
customDomains = ["secure.example.com"]

[[proxies]]
name = "dns"
type = "udp"
localIP = "8.8.8.8"
localPort = 53
remotePort = 5353

[[proxies]]
name = "private-ssh"
type = "stcp"
localIP = "127.0.0.1"
localPort = 22
sk = "private-secret"

[[visitors]]
name = "private-ssh-visitor"
type = "stcp"
serverName = "private-ssh"
bindAddr = "127.0.0.1"
bindPort = 16022
sk = "private-secret"
```

Run:

```bash
cargo run --bin frps -- -c conf/frps.toml
cargo run --bin frpc -- -c conf/frpc.toml
```

Parity roadmap:

- TCP: first milestone implemented.
- HTTP: basic Host based forwarding implemented; path routing, header rewrite, basic auth, real IP headers, and groups remain.
- UDP: basic request/response forwarding implemented; NAT session reuse, batching, and packet-level optimizations remain.
- HTTPS: basic SNI sniffing and raw TLS passthrough routing implemented; wildcard domains and advanced fallback behavior remain.
- Health checks: TCP health checks implemented; HTTP checks and richer status reporting remain.
- Connection pool: pre-opened TCP work connections implemented through `poolCount`.
- Bandwidth limit: per-proxy byte throttling implemented for stream proxies.
- Dashboard: lightweight built-in status page and JSON API implemented.
- Plugins: basic plain-HTTP login and new-proxy hooks implemented.
- Hot reload: frpc watches config file mtime and reconnects on change.
- QUIC/KCP: real control/work transports implemented and covered by end-to-end proxy tests.
- STCP: private visitor flow implemented for TCP streams.
- SUDP/XTCP/P2P: add UDP visitor and full peer-to-peer data plane on top of the NAT controller.
- Transport: add TLS, WebSocket, QUIC, KCP, TCP muxing, connection pools.
- Runtime controls: add hot reload, admin API, status API, metrics, dashboard.
- Policy: add allow ports, bandwidth limits, health checks, groups, load balancing.
- Plugins: add server and client plugin hook protocols.
- VirtualNet and SSH tunnel gateway: separate milestones.
