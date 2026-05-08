# frp-rs parity map

This file tracks the Rust rewrite against the upstream `fatedier/frp` feature surface.

| Area | Status | Notes |
| --- | --- | --- |
| `frps` / `frpc` binaries | Implemented | `cargo run --bin frps`, `cargo run --bin frpc`. |
| TOML config loading | Implemented | Supports frp-style core fields. More legacy and include-file behavior remains. |
| Token authentication | Implemented | Shared `[auth].token` checked on login, work connections, and ping. |
| TCP proxy | Implemented | Control connection, work connection request, remote listener, bidirectional forwarding. |
| UDP proxy | Basic implemented | Request/response packet forwarding works. Needs NAT session reuse, batching, and perf work. |
| HTTP proxy | Basic implemented | Routes by `Host` / `customDomains`. Needs path routing, header rewrite, basic auth, real IP headers, groups. |
| HTTPS proxy | Basic implemented | Routes by TLS SNI / `customDomains` and passes TLS through untouched. Needs wildcard domains and fallback handling. |
| Connection pool | Basic implemented | `poolCount` pre-opens work connections and refills after use. |
| Bandwidth limiting | Basic implemented | `bandwidthLimit` throttles TCP/HTTP/HTTPS stream copy. |
| Hot reload | Basic implemented | `frpc` watches config mtime and reconnects when the file changes. |
| Dashboard | Basic implemented | Built-in HTML status page and `/api/status` JSON endpoint. |
| Plugins | Basic implemented | Server-side plain HTTP hooks for login and new-proxy events. |
| TCPMux HTTP CONNECT | Not implemented | Needs HTTP CONNECT parser and grouped mux listeners. |
| STCP / SUDP | STCP implemented | STCP has visitor config, secret key auth, local visitor listener, and end-to-end TCP forwarding. SUDP remains. |
| XTCP / P2P | Controller implemented | NAT hole punching controller and control-channel messages are present. Direct peer data plane remains. |
| TLS transport | Not implemented | Add rustls and certificate config parity. |
| WebSocket transport | Not implemented | Add HTTP upgrade transport. |
| QUIC transport | Implemented | Uses `quinn` for control/work connections and has end-to-end proxy coverage. Current client uses self-signed/insecure verification for local deployment. |
| KCP transport | Implemented | Uses `tokio_kcp` for control/work connections and has end-to-end proxy coverage. |
| TCP stream multiplexing | Not implemented | Candidate crate: `yamux`. |
| Load balancing / groups | Not implemented | Needs proxy registry and selection policy. |
| Health checks | Basic implemented | TCP health checks register healthy proxies and close unhealthy ones. HTTP checks and status API remain. |
| Allow ports | Not implemented | Add server-side remote port policy. |
| Admin API | Not implemented | Dashboard status exists; mutation/admin endpoints remain. |
| Client plugins | Not implemented | Add built-in and external client plugin hooks. |
| SSH tunnel gateway | Not implemented | Needs SSH server integration. |
| VirtualNet | Not implemented | Needs TUN/TAP support per platform. |

Current tested flows:

- TCP echo through `frps` and `frpc`.
- UDP echo through `frps` and `frpc`.
- HTTP request routed by `Host` through `frps` and `frpc`.
- HTTPS passthrough request routed by TLS SNI through `frps` and `frpc`.
- TCP health check closing an unhealthy proxy listener.
- Dashboard status JSON endpoint.
- TCP proxy with pooled work connections.
- STCP visitor forwarding through a local visitor listener.
- TCP proxy over KCP transport.
- TCP proxy over QUIC transport.
- NAT hole controller candidate exchange.
- Raw QUIC and KCP transport round trips.
