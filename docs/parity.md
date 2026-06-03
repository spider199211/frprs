# frp-rs parity map

This file tracks the Rust rewrite against the upstream `fatedier/frp` feature surface.

| Area | Status | Notes |
| --- | --- | --- |
| `frps` / `frpc` binaries | Implemented | `cargo run --bin frps`, `cargo run --bin frpc`. |
| TOML config loading | Implemented | Supports frp-style core fields. More legacy and include-file behavior remains. |
| Token authentication | Implemented | Shared `[auth].token` checked on login, work connections, and ping. |
| TCP proxy | Implemented | Control connection, work connection request, remote listener, bidirectional forwarding. |
| UDP proxy | Implemented | Request/response forwarding, local NAT session reuse, bidirectional batching, batch destination caching, and groups are implemented. Deeper packet-path optimizations remain. |
| HTTP proxy | Implemented | Routes by `Host`, wildcard domains, `locations`, header rewrite, Basic Auth, real IP headers, and groups. |
| HTTPS proxy | Implemented | Routes by TLS SNI / `customDomains`, wildcard domains, `*` fallback, groups, and raw passthrough. |
| Connection pool | Implemented | `poolCount` pre-opens work connections, coalesces replenishment, requests only immediate waiter demand, and reuses QUIC client sessions for control/work/visitor streams. |
| Bandwidth limiting | Implemented | `bandwidthLimit` throttles TCP/HTTP/HTTPS stream copy. |
| Hot reload | Implemented | `frpc` watches config mtime and reconnects when the file changes. |
| Dashboard | Implemented | Built-in HTML status page plus clients, proxies, groups, status, and metrics JSON APIs. |
| Plugins | Implemented | Server-side plain HTTP hooks for login, new-proxy, TCP/UDP/SUDP/HTTP/HTTPS/TCPMUX new-user-connection, and close-proxy events; client-side local-connect hook covers TCP/UDP/SUDP. |
| TCPMux HTTP CONNECT | Implemented | HTTP CONNECT parser, domain routing, and grouped mux listeners are implemented. |
| STCP / SUDP | Implemented | STCP and SUDP have visitor config, `sk` auth, local visitor listeners, group load balancing, and end-to-end forwarding. |
| XTCP / P2P | Implemented | NAT controller, multiple peers per transaction, candidate exchange, async notifications, XTCP direct data plane, SUDP direct data plane, probing/retry, owner punch retry, and fallback are implemented. Complex NAT scenarios still need broader validation and tuning. |
| TLS transport | Implemented | Uses rustls for control/work connections and has end-to-end proxy coverage. |
| WebSocket transport | Implemented | Uses HTTP upgrade transport and has end-to-end proxy coverage. |
| QUIC transport | Implemented | Uses `quinn` for control/work connections, reuses one client connection for multiple bidirectional streams, and has end-to-end proxy coverage. Current client uses self-signed/insecure verification for local deployment. |
| KCP transport | Implemented | Uses `tokio_kcp` for control/work connections and has end-to-end proxy coverage. |
| TCP stream multiplexing | Not implemented | Candidate crate: `yamux`. |
| Load balancing / groups | Implemented | TCP/UDP/HTTP/HTTPS/TCPMUX/STCP/XTCP/SUDP groups are implemented. |
| Health checks | Implemented | TCP and HTTP checks report proxy health and can close unhealthy proxies. |
| Allow ports | Implemented | Server-side remote port policy and runtime Admin API updates are implemented. |
| Admin API | Implemented | Supports metrics reset, proxy/group/client close operations, and runtime `allowPorts` updates. |
| Client plugins | Implemented | `localConnectURL` HTTP hook supports TCP/UDP/SUDP local connects or sessions. |
| SSH tunnel gateway | Not implemented | Needs SSH server integration. |
| VirtualNet | Not implemented | Needs TUN/TAP support per platform. |

Current tested flows:

- TCP echo through `frps` and `frpc`.
- UDP echo through `frps` and `frpc`.
- HTTP request routed by `Host` through `frps` and `frpc`.
- HTTPS passthrough request routed by TLS SNI through `frps` and `frpc`.
- TCP health check closing an unhealthy proxy listener.
- Dashboard status JSON endpoint.
- Dashboard admin close, metrics reset, and allowPorts update endpoints.
- TCP proxy with pooled work connections.
- STCP visitor forwarding through a local visitor listener.
- STCP, XTCP, UDP, HTTP, HTTPS, TCPMUX, and SUDP group load balancing.
- SUDP visitor forwarding by relay and direct data path.
- Server and client plugin hooks for TCP/UDP/SUDP plus HTTP-family proxy events.
- TCP proxy over TLS and WebSocket transports.
- TCP proxy over KCP transport.
- TCP proxy over QUIC transport.
- QUIC transport reusing one connection for multiple bidirectional streams.
- NAT hole controller candidate exchange.
- Raw TLS, WebSocket, QUIC, and KCP transport round trips.
