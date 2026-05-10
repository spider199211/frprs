# frp-rs

This is a Rust rewrite scaffold for `fatedier/frp`.

## 中文说明

`frp-rs` 是一个用 Rust 重写 `fatedier/frp` 的实验性实现。当前版本已经实现了反向代理的核心链路，并补充了 STCP visitor、NAT 打洞控制器、QUIC/KCP 传输层等能力。

当前已实现：

- `frps` 服务端程序
- `frpc` 客户端程序
- frp 风格 TOML 配置加载
- token 认证
- 控制连接登录
- 动态 TCP 远端监听注册
- UDP 请求/响应包转发
- HTTP 虚拟主机转发，按 `Host` 和 `customDomains` 路由
- HTTPS 透传，按 TLS SNI 和 `customDomains` 路由
- TCP 健康检查，服务健康时注册代理，不健康时关闭代理
- 通过 `poolCount` 预创建 work connection 连接池
- 通过 `bandwidthLimit` 对 TCP/HTTP/HTTPS 做带宽限制
- `frpc` 根据配置文件修改时间自动热加载
- 轻量级 `frps` Dashboard，支持 `/` 和 `/api/status`
- 服务端 HTTP 插件钩子，支持登录和新代理事件
- STCP 私有 visitor 协议，支持 `sk` 密钥校验和本地 visitor 监听
- NAT 打洞控制器和控制通道候选地址交换消息
- 通过 `transport.protocol` 支持 TCP、QUIC、KCP 控制/工作连接传输
- 按 visitor 请求 work connection
- 双向 TCP 字节转发
- 客户端断线自动重连

服务端配置示例：

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

客户端配置示例：

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

运行：

```bash
cargo run --bin frps -- -c conf/frps.toml
cargo run --bin frpc -- -c conf/frpc.toml
```

## frp vs frp-rs 性能压测对比

本次压测基于本机 TCP echo 代理链路，分别启动官方 `frp` 和本项目 `frp-rs`，对 100 到 500 并发按 50 一档递增压测，每档每个项目运行 3 次取平均值。

压测条件：

- 时间：2026-05-10 06:26:36
- Payload：256 bytes echo request / response
- Transport：TCP control/work connection
- 连接池：`poolCount = 256`
- 并发档位：100、150、200、250、300、350、400、450、500
- 每档运行次数：3 次
- 连接启动斜坡：1000 ms

| Project | Concurrency | Runs | Avg Requests | Avg Errors | Avg RPS | Avg MiB/s | Avg ms | Avg P95 ms | Avg P99 ms | Avg CPU sec / 1k req | Avg Peak WS MiB | Avg Peak Private MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| frp | 100 | 3 | 20000/20000 | 0.0 | 12756.9 | 6.23 | 7.817 | 6.020 | 19.539 | 0.1198 | 33.13 | 39.92 |
| frp-rs | 100 | 3 | 20000/20000 | 0.0 | 12681.9 | 6.19 | 7.777 | 6.380 | 13.750 | 0.1333 | 10.01 | 5.20 |
| frp | 150 | 3 | 30000/30000 | 0.0 | 12157.3 | 5.94 | 12.290 | 9.599 | 274.822 | 0.0983 | 39.10 | 46.58 |
| frp-rs | 150 | 3 | 30000/30000 | 0.0 | 11734.3 | 5.73 | 12.357 | 9.967 | 271.113 | 0.1227 | 10.99 | 6.15 |
| frp | 200 | 3 | 40000/40000 | 0.0 | 9314.2 | 4.55 | 21.559 | 18.089 | 295.282 | 0.1147 | 44.35 | 54.99 |
| frp-rs | 200 | 3 | 40000/40000 | 0.0 | 9617.9 | 4.70 | 20.917 | 16.627 | 282.362 | 0.1470 | 11.94 | 7.11 |
| frp | 250 | 3 | 50000/50000 | 0.0 | 10434.3 | 5.09 | 23.890 | 24.490 | 277.368 | 0.1014 | 49.61 | 60.19 |
| frp-rs | 250 | 3 | 50000/50000 | 0.0 | 9911.6 | 4.84 | 25.172 | 26.798 | 276.037 | 0.1173 | 13.19 | 8.61 |
| frp | 300 | 3 | 60000/60000 | 0.0 | 10201.7 | 4.98 | 28.747 | 23.225 | 291.939 | 0.1019 | 55.94 | 67.19 |
| frp-rs | 300 | 3 | 60000/60000 | 0.0 | 10111.3 | 4.94 | 29.086 | 23.015 | 289.302 | 0.1131 | 14.23 | 9.70 |
| frp | 350 | 3 | 70000/70000 | 0.0 | 9987.4 | 4.88 | 34.864 | 30.262 | 301.588 | 0.1035 | 61.56 | 74.66 |
| frp-rs | 350 | 3 | 70000/70000 | 0.0 | 10128.5 | 4.95 | 34.281 | 29.242 | 297.696 | 0.1314 | 15.12 | 10.59 |
| frp | 400 | 3 | 80000/80000 | 0.0 | 9396.7 | 4.59 | 41.882 | 142.480 | 311.109 | 0.1062 | 66.77 | 79.99 |
| frp-rs | 400 | 3 | 80000/80000 | 0.0 | 9797.9 | 4.78 | 40.450 | 140.538 | 307.287 | 0.1352 | 16.60 | 12.43 |
| frp | 450 | 3 | 90000/90000 | 0.0 | 9446.5 | 4.61 | 47.155 | 295.308 | 321.961 | 0.1156 | 73.89 | 91.51 |
| frp-rs | 450 | 3 | 90000/90000 | 0.0 | 9599.7 | 4.69 | 46.498 | 296.125 | 312.930 | 0.1238 | 17.56 | 13.47 |
| frp | 500 | 3 | 100000/100000 | 0.0 | 9196.9 | 4.49 | 53.627 | 304.991 | 332.477 | 0.1106 | 81.63 | 100.13 |
| frp-rs | 500 | 3 | 100000/100000 | 0.0 | 9355.8 | 4.57 | 53.060 | 302.225 | 322.582 | 0.1335 | 18.43 | 14.28 |

结论：

- 稳定性：两边在 100 到 500 并发下都完成了全部请求，错误数均为 0。
- 吞吐：`frp-rs` 在 200、350、400、450、500 并发档位的平均 RPS 高于官方 `frp`；100、150、250、300 并发档位略低或基本接近。
- 延迟：`frp-rs` 在 200、300、350、400、500 并发档位的 P95/P99 整体优于或接近官方 `frp`；450 并发 P95 略高但 P99 更低。
- 内存：`frp-rs` 峰值工作集和私有内存明显低于官方 `frp`。500 并发时，`frp-rs` 峰值工作集约 18.43 MiB，官方 `frp` 约 81.63 MiB。
- CPU：`frp-rs` 当前 CPU/千请求多数档位高于官方 `frp`，说明后续还有协议调度、连接复用、任务分配和转发路径上的优化空间。
- 压测边界：本次是 localhost 压测，400 并发以上 P95/P99 同时抬升，已经明显受到本机端口、调度和压测端负载影响；它更适合比较代理开销，不等同于公网链路表现。

完整压测数据见 `bench/results/summary.md` 和 `bench/results/all_results.json`。

功能路线图：

- TCP：已实现基础反向代理链路。
- HTTP：已实现按 `Host` 的基础转发；路径路由、请求头改写、Basic Auth、真实 IP 请求头、分组等仍待完善。
- UDP：已实现基础请求/响应转发；NAT 会话复用、批量处理和包级优化仍待完善。
- HTTPS：已实现 SNI 嗅探和原始 TLS 透传；通配域名和高级 fallback 行为仍待完善。
- 健康检查：已实现 TCP 健康检查；HTTP 检查和更丰富的状态上报仍待完善。
- 连接池：已通过 `poolCount` 实现预创建 work connection。
- 带宽限制：已实现按代理限速。
- Dashboard：已实现内置状态页面和 JSON API。
- 插件：已实现服务端登录和新代理 plain HTTP 钩子。
- 热加载：`frpc` 会监听配置文件修改时间并自动重连。
- QUIC/KCP：已实现真实控制/工作连接传输，并有端到端代理测试覆盖。
- STCP：已实现 TCP 流的私有 visitor 转发。
- SUDP/XTCP/P2P：已具备 NAT 控制器基础，还需要补 UDP visitor 和完整 P2P 数据面。
- Transport：后续继续补 TLS、WebSocket、TCP mux 等传输能力。
- 运行时控制：后续补 Admin API、更多状态 API、指标等。
- 策略能力：后续补端口白名单、分组、负载均衡等。
- 插件能力：后续补客户端插件钩子和更多协议兼容。
- VirtualNet 和 SSH tunnel gateway：作为独立里程碑实现。

## English

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
