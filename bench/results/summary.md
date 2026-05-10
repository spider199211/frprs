# frp vs frp-rs TCP Proxy Benchmark Matrix

- Time: 2026-05-10 06:26:36
- Payload: 256 bytes echo request / response
- Transport: TCP control/work connection
- Optimized pool count: 256
- Concurrency levels: 100, 150, 200, 250, 300, 350, 400, 450, 500
- Runs per project/level: 3
- Connection ramp: 1000 ms

| Project | Concurrency | Runs | Avg Connected | Avg Requests | Avg Errors | Avg RPS | Avg MiB/s | Avg ms | Avg P95 ms | Avg P99 ms | Avg CPU sec | Avg CPU sec / 1k req | Avg Peak WS MiB | Avg Peak Private MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| frp | 100 | 3 | 100.0 | 20000/20000 | 0.0 | 12756.9 | 6.23 | 7.817 | 6.020 | 19.539 | 2.396 | 0.1198 | 33.13 | 39.92 |
| frp-rs | 100 | 3 | 100.0 | 20000/20000 | 0.0 | 12681.9 | 6.19 | 7.777 | 6.380 | 13.750 | 2.667 | 0.1333 | 10.01 | 5.20 |
| frp | 150 | 3 | 150.0 | 30000/30000 | 0.0 | 12157.3 | 5.94 | 12.290 | 9.599 | 274.822 | 2.948 | 0.0983 | 39.10 | 46.58 |
| frp-rs | 150 | 3 | 150.0 | 30000/30000 | 0.0 | 11734.3 | 5.73 | 12.357 | 9.967 | 271.113 | 3.682 | 0.1227 | 10.99 | 6.15 |
| frp | 200 | 3 | 200.0 | 40000/40000 | 0.0 | 9314.2 | 4.55 | 21.559 | 18.089 | 295.282 | 4.589 | 0.1147 | 44.35 | 54.99 |
| frp-rs | 200 | 3 | 200.0 | 40000/40000 | 0.0 | 9617.9 | 4.70 | 20.917 | 16.627 | 282.362 | 5.880 | 0.1470 | 11.94 | 7.11 |
| frp | 250 | 3 | 250.0 | 50000/50000 | 0.0 | 10434.3 | 5.09 | 23.890 | 24.490 | 277.368 | 5.068 | 0.1014 | 49.61 | 60.19 |
| frp-rs | 250 | 3 | 250.0 | 50000/50000 | 0.0 | 9911.6 | 4.84 | 25.172 | 26.798 | 276.037 | 5.865 | 0.1173 | 13.19 | 8.61 |
| frp | 300 | 3 | 300.0 | 60000/60000 | 0.0 | 10201.7 | 4.98 | 28.747 | 23.225 | 291.939 | 6.115 | 0.1019 | 55.94 | 67.19 |
| frp-rs | 300 | 3 | 300.0 | 60000/60000 | 0.0 | 10111.3 | 4.94 | 29.086 | 23.015 | 289.302 | 6.786 | 0.1131 | 14.23 | 9.70 |
| frp | 350 | 3 | 350.0 | 70000/70000 | 0.0 | 9987.4 | 4.88 | 34.864 | 30.262 | 301.588 | 7.245 | 0.1035 | 61.56 | 74.66 |
| frp-rs | 350 | 3 | 350.0 | 70000/70000 | 0.0 | 10128.5 | 4.95 | 34.281 | 29.242 | 297.696 | 9.198 | 0.1314 | 15.12 | 10.59 |
| frp | 400 | 3 | 400.0 | 80000/80000 | 0.0 | 9396.7 | 4.59 | 41.882 | 142.480 | 311.109 | 8.495 | 0.1062 | 66.77 | 79.99 |
| frp-rs | 400 | 3 | 400.0 | 80000/80000 | 0.0 | 9797.9 | 4.78 | 40.450 | 140.538 | 307.287 | 10.812 | 0.1352 | 16.60 | 12.43 |
| frp | 450 | 3 | 450.0 | 90000/90000 | 0.0 | 9446.5 | 4.61 | 47.155 | 295.308 | 321.961 | 10.401 | 0.1156 | 73.89 | 91.51 |
| frp-rs | 450 | 3 | 450.0 | 90000/90000 | 0.0 | 9599.7 | 4.69 | 46.498 | 296.125 | 312.930 | 11.146 | 0.1238 | 17.56 | 13.47 |
| frp | 500 | 3 | 500.0 | 100000/100000 | 0.0 | 9196.9 | 4.49 | 53.627 | 304.991 | 332.477 | 11.062 | 0.1106 | 81.63 | 100.13 |
| frp-rs | 500 | 3 | 500.0 | 100000/100000 | 0.0 | 9355.8 | 4.57 | 53.060 | 302.225 | 322.582 | 13.349 | 0.1335 | 18.43 | 14.28 |

## Code Optimizations

- frp-rs server work pool uses a `VecDeque` plus `Notify` and atomic counters instead of waiting on an `mpsc::Receiver` behind a mutex.
- `ReqWorkConn` carries a `count`, allowing batched work-connection replenishment.
- The server tracks active waiters and requests extra work connections when demand exceeds the configured pool size.
- Work-pool replenishment is scheduled in the background so the current visitor is not blocked by refill messages.
- TCP listeners use an explicit backlog of 4096 where applicable.
- TCP streams on the control, visitor, and local service paths use `TCP_NODELAY`.
- Relay forwarding uses Tokio `copy_bidirectional` when no bandwidth limit is configured.

## Config Optimizations

- frp: `transport.poolCount = 256`, `transport.maxPoolCount = 256`, `transport.tcpMux = false`, `transport.tls.enable = false`, log level `warn`.
- frp-rs: `poolCount = 256`, `RUST_LOG=warn`.
- Echo server backlog is set to 4096 to reduce local benchmark harness bottlenecks.

## Notes

- This benchmark runs on localhost, so it mainly compares proxy overhead rather than real network behavior.
- CPU is sampled from Windows process CPU time deltas for `frps` + `frpc`.
- Memory is the average of each run's peak working set/private memory.
- Raw per-run data is available in `all_results.json`.
