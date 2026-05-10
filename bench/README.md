# TCP proxy benchmark

This directory contains a small localhost benchmark for comparing official frp and frp-rs TCP proxy overhead.

The benchmark starts:

- a local TCP echo server
- `frps`
- `frpc`
- an asyncio load generator

It samples CPU and memory for `frps` plus `frpc` through Windows `Get-Process`, then writes `results.json` / `report.md` for single runs or `all_results.json` / `summary.md` for matrix runs.

## Usage

Build frp-rs first:

```powershell
cargo build --release --bins
```

Run frp-rs only:

```powershell
python bench\tcp_proxy_benchmark.py --projects frp-rs --concurrency 200 --requests-per-conn 200 --output bench/results/frprs-tcp-c200-r200
```

Run official frp and frp-rs:

```powershell
python bench\tcp_proxy_benchmark.py --projects frp,frp-rs --concurrency 200 --requests-per-conn 200 --output bench/results/tcp-c200-r200
```

Run an optimized matrix, 100 to 500 concurrency in 50-connection steps, 3 runs per level:

```powershell
python bench\tcp_proxy_benchmark.py --projects frp,frp-rs --concurrency-levels 100-500 --runs 3 --requests-per-conn 200 --connect-ramp-ms 1000 --output bench/results/optimized-matrix-c100-c500-r3-ramp
```

The benchmark config uses a larger work-connection pool, warning-level logs, and a 1 second connection ramp for matrix runs so the result is closer to steady-state proxy throughput than a pure simultaneous-connect burst test.

If the official frp release zip is blocked by Windows Security, download or build frp elsewhere and pass the directory containing `frps.exe` and `frpc.exe`:

```powershell
python bench\tcp_proxy_benchmark.py --frp-bin-dir C:\path\to\frp --projects frp,frp-rs --concurrency 200 --requests-per-conn 200
```

## Limits

This is a localhost TCP echo benchmark. It is useful for proxy overhead, resource usage, concurrency, and short-run stability checks. It does not measure WAN behavior, NAT traversal, UDP, QUIC, KCP, TLS handshake cost, dashboard overhead, or long-running production stability.
