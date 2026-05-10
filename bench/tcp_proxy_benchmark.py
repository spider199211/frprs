import argparse
import asyncio
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import textwrap
import time
import urllib.request
import zipfile
from pathlib import Path


FRP_VERSION = "0.68.1"
FRP_DOWNLOAD = (
    "https://github.com/fatedier/frp/releases/download/"
    f"v{FRP_VERSION}/frp_{FRP_VERSION}_windows_amd64.zip"
)
FRP_ZIP_NAME = f"frp_{FRP_VERSION}_windows_amd64.zip"


def find_free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


async def echo_server(host, port, stop_event):
    async def handle(reader, writer):
        try:
            while True:
                data = await reader.readexactly(PAYLOAD_SIZE)
                writer.write(data)
                await writer.drain()
        except asyncio.IncompleteReadError:
            pass
        except ConnectionResetError:
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    server = await asyncio.start_server(handle, host, port, backlog=4096)
    async with server:
        while not stop_event.is_set():
            await asyncio.sleep(0.1)
    server.close()
    await server.wait_closed()


PAYLOAD_SIZE = 256
OPTIMIZED_POOL_COUNT = 256


async def run_load(host, port, concurrency, requests_per_conn, connect_ramp_ms=0):
    payload = b"x" * PAYLOAD_SIZE
    latencies = []
    errors = 0
    connections = []
    connect_start = time.perf_counter()

    async def connect_worker(index):
        nonlocal errors
        if connect_ramp_ms > 0 and concurrency > 1:
            await asyncio.sleep((connect_ramp_ms / 1000) * index / (concurrency - 1))
        try:
            reader, writer = await asyncio.open_connection(host, port)
        except Exception:
            errors += requests_per_conn
            return
        connections.append((reader, writer))

    await asyncio.gather(*(connect_worker(idx) for idx in range(concurrency)))
    connect_elapsed = time.perf_counter() - connect_start
    start = time.perf_counter()

    async def request_worker(reader, writer):
        nonlocal errors
        completed = 0
        try:
            for _ in range(requests_per_conn):
                t0 = time.perf_counter()
                writer.write(payload)
                await writer.drain()
                data = await reader.readexactly(PAYLOAD_SIZE)
                completed += 1
                if data != payload:
                    errors += 1
                    continue
                latencies.append((time.perf_counter() - t0) * 1000)
        except Exception:
            errors += requests_per_conn - completed
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    await asyncio.gather(
        *(request_worker(reader, writer) for reader, writer in connections)
    )
    elapsed = time.perf_counter() - start
    summary = summarize_load(latencies, errors, elapsed, concurrency, requests_per_conn)
    summary["connected"] = len(connections)
    summary["connect_seconds"] = connect_elapsed
    return summary


def percentile(values, pct):
    if not values:
        return None
    values = sorted(values)
    idx = int(round((pct / 100) * (len(values) - 1)))
    return values[idx]


def summarize_load(latencies, errors, elapsed, concurrency, requests_per_conn):
    total = concurrency * requests_per_conn
    ok = len(latencies)
    return {
        "concurrency": concurrency,
        "requests_per_conn": requests_per_conn,
        "total_requests": total,
        "ok_requests": ok,
        "errors": errors,
        "duration_seconds": elapsed,
        "requests_per_second": ok / elapsed if elapsed else 0,
        "throughput_mib_per_second": (ok * PAYLOAD_SIZE * 2) / elapsed / 1024 / 1024
        if elapsed
        else 0,
        "latency_ms": {
            "min": min(latencies) if latencies else None,
            "avg": statistics.fmean(latencies) if latencies else None,
            "p50": percentile(latencies, 50),
            "p95": percentile(latencies, 95),
            "p99": percentile(latencies, 99),
            "max": max(latencies) if latencies else None,
        },
    }


def mean(values):
    values = [value for value in values if value is not None]
    return statistics.fmean(values) if values else None


def powershell_json(script):
    out = subprocess.check_output(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script],
        text=True,
        stderr=subprocess.DEVNULL,
    ).strip()
    if not out:
        return []
    return json.loads(out)


def sample_processes(pids):
    ids = ",".join(str(pid) for pid in pids)
    script = (
        f"Get-Process -Id {ids} -ErrorAction SilentlyContinue | "
        "Select-Object Id,ProcessName,CPU,WorkingSet64,PrivateMemorySize64 | "
        "ConvertTo-Json -Compress"
    )
    try:
        value = powershell_json(script)
    except Exception:
        return []
    return value if isinstance(value, list) else [value]


async def monitor_processes(pids, stop_event, interval=0.5):
    samples = []
    while not stop_event.is_set():
        samples.append({"time": time.time(), "processes": sample_processes(pids)})
        await asyncio.sleep(interval)
    samples.append({"time": time.time(), "processes": sample_processes(pids)})
    return samples


def summarize_process_samples(samples):
    by_pid = {}
    for sample in samples:
        for proc in sample["processes"]:
            pid = proc["Id"]
            by_pid.setdefault(pid, {"name": proc["ProcessName"], "samples": []})
            by_pid[pid]["samples"].append(proc)

    total_cpu_seconds = 0.0
    peak_working_set = 0
    peak_private = 0
    per_process = []
    for pid, info in by_pid.items():
        points = info["samples"]
        cpu_values = [float(p.get("CPU") or 0) for p in points]
        working = [int(p.get("WorkingSet64") or 0) for p in points]
        private = [int(p.get("PrivateMemorySize64") or 0) for p in points]
        cpu_delta = max(cpu_values) - min(cpu_values) if cpu_values else 0
        total_cpu_seconds += cpu_delta
        peak_working_set = max(peak_working_set, max(working or [0]))
        peak_private = max(peak_private, max(private or [0]))
        per_process.append(
            {
                "pid": pid,
                "name": info["name"],
                "cpu_seconds_delta": cpu_delta,
                "peak_working_set_mib": max(working or [0]) / 1024 / 1024,
                "peak_private_mib": max(private or [0]) / 1024 / 1024,
            }
        )

    return {
        "total_cpu_seconds_delta": total_cpu_seconds,
        "peak_working_set_mib": peak_working_set / 1024 / 1024,
        "peak_private_mib": peak_private / 1024 / 1024,
        "processes": per_process,
    }


def wait_for_port(port, timeout=10):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError as err:
            last = err
            time.sleep(0.1)
    raise RuntimeError(f"port {port} did not become ready: {last}")


def terminate_process(proc):
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def write_text(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).strip() + "\n", encoding="utf-8")


def parse_levels(value):
    levels = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            start = int(start)
            end = int(end)
            levels.extend(range(start, end + 1, 50))
        else:
            levels.append(int(part))
    return sorted(dict.fromkeys(levels))


def is_zipfile(path):
    try:
        return zipfile.is_zipfile(path)
    except OSError:
        return False


def download_file(url, target):
    tmp = target.with_suffix(target.suffix + ".part")
    tmp.unlink(missing_ok=True)
    curl = shutil.which("curl.exe") or shutil.which("curl")
    if curl:
        subprocess.run(
            [
                curl,
                "-L",
                "--http1.1",
                "--fail",
                "--retry",
                "10",
                "--retry-all-errors",
                "--connect-timeout",
                "20",
                "-o",
                str(tmp),
                url,
            ],
            check=True,
        )
    else:
        urllib.request.urlretrieve(url, tmp)
    tmp.replace(target)


def ensure_frp(root):
    tools = root / "bench" / "tools"
    frp_dir = tools / f"frp_{FRP_VERSION}_windows_amd64"
    frps = frp_dir / "frps.exe"
    frpc = frp_dir / "frpc.exe"
    if frps.exists() and frpc.exists():
        return frps, frpc

    tools.mkdir(parents=True, exist_ok=True)
    zip_path = tools / FRP_ZIP_NAME
    if zip_path.exists() and not is_zipfile(zip_path):
        print(f"removing invalid archive {zip_path}", flush=True)
        zip_path.unlink(missing_ok=True)
    if not zip_path.exists():
        print(f"downloading {FRP_DOWNLOAD}", flush=True)
        download_file(FRP_DOWNLOAD, zip_path)
    if not is_zipfile(zip_path):
        zip_path.unlink(missing_ok=True)
        raise RuntimeError(f"downloaded archive is not a valid zip: {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(tools)

    if not frps.exists() or not frpc.exists():
        raise RuntimeError(f"frp binaries not found after extracting {zip_path}")
    return frps, frpc


def skipped_result(name, reason):
    return {
        "name": name,
        "skipped": True,
        "reason": reason,
    }


async def benchmark_case(
    name,
    frps_path,
    frpc_path,
    runtime_dir,
    concurrency,
    requests_per_conn,
    connect_ramp_ms=0,
):
    control_port = find_free_port()
    remote_port = find_free_port()
    echo_port = find_free_port()
    token = "bench-secret"

    if name == "frp":
        server_cfg = f"""
        bindAddr = "127.0.0.1"
        bindPort = {control_port}

        auth.token = "{token}"

        log.level = "warn"

        transport.maxPoolCount = {OPTIMIZED_POOL_COUNT}
        transport.tcpMux = false
        transport.tcpMuxKeepaliveInterval = 30
        transport.tls.force = false
        """
        client_cfg = f"""
        serverAddr = "127.0.0.1"
        serverPort = {control_port}

        auth.token = "{token}"

        log.level = "warn"

        transport.protocol = "tcp"
        transport.poolCount = {OPTIMIZED_POOL_COUNT}
        transport.tcpMux = false
        transport.tcpMuxKeepaliveInterval = 30
        transport.tls.enable = false

        [[proxies]]
        name = "tcp-echo"
        type = "tcp"
        localIP = "127.0.0.1"
        localPort = {echo_port}
        remotePort = {remote_port}
        """
    else:
        server_cfg = f"""
        bindAddr = "127.0.0.1"
        bindPort = {control_port}
        proxyBindAddr = "127.0.0.1"
        vhostHTTPPort = 0
        vhostHTTPSPort = 0
        dashboardAddr = "127.0.0.1"
        dashboardPort = 0

        [auth]
        token = "{token}"

        [transport]
        protocol = "tcp"
        """
        client_cfg = f"""
        serverAddr = "127.0.0.1"
        serverPort = {control_port}
        poolCount = {OPTIMIZED_POOL_COUNT}

        [auth]
        token = "{token}"

        [transport]
        protocol = "tcp"

        [[proxies]]
        name = "tcp-echo"
        type = "tcp"
        localIP = "127.0.0.1"
        localPort = {echo_port}
        remotePort = {remote_port}
        """

    case_dir = runtime_dir / name
    server_cfg_path = case_dir / "frps.toml"
    client_cfg_path = case_dir / "frpc.toml"
    write_text(server_cfg_path, server_cfg)
    write_text(client_cfg_path, client_cfg)

    logs = case_dir / "logs"
    logs.mkdir(parents=True, exist_ok=True)

    stop_echo = asyncio.Event()
    echo_task = asyncio.create_task(echo_server("127.0.0.1", echo_port, stop_echo))

    frps_log = open(logs / "frps.log", "w", encoding="utf-8")
    frpc_log = open(logs / "frpc.log", "w", encoding="utf-8")
    frps = subprocess.Popen(
        [str(frps_path), "-c", str(server_cfg_path)],
        stdout=frps_log,
        stderr=subprocess.STDOUT,
        cwd=str(case_dir),
        env={**os.environ, "RUST_LOG": "warn"} if name == "frp-rs" else None,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
    )
    await asyncio.sleep(0.8)
    frpc = subprocess.Popen(
        [str(frpc_path), "-c", str(client_cfg_path)],
        stdout=frpc_log,
        stderr=subprocess.STDOUT,
        cwd=str(case_dir),
        env={**os.environ, "RUST_LOG": "warn"} if name == "frp-rs" else None,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
    )

    try:
        wait_for_port(remote_port, timeout=15)
        await asyncio.sleep(0.5)
        stop_monitor = asyncio.Event()
        monitor_task = asyncio.create_task(
            monitor_processes([frps.pid, frpc.pid], stop_monitor)
        )
        load = await run_load(
            "127.0.0.1",
            remote_port,
            concurrency,
            requests_per_conn,
            connect_ramp_ms,
        )
        stop_monitor.set()
        samples = await monitor_task
        proc = summarize_process_samples(samples)
        load["cpu_seconds_per_1k_requests"] = (
            proc["total_cpu_seconds_delta"] / load["ok_requests"] * 1000
            if load["ok_requests"]
            else None
        )
        return {
            "name": name,
            "frps_pid": frps.pid,
            "frpc_pid": frpc.pid,
            "ports": {
                "control": control_port,
                "remote": remote_port,
                "echo": echo_port,
            },
            "load": load,
            "processes": proc,
            "logs": {
                "frps": str(logs / "frps.log"),
                "frpc": str(logs / "frpc.log"),
            },
        }
    finally:
        terminate_process(frpc)
        terminate_process(frps)
        stop_echo.set()
        await echo_task
        frps_log.close()
        frpc_log.close()


def render_markdown(results, output_path):
    lines = [
        "# frp vs frp-rs TCP Proxy Benchmark",
        "",
        f"- Time: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Payload: {PAYLOAD_SIZE} bytes echo request / response",
        "- Transport: TCP control/work connection",
        "",
        "| Project | Concurrency | Connected | Requests | Errors | RPS | MiB/s | Avg ms | P95 ms | P99 ms | CPU sec | CPU sec / 1k req | Peak WS MiB | Peak Private MiB |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for result in results:
        if result.get("skipped"):
            lines.append(
                f"| {result['name']} (skipped: {result['reason']}) |  |  |  |  |  |  |  |  |  |  |  |  |  |"
            )
            continue
        load = result["load"]
        lat = load["latency_ms"]
        proc = result["processes"]
        lines.append(
            "| {name} | {conc} | {connected} | {ok}/{total} | {err} | {rps:.1f} | {mib:.2f} | "
            "{avg:.3f} | {p95:.3f} | {p99:.3f} | {cpu:.3f} | {cpu1k:.4f} | "
            "{ws:.2f} | {priv:.2f} |".format(
                name=result["name"],
                conc=load["concurrency"],
                connected=load.get("connected", load["concurrency"]),
                ok=load["ok_requests"],
                total=load["total_requests"],
                err=load["errors"],
                rps=load["requests_per_second"],
                mib=load["throughput_mib_per_second"],
                avg=lat["avg"] or 0,
                p95=lat["p95"] or 0,
                p99=lat["p99"] or 0,
                cpu=proc["total_cpu_seconds_delta"],
                cpu1k=load["cpu_seconds_per_1k_requests"] or 0,
                ws=proc["peak_working_set_mib"],
                priv=proc["peak_private_mib"],
            )
        )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This benchmark runs on localhost, so it mainly compares proxy overhead rather than real network behavior.",
            "- CPU is sampled from Windows process CPU time deltas for `frps` + `frpc`.",
            "- Memory is the peak working set/private memory observed during the test.",
            "- Stability is represented by completed requests and error count for this run.",
        ]
    )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def summarize_runs(results):
    groups = {}
    for result in results:
        if result.get("skipped"):
            continue
        load = result["load"]
        key = (result["name"], load["concurrency"])
        groups.setdefault(key, []).append(result)

    rows = []
    for (name, concurrency), items in sorted(groups.items(), key=lambda item: (item[0][1], item[0][0])):
        loads = [item["load"] for item in items]
        lats = [load["latency_ms"] for load in loads]
        procs = [item["processes"] for item in items]
        rows.append(
            {
                "name": name,
                "concurrency": concurrency,
                "runs": len(items),
                "connected_avg": mean([load.get("connected") for load in loads]),
                "connect_seconds_avg": mean([load.get("connect_seconds") for load in loads]),
                "total_requests_avg": mean([load["total_requests"] for load in loads]),
                "ok_requests_avg": mean([load["ok_requests"] for load in loads]),
                "errors_avg": mean([load["errors"] for load in loads]),
                "requests_per_second_avg": mean(
                    [load["requests_per_second"] for load in loads]
                ),
                "throughput_mib_per_second_avg": mean(
                    [load["throughput_mib_per_second"] for load in loads]
                ),
                "latency_ms_avg": mean([lat["avg"] for lat in lats]),
                "latency_ms_p95_avg": mean([lat["p95"] for lat in lats]),
                "latency_ms_p99_avg": mean([lat["p99"] for lat in lats]),
                "cpu_seconds_avg": mean(
                    [proc["total_cpu_seconds_delta"] for proc in procs]
                ),
                "cpu_seconds_per_1k_requests_avg": mean(
                    [load["cpu_seconds_per_1k_requests"] for load in loads]
                ),
                "peak_working_set_mib_avg": mean(
                    [proc["peak_working_set_mib"] for proc in procs]
                ),
                "peak_private_mib_avg": mean(
                    [proc["peak_private_mib"] for proc in procs]
                ),
            }
        )
    return rows


def render_matrix_markdown(results, output_path, levels, runs, connect_ramp_ms):
    rows = summarize_runs(results)
    lines = [
        "# frp vs frp-rs TCP Proxy Benchmark Matrix",
        "",
        f"- Time: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Payload: {PAYLOAD_SIZE} bytes echo request / response",
        "- Transport: TCP control/work connection",
        f"- Optimized pool count: {OPTIMIZED_POOL_COUNT}",
        f"- Concurrency levels: {', '.join(str(level) for level in levels)}",
        f"- Runs per project/level: {runs}",
        f"- Connection ramp: {connect_ramp_ms} ms",
        "",
        "| Project | Concurrency | Runs | Avg Connected | Avg Requests | Avg Errors | Avg RPS | Avg MiB/s | Avg ms | Avg P95 ms | Avg P99 ms | Avg CPU sec | Avg CPU sec / 1k req | Avg Peak WS MiB | Avg Peak Private MiB |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            "| {name} | {conc} | {runs} | {connected:.1f} | {ok:.0f}/{total:.0f} | {err:.1f} | "
            "{rps:.1f} | {mib:.2f} | {avg:.3f} | {p95:.3f} | {p99:.3f} | "
            "{cpu:.3f} | {cpu1k:.4f} | {ws:.2f} | {priv:.2f} |".format(
                name=row["name"],
                conc=row["concurrency"],
                runs=row["runs"],
                connected=row["connected_avg"] or 0,
                ok=row["ok_requests_avg"] or 0,
                total=row["total_requests_avg"] or 0,
                err=row["errors_avg"] or 0,
                rps=row["requests_per_second_avg"] or 0,
                mib=row["throughput_mib_per_second_avg"] or 0,
                avg=row["latency_ms_avg"] or 0,
                p95=row["latency_ms_p95_avg"] or 0,
                p99=row["latency_ms_p99_avg"] or 0,
                cpu=row["cpu_seconds_avg"] or 0,
                cpu1k=row["cpu_seconds_per_1k_requests_avg"] or 0,
                ws=row["peak_working_set_mib_avg"] or 0,
                priv=row["peak_private_mib_avg"] or 0,
            )
        )

    lines.extend(
        [
            "",
            "## Config Optimizations",
            "",
            f"- frp: `transport.poolCount = {OPTIMIZED_POOL_COUNT}`, `transport.maxPoolCount = {OPTIMIZED_POOL_COUNT}`, `transport.tcpMux = false`, `transport.tls.enable = false`, log level `warn`.",
            f"- frp-rs: `poolCount = {OPTIMIZED_POOL_COUNT}`, `RUST_LOG=warn`.",
            "- Echo server backlog is set to 4096 to reduce local benchmark harness bottlenecks.",
            "",
            "## Notes",
            "",
            "- This benchmark runs on localhost, so it mainly compares proxy overhead rather than real network behavior.",
            "- CPU is sampled from Windows process CPU time deltas for `frps` + `frpc`.",
            "- Memory is the average of each run's peak working set/private memory.",
            "- Raw per-run data is available in `all_results.json`.",
        ]
    )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrency", type=int, default=200)
    parser.add_argument(
        "--concurrency-levels",
        help="Comma-separated levels or a 50-step range like 100-500. Enables matrix mode.",
    )
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--requests-per-conn", type=int, default=200)
    parser.add_argument(
        "--connect-ramp-ms",
        type=int,
        default=0,
        help="Spread connection establishment across this many milliseconds.",
    )
    parser.add_argument("--output", default="bench/results/latest")
    parser.add_argument(
        "--projects",
        default="frp,frp-rs",
        help="Comma-separated list: frp,frp-rs",
    )
    parser.add_argument(
        "--frp-bin-dir",
        help="Directory containing official frps.exe and frpc.exe.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    output_dir = (root / args.output).resolve()
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    selected = [item.strip() for item in args.projects.split(",") if item.strip()]

    results = []
    cases = []
    if "frp" in selected:
        try:
            if args.frp_bin_dir:
                frp_bin_dir = Path(args.frp_bin_dir).resolve()
                frp_frps = frp_bin_dir / "frps.exe"
                frp_frpc = frp_bin_dir / "frpc.exe"
                if not frp_frps.exists() or not frp_frpc.exists():
                    raise RuntimeError(f"frps.exe/frpc.exe not found in {frp_bin_dir}")
            else:
                frp_frps, frp_frpc = ensure_frp(root)
            cases.append(("frp", frp_frps, frp_frpc))
        except Exception as err:
            results.append(skipped_result("frp", str(err)))
    if "frp-rs" in selected:
        frprs_frps = root / "target" / "release" / "frps.exe"
        frprs_frpc = root / "target" / "release" / "frpc.exe"
        if not frprs_frps.exists() or not frprs_frpc.exists():
            raise RuntimeError("frp-rs release binaries not found; run cargo build --release --bins")
        cases.append(("frp-rs", frprs_frps, frprs_frpc))

    if args.concurrency_levels:
        levels = parse_levels(args.concurrency_levels)
        if not levels:
            raise RuntimeError("no concurrency levels parsed")
        all_results = results[:]
        for concurrency in levels:
            for run_idx in range(1, args.runs + 1):
                for name, frps, frpc in cases:
                    print(
                        f"benchmarking {name} concurrency={concurrency} run={run_idx}/{args.runs}...",
                        flush=True,
                    )
                    runtime_dir = (
                        output_dir / "runtime" / f"c{concurrency}" / f"run{run_idx}"
                    )
                    result = await benchmark_case(
                        name,
                        frps,
                        frpc,
                        runtime_dir,
                        concurrency,
                        args.requests_per_conn,
                        args.connect_ramp_ms,
                    )
                    result["run"] = run_idx
                    all_results.append(result)

        (output_dir / "all_results.json").write_text(
            json.dumps(all_results, indent=2), encoding="utf-8"
        )
        (output_dir / "summary.json").write_text(
            json.dumps(summarize_runs(all_results), indent=2), encoding="utf-8"
        )
        render_matrix_markdown(
            all_results,
            output_dir / "summary.md",
            levels,
            args.runs,
            args.connect_ramp_ms,
        )
        print(output_dir / "summary.md")
        return

    runtime_dir = output_dir / "runtime"
    for name, frps, frpc in cases:
        print(f"benchmarking {name}...", flush=True)
        results.append(
            await benchmark_case(
                name,
                frps,
                frpc,
                runtime_dir,
                args.concurrency,
                args.requests_per_conn,
                args.connect_ramp_ms,
            )
        )

    (output_dir / "results.json").write_text(
        json.dumps(results, indent=2), encoding="utf-8"
    )
    render_markdown(results, output_dir / "report.md")
    print(output_dir / "report.md")


if __name__ == "__main__":
    if os.name != "nt":
        print("This benchmark script currently targets Windows process sampling.", file=sys.stderr)
        sys.exit(1)
    asyncio.run(main())
