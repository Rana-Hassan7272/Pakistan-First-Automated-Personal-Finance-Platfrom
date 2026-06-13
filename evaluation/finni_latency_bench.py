#!/usr/bin/env python3
"""Finni / RAG latency benchmark — p50/p95/p99 per routing path."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

SCENARIOS: dict[str, str] = {
    "direct": "hello",
    "fast_balance": "what is my account balance",
    "fast_spending": "how much did I spend this month",
    "rag_kb": "how is zakat calculated on gold in Pakistan",
    "full_agent": "analyze my food spending trends and suggest cuts",
}


def _percentile(sorted_ms: list[float], p: float) -> float:
    if not sorted_ms:
        return 0.0
    k = (len(sorted_ms) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_ms) - 1)
    if f == c:
        return sorted_ms[f]
    return sorted_ms[f] + (sorted_ms[c] - sorted_ms[f]) * (k - f)


def _summarize(latencies: list[float]) -> dict[str, float]:
    latencies = sorted(latencies)
    return {
        "p50": round(_percentile(latencies, 50), 2),
        "p95": round(_percentile(latencies, 95), 2),
        "p99": round(_percentile(latencies, 99), 2),
        "mean": round(statistics.mean(latencies), 2),
        "min": round(min(latencies), 2),
        "max": round(max(latencies), 2),
    }


def _post_chat(host: str, user_id: str, message: str, timeout: float) -> tuple[int, float, str]:
    url = f"{host.rstrip('/')}/api/v1/advisor/chat"
    body = json.dumps({"user_id": user_id, "message": message}).encode("utf-8")
    req = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "Accept": "text/event-stream"},
        method="POST",
    )
    t0 = time.perf_counter()
    path = ""
    try:
        with urlopen(req, timeout=timeout) as resp:
            _ = resp.read()
            code = resp.status
            path = resp.headers.get("X-Path", "")
    except HTTPError as exc:
        code = exc.code
        _ = exc.read(4096)
    dt = (time.perf_counter() - t0) * 1000.0
    return code, dt, path


def _bench_api(
    host: str,
    user_id: str,
    message: str,
    n: int,
    warmup: int,
    timeout: float,
    pause: float,
) -> dict:
    latencies: list[float] = []
    paths: list[str] = []
    errors: list[str] = []
    total = warmup + n
    for i in range(total):
        try:
            code, ms, path = _post_chat(host, user_id, message, timeout)
            if 200 <= code < 300:
                if i >= warmup:
                    latencies.append(ms)
                    paths.append(path)
            else:
                errors.append(f"HTTP {code} {ms:.0f}ms")
        except URLError as exc:
            errors.append(str(exc))
        if pause > 0 and i + 1 < total:
            time.sleep(pause)
    out: dict = {"message": message, "n_ok": len(latencies), "n_requested": n, "warmup": warmup}
    if latencies:
        out["latency_ms"] = _summarize(latencies)
        out["x_path"] = paths[-1] if paths else ""
        out["x_path_samples"] = list(dict.fromkeys(paths))
    if errors:
        out["errors"] = errors[:8]
    return out


def _bench_local_cp1(message: str, n: int, warmup: int) -> dict:
    from backend.rag.self_rag import decide_kb_retrieval

    latencies: list[float] = []
    for i in range(warmup + n):
        t0 = time.perf_counter()
        decide_kb_retrieval(message)
        ms = (time.perf_counter() - t0) * 1000.0
        if i >= warmup:
            latencies.append(ms)
    return {"message": message, "n_ok": len(latencies), "latency_ms": _summarize(latencies), "handler": "cp1_heuristic"}


def _bench_local_rag(message: str, user_id: str | None, n: int, warmup: int) -> dict:
    from backend.rag.pipeline import run_rag_query

    latencies: list[float] = []
    errors: list[str] = []
    for i in range(warmup + n):
        t0 = time.perf_counter()
        try:
            run_rag_query(message, user_id=user_id)
        except Exception as exc:
            errors.append(str(exc)[:120])
            continue
        ms = (time.perf_counter() - t0) * 1000.0
        if i >= warmup:
            latencies.append(ms)
    out: dict = {"message": message, "n_ok": len(latencies), "handler": "run_rag_query"}
    if latencies:
        out["latency_ms"] = _summarize(latencies)
    if errors:
        out["errors"] = errors[:5]
    return out


def _bench_local_fast(user_id: str, message: str, n: int, warmup: int) -> dict:
    from backend.agents.advisor_fast import try_fast_agent_response

    latencies: list[float] = []
    for i in range(warmup + n):
        t0 = time.perf_counter()
        try_fast_agent_response(user_id, message)
        ms = (time.perf_counter() - t0) * 1000.0
        if i >= warmup:
            latencies.append(ms)
    return {"message": message, "n_ok": len(latencies), "handler": "advisor_fast", "latency_ms": _summarize(latencies)}


def _format_table(results: dict[str, dict]) -> list[str]:
    lines = [
        "FinGuard Finni latency benchmark",
        f"Recorded: {results['timestamp']}",
        f"mode={results['mode']}  host={results.get('host', 'local')}  user_id={results.get('user_id', '')}",
        "",
        f"{'scenario':<16} {'n':>4} {'p50':>10} {'p95':>10} {'p99':>10} {'mean':>10}  path/handler",
        "-" * 86,
    ]
    for name, row in results["scenarios"].items():
        lat = row.get("latency_ms") or {}
        handler = row.get("x_path") or row.get("handler") or ""
        if not lat:
            lines.append(f"{name:<16} {row.get('n_ok', 0):>4}   (no successful samples)")
            continue
        lines.append(
            f"{name:<16} {row.get('n_ok', 0):>4} "
            f"{lat.get('p50', 0):>9.0f}ms {lat.get('p95', 0):>9.0f}ms {lat.get('p99', 0):>9.0f}ms "
            f"{lat.get('mean', 0):>9.0f}ms  {handler}"
        )
    return lines


def main() -> int:
    ap = argparse.ArgumentParser(description="Finni / RAG latency p50/p95/p99 benchmark")
    ap.add_argument("--host", default="http://127.0.0.1:8000")
    ap.add_argument("--user-id", default="load-test-user")
    ap.add_argument("-n", type=int, default=10, help="Measured iterations per scenario")
    ap.add_argument("--warmup", type=int, default=1, help="Warmup iterations (excluded from stats)")
    ap.add_argument("--timeout", type=float, default=120.0)
    ap.add_argument("--pause", type=float, default=0.5, help="Pause between API requests (seconds)")
    ap.add_argument(
        "--scenario",
        choices=[*SCENARIOS.keys(), "all"],
        default="all",
        help="Which scenario to run (default: all)",
    )
    ap.add_argument(
        "--mode",
        choices=["api", "local"],
        default="api",
        help="api=live SSE endpoint; local=in-process CP1/RAG/advisor_fast",
    )
    ap.add_argument(
        "-o",
        type=Path,
        default=_REPO / "evidence" / "finni_latency_bench.txt",
    )
    args = ap.parse_args()

    names = list(SCENARIOS.keys()) if args.scenario == "all" else [args.scenario]
    scenario_results: dict[str, dict] = {}

    if args.mode == "api":
        for name in names:
            msg = SCENARIOS[name]
            scenario_results[name] = _bench_api(
                args.host, args.user_id, msg, args.n, args.warmup, args.timeout, args.pause
            )
    else:
        for name in names:
            msg = SCENARIOS[name]
            if name == "direct":
                scenario_results[name] = _bench_local_cp1(msg, args.n, args.warmup)
            elif name in ("fast_balance", "fast_spending"):
                scenario_results[name] = _bench_local_fast(args.user_id, msg, args.n, args.warmup)
            elif name == "rag_kb":
                scenario_results[name] = _bench_local_rag(msg, None, args.n, args.warmup)
            else:
                scenario_results[name] = {
                    "message": msg,
                    "n_ok": 0,
                    "skipped": "full_agent requires --mode api",
                }

    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "mode": args.mode,
        "host": args.host if args.mode == "api" else None,
        "user_id": args.user_id,
        "n_per_scenario": args.n,
        "warmup": args.warmup,
        "scenarios": scenario_results,
    }

    lines = _format_table(payload)
    lines.extend(["", "json: " + json.dumps(payload, default=str)])
    args.o.parent.mkdir(parents=True, exist_ok=True)
    args.o.write_text("\n".join(lines) + "\n", encoding="utf-8")
    json_path = args.o.with_suffix(".json")
    json_path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"\nWrote {args.o} and {json_path}")

    ok = any((row.get("n_ok") or 0) > 0 for row in scenario_results.values())
    if not ok:
        print("\nNo successful samples. For API mode start uvicorn; for local RAG set Supabase + KB env.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
