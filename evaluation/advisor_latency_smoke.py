#!/usr/bin/env python3
"""Lightweight advisor chat latency (p50/p95/p99) without Locust. API must be running."""

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
_MSG = "What is zakat in Islamic finance?"


def _percentile(sorted_ms: list[float], p: float) -> float:
    if not sorted_ms:
        return 0.0
    k = (len(sorted_ms) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_ms) - 1)
    if f == c:
        return sorted_ms[f]
    return sorted_ms[f] + (sorted_ms[c] - sorted_ms[f]) * (k - f)


def _post_chat(host: str, user_id: str, timeout: float) -> tuple[int, float, str]:
    url = f"{host.rstrip('/')}/api/v1/advisor/chat"
    body = json.dumps({"user_id": user_id, "message": _MSG}).encode("utf-8")
    req = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "Accept": "text/event-stream"},
        method="POST",
    )
    t0 = time.perf_counter()
    try:
        with urlopen(req, timeout=timeout) as resp:
            _ = resp.read(65536)
            code = resp.status
    except HTTPError as exc:
        code = exc.code
        _ = exc.read(4096)
    dt = (time.perf_counter() - t0) * 1000.0
    return code, dt, ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="http://127.0.0.1:8000")
    ap.add_argument("-n", type=int, default=10)
    ap.add_argument("--timeout", type=float, default=120.0)
    ap.add_argument("--user-id", default="load-test-user")
    ap.add_argument("-o", type=Path, default=_REPO / "evidence" / "advisor_latency_smoke.txt")
    args = ap.parse_args()

    latencies: list[float] = []
    codes: list[int] = []
    errors: list[str] = []

    for i in range(args.n):
        try:
            code, ms, _ = _post_chat(args.host, args.user_id, args.timeout)
            codes.append(code)
            if 200 <= code < 300:
                latencies.append(ms)
            else:
                errors.append(f"req{i+1} HTTP {code} {ms:.0f}ms")
        except URLError as exc:
            errors.append(f"req{i+1} {exc}")
        time.sleep(1.0)

    if not latencies:
        args.o.write_text(
            f"No successful requests. errors:\n" + "\n".join(errors) + "\n",
            encoding="utf-8",
        )
        print("All requests failed. Is uvicorn running?")
        for e in errors[:5]:
            print(e)
        return 1

    latencies.sort()
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "host": args.host,
        "n_ok": len(latencies),
        "n_total": args.n,
        "http_codes": codes,
        "latency_ms": {
            "p50": round(_percentile(latencies, 50), 2),
            "p95": round(_percentile(latencies, 95), 2),
            "p99": round(_percentile(latencies, 99), 2),
            "mean": round(statistics.mean(latencies), 2),
            "min": round(min(latencies), 2),
            "max": round(max(latencies), 2),
        },
    }
    lines = [
        "FinGuard advisor POST /api/v1/advisor/chat latency smoke",
        f"Recorded: {summary['timestamp']}",
        f"host={args.host}  n_ok={summary['n_ok']}/{summary['n_total']}",
        "",
        f"p50: {summary['latency_ms']['p50']} ms",
        f"p95: {summary['latency_ms']['p95']} ms",
        f"p99: {summary['latency_ms']['p99']} ms",
        f"mean: {summary['latency_ms']['mean']} ms",
        "",
        "json: " + json.dumps(summary),
    ]
    if errors:
        lines.extend(["", "errors:", *errors])
    args.o.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
