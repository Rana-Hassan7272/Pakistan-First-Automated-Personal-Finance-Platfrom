#!/usr/bin/env python3
"""
Headless Locust against POST /api/v1/advisor/chat.

Requires API running: uvicorn backend.api.main:app --port 8000

Usage:
  python evaluation/run_locust_advisor.py --host http://127.0.0.1:8000 -u 5 -t 60
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_LOCUSTFILE = _REPO / "backend" / "rag" / "evaluation" / "locustfile.py"


def _parse_locust_stats(log_text: str) -> dict:
    out: dict = {}
    for line in log_text.splitlines():
        m = re.search(r"(\d+)\s+POST.*advisor.*\|\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)", line)
        if m:
            out["requests"] = int(m.group(1))
            out["p50_ms"] = float(m.group(4))
            out["p95_ms"] = float(m.group(5))
            out["p99_ms"] = float(m.group(6))
    agg = re.search(
        r"Aggregated\s+\|\s+(\d+).*?\|\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)",
        log_text,
    )
    if agg:
        out["aggregated_requests"] = int(agg.group(1))
        out["aggregated_p50_ms"] = float(agg.group(4))
        out["aggregated_p95_ms"] = float(agg.group(5))
        out["aggregated_p99_ms"] = float(agg.group(6))
    fail = re.search(r"(\d+)\s+failures?", log_text, re.I)
    if fail:
        out["failures"] = int(fail.group(1))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="http://127.0.0.1:8000")
    ap.add_argument("-u", type=int, default=5, help="Concurrent users")
    ap.add_argument("-r", type=float, default=1.0, help="Spawn rate")
    ap.add_argument("-t", type=str, default="60s", help="Duration e.g. 60s")
    ap.add_argument(
        "-o",
        type=Path,
        default=_REPO / "evidence" / "locust_advisor.txt",
    )
    args = ap.parse_args()

    cmd = [
        sys.executable,
        "-m",
        "locust",
        "-f",
        str(_LOCUSTFILE),
        f"--host={args.host.rstrip('/')}",
        "--headless",
        "-u",
        str(args.u),
        "-r",
        str(args.r),
        "-t",
        args.t,
        "--only-summary",
    ]
    print("Running:", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    log = (proc.stdout or "") + "\n" + (proc.stderr or "")
    parsed = _parse_locust_stats(log)
    ts = datetime.now(timezone.utc).isoformat()

    args.o.parent.mkdir(parents=True, exist_ok=True)
    body = [
        f"FinGuard Locust — POST /api/v1/advisor/chat",
        f"Recorded: {ts}",
        f"host={args.host} users={args.u} spawn_rate={args.r} duration={args.t}",
        f"exit_code={proc.returncode}",
        "",
        "parsed (ms, from Locust summary table):",
        json.dumps(parsed, indent=2),
        "",
        "---- raw locust output ----",
        log,
    ]
    args.o.write_text("\n".join(body), encoding="utf-8")
    print(f"Wrote {args.o}")
    if proc.returncode != 0:
        print("Locust exited non-zero (API down, auth, or rate limit?). See log file.")
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
