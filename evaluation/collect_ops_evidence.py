#!/usr/bin/env python3
"""Fetch /health and /metrics into evidence/ (API must be running)."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

_REPO = Path(__file__).resolve().parents[1]


def _get(url: str, timeout: float = 15.0) -> tuple[int, str]:
    req = Request(url, headers={"Accept": "*/*"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="http://127.0.0.1:8000")
    ap.add_argument("--evidence-dir", type=Path, default=_REPO / "evidence")
    args = ap.parse_args()
    host = args.host.rstrip("/")
    ts = datetime.now(timezone.utc).isoformat()
    out_dir = args.evidence_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    health_path = out_dir / "health.json"
    metrics_path = out_dir / "prometheus.txt"

    try:
        code, body = _get(f"{host}/health")
        health_path.write_text(
            f"# fetched {ts}\n# HTTP {code}\n{body}\n", encoding="utf-8"
        )
        print(f"Wrote {health_path}")
    except URLError as exc:
        print(f"health failed: {exc}")
        return 1

    try:
        code, body = _get(f"{host}/metrics")
        header = f"# fetched {ts}\n# HTTP {code}\n# host {host}\n\n"
        metrics_path.write_text(header + body, encoding="utf-8")
        print(f"Wrote {metrics_path} ({len(body)} bytes)")
    except URLError as exc:
        print(f"metrics failed: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
