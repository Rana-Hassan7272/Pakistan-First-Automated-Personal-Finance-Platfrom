#!/usr/bin/env python3
"""BERT layer-7 inference latency bench (p50/p95/p99). Writes to evidence/."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

_SAMPLES = [
    (
        "JazzCash",
        3200.0,
        "PKR 3,200 sent to KFC Lahore via JazzCash. Avl Bal PKR 12,000",
    ),
    (
        "UBP",
        15000.0,
        "Your A/c 1234 debited PKR 15,000.00 on POS PURCHASE - Imtiaz Super Market",
    ),
    (
        "EasyPaisa",
        500.0,
        "Trx ID: ABC123 You sent PKR 500 to Muhammad Hassan via EasyPaisa",
    ),
    (
        "ATM",
        10000.0,
        "Cash withdrawal of PKR 10,000 from HBL ATM Gulberg Lahore",
    ),
    (
        "Salary",
        85000.0,
        "Salary credited PKR 85,000 to your account. Ref: SAL-JAN-2026",
    ),
]


def _percentile(sorted_ms: list[float], p: float) -> float:
    if not sorted_ms:
        return 0.0
    k = (len(sorted_ms) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_ms) - 1)
    if f == c:
        return sorted_ms[f]
    return sorted_ms[f] + (sorted_ms[c] - sorted_ms[f]) * (k - f)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=100, help="Total inferences (cycles samples)")
    ap.add_argument(
        "-o",
        type=Path,
        default=_REPO / "evidence" / "bert_inference_latency.txt",
    )
    args = ap.parse_args()

    from backend.etl.merchant import bert_categorizer

    if not bert_categorizer.is_available():
        print("BERT checkpoint not found. Set BERT_CHECKPOINT_DIR or train/export model.")
        return 1

    def _sub(cat: str) -> str:
        return cat

    latencies: list[float] = []
    for i in range(args.n):
        m, amt, raw = _SAMPLES[i % len(_SAMPLES)]
        bert_categorizer.resolve(m, amt, raw, _sub)
        ms = bert_categorizer.get_last_bert_latency_ms()
        if ms is not None:
            latencies.append(float(ms))

    if len(latencies) < 5:
        print("Too few successful inferences.")
        return 1

    latencies.sort()
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "n": len(latencies),
        "checkpoint": (os.getenv("BERT_CHECKPOINT_DIR") or "backend/ml/bert/artifacts/bert_txn_production"),
        "latency_ms": {
            "min": round(min(latencies), 2),
            "mean": round(statistics.mean(latencies), 2),
            "p50": round(_percentile(latencies, 50), 2),
            "p95": round(_percentile(latencies, 95), 2),
            "p99": round(_percentile(latencies, 99), 2),
            "max": round(max(latencies), 2),
        },
    }

    args.o.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "FinGuard BERT inference latency (layer 7 forward pass)",
        f"Recorded: {summary['timestamp']}",
        f"n={summary['n']}  checkpoint={summary['checkpoint']}",
        "",
        f"p50: {summary['latency_ms']['p50']} ms",
        f"p95: {summary['latency_ms']['p95']} ms",
        f"p99: {summary['latency_ms']['p99']} ms",
        f"mean: {summary['latency_ms']['mean']} ms",
        f"min: {summary['latency_ms']['min']} ms  max: {summary['latency_ms']['max']} ms",
        "",
        "json: " + json.dumps(summary),
    ]
    args.o.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
