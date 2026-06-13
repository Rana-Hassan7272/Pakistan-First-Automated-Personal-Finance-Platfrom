"""
P9.8 + Phase 11 — Weekly RAG evaluation (configs A/B/C) with faithfulness alerting.

Requires Airflow worker image with repo root, ``.env``, KB ingested, and optional
``MLFLOW_TRACKING_URI`` / evaluator keys for full RAGAS metrics.

Phase 11 additions:
- Compare current faithfulness to previous week's MLflow run
- If drop > 5 points → Slack webhook alert

Manual parity::

    py -3 -m backend.rag.evaluation.run_ragas --config all --output-dir backend/rag/evaluation/out
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from dotenv import load_dotenv

load_dotenv(os.path.join(_repo_root, ".env"))

_DEFAULT_ARGS = {
    "owner": "finguard",
    "retries": 1,
    "email_on_failure": False,
}


def _get_last_week_faithfulness(config: str) -> float | None:
    """
    Phase 11: Fetch previous week's faithfulness score from MLflow.
    Returns None if no previous run found.
    """
    try:
        import mlflow
        from backend.rag.config import get_rag_eval_mlflow_experiment

        experiment_name = get_rag_eval_mlflow_experiment()
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if not experiment:
            return None

        # Search for runs from last 14 days for this config
        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            filter_string=f"tags.config = '{config}'",
            order_by=["start_time DESC"],
            max_results=1,
        )

        if runs.empty:
            return None

        last_faithfulness = runs.iloc[0].get("metrics.faithfulness_mean")
        return float(last_faithfulness) if last_faithfulness is not None else None

    except Exception as exc:
        print(f"Could not fetch last week's faithfulness: {exc}")
        return None


def _send_slack_alert(current: float, previous: float, config: str, drop: float) -> None:
    """Phase 11: Send Slack webhook alert for significant faithfulness drop."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("Warning: SLACK_WEBHOOK_URL not set, skipping alert")
        return

    try:
        import requests

        message = {
            "text": f":warning: *RAGAS Faithfulness Alert*",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "RAG Faithfulness Score Drop Detected",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Config:*\n{config}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Drop:*\n{drop:.2f} points",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Current:*\n{current:.3f}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Previous:*\n{previous:.3f}",
                        },
                    ],
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"Threshold: >5 point drop triggers this alert | DAG: `rag_ragas_weekly_dag`",
                        }
                    ],
                },
            ],
        }

        resp = requests.post(webhook_url, json=message, timeout=10)
        resp.raise_for_status()
        print(f"Slack alert sent for config {config} (drop: {drop:.2f})")

    except Exception as exc:
        print(f"Failed to send Slack alert: {exc}")


def _check_faithfulness_alert(config: str, summary: dict) -> None:
    """
    Phase 11: Check if faithfulness dropped significantly vs last week.
    If drop > 5 points, send Slack alert.
    """
    current_faithfulness = summary.get("faithfulness_mean")
    if current_faithfulness is None:
        print(f"No faithfulness score for config {config}, skipping alert check")
        return

    previous_faithfulness = _get_last_week_faithfulness(config)
    if previous_faithfulness is None:
        print(f"No previous faithfulness score for config {config}, skipping alert")
        return

    drop = previous_faithfulness - current_faithfulness
    THRESHOLD = 5.0  # Phase 11: Alert if faithfulness drops > 5 points

    print(f"Config {config}: faithfulness {previous_faithfulness:.3f} → {current_faithfulness:.3f} (drop: {drop:.2f})")

    if drop > THRESHOLD:
        print(f"ALERT: Faithfulness drop ({drop:.2f}) exceeds threshold ({THRESHOLD})")
        _send_slack_alert(current_faithfulness, previous_faithfulness, config, drop)
    else:
        print(f"No alert needed: drop ({drop:.2f}) within threshold ({THRESHOLD})")


def run_ragas_weekly(**_context):
    import subprocess
    import json

    out = os.path.join(_repo_root, "backend", "rag", "evaluation", "out")
    os.makedirs(out, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "backend.rag.evaluation.run_ragas",
        "--config",
        "all",
        "--output-dir",
        out,
    ]
    if os.getenv("RAGAS_INCLUDE_LLM", "").strip().lower() in ("1", "true", "yes"):
        cmd.append("--ragas")
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd, cwd=_repo_root)

    # Phase 11: Check faithfulness for each config and alert if drop > 5 points
    configs = ["A", "B", "C"]
    for cfg in configs:
        summary_path = os.path.join(out, f"summary_{cfg}.json")
        if os.path.exists(summary_path):
            try:
                with open(summary_path) as f:
                    summary = json.load(f)
                _check_faithfulness_alert(cfg, summary)
            except Exception as exc:
                print(f"Failed to check alert for config {cfg}: {exc}")

    return {"output_dir": out}


with DAG(
    dag_id="rag_ragas_weekly_dag",
    description="Phase 9.7 + 11: run RAG configs A/B/C weekly with faithfulness alerting (Sunday 03:00 UTC)",
    schedule_interval="0 3 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["rag", "phase9", "phase11", "evaluation", "alerting"],
) as dag:
    PythonOperator(
        task_id="run_ragas_all_configs",
        python_callable=run_ragas_weekly,
    )
