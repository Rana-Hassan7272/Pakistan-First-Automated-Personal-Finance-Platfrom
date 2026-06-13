from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "finguard-ml-ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _run_production_metrics() -> None:
    root = _repo_root()
    env = dict(os.environ)
    sep = os.pathsep
    rp = str(root)
    env["PYTHONPATH"] = f"{rp}{sep}{env['PYTHONPATH']}" if env.get("PYTHONPATH") else rp
    script = root / "backend" / "ml" / "production_mlflow_weekly.py"
    cmd = [sys.executable, str(script)]
    logger.info("production_mlflow_weekly %s", cmd)
    subprocess.run(cmd, cwd=str(root), env=env, check=True)


with DAG(
    dag_id="production_metrics_weekly_dag",
    default_args=_DEFAULT_ARGS,
    description="Weekly Supabase snapshot: BERT correction rates + fraud FP rate + agent metrics into MLflow",
    schedule_interval="0 5 * * 1",  # Every Monday at 05:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["mlflow", "production", "agents", "weekly"],
) as dag_metrics:
    PythonOperator(
        task_id="log_production_metrics_mlflow_weekly",
        python_callable=_run_production_metrics,
    )
