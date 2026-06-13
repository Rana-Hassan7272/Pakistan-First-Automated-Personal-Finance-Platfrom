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
    "owner": "finguard-lstm",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _run_lstm_cli(subcommand: str) -> None:
    root = _repo_root()
    backend = root / "backend"
    env = dict(os.environ)
    sep = os.pathsep
    bp = str(backend)
    env["PYTHONPATH"] = f"{bp}{sep}{env['PYTHONPATH']}" if env.get("PYTHONPATH") else bp
    script = backend / "ml" / "lstm" / "run_pipeline.py"
    cmd = [sys.executable, str(script), subcommand]
    logger.info("lstm_cli %s", cmd)
    subprocess.run(cmd, cwd=str(root), env=env, check=True)


def _monthly_backtest() -> None:
    _run_lstm_cli("backtest")


with DAG(
    dag_id="lstm_monthly_backtest_dag",
    default_args=_DEFAULT_ARGS,
    description="Monthly LSTM backtest vs stored Sunday predictions; MLflow MAE/RMSE (Phase 11: cron 2am)",
    schedule_interval="0 2 1 * *",  # Phase 11: Changed from 0 8 1 * * to 0 2 1 * *
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["lstm", "backtest", "ml", "phase11"],
) as dag_monthly:
    PythonOperator(
        task_id="run_lstm_monthly_backtest",
        python_callable=_monthly_backtest,
    )
