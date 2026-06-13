"""
Nightly maintenance for Phase 6 merchant intelligence.

Jobs:
- KG consolidation for near-duplicate merchant fingerprints
- Drift metric snapshot generation
- Stale feedback cleanup/retry policy execution
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "fingard-merchant-intelligence",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _consolidate_kg() -> dict:
    """
    Merge near-duplicate KG rows by (merchant_name_pattern, geo-cell-ish bucket).
    Keeps the row with highest confidence as canonical and folds counters.
    """
    from backend.api.core.supabase_client import get_supabase_admin_client

    db = get_supabase_admin_client()
    response = (
        db.table("merchant_knowledge_graph")
        .select("merchant_id, merchant_name_pattern, location_lat, location_lon, confidence, total_observations")
        .order("merchant_name_pattern")
        .execute()
    )
    rows = response.data or []

    buckets: dict[str, list[dict]] = {}
    for row in rows:
        name = (row.get("merchant_name_pattern") or "").strip().lower()
        lat = row.get("location_lat")
        lon = row.get("location_lon")
        geo = f"{round(lat, 2)}:{round(lon, 2)}" if lat is not None and lon is not None else "nogeo"
        key = f"{name}:{geo}"
        buckets.setdefault(key, []).append(row)

    merged = 0
    removed = 0
    for _key, items in buckets.items():
        if len(items) <= 1:
            continue

        items_sorted = sorted(
            items,
            key=lambda i: (
                float(i.get("confidence") or 0.0),
                int(i.get("total_observations") or 0),
            ),
            reverse=True,
        )
        canonical = items_sorted[0]
        duplicates = items_sorted[1:]

        obs_sum = sum(int(i.get("total_observations") or 0) for i in items_sorted)
        confidence_max = max(float(i.get("confidence") or 0.0) for i in items_sorted)

        db.table("merchant_knowledge_graph").update(
            {
                "total_observations": obs_sum,
                "confidence": confidence_max,
            }
        ).eq("merchant_id", canonical["merchant_id"]).execute()

        duplicate_ids = [d["merchant_id"] for d in duplicates if d.get("merchant_id")]
        if duplicate_ids:
            db.table("merchant_knowledge_graph").delete().in_("merchant_id", duplicate_ids).execute()
            removed += len(duplicate_ids)
            merged += 1

    logger.info("KG consolidation complete: merged_groups=%d removed_rows=%d", merged, removed)
    return {"merged_groups": merged, "removed_rows": removed}


def _snapshot_drift() -> dict:
    """
    Runs the merchant drift monitor and persists a daily snapshot.
    """
    from backend.etl.merchant.drift_monitor import MerchantCategorizationDriftMonitor

    monitor = MerchantCategorizationDriftMonitor()
    report = monitor.run()
    logger.info("Drift snapshot stored with %d alerts", len(report.alerts))
    return {
        "generated_at": report.generated_at,
        "alert_count": len(report.alerts),
        "sufficient_data": report.sufficient_data,
    }


def _cleanup_feedback() -> dict:
    """
    Stale feedback policy:
    - Mark very old pending feedback as expired
    - Mark moderately old pending feedback as retry_queued (single retry path)
    """
    from datetime import datetime, timezone
    from backend.api.core.supabase_client import get_supabase_admin_client

    db = get_supabase_admin_client()
    now = datetime.now(timezone.utc)
    expire_before = (now - timedelta(days=14)).isoformat()
    retry_before = (now - timedelta(days=3)).isoformat()

    expired = (
        db.table("categorization_feedback")
        .update({"status": "expired", "updated_at": now.isoformat()})
        .eq("status", "pending")
        .lt("created_at", expire_before)
        .execute()
    )

    retried = (
        db.table("categorization_feedback")
        .update({"status": "retry_queued", "updated_at": now.isoformat()})
        .eq("status", "pending")
        .lt("created_at", retry_before)
        .gte("created_at", expire_before)
        .execute()
    )

    expired_count = len(expired.data or [])
    retry_count = len(retried.data or [])
    logger.info("Feedback maintenance complete: expired=%d retry_queued=%d", expired_count, retry_count)
    return {"expired": expired_count, "retry_queued": retry_count}


def _sync_verified_feedback_to_kg() -> dict:
    """
    Weekly-ish backfill: push verified transaction categorizations into KG.
    """
    from backend.api.core.supabase_client import get_supabase_admin_client
    from backend.etl.merchant.feedback_handler import sync_verified_transactions_to_knowledge_graph

    db = get_supabase_admin_client()
    result = sync_verified_transactions_to_knowledge_graph(
        supabase=db,
        user_id=None,
        days=14,
        limit=5000,
    )
    logger.info(
        "Verified feedback sync complete: scanned=%d synced=%d skipped=%d",
        result.get("scanned_rows", 0),
        result.get("synced_rows", 0),
        result.get("skipped_rows", 0),
    )
    return result


def _run_schema_sanity_checks() -> dict:
    """
    Daily sanity checks:
    - future transaction dates
    - tx amount > 10M PKR
    - duplicate transaction IDs
    - pending staging older than 7 days
    """
    from scripts.schema_sanity_check import run_schema_sanity_checks

    result = run_schema_sanity_checks()
    logger.info(
        "Schema sanity checks: future_dates=%d over_10m=%d dup_tx_ids=%d stale_pending=%d",
        result.get("future_transaction_dates", 0),
        result.get("transactions_over_10m_pkr", 0),
        result.get("duplicate_transaction_ids", 0),
        result.get("stale_pending_over_7_days", 0),
    )
    return result


def _refresh_gmail_tokens() -> dict:
    """
    Refresh Gmail access tokens for active connections before expiry.
    """
    from datetime import datetime, timedelta, timezone

    import httpx

    from backend.api.core.config import GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET
    from backend.api.core.supabase_client import get_supabase_admin_client

    if not GOOGLE_OAUTH_CLIENT_ID or not GOOGLE_OAUTH_CLIENT_SECRET:
        logger.warning("Gmail token refresh skipped: Google OAuth client id/secret not configured.")
        return {"checked": 0, "refreshed": 0, "failed": 0, "skipped": 0}

    db = get_supabase_admin_client()
    now = datetime.now(timezone.utc)
    refresh_horizon = (now + timedelta(hours=24)).isoformat()

    rows = (
        db.table("gmail_connections")
        .select("connection_id,user_id,refresh_token,expiry,is_active")
        .eq("is_active", True)
        .or_(f"expiry.is.null,expiry.lte.{refresh_horizon}")
        .execute()
    ).data or []

    checked = 0
    refreshed = 0
    failed = 0
    skipped = 0
    for row in rows:
        checked += 1
        refresh_token = row.get("refresh_token")
        if not refresh_token:
            skipped += 1
            continue
        try:
            resp = httpx.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": GOOGLE_OAUTH_CLIENT_ID,
                    "client_secret": GOOGLE_OAUTH_CLIENT_SECRET,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                },
                timeout=30.0,
            )
            resp.raise_for_status()
            payload = resp.json()
            access_token = payload.get("access_token")
            expires_in = int(payload.get("expires_in") or 3600)
            if not access_token:
                failed += 1
                continue
            new_expiry = (now + timedelta(seconds=expires_in)).isoformat()
            db.table("gmail_connections").update(
                {
                    "access_token": access_token,
                    "expiry": new_expiry,
                    "error_detail": None,
                }
            ).eq("connection_id", row["connection_id"]).execute()
            refreshed += 1
        except Exception as exc:
            failed += 1
            db.table("gmail_connections").update(
                {"error_detail": f"refresh_token_failed: {exc}"}
            ).eq("connection_id", row["connection_id"]).execute()

    logger.info(
        "Gmail token refresh complete: checked=%d refreshed=%d failed=%d skipped=%d",
        checked,
        refreshed,
        failed,
        skipped,
    )
    return {"checked": checked, "refreshed": refreshed, "failed": failed, "skipped": skipped}


def _generate_quality_alerts() -> dict:
    """
    Generate threshold alerts and persist to system_alerts.
    """
    from datetime import datetime, timedelta, timezone

    from backend.api.core.supabase_client import get_supabase_admin_client

    db = get_supabase_admin_client()
    now = datetime.now(timezone.utc)
    since_24h = (now - timedelta(hours=24)).isoformat()
    pending_cutoff = (now - timedelta(hours=6)).isoformat()

    created = 0

    def _has_open_alert(alert_type: str) -> bool:
        rows = (
            db.table("system_alerts")
            .select("id")
            .eq("alert_type", alert_type)
            .eq("is_resolved", False)
            .limit(1)
            .execute()
        ).data or []
        return len(rows) > 0

    def _create_alert(alert_type: str, threshold_value: float, current_value: float, severity: str, metadata: dict):
        nonlocal created
        if _has_open_alert(alert_type):
            return
        db.table("system_alerts").insert(
            {
                "alert_type": alert_type,
                "threshold_value": threshold_value,
                "current_value": current_value,
                "severity": severity,
                "metadata": metadata,
                "breach_timestamp": now.isoformat(),
                "is_resolved": False,
            }
        ).execute()
        created += 1

    # 1) Parse success rate below 90%
    staging_24h = (
        db.table("ingestion_staging")
        .select("status")
        .gte("created_at", since_24h)
        .execute()
    ).data or []
    processed = sum(1 for x in staging_24h if str(x.get("status") or "") == "processed")
    failed = sum(
        1 for x in staging_24h if str(x.get("status") or "") in {"failed", "permanently_failed"}
    )
    total = processed + failed
    parse_rate = (processed / total) if total else 1.0
    if total > 0 and parse_rate < 0.90:
        _create_alert(
            alert_type="parse_rate_drop",
            threshold_value=0.90,
            current_value=round(parse_rate, 4),
            severity="critical",
            metadata={"window": "24h", "processed": processed, "failed": failed},
        )

    # 2) ETL batch fails completely (any failed jobs in last 24h)
    failed_jobs = (
        db.table("etl_job_runs")
        .select("job_id", count="exact")
        .eq("status", "failed")
        .gte("requested_at", since_24h)
        .execute()
    ).count or 0
    if failed_jobs > 0:
        _create_alert(
            alert_type="etl_batch_fail",
            threshold_value=0,
            current_value=float(failed_jobs),
            severity="critical",
            metadata={"window": "24h"},
        )

    # 3) Gmail sync fails 3 times in a row (exact from gmail_sync_events)
    event_rows = (
        db.table("gmail_sync_events")
        .select("user_id,status,created_at")
        .gte("created_at", (now - timedelta(days=7)).isoformat())
        .order("created_at", desc=True)
        .limit(5000)
        .execute()
    ).data or []
    by_user: dict[str, list[str]] = {}
    for row in event_rows:
        uid = str(row.get("user_id") or "")
        if not uid:
            continue
        by_user.setdefault(uid, []).append(str(row.get("status") or ""))
    users_with_three_fails = 0
    for _uid, statuses in by_user.items():
        if len(statuses) >= 3 and statuses[0] == "failed" and statuses[1] == "failed" and statuses[2] == "failed":
            users_with_three_fails += 1
    if users_with_three_fails > 0:
        _create_alert(
            alert_type="gmail_sync_fail",
            threshold_value=3,
            current_value=float(users_with_three_fails),
            severity="warning",
            metadata={"window": "7d", "rule": "3_consecutive_failures"},
        )

    # 4) >50 staging rows stuck in pending >6h
    stuck = (
        db.table("ingestion_staging")
        .select("staging_id", count="exact")
        .eq("status", "pending_processing")
        .lt("created_at", pending_cutoff)
        .execute()
    ).count or 0
    if stuck > 50:
        _create_alert(
            alert_type="staging_stuck",
            threshold_value=50,
            current_value=float(stuck),
            severity="warning",
            metadata={"older_than_hours": 6},
        )

    logger.info("Quality alert generation complete: created=%d", created)
    return {"alerts_created": created, "parse_rate_24h": round(parse_rate, 4), "stuck_rows": stuck}


def _auto_process_dlq() -> dict:
    """
    Weekly DLQ auto-processing for transient ETL failures.
    - Retry transient failures by re-queueing staging rows
    - If retry_attempts >= 3, mark as ignored
    """
    from datetime import datetime, timezone

    from backend.api.core.supabase_client import get_supabase_admin_client

    db = get_supabase_admin_client()
    now_iso = datetime.now(timezone.utc).isoformat()

    rows = (
        db.table("etl_failed_records")
        .select("failed_id,staging_id,status,errors,retry_attempts")
        .eq("status", "needs_review")
        .order("created_at", desc=False)
        .limit(1000)
        .execute()
    ).data or []

    retried = 0
    ignored = 0
    skipped = 0

    transient_markers = (
        "quota",
        "429",
        "timeout",
        "timed out",
        "connection",
        "network",
        "temporarily unavailable",
        "503",
        "502",
        "504",
        "llm_error",
    )

    for row in rows:
        failed_id = row.get("failed_id")
        staging_id = row.get("staging_id")
        attempts = int(row.get("retry_attempts") or 0)
        errors = row.get("errors")
        error_blob = str(errors).lower()

        is_transient = any(marker in error_blob for marker in transient_markers)
        if not is_transient:
            skipped += 1
            continue

        if attempts >= 3:
            db.table("etl_failed_records").update(
                {"status": "ignored", "last_retry_at": now_iso}
            ).eq("failed_id", failed_id).execute()
            ignored += 1
            continue

        if not staging_id:
            db.table("etl_failed_records").update(
                {
                    "retry_attempts": attempts + 1,
                    "last_retry_at": now_iso,
                    "status": "ignored",
                }
            ).eq("failed_id", failed_id).execute()
            ignored += 1
            continue

        db.table("ingestion_staging").update(
            {
                "status": "pending_processing",
                "error_message": None,
                "processed_at": None,
                "last_retry_at": now_iso,
            }
        ).eq("staging_id", staging_id).execute()

        db.table("etl_failed_records").update(
            {
                "retry_attempts": attempts + 1,
                "last_retry_at": now_iso,
                "status": "resolved",
            }
        ).eq("failed_id", failed_id).execute()
        retried += 1

    logger.info(
        "DLQ auto-processing complete: retried=%d ignored=%d skipped=%d",
        retried,
        ignored,
        skipped,
    )
    return {"retried": retried, "ignored": ignored, "skipped_non_transient": skipped}


with DAG(
    dag_id="merchant_intelligence_maintenance_dag",
    default_args=_DEFAULT_ARGS,
    description="FinGuard Phase 6 nightly merchant intelligence maintenance",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["merchant", "maintenance", "phase6"],
) as dag:
    t_consolidate = PythonOperator(
        task_id="consolidate_merchant_kg",
        python_callable=_consolidate_kg,
    )
    t_drift = PythonOperator(
        task_id="snapshot_drift_metrics",
        python_callable=_snapshot_drift,
    )
    t_feedback = PythonOperator(
        task_id="cleanup_feedback",
        python_callable=_cleanup_feedback,
    )
    t_sync_verified = PythonOperator(
        task_id="sync_verified_feedback_to_kg",
        python_callable=_sync_verified_feedback_to_kg,
    )
    t_schema_sanity = PythonOperator(
        task_id="run_schema_sanity_checks",
        python_callable=_run_schema_sanity_checks,
    )
    t_refresh_gmail_tokens = PythonOperator(
        task_id="refresh_gmail_tokens",
        python_callable=_refresh_gmail_tokens,
    )
    t_generate_alerts = PythonOperator(
        task_id="generate_quality_alerts",
        python_callable=_generate_quality_alerts,
    )
    t_auto_process_dlq = PythonOperator(
        task_id="auto_process_dlq",
        python_callable=_auto_process_dlq,
    )

    t_consolidate >> t_drift >> t_feedback >> t_sync_verified >> t_schema_sanity >> t_refresh_gmail_tokens >> t_generate_alerts >> t_auto_process_dlq
