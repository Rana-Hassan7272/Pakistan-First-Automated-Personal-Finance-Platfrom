# FinGuard AI — Production deployment

This document describes the **live production setup** as deployed: split backend (Railway API + Alibaba worker), shared Redis, Supabase, Hugging Face ML artifacts, Expo mobile builds, and GitHub Actions.

---

## Architecture overview

```text
┌─────────────────┐     HTTPS      ┌──────────────────────┐
│  Android APK    │ ──────────────►│  Railway (API only)  │
│  (EAS preview)  │                │  FastAPI + uvicorn   │
└────────┬────────┘                └──────────┬───────────┘
         │                                    │
         │ Supabase auth / DB                 │ Celery enqueue
         ▼                                    ▼
┌─────────────────┐                ┌──────────────────────┐
│    Supabase     │◄───────────────│   Upstash Redis      │
│  Postgres + Auth│                │   (TLS, DB 0)        │
│  Storage        │                └──────────┬───────────┘
└────────▲────────┘                           │
         │                                    │ task consume
         │ read/write                         ▼
         │                         ┌──────────────────────┐
         └─────────────────────────│  Alibaba Cloud ECS   │
                                   │  Celery worker       │
                                   │  ETL · Gmail · OCR   │
                                   │  BERT · PDF/CV       │
                                   │  Celery Beat (cron)  │
                                   └──────────┬───────────┘
                                              │
                                              ▼
                                   ┌──────────────────────┐
                                   │  Hugging Face Hub    │
                                   │  hassan7272/finguard-ml │
                                   │  (model files sync)  │
                                   └──────────────────────┘
```

**Important:** Railway and Alibaba are **separate servers**. They are connected only through **Upstash Redis** (task queue) and **Supabase** (data). The mobile app talks **only to the Railway API URL**, not to Alibaba.

---

## What runs where

| Component | Platform | Role |
|-----------|----------|------|
| **API** | Railway (~1 GB) | HTTP API, auth, mobile ingest, enqueues Celery tasks |
| **Worker** | Alibaba ECS Singapore (2 vCPU, 4 GiB) | Celery: SMS/notification ETL, Gmail sync, OCR, BERT categorization |
| **Celery Beat** | Alibaba ECS (same VM, second container) | Scheduled tasks: goal nudges, bill reminders, balance drift, merchant cleanup |
| **Redis** | Upstash | Celery broker + result backend + app cache (single DB `0`) |
| **Database / Auth / Files** | Supabase | Users, transactions, staging, documents bucket |
| **ML weights** | Hugging Face private repo `hassan7272/finguard-ml` | BERT, anomaly, LSTM, RAG indices — synced on worker start |
| **LLM (advisor / fallback categories)** | Groq (+ optional Google) | External API keys on API + worker |
| **Mobile app** | Expo EAS (`preview` profile) | Android APK; env from EAS **preview** environment |
| **Web app** (`apps/web`) | Not deployed to prod yet | Local / future hosting |
| **GitHub Actions** | GitHub | CI tests, parser eval, weekly ML ops, RAG smoke |

---

## Backend — Railway (API only)

**Repo:** GitHub → Railway auto-deploy from `main`  
**Config:** `railway.toml` / `railway.api.toml`  
**Start command:** `sh scripts/start-api-only.sh`  
**Health check:** `GET /health`  
**Dockerfile:** root `Dockerfile`

### API environment (required)

```env
FINGUARD_SERVICE_ROLE=api
ENVIRONMENT=production

SUPABASE_URL=...
SUPABASE_ANON_KEY=...
SUPABASE_SERVICE_ROLE_KEY=...
SUPABASE_JWT_SECRET=...

REDIS_URL=rediss://default:PASSWORD@HOST.upstash.io:6379/0?ssl_cert_reqs=CERT_REQUIRED
CELERY_BROKER_URL=rediss://default:PASSWORD@HOST.upstash.io:6379/0?ssl_cert_reqs=CERT_REQUIRED
CELERY_RESULT_BACKEND=rediss://default:PASSWORD@HOST.upstash.io:6379/0?ssl_cert_reqs=CERT_REQUIRED

GROQ_API_KEY=...
GOOGLE_OAUTH_CLIENT_ID=...
GOOGLE_OAUTH_CLIENT_SECRET=...
# Other keys as in local .env (OpenRouter, etc.)

FINGUARD_SYNC_ML_ON_START=0
FINGUARD_PREWARM_OCR=0
FINGUARD_PREWARM_BERT=0
```

**Do not** run a Celery worker on Railway in this setup. **Do not** use Railway internal Redis (`redis.railway.internal`) — the Alibaba worker cannot reach it.

### API behaviour

- Receives mobile requests and writes to Supabase staging.
- Dispatches Celery tasks to Upstash (`process_pending_batch`, `gmail_sync`, `cv.process_document`, etc.).
- With `FINGUARD_SERVICE_ROLE=api`, heavy ETL is **not** run inline on the API container.

---

## Backend — Alibaba Cloud ECS (worker)

**Region:** Singapore  
**Shape:** `VM.Standard.A1.Flex` or x86 equivalent — **2 vCPU, 4 GiB RAM** (trial)  
**OS:** Ubuntu 22.04  
**Deploy path on server:** `~/finguard` (git clone)

### Worker environment

Same Supabase + Redis + Groq/Google keys as API, plus:

```env
FINGUARD_SERVICE_ROLE=worker
FINGUARD_DEFER_PREWARM=1
FINGUARD_SYNC_ML_ON_START=1
ML_ARTIFACTS_SOURCE=hf
HF_ML_REPO_ID=hassan7272/finguard-ml
HF_TOKEN=hf_...   # read token, no quotes in .env

FINGUARD_PREWARM_BERT=1
FINGUARD_PREWARM_OCR=1
PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK=True
```

**Redis:** same three Upstash URLs as Railway (`/0` only — Upstash does not support `/1` or `/2`).

**`.env` rules for Docker:** no quotes around values (`HF_ML_REPO_ID=hassan7272/finguard-ml`, not `"..."`).

### Build and run on ECS

```bash
cd ~/finguard
git pull origin main
docker build -t finguard-worker .
docker rm -f finguard-worker
docker run -d --name finguard-worker --restart unless-stopped \
  --env-file .env \
  finguard-worker sh scripts/start-worker-only.sh
docker logs -f finguard-worker
```

**Start script:** `scripts/start-worker-only.sh`  
**Celery queues:** `celery,etl,ml,default` (required — default Celery queue name is `celery`).

### Expected worker logs

1. HF `snapshot_download` → `bert_ready: true` (first boot or after fresh container)
2. `Connected to rediss://...`
3. `Celery worker READY`
4. OCR + BERT prewarm complete
5. On app activity: `etl.process_pending_batch`, `etl.gmail_sync`, `cv.process_document`, etc.

### Security

- **Inbound:** SSH (22) from your IP only.
- **No** public port 8000 on ECS — worker is background-only.

### Ops notes

- Closing SSH does **not** stop the worker (`--restart unless-stopped`).
- Stopping/deleting the ECS instance stops ETL/Gmail/OCR until it is back.
- After `git pull`, rebuild the Docker image and recreate the container.

---

## Backend — Alibaba Cloud ECS (Celery Beat)

**Purpose:** Runs **scheduled** jobs (pushes and daily maintenance). The worker handles tasks when users use the app; Beat fires tasks on a timer even if nobody opens the app.

**Run only one Beat instance** (same Alibaba VM as the worker — not on Railway).

### Scheduled tasks (`backend/etl/worker_app.py`)

| Beat name | Task | Interval |
|-----------|------|----------|
| `daily-gmail-sync` | `etl.dispatch_scheduled_gmail_sync` | 24 hours |
| `weekly-goal-nudges` | `engagement.send_weekly_goal_nudges` | 7 days |
| `weekly-balance-drift-nudges` | `engagement.send_weekly_balance_drift_nudges` | 7 days |
| `daily-bill-reminders` | `engagement.send_bill_reminder_pushes` | 24 hours |
| `monthly-merchant-cleanup-nudges` | `engagement.send_monthly_merchant_cleanup_nudges` | ~4 weeks |
| `promote-staging-failures-to-golden` | `etl.promote_staging_failures_to_golden` | 24 hours |

Push nudges need Expo push credentials in `.env` (same file as worker). Without Beat, ETL/Gmail/OCR still work; automatic reminder pushes do not.

### Start Beat (after worker image is built)

```bash
docker rm -f finguard-beat 2>/dev/null || true
docker run -d --name finguard-beat --restart unless-stopped \
  --env-file .env \
  finguard-worker \
  celery -A backend.etl.worker_app beat --loglevel=info
docker logs --tail 20 finguard-beat
```

### Expected Beat logs

```
celery beat v5.4.0 (opalescent) is starting.
...
beat: Starting...
```

Later (when a task is due): `Scheduler: Sending due task ...` in beat logs; worker logs show the task running.

### Verify both containers

```bash
docker ps
docker logs --tail 5 finguard-worker
docker logs --tail 5 finguard-beat
```

---

## Upstash Redis

| Variable | Purpose |
|----------|---------|
| `REDIS_URL` | App cache / rate limiting |
| `CELERY_BROKER_URL` | Task queue (API → worker) |
| `CELERY_RESULT_BACKEND` | Celery results |

**Upstash constraints:**

- Use database **`/0` only** for all three URLs.
- Use **`rediss://`** with `?ssl_cert_reqs=CERT_REQUIRED` (Celery requirement).
- Same values on **Railway** and **Alibaba**.

`backend/api/core/config.py` auto-appends `ssl_cert_reqs` for `rediss://` if missing.

---

## Hugging Face ML artifacts

**Repo:** `hassan7272/finguard-ml` (private)  
**Sync module:** `backend/deploy/ml_artifacts_hf.py`

**Paths synced:**

- `backend/ml/bert/artifacts/bert_txn_production`
- `backend/ml/anomaly/artifacts`
- `backend/ml/lstm/artifacts`
- `backend/rag/indices`

Worker runs sync on start when checkpoint missing (`start-worker-only.sh`). API does **not** sync ML at runtime.

**Retrain workflow:** train locally → upload to HF → restart worker container.

---

## Frontend — Mobile (production)

**Path:** `frontend/mobile`  
**Build:** Expo EAS  
**Profile:** `preview` (internal APK)

### EAS environment (`preview`)

Set in [Expo dashboard](https://expo.dev) → project → **Environment variables** → **preview**:

```env
EXPO_PUBLIC_API_URL=https://YOUR-RAILWAY-SERVICE.up.railway.app
EXPO_PUBLIC_SUPABASE_URL=https://YOUR-PROJECT.supabase.co
EXPO_PUBLIC_SUPABASE_ANON_KEY=eyJ...
```

### Build command

```bash
cd frontend/mobile
npx eas build --platform android --profile preview
```

Download APK from Expo when the build finishes. Distribute via Drive / direct install.

**The app never points at Alibaba** — only Railway API + Supabase.

---

## Frontend — Web (not in prod)

`apps/web` (Next.js) is for local/dev and admin-style pages. It is **not** part of the current production deploy described above.

---

## GitHub repository

**Remote:** GitHub (e.g. `Rana-Hassan7272/finguard` — adjust to your actual repo name)

### GitHub Actions workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `.github/workflows/ci.yml` | Push/PR to `main`/`master` | `pytest` backend tests; parser golden harness artifact |
| `.github/workflows/weekly-maintenance.yml` | Sundays 03:00 UTC + manual | BERT correction gate, live feedback export, optional retrain issue, ML metrics |
| `.github/workflows/rag-phase9-smoke.yml` | Push/PR | RAG unit/smoke tests (`FINGUARD_RAG_MOCK=1`) |
| `.github/workflows/phase6-quality-gate.yml` | PR + manual | Node + Python quality gate on PRs |

### GitHub repository secrets (for CI)

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

---

## Deploy / update checklist

### API (Railway)

1. Push to `main` → Railway redeploys (or manual deploy).
2. Confirm `/health` returns 200.
3. Confirm Redis env vars match worker.

### Worker + Beat (Alibaba)

1. `git pull` on ECS.
2. `docker build -t finguard-worker .`
3. Recreate worker: `--env-file .env` + `start-worker-only.sh`.
4. Recreate beat (same image, beat command) — see Celery Beat section above.
5. `docker ps` → `finguard-worker` and `finguard-beat` both Up.
6. Worker logs → connected + ready; beat logs → `beat: Starting...`.

### Mobile

1. Update EAS `preview` env if API URL changed.
2. `eas build --profile preview --platform android`.
3. Install new APK on test devices.

---

## Smoke tests (production)

| Test | Expect |
|------|--------|
| `GET https://RAILWAY-URL/health` | 200 |
| Worker logs after SMS sync | `etl.process_pending_batch` |
| Gmail sync from app | `etl.gmail_sync` in worker logs |
| Image / PDF upload | `cv.process_document` in worker logs |
| Supabase | New rows in `transactions` / staging |
| Upstash dashboard | Redis activity during tests |
| `docker logs finguard-beat` | `beat: Starting...` (no crash loop) |

---

## Troubleshooting (quick)

| Symptom | Likely cause | Fix |
|---------|----------------|-----|
| Worker idle, no task logs | Wrong Celery queues | Worker must use `-Q celery,etl,ml,default` |
| `Only 0th database is supported` | Upstash `/1` or `/2` | Use `/0` for all Redis URLs |
| `ssl_cert_reqs` Celery error | `rediss://` without SSL param | Add `?ssl_cert_reqs=CERT_REQUIRED` |
| HF `Repo id must use...` | Quoted `HF_ML_REPO_ID` in `.env` | Remove quotes |
| API works, no processing | Worker down or Redis mismatch | Same Upstash URLs on both; ECS running |
| OCR slow / OOM on 4 GB | RAM limit | `FINGUARD_PREWARM_OCR=0`; upgrade ECS RAM |

---

## Cost summary (approximate)

| Service | Tier |
|---------|------|
| Railway API | ~1 GB plan |
| Alibaba ECS | Free trial credit / pay-as-you-go after trial |
| Upstash Redis | Free tier |
| Supabase | Project plan |
| Hugging Face | Private model storage (free tier) |
| Groq | API usage |
| Expo EAS | Build minutes per Expo plan |

---

## Key files in repo

| File | Purpose |
|------|---------|
| `Dockerfile` | API + worker image |
| `scripts/start-api-only.sh` | Railway start |
| `scripts/start-worker-only.sh` | ECS worker start |
| `railway.toml` / `railway.api.toml` | Railway deploy config |
| `railway.worker.toml` | Reference worker config (worker runs on ECS, not Railway) |
| `backend/api/core/config.py` | Redis / Celery URL normalization |
| `backend/etl/worker_app.py` | Celery app + ML prewarm |
| `frontend/mobile/eas.json` | EAS build profiles |

---

*Last updated: production split deploy — Railway API + Alibaba worker + Celery Beat + Upstash + EAS mobile.*
