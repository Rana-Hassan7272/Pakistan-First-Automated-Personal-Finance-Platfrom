# PDF Extraction + Gmail Sync — Full Implementation Plan

Use this document to port the FinGuard PDF statement parser and Gmail sync into another project. All logic lives in Supabase Edge Functions (Deno) plus two server OAuth routes and a client hook.

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│ FRONTEND                                                                 │
│  settings.tsx  → Connect Gmail, Upload PDF, manual sync                  │
│  dashboard.tsx → auto-sync every 5 min                                   │
│  use-gmail-sync.ts → POST /functions/v1/gmail-sync                       │
│  api/gmail/connect + api/gmail/callback → Google OAuth (server routes)   │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│ SUPABASE EDGE FUNCTIONS (Deno)                                           │
│  gmail-sync/       → fetch emails, parse txns, queue PDF statements      │
│  parse-statement/  → extract PDF text, parse rows, insert transactions   │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│ DATABASE + STORAGE                                                       │
│  gmail_connections, oauth_states, statement_imports, transactions          │
│  storage bucket: statements/{user_id}/{uuid}.pdf                           │
└─────────────────────────────────────────────────────────────────────────┘
```

**Two ingestion paths for PDFs:**
1. Gmail sync finds statement email → inserts `statement_imports` row → triggers `parse-statement`
2. User uploads PDF to storage → inserts `statement_imports` row → triggers `parse-statement`

**Two parsing strategies for PDF text:**
1. **Easypaisa** → deterministic JS parser (no AI, fast, accurate)
2. **All other banks** → Gemini via AI gateway with structured tool call

**Two parsing strategies for email body:**
1. **Known PK templates** → deterministic regex parsers (Easypaisa, NayaPay, ABL, JazzCash)
2. **Unknown format** → Gemini AI fallback with structured tool call

---

## 2. Database Schema (Required Tables)

### 2.1 `transactions` (core — must exist first)

```sql
-- enums assumed: txn_type ('debit','credit','transfer'), txn_source ('manual','gmail','sms','receipt')

create table public.transactions (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  amount numeric(14,2) not null,
  currency text not null default 'PKR',
  merchant text,
  category_id uuid references public.categories(id) on delete set null,
  type txn_type not null default 'debit',
  source txn_source not null default 'manual',
  occurred_at timestamptz not null default now(),
  notes text,
  bank_source text,
  confidence integer default 100 check (confidence between 0 and 100),
  explanation text,
  raw_input text,
  gmail_message_id text,
  is_anomaly boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index idx_transactions_user_gmail_msg
  on public.transactions (user_id, gmail_message_id)
  where gmail_message_id is not null;

create index idx_transactions_dedup
  on public.transactions (user_id, amount, occurred_at);
```

### 2.2 `gmail_connections`

```sql
create table public.gmail_connections (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null unique references auth.users(id) on delete cascade,
  email text not null,
  refresh_token text not null,
  access_token text,
  token_expires_at timestamptz,
  scope text,
  connected_at timestamptz not null default now(),
  last_synced_at timestamptz,
  backfill_days integer not null default 90,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
-- RLS: user can SELECT/INSERT/UPDATE/DELETE own row only
-- Edge functions use SERVICE_ROLE to read/write tokens
```

### 2.3 `oauth_states` (CSRF for Gmail connect)

```sql
create table public.oauth_states (
  state text primary key,
  user_id uuid not null,
  created_at timestamptz not null default now(),
  expires_at timestamptz not null default (now() + interval '10 minutes')
);
-- No user RLS policies — only service role accesses this
```

### 2.4 `statement_imports` (PDF processing queue)

```sql
create table public.statement_imports (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  gmail_message_id text,
  attachment_id text,
  file_name text not null,
  bank_source text,
  status text not null default 'pending',  -- pending | processing | done | failed
  transactions_extracted integer not null default 0,
  error text,
  source text not null default 'gmail',    -- gmail | upload
  storage_path text,                       -- for manual uploads
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index statement_imports_unique
  on public.statement_imports(user_id, gmail_message_id, attachment_id);

create index statement_imports_user_idx
  on public.statement_imports(user_id, created_at desc);
```

### 2.5 Storage bucket `statements`

```sql
insert into storage.buckets (id, name, public)
values ('statements', 'statements', false);

-- RLS: auth.uid()::text = (storage.foldername(name))[1]
-- Path pattern: {user_id}/{uuid}.pdf
```

### 2.6 `categories` (system categories used by parsers)

Required category names (case-insensitive lookup):
`Food, Transport, Bills, Shopping, Transfers, Loans, Salary, Entertainment, Health, Education, Savings, Other`

---

## 3. Environment Variables

### Supabase Edge Function secrets (Dashboard → Edge Functions → Secrets)

| Variable | Purpose |
|----------|---------|
| `SUPABASE_URL` | Project URL |
| `SUPABASE_ANON_KEY` | Validate user JWT |
| `SUPABASE_SERVICE_ROLE_KEY` | Admin DB/storage, trigger other functions |
| `LOVABLE_API_KEY` | AI gateway auth (replace with direct Gemini/OpenAI key in production) |
| `GOOGLE_CLIENT_ID` | Gmail OAuth |
| `GOOGLE_CLIENT_SECRET` | Gmail OAuth token refresh |

### Server routes (Cloudflare/Vercel/Node env)

| Variable | Purpose |
|----------|---------|
| `GOOGLE_CLIENT_ID` | OAuth connect/callback |
| `GOOGLE_CLIENT_SECRET` | Token exchange |
| `SUPABASE_URL` | Auth validation |
| `SUPABASE_PUBLISHABLE_KEY` | User token validation |
| `SUPABASE_SERVICE_ROLE_KEY` | Store gmail_connections |

### Frontend

| Variable | Purpose |
|----------|---------|
| `VITE_SUPABASE_URL` | Call edge functions |
| `VITE_SUPABASE_PUBLISHABLE_KEY` | Supabase client |

---

## 4. Gmail OAuth Flow

### 4.1 Connect route: `GET /api/gmail/connect`

**Query params:** `token=<supabase_access_token>&days=<1-365 backfill days>`

**Steps:**
1. Validate Supabase JWT → get `userId`
2. Generate CSRF state: `crypto.randomUUID() + "." + crypto.randomUUID()`
3. Insert into `oauth_states` with 10 min expiry
4. Encode state as `{state}|{days}`
5. Redirect to Google OAuth:

```
https://accounts.google.com/o/oauth2/v2/auth
  ?client_id=GOOGLE_CLIENT_ID
  &redirect_uri={origin}/api/gmail/callback
  &response_type=code
  &scope=https://www.googleapis.com/auth/gmail.readonly openid email profile
  &access_type=offline
  &prompt=consent
  &state={state}|{days}
```

**Why `prompt=consent`:** Forces refresh_token on reconnect.

### 4.2 Callback route: `GET /api/gmail/callback`

**Steps:**
1. Parse `state` → split on `|` → validate against `oauth_states`, check expiry, delete state row
2. Exchange `code` at `https://oauth2.googleapis.com/token` with `grant_type=authorization_code`
3. Require `refresh_token` in response (fail if missing — user must reconnect with consent)
4. Fetch email from `https://www.googleapis.com/oauth2/v2/userinfo`
5. Upsert `gmail_connections`:
   - `user_id`, `email`, `refresh_token`, `access_token`, `token_expires_at`, `scope`, `backfill_days`
6. Redirect to `/settings?gmail=connected`

### 4.3 Token refresh (used in gmail-sync and parse-statement)

```typescript
async function refreshAccessToken(refreshToken: string) {
  const r = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: GOOGLE_CLIENT_ID,
      client_secret: GOOGLE_CLIENT_SECRET,
      refresh_token: refreshToken,
      grant_type: "refresh_token",
    }),
  });
  const j = await r.json();
  if (!r.ok) throw new Error("Token refresh failed");
  return { access_token: j.access_token, expires_in: j.expires_in };
}
```

Refresh when `token_expires_at - 60_000 < Date.now()`.

---

## 5. Gmail Sync Edge Function

**File:** `supabase/functions/gmail-sync/index.ts` + `email-parser.ts`

**Endpoint:** `POST /functions/v1/gmail-sync`
**Auth:** `Authorization: Bearer <user_jwt>`

### 5.1 Constants

```typescript
const FINANCE_QUERY =
  '(transaction OR receipt OR payment OR invoice OR debited OR credited OR purchase OR "your order" OR "order confirmation" OR statement OR "e-statement" OR "account statement" OR "monthly statement" OR refund OR payslip OR salary OR "salary slip" OR "pay slip") -category:promotions -category:social';

const STATEMENT_KEYWORDS = /(statement|e-?statement|monthly statement|account statement|payslip|salary slip|pay slip)/i;
const BANK_SENDERS = /(hbl|habibbank|meezan|ubl|unitedbank|mcb|alliedbank|abl|bankalfalah|standardchartered|jazzcash|easypaisa|sadapay|nayapay|faysalbank|js bank|askari)/i;

const SYNC_LOOKBACK_MS = 48 * 60 * 60 * 1000;  // 48h overlap on incremental sync
const MAX_MESSAGES_PER_RUN = 120;               // process cap per invocation
// Pagination: max 1000 messages, 20 pages × 100
```

### 5.2 Sync window logic

```typescript
const lastSync = conn.last_synced_at ? new Date(conn.last_synced_at) : null;
const backfillDays = conn.backfill_days ?? 90;
const sinceDate = lastSync
  ? new Date(lastSync.getTime() - SYNC_LOOKBACK_MS)   // overlap prevents missed emails
  : new Date(Date.now() - backfillDays * 86400_000);  // first sync: full backfill
const afterEpoch = Math.floor(sinceDate.getTime() / 1000);
const q = `${FINANCE_QUERY} after:${afterEpoch}`;
```

### 5.3 Gmail API calls

**List messages (paginated):**
```
GET https://gmail.googleapis.com/gmail/v1/users/me/messages?q={q}&maxResults=100&pageToken={token}
```

**Fetch full message:**
```
GET https://gmail.googleapis.com/gmail/v1/users/me/messages/{id}?format=full
```

**Download attachment (used by parse-statement, not gmail-sync directly):**
```
GET https://gmail.googleapis.com/gmail/v1/users/me/messages/{messageId}/attachments/{attachmentId}
```

### 5.4 Skip already-imported messages

Before processing, query:
- `transactions.gmail_message_id IN (messageIds)`
- `statement_imports.gmail_message_id IN (messageIds)`

Filter those out from `toProcess`.

### 5.5 Email body extraction

```typescript
function decodeBase64Url(s: string) {
  s = s.replace(/-/g, "+").replace(/_/g, "/");
  while (s.length % 4) s += "=";
  return atob(s);
}

function extractBody(payload: any): string {
  const parts: string[] = [];
  function walk(p: any) {
    if (!p) return;
    if (p.body?.data) {
      const decoded = decodeBase64Url(p.body.data);
      parts.push(decoded.replace(/<style[\s\S]*?<\/style>/gi, "")
        .replace(/<[^>]+>/g, " ")
        .replace(/\s+/g, " "));
    }
    if (p.parts) p.parts.forEach(walk);
  }
  walk(payload);
  return parts.join("\n").slice(0, 8000);
}
```

### 5.6 PDF attachment detection

```typescript
function findPdfAttachments(payload: any) {
  const out = [];
  function walk(p: any) {
    if (!p) return;
    const fn = (p.filename ?? "").toLowerCase();
    const isPdf = p.mimeType === "application/pdf" || fn.endsWith(".pdf");
    if (isPdf && p.body?.attachmentId) {
      out.push({ filename: p.filename || "statement.pdf", attachmentId: p.body.attachmentId, mimeType: p.mimeType || "application/pdf" });
    }
    if (p.parts) p.parts.forEach(walk);
  }
  walk(payload);
  return out;
}
```

### 5.7 Statement PDF queue logic

When processing each message:

```typescript
const isStatementSubject = STATEMENT_KEYWORDS.test(subject);
const isBankSender = BANK_SENDERS.test(from);
const pdfAttachments = findPdfAttachments(m.payload);

if (pdfAttachments.length && (isStatementSubject || isBankSender)) {
  const bank = detectBank(`${subject} ${from}`) ?? null;
  for (const att of pdfAttachments) {
    // skip if already in statement_imports (user_id + gmail_message_id + attachment_id)
    await insert statement_imports {
      user_id, gmail_message_id: id, attachment_id: att.attachmentId,
      file_name: att.filename, bank_source: bank, status: 'pending'
    };
    statementsQueued++;
  }
  // continue to also try parsing email body for summary txn
}
```

### 5.8 Bank detection from text

```typescript
function detectBank(text: string): string | null {
  if (/hbl|habib bank/i.test(text)) return "HBL";
  if (/meezan/i.test(text)) return "Meezan";
  if (/\bubl\b|united bank/i.test(text)) return "UBL";
  if (/\bmcb\b/i.test(text)) return "MCB";
  if (/allied|\babl\b/i.test(text)) return "Allied";
  if (/jazzcash|jazz cash/i.test(text)) return "JazzCash";
  if (/easypaisa|easy ?paisa/i.test(text)) return "Easypaisa";
  if (/sadapay/i.test(text)) return "SadaPay";
  if (/nayapay/i.test(text)) return "NayaPay";
  if (/faysal/i.test(text)) return "Faysal";
  if (/standard chartered|\bscb\b/i.test(text)) return "SCB";
  return null;
}
```

### 5.9 Transaction email parsing order

```typescript
let parsed = parseFinanceEmail(subject, from, body);  // deterministic first
if (!parsed) {
  parsed = await aiExtract(body, subject, from, AI_API_KEY);  // AI fallback
}
if (!parsed.is_transaction || !parsed.amount) skip;
```

### 5.10 Gmail-wins dedupe vs SMS

```typescript
// Find SMS transaction with same amount ±24h, no gmail_message_id
const lo = occurredAt - 86400_000;
const hi = occurredAt + 86400_000;
const dupes = await transactions
  .eq("user_id", userId)
  .eq("amount", amount)
  .gte("occurred_at", lo)
  .lte("occurred_at", hi)
  .is("gmail_message_id", null);

const smsMatch = dupes.find(d => d.source === "sms");
if (smsMatch) UPDATE transaction with Gmail data;  // replaced++
else INSERT new transaction;                         // imported++
```

### 5.11 Trigger PDF parsing after sync

```typescript
if (pendingStatementCount > 0 || statementsQueued > 0) {
  fetch(`${SUPABASE_URL}/functions/v1/parse-statement`, {
    method: "POST",
    headers: { Authorization: authHeader, "Content-Type": "application/json" },
    body: JSON.stringify({ trigger: "gmail-sync" }),
  });  // fire-and-forget
}
await update gmail_connections.last_synced_at = now();
```

**Response JSON:**
```json
{ "ok": true, "scanned": 500, "processed": 120, "imported": 15, "replaced": 3, "skipped": 90, "errors": 2, "statementsQueued": 1 }
```

---

## 6. Email Parser (Deterministic)

**File:** `supabase/functions/gmail-sync/email-parser.ts`

**Return type:**
```typescript
type ParsedEmail = {
  is_transaction: boolean;
  amount: number;
  merchant: string;
  type: "debit" | "credit" | "transfer";
  occurred_at?: string;
  bank_source: string;
  category: string;
  confidence: number;
  explanation: string;
};
```

**Entry point:** `parseFinanceEmail(subject, from, body)` tries in order:
1. `parseEasypaisa`
2. `parseNayaPay`
3. `parseABL`
4. `parseJazzCash`
5. returns `null` → caller uses AI

### 6.1 Category picker (shared helper)

```typescript
function pickCategory(text: string, merchant: string): string {
  const t = `${text} ${merchant}`.toLowerCase();
  if (/salary|payroll|wages/.test(t)) return "Salary";
  if (/food|restaurant|kfc|mcdonald|foodpanda|cheetay|pizza|burger|cafe/.test(t)) return "Food";
  if (/uber|careem|indrive|bykea|fuel|petrol|psoshell|psogas/.test(t)) return "Transport";
  if (/electric|k-?electric|sui|gas|wapda|ptcl|internet|nayatel|stormfiber|bill/.test(t)) return "Bills";
  if (/daraz|amazon|aliexpress|shop|store|mart/.test(t)) return "Shopping";
  if (/transfer|raast|ibft|wallet|easypaisa|jazzcash|nayapay|sadapay/.test(t)) return "Transfers";
  if (/loan|emi|installment/.test(t)) return "Loans";
  if (/netflix|spotify|youtube|prime|disney|oracle|google|apple|microsoft|subscription/.test(t)) return "Entertainment";
  if (/pharma|hospital|clinic|doctor|medic/.test(t)) return "Health";
  if (/school|college|university|tuition|course|udemy/.test(t)) return "Education";
  return "Other";
}
```

### 6.2 Easypaisa email parser

**Trigger:** `/easypaisa|telenor ?bank|telenorbank/i` in from+subject

**Amount regex:**
```
/(?:Transfer amount|Total|Amount(?: Paid)?)\s*Rs\.?\s*([\d,]+(?:\.\d+)?)/i
OR /Rs\.?\s*([\d,]+(?:\.\d+)?)\s*on\s*\d{1,2}-\w{3}-\d{4}/i
```

**Date regex:** `/(\d{1,2})-(\w{3})-(\d{4})\s+(\d{2}):(\d{2}):(\d{2})/`

**Merchant:** `Receiver Name\s+([A-Z][A-Z\s.]+?)` OR `Money Transfer to (\w+)`

**Type:** credit if `/received|credited|deposit/i` else debit

**Confidence:** 98

### 6.3 NayaPay email parser

**Trigger:** `/nayapay/i` in from+subject

**Amount:** `/Total Amount\s*Rs\.?\s*([\d,]+(?:\.\d+)?)/i` OR first `Rs. X`

**Reversal detection:** `/reversed|reversal|unsuccessful|failed/i` → type credit

**Merchant:** `/at\s+([A-Z0-9][A-Z0-9 .&'-]{2,60}?)\s+(?:was|reversed|Reversal|on)/`

**Date:** `/(\d{1,2})\s+(\w{3})\s+(\d{4}),\s+(\d{2}):(\d{2})\s*(AM|PM)/i`

**Confidence:** 96

### 6.4 Allied Bank (myABL) parser

**Trigger:** `/abl\.com|allied|myabl/i`

**Amount:** `/PKR\s*([\d,]+(?:\.\d+)?)\s*(?:have been|has been)\s*(sent|credited|debited|received)/i`

**Merchant:** `Beneficiary Name\s*:?\s*([A-Z][A-Z\s.]+?)` OR `Transaction Description\s*:?\s*([A-Za-z ]+?)`

**Category:** always "Transfers"

**Confidence:** 97

### 6.5 JazzCash parser

**Trigger:** `/jazzcash|jazz cash/i`

**Amount:** first `/Rs\.?\s*([\d,]+(?:\.\d+)?)/`

**Merchant:** hardcoded "JazzCash"

**Confidence:** 90

---

## 7. Gmail AI Fallback Prompt

**Function:** `aiExtract(body, subject, from, apiKey)`

**Endpoint:** `POST https://ai.gateway.lovable.dev/v1/chat/completions`
**Model:** `google/gemini-2.5-flash`
**Tool choice:** forced function `extract`

### System prompt (exact):

```
You extract financial transactions from emails for Pakistani users. Currency is PKR. Categories: Food, Transport, Bills, Shopping, Transfers, Loans, Salary, Entertainment, Health, Education, Savings, Other. Be DECISIVE: for well-known PK merchants (Foodpanda, Telenor, Easypaisa, KFC, Daraz, K-Electric, etc.) give 90-99% confidence. If the email is NOT a real single financial transaction (marketing, OTP only, or a statement summary which will be parsed separately from PDF), set is_transaction=false.
```

### User message:

```
From: {from}
Subject: {subject}

Body:
{rawText}
```

### Tool schema `extract`:

```json
{
  "type": "function",
  "function": {
    "name": "extract",
    "parameters": {
      "type": "object",
      "properties": {
        "is_transaction": { "type": "boolean" },
        "amount": { "type": ["number", "null"] },
        "merchant": { "type": ["string", "null"] },
        "type": { "type": ["string", "null"], "enum": ["debit", "credit", "transfer", null] },
        "occurred_at": { "type": ["string", "null"] },
        "bank_source": { "type": ["string", "null"] },
        "category": { "type": "string", "enum": ["Food","Transport","Bills","Shopping","Transfers","Loans","Salary","Entertainment","Health","Education","Savings","Other"] },
        "confidence": { "type": "number" },
        "explanation": { "type": "string" }
      },
      "required": ["is_transaction", "category", "confidence", "explanation"],
      "additionalProperties": false
    }
  }
}
```

**Error handling:** 429 → throw "rate-limit", skip message (don't kill whole sync)

---

## 8. PDF Statement Edge Function

**File:** `supabase/functions/parse-statement/index.ts` + `easypaisa-parser.ts`

**Endpoint:** `POST /functions/v1/parse-statement`
**Body (optional):** `{ "statement_id": "uuid", "trigger": "continue" | "gmail-sync" | "manual" }`

### 8.1 Constants

```typescript
const CATEGORIES = ["Food","Transport","Bills","Shopping","Transfers","Loans","Salary","Entertainment","Health","Education","Savings","Other"];
const MAX_PAGES = 500;
const CHUNK_SIZE = 16;           // pages per AI/parsing chunk
const MAX_CHUNKS_PER_RUN = 8;    // resume after this many chunks
const STALE_PROCESSING_MS = 2 * 60_000;  // reclaim stuck "processing" rows
const PROGRESS_PREFIX = "__progress__:";
```

### 8.2 Queue selection logic

Pick statements where:
- `user_id = current user`
- If `statement_id` in body → that specific row
- Else → rows with:
  - `status IN ('pending', 'failed')`
  - OR `status = 'processing' AND updated_at < now - 2min` (stale reclaim)
- Order by `updated_at ASC`, limit 2 per run

### 8.3 PDF byte sources

```typescript
if (statement.storage_path) {
  bytes = await admin.storage.from("statements").download(storage_path);
} else if (statement.gmail_message_id && statement.attachment_id) {
  token = await getGmailAccessToken(userId);  // refresh if needed
  bytes = await downloadFromGmail(messageId, attachmentId, token);
} else {
  throw "Statement has no source file attached";
}
```

### 8.4 PDF text extraction (unpdf)

```typescript
import { extractText, getDocumentProxy } from "https://esm.sh/unpdf@0.12.1";

async function pdfToPages(bytes: Uint8Array) {
  const pdf = await getDocumentProxy(bytes);
  const { text, totalPages } = await extractText(pdf, { mergePages: false });
  const pages = (Array.isArray(text) ? text : [String(text ?? "")])
    .map(page => page.replace(/[ \t]+/g, " ").replace(/\n{3,}/g, "\n\n").trim());
  return {
    pages: pages.slice(0, MAX_PAGES),
    totalPages: Math.min(totalPages, MAX_PAGES),
    wasTrimmed: totalPages > MAX_PAGES,
  };
}
```

**Empty PDF check:** filter pages with `length > 20`. If none:
```
status = failed
error = "PDF appears empty or scanned. OCR support is not available yet for this statement."
```

### 8.5 Easypaisa fast path detection

```typescript
const fullText = pages.join("\n");
const easypaisaMode = isEasypaisaText(fullText);

function isEasypaisaText(text: string): boolean {
  const hits = [
    /easypaisa/i.test(text),
    /Transaction ID\s*\|\s*Amount\s*\|\s*Tax\s*\|\s*Fees\s*\|\s*Discount\s*\|\s*Total/i.test(text),
    /STATEMENT OF ACCOUNT/i.test(text),
    /TMFB/i.test(text),
  ].filter(Boolean).length;
  return hits >= 2;  // need at least 2 signals
}
```

If Easypaisa detected → use `parseEasypaisaText(chunkText)` — **no AI call**.

### 8.6 Chunk processing loop

For each chunk of 16 pages:

1. Skip if chunk text (no whitespace) < 50 chars
2. Parse rows:
   - Easypaisa → `parseEasypaisaText(chunkText)`
   - Other → `aiStructureStatement(chunkText, fileName, bank, API_KEY)`
3. Normalize rows: validate date, abs(amount), default merchant "Statement entry"
4. Batch dedup against existing transactions in date range ±24h
5. Batch insert into `transactions`
6. Fire-and-forget `embed-document` for RAG (optional)
7. Update progress in `statement_imports.error` as encoded JSON

**Resume:** if `chunksProcessedThisRun >= 8` and more pages remain:
- Set `status = pending`, save `nextPageIndex` in progress
- Fire `parse-statement` again with `{ statement_id, trigger: "continue" }`

**Done:** if `inserted > 0` → `status = done`, trigger `ai-insights`
**Failed:** if `inserted === 0` → `status = failed`

### 8.7 Dedup key for statement rows

```typescript
const merchKey = merchant.toLowerCase().replace(/[^a-z0-9]+/g, "").slice(0, 24);
const key = `${Math.round(amount * 100)}|${Math.floor(timestamp / 60_000)}|${merchKey}`;
```

Skip if key exists in DB or already inserted in same chunk.

### 8.8 Transaction insert payload

```typescript
{
  user_id,
  amount,
  currency: "PKR",
  merchant,
  type,                          // debit | credit | transfer
  source: "gmail" | "manual",
  category_id,                   // mapped from category name
  occurred_at: ISO string,
  bank_source: detectedBank,
  confidence,
  explanation: `Extracted from ${bank} statement (${file_name})`,
  raw_input: `Statement row: ${merchant} ${amount} ${type}`,
  gmail_message_id: statement.gmail_message_id ?? null,
}
```

---

## 9. Easypaisa PDF Parser (Deterministic)

**File:** `supabase/functions/parse-statement/easypaisa-parser.ts`

### Expected PDF text layout

```
Apr 23, 2026
06:57 AM
Bundles - Ufone - UPower Rs120 All In One through APP 502.28 - (120.00) 382.28
Transaction ID | Amount | Tax | Fees | Discount | Total
48868080055 120.00 0.0 0.00 0.00 120.00
```

### Parsing algorithm

1. Split text into lines, normalize whitespace
2. Find date line: `/^([A-Za-z]{3}\s+\d{1,2},\s+\d{4})$/` OR `/^(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})$/`
3. Next line must be time: `/^(\d{1,2}:\d{2}\s*(AM|PM))$/i`
4. Look ahead 1-4 lines for description with trailing balance numbers:
   ```
   /([\d,]+\.\d{2}|-)\s+(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2}|-)\s+(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2}|-)\s+([\d,]+\.\d{2})\s*$/
   ```
   Groups: opening, incoming, outgoing, closing
5. Merchant = text before trailing numbers, strip `through APP`
6. Skip if `/balance b\/f/i`
7. Amount = outgoing if > 0 (debit), else incoming (credit)
8. Category from `categorize(merchant, type)` regex rules
9. Confidence: 96

### Easypaisa category rules

```typescript
/raast|p2p|money transfer|bank transfer/ → Transfers
/ufone|jazz|telenor|zong|bundle|upower|mobile load/ → Bills
/food|restaurant|kfc|grocery|mart/ → Food
/uber|careem|fuel|petrol/ → Transport
/electric|gas|wapda|ptcl|internet/ → Bills
/loan|emi/ → Loans
/savings|investment/ → Savings
type === "credit" → Salary (default)
else → Other
```

---

## 10. PDF AI Parser Prompt (Non-Easypaisa Banks)

**Function:** `aiStructureStatement(text, fileName, bank, apiKey)`

**Endpoint:** `POST https://ai.gateway.lovable.dev/v1/chat/completions`
**Model:** `google/gemini-2.5-flash`
**Tool choice:** forced function `emit_rows`

### System prompt (exact):

```
You are FinGuard's Pakistani bank/wallet statement parser. Currency is PKR.
Categories: Food, Transport, Bills, Shopping, Transfers, Loans, Salary, Entertainment, Health, Education, Savings, Other.

STATEMENT FORMATS you must handle:
- Easypaisa: Date | Transaction Detail | Opening Balance | Incoming | Outgoing | Closing Balance.
- The next line can contain Transaction ID | Amount | Tax | Fees | Discount | Total.
- HBL / UBL / MCB / Meezan and similar banks: Date | Description | Withdrawal | Deposit | Balance.
- Ignore opening balances, closing balances, headers, disclaimers and totals.

RULES:
- Extract every real transaction row from the provided pages only.
- Keep dates as YYYY-MM-DD.
- Amount must be positive PKR.
- Choose a category decisively.
- Do not invent transactions.
```

### User message:

```
Bank: {bank ?? "unknown"}. File: {fileName}.

Chunk text:
--- PAGE 1 ---
{page1 text}

--- PAGE 2 ---
{page2 text}
...
```

Max text slice: 120,000 chars per chunk.

### Tool schema `emit_rows`:

```json
{
  "type": "function",
  "function": {
    "name": "emit_rows",
    "description": "Return all extracted transaction rows from the provided statement pages.",
    "parameters": {
      "type": "object",
      "properties": {
        "rows": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "occurred_at": { "type": "string" },
              "amount": { "type": "number" },
              "type": { "type": "string", "enum": ["debit", "credit", "transfer"] },
              "merchant": { "type": "string" },
              "category": { "type": "string", "enum": ["Food","Transport","Bills","Shopping","Transfers","Loans","Salary","Entertainment","Health","Education","Savings","Other"] },
              "confidence": { "type": "number" }
            },
            "required": ["occurred_at", "amount", "type", "merchant", "category", "confidence"],
            "additionalProperties": false
          }
        }
      },
      "required": ["rows"],
      "additionalProperties": false
    }
  }
}
```

### AI error handling

| Status | Action |
|--------|--------|
| 429 rate-limit | Set status `pending`, save progress, user retries later |
| 402 credits-exhausted | Same as 429 with note "Add credits" |
| Other error | Set status `failed`, store error message |

---

## 11. Progress Tracking

Progress stored in `statement_imports.error` as:

```
__progress__:{json}
```

**JSON shape:**
```json
{
  "stage": "Parsing transactions",
  "processedPages": 32,
  "totalPages": 80,
  "processedChunks": 2,
  "totalChunks": 5,
  "inserted": 45,
  "nextPageIndex": 32,
  "note": "Pages 17-32"
}
```

**Frontend decode** (`src/lib/statement-progress.ts`):
- `decodeStatementProgress(error)` → parse JSON after prefix
- `getStatementProgressPercent()` → `processedChunks / totalChunks * 100`
- `getStatementProgressText()` → human-readable status line

---

## 12. Frontend Integration

### 12.1 Manual PDF upload (`settings.tsx`)

```typescript
async function handleFile(file: File) {
  // validate: PDF only, max 10MB
  const path = `${user.id}/${crypto.randomUUID()}.pdf`;
  await supabase.storage.from("statements").upload(path, file, { contentType: "application/pdf" });

  const { data: row } = await supabase.from("statement_imports").insert({
    user_id: user.id,
    file_name: file.name,
    bank_source: bank.trim() || null,
    status: "pending",
    source: "upload",
    storage_path: path,
  }).select("id").single();

  // fire-and-forget
  fetch(`${VITE_SUPABASE_URL}/functions/v1/parse-statement`, {
    method: "POST",
    headers: { Authorization: `Bearer ${session.access_token}` },
    body: JSON.stringify({ statement_id: row.id }),
  });
}
```

### 12.2 Gmail connect button

```typescript
window.location.href = `/api/gmail/connect?token=${session.access_token}&days=${backfillDays}`;
```

### 12.3 Auto sync hook (`use-gmail-sync.ts`)

```typescript
const AUTO_SYNC_INTERVAL_MS = 5 * 60 * 1000;

async function syncNow() {
  await fetch(`${VITE_SUPABASE_URL}/functions/v1/gmail-sync`, {
    method: "POST",
    headers: { Authorization: `Bearer ${session.access_token}` },
    body: JSON.stringify({ trigger: "manual" }),
  });
}

// On mount + every 5 min if connection exists
useEffect(() => {
  if (connection && Date.now() - lastSync >= 5min) syncNow({ silent: true });
  setInterval(() => syncNow({ silent: true }), 5 * 60 * 1000);
}, [connection]);
```

### 12.4 Retry failed statement (dashboard)

```typescript
fetch(`${VITE_SUPABASE_URL}/functions/v1/parse-statement`, {
  method: "POST",
  headers: { Authorization: `Bearer ${session.access_token}` },
  body: JSON.stringify({ statement_id: failedRow.id }),
});
```

---

## 13. Files to Copy Into Your Project

| Source file | Purpose |
|-------------|---------|
| `supabase/functions/gmail-sync/index.ts` | Gmail sync main logic |
| `supabase/functions/gmail-sync/email-parser.ts` | Deterministic email parsers |
| `supabase/functions/parse-statement/index.ts` | PDF parsing main logic |
| `supabase/functions/parse-statement/easypaisa-parser.ts` | Easypaisa PDF parser |
| `src/routes/api.gmail.connect.ts` | OAuth start |
| `src/routes/api.gmail.callback.ts` | OAuth callback |
| `src/hooks/use-gmail-sync.ts` | Client sync hook |
| `src/lib/statement-progress.ts` | Progress UI helpers |
| `supabase/migrations/*gmail*` | DB schema for gmail |
| `supabase/migrations/*statement*` | DB schema for statements + storage |

---

## 14. Production Migration Checklist

When moving off Lovable platform:

### Replace AI gateway

Change all:
```
https://ai.gateway.lovable.dev/v1/chat/completions
Authorization: Bearer LOVABLE_API_KEY
model: google/gemini-2.5-flash
```

To direct Google AI:
```
https://generativelanguage.googleapis.com/v1beta/openai/chat/completions
Authorization: Bearer GOOGLE_AI_API_KEY
model: gemini-2.5-flash
```

Same prompts and tool schemas work unchanged. Tool calling format is OpenAI-compatible.

### Gmail OAuth production

1. Create Google Cloud project
2. Enable Gmail API
3. Configure OAuth consent screen
4. Add authorized redirect URI: `{your-domain}/api/gmail/callback`
5. Submit for verification if using `gmail.readonly` with external users

### Known limitations

| Limitation | Workaround |
|------------|------------|
| Scanned/image PDFs | Add OCR (Google Document AI, Tesseract) before text parsing |
| Only Easypaisa has deterministic PDF parser | Add more bank-specific parsers OR rely on AI |
| JazzCash email parser uses generic merchant | Improve regex for JazzCash templates |
| AI rate limits on large statements | Chunking + resume already built in |
| Lovable credits (402) | Use your own AI API key with billing |

---

## 15. End-to-End Flow Summary

### Gmail transaction email
```
User connects Gmail
→ auto-sync every 5 min
→ gmail-sync lists finance emails since last_sync - 48h
→ skip already imported message IDs
→ for each message (max 120):
    if PDF + (statement keyword OR bank sender):
        queue statement_imports → trigger parse-statement
    parse body with email-parser.ts
    if null → AI extract with Gemini prompt
    if is_transaction → insert/update transaction (Gmail wins over SMS)
→ update last_synced_at
```

### PDF statement (Gmail or upload)
```
statement_imports row created (status=pending)
→ parse-statement picks up row
→ download PDF bytes (storage or Gmail attachment)
→ unpdf extracts text per page
→ if Easypaisa text detected:
    parseEasypaisaText() per 16-page chunk
  else:
    aiStructureStatement() with Gemini + emit_rows tool
→ dedup + batch insert transactions
→ if more pages: save progress, re-trigger self
→ if done: status=done, trigger ai-insights
```

---

## 16. Why It Works Well (Design Decisions)

1. **Deterministic first, AI second** — known PK bank email formats parsed with regex (98% confidence, zero cost)
2. **Easypaisa PDF fast path** — no AI needed for most common wallet statements
3. **Structured tool calls** — forces JSON output, not free-text parsing
4. **PK-specific prompts** — names HBL/UBL/MCB/Meezan column layouts explicitly
5. **48h sync overlap** — never misses emails at boundary
6. **Gmail wins dedupe** — upgrades SMS entries with richer Gmail merchant data
7. **Chunked PDF processing** — handles 100+ page statements without timeout
8. **Resumable progress** — encoded in DB, survives function restarts
9. **Statement vs transaction split** — PDF statements queued separately, email body not double-counted as full statement
