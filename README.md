# Transaction Webhook Service

A Python service that receives transaction webhooks from payment processors (like Razorpay),
acknowledges them right away with `202 Accepted`, and processes each transaction reliably in the
background. Every transaction is processed exactly once, even when the same webhook arrives
many times or the server restarts in the middle of processing.

**Live API:** `https://<your-service>.onrender.com` &nbsp;·&nbsp; Interactive docs: `/docs`

> The service runs on Render's free tier, which sleeps after 15 minutes without traffic. The first
> request after a sleep can take up to a minute while the instance wakes. Send a `GET /` first to
> wake it before checking response times. (An uptime monitor pinging `/` every 5 minutes keeps it awake.)

---

## API

### `POST /v1/webhooks/transactions`

```json
{
  "transaction_id": "txn_abc123def456",
  "source_account": "acc_user_789",
  "destination_account": "acc_merchant_456",
  "amount": 1500,
  "currency": "INR"
}
```

Always returns **`202 Accepted`** for a valid payload, including repeats of a transaction already seen:

```json
{ "message": "Webhook accepted", "transaction_id": "txn_abc123def456", "duplicate": false }
```

`duplicate` is `true` when this `transaction_id` was already received. Duplicates do not start new
processing. Invalid payloads (missing fields, `amount <= 0`, more than 2 decimal places, currency
that isn't a 3-letter code) return `422`.

### `GET /v1/transactions/{transaction_id}`

```json
[{
  "transaction_id": "txn_abc123def456",
  "source_account": "acc_user_789",
  "destination_account": "acc_merchant_456",
  "amount": 1500.0,
  "currency": "INR",
  "status": "PROCESSING",
  "created_at": "2024-01-15T10:30:00Z",
  "processed_at": null
}]
```

`status` changes to `PROCESSED` and `processed_at` is filled in about 30 seconds after
`created_at`. Unknown ids return `404`.

### `GET /`

```json
{ "status": "HEALTHY", "current_time": "2024-01-15T10:30:00Z" }
```

Each response also carries an `X-Process-Time-Ms` header showing the server-side handling time.

---

## Running locally

Requires Python 3.11+ and a PostgreSQL database. A free [Neon](https://neon.tech) database works,
and so does any local Postgres.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
cp .env.example .env               # then set DATABASE_URL
uvicorn app.main:app --reload
```

The table is created automatically on startup. Open http://localhost:8000/docs to try the endpoints.

### Configuration

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | (required) | Postgres connection string |
| `PROCESSING_DELAY_SECONDS` | `30` | Simulated external-API time per transaction |
| `LEASE_SECONDS` | `max(2×delay, delay+30)` | How long a worker owns a transaction before another may retry it |
| `SWEEP_INTERVAL_SECONDS` | `10` | How often unfinished transactions are re-checked |
| `RETRY_BACKOFF_SECONDS` | `5` | Wait before retrying a failed attempt |
| `API_POOL_SIZE` / `WORKER_POOL_SIZE` | `15` / `5` | DB connections reserved for requests / for background work |

---

## Testing

### Automated tests

The tests run against a real Postgres database with the delay shortened to 1 second, so they
finish in about 15 seconds. **Use a separate, throwaway database:** the tests drop and re-create
the `transactions` table.

```bash
export TEST_DATABASE_URL="postgresql://..."   # Windows PowerShell: $env:TEST_DATABASE_URL="..."
pytest -v
```

Covered: health check, 202 response time, PROCESSING → PROCESSED transition, 10 concurrent duplicates
creating exactly one transaction processed once, 50 concurrent webhooks staying under 500ms, input
validation, 404s, recovery of a transaction whose worker crashed, and retry after a failed attempt.

### End-to-end check against a running deployment

`scripts/smoke_test.py` runs the four success criteria against any URL, using the real 30-second delay:

```bash
python scripts/smoke_test.py https://<your-service>.onrender.com
python scripts/smoke_test.py http://localhost:8000 --load 100
```

Example output:

```
1. Single transaction
  PASS webhook returned 202 in 21ms
  PASS acknowledged within 500ms
  PASS status is PROCESSING right after the webhook
  PASS PROCESSED after ~30s (created_at=...T04:56:30Z, processed_at=...T04:57:00Z)
2. Duplicate prevention
  PASS all 5 duplicate webhooks returned 202
  PASS exactly one webhook created the transaction (got 1)
3. Performance: 100 concurrent webhooks
  PASS all 100 returned 202
     p50=180ms p95=421ms max=430ms (client-observed, includes network)
...
All checks passed.
```

### Manual checks with curl

```bash
URL=http://localhost:8000

curl -i -X POST $URL/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'

curl $URL/v1/transactions/txn_abc123def456     # PROCESSING
sleep 31
curl $URL/v1/transactions/txn_abc123def456     # PROCESSED
```

Send the same POST several times and you get `202` every time with `"duplicate": true`. The
transaction is still processed only once.

---

## Deployment (Render + Neon)

1. Create a Neon project and copy its connection string.
2. On Render, choose **New → Blueprint** and select this repository. `render.yaml` defines the web service.
3. Set `DATABASE_URL` to the Neon connection string when prompted.
4. After the deploy finishes, run `python scripts/smoke_test.py https://<service>.onrender.com`.

---

## Technical choices

### Stack
- **FastAPI + Uvicorn.** Async request handling, so one process can hold thousands of in-flight
  30-second jobs (they are just sleeping coroutines) and still answer new webhooks in milliseconds.
  Pydantic validates the payload, and OpenAPI docs come for free.
- **PostgreSQL on Neon via asyncpg.** A managed cloud database with transactions, a unique primary
  key for idempotency, and row-level updates for safe concurrent claiming. asyncpg is the fastest
  async Postgres driver for Python.
- **Render.** Deploys straight from GitHub with `render.yaml`, gives HTTPS, and has a free tier.

### How a webhook flows

```
POST webhook ──► validate ──► INSERT … ON CONFLICT DO NOTHING ──► 202 Accepted
                                       │ (new row only)
                                       ▼
                       background task: claim lease ─► wait 30s ─► mark PROCESSED
                                       ▲
     sweeper (every 10s): re-queues PROCESSING rows with no active lease
```

1. **Acknowledge fast.** The request path does one indexed `INSERT` and returns. Processing starts
   as a background asyncio task, so the response time does not depend on processing time. Requests
   and workers use **separate connection pools**, so heavy background load can't hold up a webhook.
   Measured server-side handling is about 10–20ms, and stays there while hundreds of transactions
   are processing.
2. **Idempotency comes from the database.** `transaction_id` is the primary key, and the insert is
   `ON CONFLICT DO NOTHING RETURNING`. When duplicates arrive at the same moment, possibly on
   different instances, exactly one insert succeeds and only that request starts processing. The
   others get a normal `202` with `duplicate: true` rather than an error, which is what payment
   providers expect, since they retry on anything other than 2xx.
3. **Exactly-once processing uses a lease.** Before a worker processes a transaction it runs
   `UPDATE … SET locked_until = now() + lease WHERE status = 'PROCESSING' AND lease expired`.
   Only one worker can win that update. The final update to `PROCESSED` is also conditional, so
   a transaction can never be completed twice.
4. **No lost transactions.** The row is saved *before* the `202` is sent, so an acknowledged
   transaction is always on disk. If the process crashes, is redeployed, or Render puts it to sleep
   mid-processing, its lease expires and the **sweeper** (on the same or another instance) picks it up
   again. If processing raises an error, the error is recorded in `last_error`, the attempt is
   counted in `attempts`, and the transaction is retried after a short backoff. I tested this by
   killing the server 8 seconds into processing: after restart the transaction completed normally.
5. **Status and timing data.** Each row stores `status`, `created_at`, `processed_at`, `attempts`
   and `last_error`, so you can see what happened to every transaction.

### Why not Celery/Redis or a separate queue?
For this workload, Postgres already provides everything a queue needs: durable storage, atomic
claiming, and a clear record of state. Adding a broker would mean more infrastructure to run and a
second source of truth that has to stay in sync with the database. The design scales horizontally
as it is, because every guarantee comes from the database and not from in-process state. If the
processing step became CPU-heavy, or volumes grew very large, the next step would be to move the
worker into its own service (or use SQS / Cloud Tasks) while keeping the same table and lease logic.

### Trade-offs and production notes
- A crashed worker's transaction is retried only after its lease expires (60s by default), so
  recovery after a crash takes up to `lease + sweep interval`. This is the price of never processing
  a transaction twice.
- Real webhooks should verify the provider's signature (for example, Razorpay's
  `X-Razorpay-Signature` HMAC). It is left out here because the assessment payload doesn't include
  one, but it would sit in the request path as a fast dependency.
- If a duplicate webhook arrives with a *different* payload for an existing `transaction_id`, it is
  acknowledged and ignored; the first version received is kept.

---

## Project structure

```
app/
  main.py        FastAPI app, routes, lifespan (pools, schema, sweeper)
  processor.py   background processing, lease handling, recovery sweeper
  db.py          schema and SQL queries
  schemas.py     request/response models and validation
  config.py      environment-based settings
tests/           pytest suite (runs against real Postgres)
scripts/
  smoke_test.py  end-to-end success-criteria check against any URL
render.yaml      Render deployment blueprint
```
