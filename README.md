# Reservation Ingestion Service

Accepts hotel/property reservations, validates them, and stores them asynchronously
via a RabbitMQ-backed job queue. See `task.md` for the full spec.

## Stack
- API: FastAPI (uvicorn)
- DB: PostgreSQL (SQLAlchemy 2 async + asyncpg)
- Queue: RabbitMQ (aio-pika)

## Run

```bash
podman compose up -d --build
# (or: podman-compose up -d --build)
```

Podman notes:
- Images are referenced fully-qualified (`docker.io/library/...`) because podman has
  no unqualified-search registries by default.
- **After code changes, run `podman compose build` before `up -d`** — podman-compose
  1.3 does not rebuild an image that already exists.
- RabbitMQ gets a named volume at `/var/lib/rabbitmq`; under rootless podman the
  image fails with `.erlang.cookie: eacces` without it.
- RabbitMQ MUST have a fixed `hostname: rabbitmq` in compose.yaml (it does): its
  node name is `rabbit@<hostname>` and without it every container recreation
  gets a new hash hostname, boots with an empty mnesia store and silently
  discards all durable queues + messages (verified: a dozen orphaned
  `rabbit@<hash>` dirs piled up in the volume).

Services: `api` (:8000), `worker` (queue consumer), `db` (:5432),
`rabbitmq` (:5672, management UI :15672 — guest/guest),
`dashboard` (:8080, static demo UI — see below).

## Scenario dashboard

Open <http://localhost:8080/> — a static page in its **own container** (`dashboard`:
nginx serving `dashboard/index.html` and proxying `/api/` → `api:8000`, same-origin,
no CORS). It is a demo UI, not part of the system: it never *causes* state, and it
degrades instead of breaking — during a DB outage the jobs/reservations panels show
a neutral "database unavailable — cooling messages down" state (the API answers
**503**, not 500), the header tracks `API ● / MQ ● / DB ●`, and a live
**Queues panel** (fed by `GET /api/queues`, RabbitMQ passive declares, **no DB
involved**) shows messages sitting in main / retry / DLQ even while everything
DB-backed is unreadable. The page itself loads even when the API is down.

Implemented scenarios (see the `SCENARIOS` registry in `dashboard/index.html`):

- **Happy Path** — submit → poll → `completed`.
- **Validation error** — three invalid payloads, each must 422 without creating a job.
- **Duplicate submission** — same payload twice: second job → `duplicate`, correlated
  with the original reservation.
- **Transient failure & retry** — guided full-DB-outage test (§4.2 below): the old
  "stop db → run → start db" order *cannot* work (the API must commit the job row
  before publishing, so nothing can be admitted while the DB is down). The scenario
  walks through: `stop worker` → Run (job committed, message parked in the main
  queue) → `stop db` → `start worker` (the message nacks into the retry queue and
  cools down for the 10-min queue-level TTL) → `start db` → wait ≤ 10 min for the
  TTL to expire. PASS = pending stall + retry-queue cool-down observed, then
  completion — nothing lost. (The worker deliberately has no `depends_on: db` in
  compose.yaml — podman turns that into a container-level dependency, so starting
  the worker would silently resurrect the DB and the outage could never be
  observed. The worker boots fine without the DB: `init_db` retries ~30 s and
  messages cool down in the retry queue.)
- **Ingest during DB outage** — stop the DB, then submit: the API answers
  `202 "queued"` (queue-first admission — the message carries the full job spec
  and becomes the durable record) even for an identical second submission. After
  `start db` the worker materializes both job rows: one `completed`, one
  `duplicate` pointing at the same reservation — nothing lost, nothing stored
  twice.
- **Permanent failure** — `num_guests = 10²⁰` passes API validation but overflows the
  INTEGER column (SQLSTATE 22xxx) → job `failed` immediately with 0 retries.
- **Dead letter & requeue** — replays the newest `dead_lettered` job via the requeue
  endpoint (the jobs table also has per-row **Requeue** buttons).
- **Webhook callback** — posts with `callback_url` pointing at the built-in sink
  (`POST/GET /hooks/sink`, in-memory, last 50) and verifies the terminal-state
  callback arrives with the right body. The worker reaches the sink via the compose
  service name `http://api:8000/hooks/sink`.
- **Bulk: partial failure** — POSTs 5 items to `/reservations/bulk`, item #3 flagged
  `simulate_error=transient` (fails once, retried via the retry queue), then a blind
  re-ingest of the whole batch must come back `duplicate`: exactly 5 unique
  reservations, nothing missed, nothing stored twice (§10).
- **Bulk: parallel submissions** — no bulk endpoint: 5 concurrent `POST /reservations`
  calls, each with distinct dates + a per-run marker (distinct idempotency keys).
  All must be admitted (202), all jobs must complete independently, and exactly
  5 unique reservations may exist (§10.4).

A **⟲ Reset state** button in the header wipes all demo state for a fresh run:
`POST /admin/reset` purges every queue (main / retry / DLQ), truncates the
`jobs` and `reservations` tables and clears the webhook sink, returning what was
removed. Queues are purged before the truncate so no queued message can
re-create rows afterwards; a message being processed at that exact moment may
still land — acceptable for the demo stack.

## API

```bash
# Submit a reservation
curl -i -X POST http://localhost:8000/reservations \
  -H 'Content-Type: application/json' \
  -d '{"checkin":"2026-07-01","checkout":"2026-07-05","num_guests":2,
       "guest_names":["Ada","Linus"],"callback_url":"http://host.containers.internal:9000/hook"}'
# -> 202 {"job_id":"...","state":"pending"}  (validation failures -> 422)
# -> with the DB down: 202 {"job_id":"...","state":"queued"} (queue-first
#    admission; handled once the DB returns — see Failure strategy)

# Poll a job
curl http://localhost:8000/jobs/<job_id>

# Live queue depths (RabbitMQ passive declares — no DB; works during an outage)
curl http://localhost:8000/queues

# List jobs by state
curl 'http://localhost:8000/jobs?state=dead_lettered'

# Replay a dead-lettered/failed job after fixing the underlying issue
curl -X POST http://localhost:8000/jobs/<job_id>/requeue

# Bulk-submit: per-item admission (207 Multi-Status), one job per item.
# `simulate_error` is a demo-only fault injector ("transient" fails the first
# delivery attempt; "permanent" fails immediately) — not stored, not part of
# the idempotency key.
curl -i -X POST http://localhost:8000/reservations/bulk \
  -H 'Content-Type: application/json' \
  -d '[{"checkin":"2026-07-01","checkout":"2026-07-05","num_guests":2,"guest_names":["Ada","Linus"]},
       {"checkin":"2026-07-02","checkout":"2026-07-04","num_guests":1,"guest_names":["Solo"],"simulate_error":"transient"}]'
# Re-submitting the same batch is safe: stored items come back as duplicate jobs.

# Demo reset (also the ⟲ Reset state button on the dashboard): purge all queue
# messages (main/retry/DLQ), truncate the jobs + reservations tables and clear
# the in-memory webhook sink.
curl -X POST http://localhost:8000/admin/reset
```

For callbacks from containers to your host, use `host.containers.internal` as the
hostname (rootless podman). A quick local webhook receiver:
`python -m http.server 9000` logs every POST.

## Queue topology

```
main exchange ──> reservations.jobs ──(transient failure)──> retry exchange
      ▲                                                          │
      │ (TTL expired: dead-lettered back)                        ▼
      └─────────────────────────────────────────── reservations.retry (per-message TTL)

poison / exhausted messages ──> reservations.dlx ──> reservations.dlq
```

## Failure strategy

- **Transient failures** (DB connection errors, timeouts, deadlocks): a retry copy
  is published to the retry queue with `expiration = min(base * 2^n, cap) + jitter`,
  then the message is ACKed — the worker never sleeps in-process, so it stays free
  and retries survive restarts. `retry_count` lives in the job row *and* message
  headers. After `max_retries` the job is `dead_lettered` and parked in the DLQ.
  During a *full* DB outage the worker cannot even read the job row; such messages
  nack into the retry queue where the queue-level TTL (10 min) acts as a cool-down
  before redelivery — nothing is lost. This also holds for a DB outage at *boot*:
  the worker waits briefly for the schema, then consumes anyway (nacking into the
  retry queue) instead of crash-looping until the DB returns.
- **Queue-first admission (ingesting while the DB is down)**: `POST /reservations`
  (and `/reservations/bulk`, per item) still answers **202** when the DB is
  unreachable — with `state: "queued"` — and publishes the *full job spec*
  (`job_id`, `idempotency_key`, `payload`, `callback_url`) instead of the bare
  payload. The worker materializes the job row on the first delivery where the DB
  is reachable (messages cool down in the retry queue until then) and proceeds
  normally. Duplicates admitted during the outage are arbitrated by the UNIQUE
  constraint at insert time: one `completed`, the other `duplicate` — nothing
  lost, nothing stored twice. `GET /jobs/{id}` returns 404 until the row exists.
- **Permanent failures** (bad payload, constraint violations other than duplicate):
  job marked `failed` immediately, no retries.
- **Idempotency**: `reservations.idempotency_key` has a UNIQUE constraint; insert +
  state update happen in one transaction and the message is ACKed only after
  commit. `IntegrityError` (duplicate key) marks the job `duplicate` — or completes
  it idempotently if the same job was redelivered after a crash. At-least-once
  delivery, exactly-once effect.
- **API during a DB outage**: DB-backed endpoints return **503** "database
  unavailable; try again" (an app-level `DBAPIError` handler), never an ambiguous
  500 — submission, listing and requeue all become retryable instead of looking
  broken.
- **Stuck jobs**: a reaper requeues rows stuck in `processing` beyond the lease
  timeout (300s).
- **Webhooks**: delivered out-of-band with exponential backoff (5 attempts,
  10s timeout); exhausted deliveries are dropped and logged with the full payload.

## Known trade-offs (documented scope)

- Per-message TTL on a single retry queue can head-of-line block under heavy
  mixed-delay load; tiered retry queues (or the delayed-message plugin) fix that.
- The API publishes after committing the job row (no outbox pattern): a crash in
  that window leaves a job in `pending`; the requeue endpoint can replay it.
- Schema management is `create_all` (no Alembic migrations) for this scope.
