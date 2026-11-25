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

Services: `api` (:8000), `worker` (queue consumer), `db` (:5432),
`rabbitmq` (:5672, management UI :15672 — guest/guest).

## Scenario dashboard

Open <http://localhost:8000/> (or `/dashboard/`) — a static page served by the API
that runs the scenarios from `SCENARIOS.md` against the live stack and verifies the
expected behavior step by step (request/response, state transitions, PASS/FAIL verdict).
Currently implemented: **Happy Path** (submit → poll → `completed`). More scenarios
(dedup, retries, DLQ/requeue, webhook) are listed as planned entries and share the
same runner — see the `SCENARIOS` registry in `app/static/index.html`.

## API

```bash
# Submit a reservation
curl -i -X POST http://localhost:8000/reservations \
  -H 'Content-Type: application/json' \
  -d '{"checkin":"2026-07-01","checkout":"2026-07-05","num_guests":2,
       "guest_names":["Ada","Linus"],"callback_url":"http://host.containers.internal:9000/hook"}'
# -> 202 {"job_id":"...","state":"pending"}  (validation failures -> 422)

# Poll a job
curl http://localhost:8000/jobs/<job_id>

# List jobs by state
curl 'http://localhost:8000/jobs?state=dead_lettered'

# Replay a dead-lettered/failed job after fixing the underlying issue
curl -X POST http://localhost:8000/jobs/<job_id>/requeue
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
  before redelivery — nothing is lost.
- **Permanent failures** (bad payload, constraint violations other than duplicate):
  job marked `failed` immediately, no retries.
- **Idempotency**: `reservations.idempotency_key` has a UNIQUE constraint; insert +
  state update happen in one transaction and the message is ACKed only after
  commit. `IntegrityError` (duplicate key) marks the job `duplicate` — or completes
  it idempotently if the same job was redelivered after a crash. At-least-once
  delivery, exactly-once effect.
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
