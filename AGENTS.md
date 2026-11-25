# AGENTS.md

## What this is

Reservation Ingestion Service: FastAPI API + async queue worker, PostgreSQL (SQLAlchemy 2 async + asyncpg), RabbitMQ (aio-pika), plus a scenario dashboard (static page in its own nginx container — demo UI, not part of the system). `task.md` is the spec; `README.md` documents topology, failure strategy, and API examples; `SCENARIOS.md` catalogs the behaviors the dashboard verifies — keep all in sync with behavior.

## Layout

- `app/api.py` — FastAPI app (`/reservations`, `/jobs`, `/jobs/{id}/requeue`, `/queues`, `/admin/reset`). Runs via uvicorn. A DBAPIError handler maps DB outages to 503 (never a bare 500).
- `app/consumer.py` — queue worker (`python -m app.consumer`). Separate process; runs the reaper task.
- `app/mq.py` — RabbitMQ topology (main/retry/DLQ) + publisher. `app/db.py` — engine, `create_all` (no Alembic).
- `app/errors.py` — failure classifier the worker relies on.
- `dashboard/` — `index.html` (all scenarios, self-contained JS) + `nginx.conf` (serves the page, proxies `/api/` → `api:8000`). The browser never talks to the API directly; `/queues` uses passive declares and must stay DB-independent (it is the dashboard's state source during outages).
- No test suite. Quick check: `python3 -m compileall -q app && rm -rf app/__pycache__`. Real verification = running the compose stack and curling the API.

## Podman (not Docker) quirks

- Stack: `podman compose up -d --build`. After code changes you MUST run `podman compose build` first — podman-compose 1.3 does not rebuild an image that already exists, so `up --build` alone silently runs stale code.
- Images in `compose.yaml` are fully qualified (`docker.io/library/...`): podman has no default search registry.
- RabbitMQ needs its named volume (`rabbitmq_data:/var/lib/rabbitmq`); under rootless podman it crashes with `.erlang.cookie: eacces` otherwise.
- RabbitMQ MUST keep a fixed `hostname: rabbitmq` (compose.yaml has it): its node name is `rabbit@<hostname>`, and without the fixed hostname every container recreation gets a new hash hostname, boots an EMPTY mnesia store, and silently discards all durable queues + messages (verified: a dozen orphaned `rabbit@<hash>` dirs piled up in the volume).
- From inside containers, reach host services via `host.containers.internal` (e.g. webhook `callback_url`).
- podman-compose can be slow; long `down`/`up` commands time out in foreground shells — run them in the background.

## aio-pika 9.x trap (pinned `<10` on purpose)

`channel.confirm_delivery()` was removed in aio-pika 9/10. Publisher confirms are enabled by default and `exchange.publish()` *returns* the broker frame — check it is `Ack` (see `Publisher._check_confirmed`). Do not "restore" confirm_delivery. Also: `RobustChannel.declare_queue(passive=True)` on a queue it already knows returns the CACHED queue object with its stale `declaration_result` (counters 0) — read live message counts via `get_underlay_channel().queue_declare(passive=True)` (see `Publisher.queue_stats`).

## Async SQLAlchemy traps

- Never read a column with server-side `onupdate`/`server_default` (e.g. `Job.updated_at`) after `commit()` — it triggers a lazy refresh and raises `MissingGreenlet`. Set `updated_at = _now()` client-side at each state transition (the worker does this everywhere).
- After a `rollback()`, reload the row via `reload_job()` (select with `populate_existing=True`) — expired attribute access from async context raises.

## Consumer correctness rules

- The UNIQUE constraint on `reservations.idempotency_key` is the dedupe guarantee. Insert + job-state update in one transaction; ACK only after commit. `IntegrityError` → `duplicate` (or idempotent re-completion when `existing.job_id == job.id`).
- Failure classification lives in `app/errors.py`: unknown exceptions default to transient; SQLSTATE `22xxx`/`23xxx` (except `23505` duplicate) are permanent. `23505` is handled as duplicate/completed, not an error.
- Queue-first admissions (task.md §1): messages carrying `idempotency_key` + `payload` whose job row does not exist are NOT dead-lettered — `_materialize_job` creates the row from the message (with `simulate_error` stripped) and proceeds. Messages without an idempotency_key and no row stay unprocessable → DLQ.
- Transient retries publish a copy to the retry queue with per-message TTL (`expiration`), then ACK — never sleep/retry in-process. During a full DB outage the handler can't read the job row, so messages nack into the retry queue and get the queue-level TTL (10 min) as cool-down; this is expected, not a stuck message. The same holds at boot: main() gives the schema a ≤15s grace (`asyncio.wait_for(shield(init_db_forever()))`), then consumes anyway — `init_db_forever` keeps retrying in the background. Never gate consumption on the DB.
- Queue declarations in `mq.py` must stay identical everywhere they are re-declared (wrong/changed queue args → RabbitMQ PRECONDITION_FAILED, channel dies). Changing queue TTL requires deleting the queue first.

## Env / infra

- Settings via env (`DATABASE_URL`, `RABBITMQ_URL`) with sane compose defaults; see `app/config.py`.
- Health checks: `GET /health`, and `podman ps` (db/rabbitmq are health-gated via `depends_on`).
- Queue depth debug: `GET /queues` (no DB needed) or `podman exec reservation-ingestion_rabbitmq_1 rabbitmqctl list_queues name messages messages_unacknowledged`.
- Full-DB-outage demo: see SCENARIOS.md §4.2 — `stop worker` → submit → `stop db` → `start worker` → `start db`. "stop db → submit" cannot work (admission needs the DB → 503). The worker intentionally has NO `depends_on: db` in compose.yaml: podman-compose turns depends_on into podman `--requires`, and then any `podman start worker` silently starts the db too, making the outage demo impossible.
