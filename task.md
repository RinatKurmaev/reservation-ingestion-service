# Task: Reservation Ingestion Service

A service that receives hotel/property booking (reservation) data, validates it,
and stores it in a database asynchronously via a RabbitMQ-backed job queue.

## Stack
- **API:** FastAPI (Python)
- **Database:** PostgreSQL (via SQLAlchemy / asyncpg)
- **Queue:** RabbitMQ (via aio-pika)

## Reservation schema
| Field         | Type      | Required | Notes                          |
|---------------|-----------|----------|--------------------------------|
| `checkin`     | date      | yes      | Check-in date                  |
| `checkout`    | date      | yes      | Check-out date                 |
| `num_guests`  | int       | yes      | Number of guests, >= 1         |
| `guest_names` | list[str] | yes      | One name per guest             |
| `callback_url`| url       | no       | Webhook target on completion   |

## 1. Endpoint — `POST /reservations`
Accepts a single reservation (schema above).

1. **Validate** (minimal rules):
   - `checkout` must be after `checkin`
   - `num_guests` >= 1
   - `guest_names` must not be empty
2. On validation failure → `422` with error details.
3. On success → publish the reservation payload to the RabbitMQ queue and return
   `202 Accepted` with the generated `job_id` immediately (do not wait for storage).
4. **DB outage exception (queue-first admission):** if the job record cannot be
   committed because PostgreSQL is unreachable, the API still returns
   `202 Accepted` with `state: "queued"` and publishes the **full job spec**
   (`job_id`, `idempotency_key`, `payload`, `callback_url`). The queue message
   is the durable record; the worker materializes the job row on the first
   delivery where the DB is reachable (before that, messages cool down in the
   retry queue, §4.2). `GET /jobs/{job_id}` returns 404 until the row exists.
   Duplicates admitted during the outage are still arbitrated by the UNIQUE
   constraint on `reservations.idempotency_key` at insert time.
5. Optional: `GET /jobs/{job_id}` to poll job state (useful when no callback_url).

## 2. Queue handler

### Topology
- Main queue: `reservations.jobs` (durable, explicit manual acks, `prefetch_count` = 1–10).
- Retry queue: `reservations.retry` — no consumers, per-message TTL, dead-letters
  back into `reservations.jobs`. Redelivery delay = `min(base * 2^retry, cap) + jitter`.
- Dead-letter queue: `reservations.dlq` — terminal parking lot for poison messages
  and exhausted retries.

### Producer (after publish)
- Compute an idempotency key: SHA-256 hash of the canonical reservation payload
  (`checkin`, `checkout`, `num_guests`, `guest_names`).
- Create a **job record** in the DB with:
  - `id` (job_id), `idempotency_key`, `payload`, `state`, `retry_count`,
    `last_error`, `created_at`, `updated_at`
- Job **state machine (full lifecycle)**:
  `pending → processing → completed | failed | duplicate | dead_lettered`
  - `pending → processing`: claimed by a worker (transition persisted *before*
    handling the message).
  - Terminal states: `completed`, `failed`, `duplicate`, `dead_lettered`.
- Publish the message **after** the job record commits (message carries `job_id`,
  `retry_count` in headers).

### Consumer (worker)
- Consume job messages from `reservations.jobs` with manual acks.
- On receipt: mark job `processing` (unless already past a terminal state), then:
  - Insert the reservation and update the job state **in a single DB transaction**,
    ACK only after commit:
    - `reservations.idempotency_key` has a **UNIQUE constraint**. On
      `IntegrityError` → the reservation already exists → mark job `duplicate`,
      ACK. (The constraint is the dedupe guarantee — no separate check-then-insert.)
    - Success → mark job `completed`, ACK.
- **Failure classification** (explicit exception taxonomy):
  - *Transient* (DB connection errors, timeouts, lock timeouts, broker blips):
    retry. Unknown errors default to transient.
  - *Permanent* (payload violates DB constraints other than duplicate,
    undeserializable payload, job record missing): no retries — mark `failed`
    (or route straight to DLQ if the job record cannot be updated at all) and ACK.
- **Retry path (transient):**
  - Increment `retry_count` in the job row *and* in the message header
    (`x-retry-count`) — the count must survive consumer restarts.
  - If `retry_count < max_retries`: NACK with `requeue=false` so the message goes
    to the retry queue (delayed redelivery via TTL + dead-lettering, see Topology).
    Do **not** sleep/retry in-process.
  - If exhausted: mark job `dead_lettered`, publish/forward the message to
    `reservations.dlq` with error headers, ACK.
- **Crash safety / stuck jobs:**
  - A crash before ACK → RabbitMQ redelivers (`message.redelivered == true`);
    the worker resumes idempotently (transaction + unique constraint make
    redelivery safe).
  - A reaper task periodically requeues job rows stuck in `processing` beyond a
    lease timeout (their messages were lost with the dead consumer).
- **QoS:** small `prefetch_count`, fair dispatch, never auto-ack.
- Unparseable messages (no `job_id`): route straight to the DLQ with error
  headers and log — nothing to update in the DB.

### Dead-letter handling
- `reservations.dlq` is inspectable, not a dead end:
  - `GET  /jobs?state=dead_lettered` — list dead-lettered jobs.
  - `POST /jobs/{job_id}/requeue` — reset state to `pending`, re-publish with
    `retry_count = 0` (manual ops fix then replay).
  - Alert on DLQ depth (e.g. a DB outage should page someone, not silently
    dead-letter a batch).

## 3. Webhook
- When a request's job reaches a terminal state (`completed`, `failed`,
  `duplicate`, or `dead_lettered`) and a `callback_url` was provided:
  POST the job result to it.
- Payload: `{ job_id, state, reservation_id?, error?, finished_at }`
  - For `duplicate`, include the `reservation_id` / `job_id` of the original
    stored reservation so the caller can correlate.
- Delivery is itself a job with retries (exponential backoff, limited attempts,
  e.g. 5); exhausted deliveries are dropped and **logged with the full payload**
  so they can be replayed manually. Deliveries must have a timeout and never
  block the reservation worker.
