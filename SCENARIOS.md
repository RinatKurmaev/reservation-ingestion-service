# Scenarios — Reservation Ingestion Service

A catalog of every scenario the service is designed to handle: what triggers it,
what the system does, and what the caller/observer sees. Full spec is in `task.md`;
operational details are in `README.md`.

---

## 1. Happy path

### 1.1 Valid reservation submitted
- **Trigger:** `POST /reservations` with a valid payload
  (`checkin` < `checkout`, `num_guests >= 1`, non-empty `guest_names`,
  optional `callback_url`).
- **Behavior:**
  1. Payload validated at the API.
  2. Idempotency key computed (SHA-256 of the canonical payload).
  3. Job row created in state `pending` and committed.
  4. Message published to `reservations.jobs` (carrying `job_id`, `retry_count` headers).
- **Result:** `202 Accepted` with `{"job_id": "...", "state": "pending"}` — the API never waits for storage.

### 1.2 Worker stores the reservation
- **Trigger:** message consumed from `reservations.jobs`.
- **Behavior:**
  1. Job marked `processing` (persisted **before** handling).
  2. Insert reservation + update job state in a **single DB transaction**.
  3. Message ACKed **only after** the transaction commits.
- **Result:** job → `completed`, reservation row exists.

### 1.3 Job polling
- **Trigger:** `GET /jobs/{job_id}` — for callers that did not provide a `callback_url`.
- **Result:** current state of the job (`pending` → `processing` → terminal state).

---

## 2. Validation / input errors

### 2.1 Invalid reservation payload
- **Triggers:** `checkout` not after `checkin`; `num_guests < 1`; empty `guest_names`;
  wrong types / malformed JSON.
- **Result:** `422` with error details. Nothing is queued, no job row is created.

---

## 3. Deduplication & idempotency

The UNIQUE constraint on `reservations.idempotency_key` is the dedupe guarantee —
there is no separate check-then-insert step.

### 3.1 Duplicate reservation (different job)
- **Trigger:** a payload whose hash already exists, submitted via a different job.
- **Behavior:** `IntegrityError` on insert → job marked `duplicate`.
- **Result:** job state `duplicate`; webhook (if any) includes the **original**
  reservation's `reservation_id` / `job_id` so the caller can correlate.

### 3.2 Redelivery of the same job after a crash
- **Trigger:** worker crashed after commit but before ACK.
- **Behavior:** RabbitMQ redelivers (`message.redelivered == true`); the insert
  fails with `IntegrityError`, but since `existing.job_id == job.id`, the job is
  completed idempotently.
- **Result:** at-least-once delivery, exactly-once effect — no duplicate rows, no false failure.

---

## 4. Transient failures (retried)

Classification lives in `app/errors.py`. **Unknown exceptions default to transient.**

### 4.1 Transient error during processing
- **Triggers:** DB connection errors, timeouts, lock/deadlock timeouts, broker blips.
- **Behavior:**
  1. `retry_count` incremented in the **job row** and in the message header
     (`x-retry-count`) — survives consumer restarts.
  2. If `retry_count < max_retries`: a copy is published to `reservations.retry`
     with per-message TTL `min(base * 2^retry, cap) + jitter`, then the original
     message is ACKed.
  3. TTL expiry dead-letters the copy back into `reservations.jobs` → redelivery.
- **Constraint:** the worker **never** sleeps or retries in-process; it stays free
  to process other messages.

### 4.2 Full DB outage
- **Trigger:** PostgreSQL unreachable — the worker cannot even read the job row.
- **Behavior:** the handler can't classify/update anything, so the message is
  nacked into the retry queue and gets the **queue-level TTL (10 min)** as a
  cool-down before redelivery.
- **Result:** nothing is lost; this is expected behavior, not stuck messages.

### 4.3 Retry budget exhausted
- **Trigger:** `retry_count >= max_retries` on a transient failure.
- **Behavior:** job marked `dead_lettered`; message forwarded to `reservations.dlq`
  with error headers; ACK.
- **Result:** message parked in the DLQ (see §6).

---

## 5. Permanent failures (no retry)

### 5.1 Permanent error during processing
- **Triggers:** payload violates a DB constraint other than duplicate;
  undeserializable payload; job record missing.
- **Result:** job marked `failed` immediately, message ACKed, no retries.

### 5.2 Job record cannot be updated at all
- **Trigger:** even reading/updating the job row fails permanently.
- **Result:** message routed **straight to the DLQ** with error headers.

### 5.3 Unparseable message
- **Trigger:** message on the queue with no `job_id` (nothing to update in the DB).
- **Result:** routed straight to `reservations.dlq` with error headers and logged.

---

## 6. Dead-letter handling (DLQ is inspectable, not a dead end)

### 6.1 Inspect dead-lettered jobs
- `GET /jobs?state=dead_lettered` — list everything parked in the DLQ.

### 6.2 Manual replay (fix, then requeue)
- **Trigger:** `POST /jobs/{job_id}/requeue` after the underlying issue is fixed.
- **Behavior:** job state reset to `pending`, message re-published with `retry_count = 0`.
- **Result:** full lifecycle can restart from scratch.

### 6.3 Alerting
- DLQ depth should be monitored/alerted on — e.g. a DB outage should page someone,
  not silently dead-letter a batch.
- Debug: `podman exec reservation-ingestion_rabbitmq_1 rabbitmqctl list_queues
  name messages messages_unacknowledged`.

---

## 7. Crash safety & stuck jobs

### 7.1 Worker crash before ACK
- **Trigger:** worker dies mid-processing (message unacked).
- **Behavior:** RabbitMQ redelivers to another (or restarted) consumer; the
  transaction + unique constraint make redelivery safe (see §3.2).

### 7.2 Message lost with a dead consumer
- **Trigger:** consumer died while its message was in flight and the channel was lost.
- **Behavior:** a **reaper task** periodically requeues job rows stuck in
  `processing` beyond the lease timeout (300s).

### 7.3 Crash between job-row commit and publish (API side)
- **Trigger:** documented trade-off — no outbox pattern.
- **Result:** a job can remain `pending` forever; the requeue endpoint (§6.2) replays it.

---

## 8. Webhooks (optional `callback_url`)

### 8.1 Delivery on terminal state
- **Trigger:** job reaches any terminal state
  (`completed`, `failed`, `duplicate`, `dead_lettered`) and a `callback_url` was provided.
- **Behavior:** POST `{ job_id, state, reservation_id?, error?, finished_at }`
  out-of-band — never blocks the reservation worker.

### 8.2 Webhook delivery failure
- **Behavior:** its own retry job with exponential backoff (5 attempts, 10s timeout).
- **Result (exhausted):** delivery dropped and **logged with the full payload**
  so it can be replayed manually.

---

## 9. State machine summary

```
pending → processing → completed
                     → duplicate
                     → failed
                     → dead_lettered
```

- `pending → processing`: claimed by a worker, persisted before handling.
- Terminal states: `completed`, `duplicate`, `failed`, `dead_lettered`.
- A worker never re-processes a message whose job is already terminal.
- `POST /jobs/{id}/requeue` resets `dead_lettered`/`failed` back to `pending`.

## 10. Documented trade-offs

- Per-message TTL on a single retry queue can head-of-line block under heavy
  mixed-delay load (tiered retry queues or the delayed-message plugin would fix it).
- No outbox pattern → the `pending`-job window in §7.3 (replayable via requeue).
- `create_all` instead of Alembic migrations for this scope.
