"""Idempotency key: SHA-256 of the canonical reservation payload."""

import hashlib
import json

# callback_url is deliberately excluded: it does not affect the stored reservation.
KEY_FIELDS = ("checkin", "checkout", "num_guests", "guest_names")


def idempotency_key(reservation: dict) -> str:
    canonical = json.dumps(
        {field: reservation[field] for field in KEY_FIELDS},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
