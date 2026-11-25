"""Failure classification for the consumer.

Transient failures are retried with backoff, then dead-lettered.
Permanent failures mark the job `failed` immediately (retrying cannot fix them).
Unknown failures default to transient: safer, since they get retried a bounded
number of times before landing in the DLQ.
"""

import asyncio


class TransientError(Exception):
    """Recoverable failure (DB connection, timeout, deadlock, ...)."""


class PermanentError(Exception):
    """Non-recoverable failure (bad payload, constraint violation, ...)."""


_TRANSIENT_SQLSTATES = {
    "08000", "08001", "08003", "08004", "08006", "08007",  # connection failures
    "40001", "40P01",                                       # serialization failure, deadlock
    "53300", "57014", "57P01", "57P02", "57P03",            # capacity / statement timeout / shutdown
    "58000", "58P01",                                       # system errors
}


def sqlstate_of(exc: BaseException) -> str | None:
    for candidate in (getattr(exc, "orig", None), getattr(exc, "__cause__", None), exc):
        code = getattr(candidate, "sqlstate", None) or getattr(candidate, "pgcode", None)
        if code:
            return str(code)
    return None


def is_unique_violation(exc: BaseException) -> bool:
    return sqlstate_of(exc) == "23505"


def classify_exception(exc: BaseException) -> type[TransientError] | type[PermanentError]:
    if isinstance(exc, PermanentError):
        return PermanentError
    if isinstance(exc, (TransientError, asyncio.TimeoutError, TimeoutError, ConnectionError, OSError)):
        return TransientError

    code = sqlstate_of(exc)
    if code is None:
        return TransientError
    if code in _TRANSIENT_SQLSTATES or code.startswith("08"):
        return TransientError
    if code.startswith("22") or code.startswith("23"):
        # Data errors and constraint violations other than duplicate-key (which the
        # consumer handles separately as `duplicate`): retrying cannot fix the payload.
        return PermanentError
    return TransientError
