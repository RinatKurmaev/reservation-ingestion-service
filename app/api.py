"""FastAPI application: POST /reservations and job inspection endpoints."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .db import SessionLocal, init_db
from .idempotency import idempotency_key
from .models import Job, JobState
from .mq import Publisher
from .models import Reservation
from .schemas import JobOut, ReservationAccepted, ReservationIn, ReservationOut

logger = logging.getLogger(__name__)
settings = get_settings()
publisher = Publisher()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await publisher.connect()
    yield
    await publisher.close()


app = FastAPI(title="Reservation Ingestion Service", lifespan=lifespan)

# Scenario dashboard (plain static HTML, same origin -> no CORS needed).
_STATIC_DIR = Path(__file__).parent / "static"
app.mount("/dashboard", StaticFiles(directory=_STATIC_DIR, html=True), name="dashboard")


@app.get("/", include_in_schema=False)
async def index() -> RedirectResponse:
    return RedirectResponse(url="/dashboard/")


async def get_session():
    async with SessionLocal() as session:
        yield session


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.post(
    "/reservations",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ReservationAccepted,
)
async def create_reservation(
    reservation: ReservationIn,
    session: AsyncSession = Depends(get_session),
) -> ReservationAccepted:
    payload = reservation.model_dump(mode="json")
    job = Job(
        id=uuid4(),
        idempotency_key=idempotency_key(payload),
        payload=payload,
        state=JobState.pending,
        callback_url=str(reservation.callback_url) if reservation.callback_url else None,
    )
    session.add(job)
    await session.commit()

    try:
        # Publisher confirms: raises if the broker does not accept the message.
        await publisher.publish_job({"job_id": str(job.id), "payload": payload})
    except Exception:
        logger.exception("failed to publish job %s", job.id)
        job.state = JobState.failed
        job.last_error = "publish to queue failed"
        await session.commit()
        raise HTTPException(status_code=503, detail="could not enqueue reservation; try again")

    return ReservationAccepted(job_id=job.id, state=job.state)


@app.get("/reservations", response_model=list[ReservationOut])
async def list_reservations(
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[Reservation]:
    result = await session.execute(
        select(Reservation).order_by(Reservation.created_at.desc()).limit(limit)
    )
    return list(result.scalars().all())


@app.get("/jobs/{job_id}", response_model=JobOut)
async def get_job(job_id: UUID, session: AsyncSession = Depends(get_session)) -> Job:
    job = await session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.get("/jobs", response_model=list[JobOut])
async def list_jobs(
    state: JobState | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[Job]:
    query = select(Job).order_by(Job.created_at.desc()).limit(limit)
    if state is not None:
        query = query.where(Job.state == state)
    result = await session.execute(query)
    return list(result.scalars().all())


@app.post("/jobs/{job_id}/requeue", response_model=JobOut)
async def requeue_job(job_id: UUID, session: AsyncSession = Depends(get_session)) -> Job:
    job = await session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if job.state not in {JobState.pending, JobState.dead_lettered, JobState.failed}:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"only pending/dead_lettered/failed jobs can be requeued (job is {job.state.value})",
        )

    job.state = JobState.pending
    job.retry_count = 0
    job.last_error = None
    await session.commit()

    try:
        await publisher.publish_job(
            {"job_id": str(job.id), "payload": job.payload},
            headers={"x-retry-count": 0},
        )
    except Exception:
        logger.exception("failed to re-publish job %s", job.id)
        job.state = JobState.failed
        job.last_error = "publish to queue failed during requeue"
        await session.commit()
        raise HTTPException(status_code=503, detail="could not re-enqueue job; try again")

    logger.info("job %s requeued for processing", job.id)
    return job
