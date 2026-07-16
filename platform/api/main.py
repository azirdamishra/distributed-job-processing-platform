# ─────────────────────────────────────────────────────────────
# api/main.py — Job Producer
# ─────────────────────────────────────────────────────────────
# Responsibilities:
#   1. Validate incoming job requests (Pydantic)
#   2. Push valid jobs onto the Redis queue (lpush)
#   3. Store initial job state (hset)
#   4. Expose a status endpoint to check any job
#
# What this file is NOT responsible for:
#   - Processing jobs (that's worker.py)
#   - Retry logic (that's worker.py)
#   - Knowing what kind of job it is (generic by design)
# ─────────────────────────────────────────────────────────────

from __future__ import annotations

import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings


# ── SETTINGS ──────────────────────────────────────────────────
# pydantic-settings reads from environment variables automatically.
# REDIS_HOST=redis in docker-compose becomes settings.redis_host here.
# No os.environ.get() scattered across the codebase.
class Settings(BaseSettings):
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    job_queue_key: str = "jobs:queue"         # the Redis list
    job_state_prefix: str = "jobs:state:"     # prefix for per-job hash
    max_retries: int = 2                       # matches the comment's "2x retries"

    class Config:
        env_file = ".env"                      # optional local .env for dev


settings = Settings()


# ── LIFESPAN ──────────────────────────────────────────────────
# Modern FastAPI (0.111+) uses lifespan instead of @app.on_event
# which is deprecated. Lifespan is an async context manager that
# owns the startup and shutdown lifecycle of the app.
#
# The Redis connection pool is created ONCE on startup and shared
# across all requests — not created per request, which is expensive.
@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── startup ───────────────────────────────────────────────
    app.state.redis = aioredis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,         # return str not bytes — quality of life
        max_connections=10,            # connection pool ceiling
    )
    # verify the connection is actually alive before accepting traffic
    await app.state.redis.ping()
    print(f"✓ Redis connected at {settings.redis_host}:{settings.redis_port}")

    yield                              # app runs here

    # ── shutdown ──────────────────────────────────────────────
    await app.state.redis.aclose()
    print("✓ Redis connection closed")


app = FastAPI(
    title="Job Queue API",
    version="0.1.0",
    description="Phase 1 — job producer for the distributed processing platform",
    lifespan=lifespan,
)


# ── ENUMS ─────────────────────────────────────────────────────
# JobType is the contract between the API and workers.
# Workers switch on this to know what pipeline to run.
# Adding a new pipeline = adding a new enum value.
class JobType(str, Enum):
    LOAD_TEST = "load_test"
    VIDEO_PROCESS = "video_process"


class JobStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CORRUPTED = "corrupted"           # exhausted all retries


# ── REQUEST MODELS ────────────────────────────────────────────
# Pydantic v2 style — field_validator instead of validator.
# This is the boundary layer: nothing invalid ever reaches Redis.

class LoadTestConfig(BaseModel):
    target_url: str
    request_count: int = Field(ge=1, le=10_000)     # ge = greater or equal
    concurrency: int = Field(ge=1, le=100)
    timeout_seconds: int = Field(default=30, ge=1)

    @field_validator("target_url")
    @classmethod
    def must_be_http(cls, v: str) -> str:
        v = v.strip()
        if not v.startswith(("http://", "https://")):
            raise ValueError("target_url must start with http:// or https://")
        return v


class VideoProcessConfig(BaseModel):
    source_path: str                                 # path to the mp4
    output_resolutions: list[str] = Field(
        default=["2k", "1080p", "720p", "480p"]     # matches the comment exactly
    )
    upload_to_cdn: bool = True


class JobRequest(BaseModel):
    job_type: JobType
    config: LoadTestConfig | VideoProcessConfig      # union type — Pydantic picks the right one

    @field_validator("config", mode="before")
    @classmethod
    def validate_config_matches_type(cls, v: Any, info: Any) -> Any:
        # Pydantic v2 passes the full validation context via info
        # We can't cross-validate fields in field_validator easily,
        # so config type enforcement happens in the endpoint instead.
        return v


# ── RESPONSE MODELS ───────────────────────────────────────────
class JobResponse(BaseModel):
    job_id: str
    job_type: JobType
    status: JobStatus
    queued_at: str
    message: str


class JobStateResponse(BaseModel):
    job_id: str
    job_type: str
    status: str
    queued_at: str
    retry_count: int
    worker_id: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    error: str | None = None
    result: dict | None = None


# ── HELPERS ───────────────────────────────────────────────────
def build_job_payload(job_id: str, job_type: JobType, config: dict) -> dict:
    """
    The canonical shape of a job in the queue.
    Workers read exactly these fields — this is the contract.
    """
    return {
        "job_id": job_id,
        "job_type": job_type.value,
        "config": config,
        "retry_count": 0,
        "max_retries": settings.max_retries,
        "queued_at": datetime.now(timezone.utc).isoformat(),
    }


async def push_job_to_queue(redis: aioredis.Redis, job: dict) -> None:
    """
    Two writes happen atomically-ish here:
      1. lpush  → job payload goes onto the queue list (workers consume from right)
      2. hset   → job state hash is created for status tracking

    Why not a Redis transaction (MULTI/EXEC)?
    For Phase 1, the risk of partial write is acceptable.
    Phase 3 will introduce proper atomic operations where needed.
    """
    job_key = f"{settings.job_state_prefix}{job['job_id']}"

    # Push the full job payload to the queue
    await redis.lpush(settings.job_queue_key, json.dumps(job))

    # Store initial state separately — this is what the status endpoint reads
    await redis.hset(job_key, mapping={
        "job_id": job["job_id"],
        "job_type": job["job_type"],
        "status": JobStatus.QUEUED.value,
        "queued_at": job["queued_at"],
        "retry_count": 0,
    })


# ── ROUTES ────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """
    Two-level health check:
      - If this endpoint responds, the API process is alive.
      - If Redis ping succeeds, the queue is reachable.
    Used by docker-compose healthcheck and later by the autoscaler.
    """
    await app.state.redis.ping()
    return {"status": "ok", "redis": "connected"}


@app.post("/jobs", response_model=JobResponse, status_code=202)
async def submit_job(request: JobRequest):
    """
    202 Accepted — not 201 Created.
    The job is accepted into the queue but not yet processed.
    This distinction matters: the caller should not assume
    the job is done just because the API responded.
    """
    job_id = str(uuid.uuid4())

    # Enforce config type matches job type
    if request.job_type == JobType.LOAD_TEST and not isinstance(request.config, LoadTestConfig):
        raise HTTPException(status_code=422, detail="job_type load_test requires LoadTestConfig")

    if request.job_type == JobType.VIDEO_PROCESS and not isinstance(request.config, VideoProcessConfig):
        raise HTTPException(status_code=422, detail="job_type video_process requires VideoProcessConfig")

    job = build_job_payload(
        job_id=job_id,
        job_type=request.job_type,
        config=request.config.model_dump(),     # pydantic v2: model_dump() not dict()
    )

    await push_job_to_queue(app.state.redis, job)

    return JobResponse(
        job_id=job_id,
        job_type=request.job_type,
        status=JobStatus.QUEUED,
        queued_at=job["queued_at"],
        message=f"Job accepted. Poll /jobs/{job_id} for status.",
    )


@app.get("/jobs/{job_id}", response_model=JobStateResponse)
async def get_job_status(job_id: str):
    """
    Reads from the job state hash, not the queue.
    The queue is write-once from the API's perspective —
    workers update the state hash as the job progresses.
    """
    job_key = f"{settings.job_state_prefix}{job_id}"
    state = await app.state.redis.hgetall(job_key)

    if not state:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    # before — never read result from hash
    raw_result = state.get("result")
    parsed_result = json.loads(raw_result) if raw_result else None
    # ↑ result was stored as a JSON string by the worker
    # hgetall gives everything back as plain strings
    # so we parse it back into a dict before returning

    return JobStateResponse(
        job_id=state["job_id"],
        job_type=state["job_type"],
        status=state["status"],
        queued_at=state["queued_at"],
        retry_count=int(state.get("retry_count", 0)),
        worker_id=state.get("worker_id"),
        started_at=state.get("started_at"),
        completed_at=state.get("completed_at"),
        error=state.get("error"),
        result=parsed_result,
    )


@app.get("/jobs")
async def list_jobs():
    """
    Returns a summary of all tracked jobs and current queue depth.
    Useful for the admin dashboard in Phase 4.
    """
    redis = app.state.redis

    # Scan all job state keys — SCAN is non-blocking unlike KEYS
    job_keys = []
    async for key in redis.scan_iter(f"{settings.job_state_prefix}*"):
        job_keys.append(key)

    jobs = []
    for key in job_keys:
        state = await redis.hgetall(key)
        if state:
            jobs.append({
                "job_id": state.get("job_id"),
                "job_type": state.get("job_type"),
                "status": state.get("status"),
                "queued_at": state.get("queued_at"),
                "retry_count": int(state.get("retry_count", 0)),
            })

    queue_depth = await redis.llen(settings.job_queue_key)

    return {
        "queue_depth": queue_depth,
        "total_jobs": len(jobs),
        "jobs": sorted(jobs, key=lambda j: j["queued_at"], reverse=True),
    }
