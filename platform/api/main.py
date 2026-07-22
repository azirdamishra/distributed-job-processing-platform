# ─────────────────────────────────────────────────────────────
# api/main.py — Job Producer (Phase 2, Step 3)
# ─────────────────────────────────────────────────────────────
# What changed from Step 1:
#   - JobStateResponse gains result field (Step 1 fix)
#   - New GET /jobs/{job_id}/aggregate endpoint (Step 3)
#   - Settings gains job_worker_prefix to match worker.py
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
class Settings(BaseSettings):
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    job_queue_key: str = "jobs:queue"
    job_state_prefix: str = "jobs:state:"
    job_worker_prefix: str = "jobs:workers:"   # must match worker.py exactly
    max_retries: int = 2

    class Config:
        env_file = ".env"


settings = Settings()


# ── LIFESPAN ──────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = aioredis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
        max_connections=10,
    )
    await app.state.redis.ping()
    print(f"✓ Redis connected at {settings.redis_host}:{settings.redis_port}")
    yield
    await app.state.redis.aclose()
    print("✓ Redis connection closed")


app = FastAPI(
    title="Job Queue API",
    version="0.2.0",
    description="Phase 2 — distributed processing platform",
    lifespan=lifespan,
)


# ── ENUMS ─────────────────────────────────────────────────────
class JobType(str, Enum):
    LOAD_TEST = "load_test"
    VIDEO_PROCESS = "video_process"


class JobStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CORRUPTED = "corrupted"


# ── REQUEST MODELS ────────────────────────────────────────────
class LoadTestConfig(BaseModel):
    target_url: str
    request_count: int = Field(ge=1, le=10_000)
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
    source_path: str
    output_resolutions: list[str] = Field(
        default=["2k", "1080p", "720p", "480p"]
    )
    upload_to_cdn: bool = True


class JobRequest(BaseModel):
    job_type: JobType
    config: LoadTestConfig | VideoProcessConfig

    @field_validator("config", mode="before")
    @classmethod
    def validate_config_matches_type(cls, v: Any, info: Any) -> Any:
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


class WorkerContribution(BaseModel):
    """One worker's result for a given job."""
    worker_id: str
    completed_at: str
    success_count: int
    error_count: int
    throughput_rps: float
    duration_seconds: float
    latency: dict
    error_breakdown: dict


class AggregateResponse(BaseModel):
    """
    Merged result across all workers that contributed to a job.
    This is what you read when multiple workers processed parts
    of the same load test.
    """
    job_id: str
    status: str
    worker_count: int
    total_requests: int
    total_success: int
    total_errors: int
    combined_throughput_rps: float
    combined_duration_seconds: float
    combined_error_breakdown: dict
    combined_latency: dict
    per_worker: dict[str, WorkerContribution]


# ── HELPERS ───────────────────────────────────────────────────
def build_job_payload(job_id: str, job_type: JobType, config: dict) -> dict:
    return {
        "job_id": job_id,
        "job_type": job_type.value,
        "config": config,
        "retry_count": 0,
        "max_retries": settings.max_retries,
        "queued_at": datetime.now(timezone.utc).isoformat(),
    }


async def push_job_to_queue(redis: aioredis.Redis, job: dict) -> None:
    job_key = f"{settings.job_state_prefix}{job['job_id']}"
    await redis.lpush(settings.job_queue_key, json.dumps(job))
    await redis.hset(job_key, mapping={
        "job_id": job["job_id"],
        "job_type": job["job_type"],
        "status": JobStatus.QUEUED.value,
        "queued_at": job["queued_at"],
        "retry_count": 0,
    })


def merge_worker_results(worker_results: list[dict]) -> dict:
    """
    Combines multiple worker result dicts into one aggregate.

    For counts (success, error): sum them.
    For throughput: sum them — workers run in parallel so their
      throughputs add up, not average out.
    For duration: take the max — the job isn't done until the
      slowest worker finishes.
    For latency percentiles: we re-derive from all individual
      worker averages as a weighted approximation. True percentile
      merging would require keeping all raw latency values which
      we intentionally discarded to save memory. This is a known
      tradeoff — good enough for operational monitoring.
    For error breakdown: sum each bucket across workers.
    """
    if not worker_results:
        return {}

    total_requests = sum(r.get("request_count", 0) for r in worker_results)
    total_success = sum(r.get("success_count", 0) for r in worker_results)
    total_errors = sum(r.get("error_count", 0) for r in worker_results)
    combined_throughput = sum(r.get("throughput_rps", 0) for r in worker_results)
    combined_duration = max(r.get("duration_seconds", 0) for r in worker_results)

    # merge error breakdowns
    combined_errors: dict[str, int] = {}
    for r in worker_results:
        for bucket, count in r.get("error_breakdown", {}).items():
            combined_errors[bucket] = combined_errors.get(bucket, 0) + count

    # weighted latency approximation across workers
    # weight each worker's latency by their request count
    latency_keys = ["avg_ms", "min_ms", "max_ms", "p50_ms", "p75_ms", "p95_ms", "p99_ms"]
    combined_latency: dict[str, float] = {}

    for key in latency_keys:
        values = [
            (r.get("latency", {}).get(key, 0), r.get("request_count", 1))
            for r in worker_results
            if r.get("latency", {}).get(key) is not None
        ]
        if values:
            if key == "min_ms":
                combined_latency[key] = min(v for v, _ in values)
            elif key == "max_ms":
                combined_latency[key] = max(v for v, _ in values)
            else:
                # weighted average by request count
                total_weight = sum(w for _, w in values)
                combined_latency[key] = round(
                    sum(v * w for v, w in values) / total_weight, 2
                ) if total_weight > 0 else 0.0

    return {
        "total_requests": total_requests,
        "total_success": total_success,
        "total_errors": total_errors,
        "combined_throughput_rps": round(combined_throughput, 2),
        "combined_duration_seconds": round(combined_duration, 3),
        "combined_error_breakdown": combined_errors,
        "combined_latency": combined_latency,
    }


# ── ROUTES ────────────────────────────────────────────────────
@app.get("/health")
async def health():
    await app.state.redis.ping()
    return {"status": "ok", "redis": "connected"}


@app.post("/jobs", response_model=JobResponse, status_code=202)
async def submit_job(request: JobRequest):
    job_id = str(uuid.uuid4())

    if request.job_type == JobType.LOAD_TEST and not isinstance(request.config, LoadTestConfig):
        raise HTTPException(status_code=422, detail="job_type load_test requires LoadTestConfig")

    if request.job_type == JobType.VIDEO_PROCESS and not isinstance(request.config, VideoProcessConfig):
        raise HTTPException(status_code=422, detail="job_type video_process requires VideoProcessConfig")

    job = build_job_payload(
        job_id=job_id,
        job_type=request.job_type,
        config=request.config.model_dump(),
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
    job_key = f"{settings.job_state_prefix}{job_id}"
    state = await app.state.redis.hgetall(job_key)

    if not state:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    raw_result = state.get("result")
    parsed_result = json.loads(raw_result) if raw_result else None

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


@app.get("/jobs/{job_id}/aggregate", response_model=AggregateResponse)
async def get_job_aggregate(job_id: str):
    """
    Reads all per-worker contribution keys for this job and
    merges them into one combined report.

    Key pattern scanned: jobs:workers:{job_id}:*
    Each key holds one worker's result for this job.

    This endpoint is most useful when:
      - Multiple workers processed parts of the same job (Phase 3+)
      - You want to compare worker performance side by side
      - You want the true combined throughput across all workers

    For single-worker jobs it returns the same data as /jobs/{id}
    but in the aggregate shape — still useful for consistency.
    """
    redis = app.state.redis

    # verify the job exists first
    job_key = f"{settings.job_state_prefix}{job_id}"
    state = await redis.hgetall(job_key)
    if not state:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    if state.get("job_type") != "load_test":
        raise HTTPException(
            status_code=400,
            detail="Aggregate endpoint is only available for load_test jobs"
        )

    # scan for all worker contribution keys for this job
    pattern = f"{settings.job_worker_prefix}{job_id}:*"
    worker_keys = []
    async for key in redis.scan_iter(pattern):
        worker_keys.append(key)

    if not worker_keys:
        raise HTTPException(
            status_code=404,
            detail=f"No worker contributions found for job {job_id}. "
                   f"Job may still be processing."
        )

    # read each worker's contribution
    worker_results = []
    per_worker: dict[str, WorkerContribution] = {}

    for key in worker_keys:
        raw = await redis.hgetall(key)
        if not raw:
            continue

        result_data = json.loads(raw["result"])
        worker_id = raw["worker_id"]
        worker_results.append(result_data)

        per_worker[worker_id] = WorkerContribution(
            worker_id=worker_id,
            completed_at=raw["completed_at"],
            success_count=result_data.get("success_count", 0),
            error_count=result_data.get("error_count", 0),
            throughput_rps=result_data.get("throughput_rps", 0.0),
            duration_seconds=result_data.get("duration_seconds", 0.0),
            latency=result_data.get("latency", {}),
            error_breakdown=result_data.get("error_breakdown", {}),
        )

    merged = merge_worker_results(worker_results)

    return AggregateResponse(
        job_id=job_id,
        status=state["status"],
        worker_count=len(worker_keys),
        per_worker=per_worker,
        **merged,
    )


@app.get("/jobs")
async def list_jobs():
    redis = app.state.redis

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