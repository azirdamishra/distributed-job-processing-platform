# ─────────────────────────────────────────────────────────────
# worker/worker.py — Job Consumer (Phase 2)
# ─────────────────────────────────────────────────────────────
# What changed from Phase 1:
#   - run_load_test() now captures full percentile distribution,
#     error bucketing by category, throughput, duration,
#     and per-worker tagging on the result
#
# What did NOT change:
#   - Worker loop (brpop, shutdown, reconnect)
#   - State machine (mark_processing, mark_completed, mark_failed_and_retry)
#   - Job routing (route_job, match statement)
#   - run_video_process() stub
# ─────────────────────────────────────────────────────────────

from __future__ import annotations

import asyncio
import json
import logging
import signal
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import httpx
import redis.asyncio as aioredis
from pydantic_settings import BaseSettings


# ── LOGGING ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("worker")


# ── SETTINGS ──────────────────────────────────────────────────
class Settings(BaseSettings):
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    job_queue_key: str = "jobs:queue"
    job_state_prefix: str = "jobs:state:"
    worker_id: str = "worker-1"
    brpop_timeout: int = 5

    class Config:
        env_file = ".env"


settings = Settings()


# ── JOB STATUS ────────────────────────────────────────────────
class JobStatus:
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CORRUPTED = "corrupted"


# ── JOB RESULT ────────────────────────────────────────────────
@dataclass
class JobResult:
    success: bool
    data: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


# ── REDIS STATE HELPERS ───────────────────────────────────────
async def mark_processing(redis: aioredis.Redis, job: dict) -> None:
    job_key = f"{settings.job_state_prefix}{job['job_id']}"
    await redis.hset(job_key, mapping={
        "status": JobStatus.PROCESSING,
        "worker_id": settings.worker_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
    })
    logger.info(f"[{job['job_id']}] → PROCESSING on {settings.worker_id}")


async def mark_completed(redis: aioredis.Redis, job: dict, result: JobResult) -> None:
    job_key = f"{settings.job_state_prefix}{job['job_id']}"
    await redis.hset(job_key, mapping={
        "status": JobStatus.COMPLETED,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "result": json.dumps(result.data),
    })
    logger.info(f"[{job['job_id']}] → COMPLETED")


async def mark_failed_and_retry(redis: aioredis.Redis, job: dict, error: str) -> None:
    job_key = f"{settings.job_state_prefix}{job['job_id']}"
    retry_count = job.get("retry_count", 0) + 1
    max_retries = job.get("max_retries", 2)

    if retry_count <= max_retries:
        job["retry_count"] = retry_count
        await redis.lpush(settings.job_queue_key, json.dumps(job))
        await redis.hset(job_key, mapping={
            "status": JobStatus.QUEUED,
            "retry_count": retry_count,
            "error": error,
            "worker_id": "",
        })
        logger.warning(
            f"[{job['job_id']}] → REQUEUED "
            f"(attempt {retry_count}/{max_retries}) | error: {error}"
        )
    else:
        await redis.hset(job_key, mapping={
            "status": JobStatus.CORRUPTED,
            "retry_count": retry_count,
            "error": error,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.error(
            f"[{job['job_id']}] → CORRUPTED "
            f"after {retry_count} attempts | error: {error}"
        )


# ── METRICS HELPERS ───────────────────────────────────────────
# Pulled out of run_load_test() so each concern has a name.
# percentile() and bucket_error() are pure functions —
# they take data in, return a value out, no side effects.
# Easy to test independently when we add tests in Phase 3.

def percentile(latencies: list[float], p: float) -> float:
    """
    Compute the p-th percentile of a sorted list.
    p=95 means: 95% of requests were faster than this value.

    Why percentiles matter more than averages:
    Average latency of 50ms sounds fine.
    But if p99 is 4000ms, 1 in 100 users waits 4 seconds.
    Averages hide the tail. Percentiles expose it.
    """
    if not latencies:
        return 0.0
    sorted_l = sorted(latencies)
    # index calculation: p=95, len=100 → index 95
    # p=95, len=10 → index 9 (last element — the worst case)
    index = int(len(sorted_l) * (p / 100))
    # clamp to last element so we never go out of bounds
    index = min(index, len(sorted_l) - 1)
    return round(sorted_l[index], 2)


def bucket_error(exc: Exception, status_code: int | None) -> str:
    """
    Categorise a failure into one of four buckets.

    Why bucketing matters:
      timeout        → target is slow, probably overloaded
      connection_error → target is unreachable or crashed
      4xx            → client-side problem (bad URL, auth, payload)
      5xx            → server-side problem (target is broken)

    These are different diagnoses. Collapsing them into "error"
    loses information you need to act on.
    """
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, (httpx.ConnectError, httpx.NetworkError)):
        return "connection_error"
    if status_code is not None:
        if 400 <= status_code < 500:
            return "4xx"
        if status_code >= 500:
            return "5xx"
    return "unknown"


# ── LOAD TEST PIPELINE ────────────────────────────────────────
async def run_load_test(config: dict) -> JobResult:
    """
    Phase 2 load test pipeline.

    New vs Phase 1:
      - error_breakdown dict instead of flat errors list
      - full percentile distribution (p50, p75, p95, p99)
      - throughput_rps — requests completed per second
      - duration_seconds — wall clock time for the full test
      - worker_id tagged on result for per-worker aggregation
      - bucket_error() categorises failures meaningfully
      - test_started_at / test_completed_at for audit trail
    """
    target_url = config["target_url"]
    request_count = config["request_count"]
    concurrency = config["concurrency"]
    timeout = config.get("timeout_seconds", 30)

    # ── result accumulator ────────────────────────────────────
    # Mutable dict updated by each concurrent request.
    # asyncio is single-threaded so no locking needed —
    # coroutines don't truly run in parallel, they interleave
    # at await points. No race conditions on this dict.
    results: dict[str, Any] = {
        "target_url": target_url,
        "request_count": request_count,
        "concurrency": concurrency,
        "worker_id": settings.worker_id,
        "success_count": 0,
        "error_count": 0,
        "error_breakdown": {
            "timeout": 0,
            "connection_error": 0,
            "4xx": 0,
            "5xx": 0,
            "unknown": 0,
        },
        "_latencies": [],           # prefixed with _ — internal, stripped before storing
    }

    semaphore = asyncio.Semaphore(concurrency)
    test_start_wall = datetime.now(timezone.utc)          # wall clock — for timestamps
    test_start_mono = asyncio.get_event_loop().time()     # monotonic — for duration

    async def fire_single_request(client: httpx.AsyncClient) -> None:
        async with semaphore:
            req_start = asyncio.get_event_loop().time()
            status_code = None
            exc_caught = None

            try:
                response = await client.get(target_url, timeout=timeout)
                status_code = response.status_code
                elapsed_ms = (asyncio.get_event_loop().time() - req_start) * 1000
                results["_latencies"].append(round(elapsed_ms, 2))

                if response.status_code < 400:
                    results["success_count"] += 1
                else:
                    # HTTP error response — still got a response, just a bad one
                    exc_caught = Exception(f"HTTP {status_code}")
                    results["error_count"] += 1
                    bucket = bucket_error(exc_caught, status_code)
                    results["error_breakdown"][bucket] += 1

            except Exception as exc:
                elapsed_ms = (asyncio.get_event_loop().time() - req_start) * 1000
                results["_latencies"].append(round(elapsed_ms, 2))
                results["error_count"] += 1
                bucket = bucket_error(exc, status_code)
                results["error_breakdown"][bucket] += 1
                logger.debug(f"Request failed: {type(exc).__name__}: {exc}")

    # ── fire all requests ─────────────────────────────────────
    async with httpx.AsyncClient() as client:
        tasks = [fire_single_request(client) for _ in range(request_count)]
        await asyncio.gather(*tasks)

    # ── compute aggregates ───────────────────────────────────
    test_end_mono = asyncio.get_event_loop().time()
    test_end_wall = datetime.now(timezone.utc)

    # use monotonic times for duration to avoid wall-clock adjustments
    duration_seconds = round(test_end_mono - test_start_mono, 3)
    latencies = results.pop("_latencies")   # remove internal key before storing

    # throughput — how many requests completed per second (guard divide-by-zero)
    throughput_rps = round(request_count / duration_seconds, 2) if duration_seconds > 0 else 0.0

    results.update({
        "duration_seconds": duration_seconds,
        "throughput_rps": throughput_rps,
        "test_started_at": test_start_wall.isoformat(),   # wall clock for timestamps
        "test_completed_at": test_end_wall.isoformat(),
    })

    if latencies:
        results["latency"] = {
            "avg_ms":  round(sum(latencies) / len(latencies), 2),
            "min_ms":  round(min(latencies), 2),
            "max_ms":  round(max(latencies), 2),
            "p50_ms":  percentile(latencies, 50),
            "p75_ms":  percentile(latencies, 75),
            "p95_ms":  percentile(latencies, 95),
            "p99_ms":  percentile(latencies, 99),
        }
    else:
        results["latency"] = {}

    # strip error buckets that had zero hits — cleaner output
    results["error_breakdown"] = {
        k: v for k, v in results["error_breakdown"].items() if v > 0
    }

    logger.info(
        f"Load test complete | "
        f"success={results['success_count']} "
        f"errors={results['error_count']} "
        f"duration={duration_seconds}s "
        f"throughput={throughput_rps}rps "
        f"p95={results['latency'].get('p95_ms')}ms"
    )

    return JobResult(success=True, data=results)


# ── VIDEO PIPELINE (stub) ─────────────────────────────────────
async def run_video_process(config: dict) -> JobResult:
    """
    Phase 2 stub — real ffmpeg + Cloudflare R2 pipeline comes
    in Track 2 once Cloudflare account is set up.
    """
    source_path = config.get("source_path")
    resolutions = config.get("output_resolutions", ["1080p", "720p", "480p"])
    logger.info(f"Video process stub | source={source_path} | resolutions={resolutions}")
    await asyncio.sleep(2)
    return JobResult(
        success=True,
        data={
            "source_path": source_path,
            "output_resolutions": resolutions,
            "note": "Phase 2 stub — real ffmpeg pipeline comes in Track 2",
        }
    )


# ── JOB ROUTER ────────────────────────────────────────────────
async def route_job(job: dict) -> JobResult:
    job_type = job.get("job_type")
    if job_type == "load_test":
        return await run_load_test(job.get("config", {}))
    elif job_type == "video_process":
        return await run_video_process(job.get("config", {}))
    else:
        return JobResult(
            success=False,
            error=f"Unknown job_type: {job.get('job_type')}"
        )


# ── WORKER CLASS ──────────────────────────────────────────────
class Worker:
    def __init__(self):
        self.redis: aioredis.Redis | None = None
        self._shutdown = False

    async def connect(self) -> None:
        self.redis = aioredis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True,
        )
        await self.redis.ping()
        logger.info(
            f"Worker {settings.worker_id} connected to Redis "
            f"at {settings.redis_host}:{settings.redis_port}"
        )

    async def disconnect(self) -> None:
        if self.redis:
            await self.redis.aclose()
            logger.info(f"Worker {settings.worker_id} disconnected from Redis")

    def handle_shutdown(self, *_) -> None:
        logger.info(f"Worker {settings.worker_id} received shutdown signal")
        self._shutdown = True

    async def process_one(self, raw: str) -> None:
        try:
            job = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to deserialize job: {e} | raw={raw[:100]}")
            return

        job_id = job.get("job_id", "unknown")
        logger.info(
            f"[{job_id}] Picked up | "
            f"type={job.get('job_type')} | "
            f"retry={job.get('retry_count', 0)}"
        )

        await mark_processing(self.redis, job)

        try:
            result = await route_job(job)
            if result.success:
                await mark_completed(self.redis, job, result)
            else:
                await mark_failed_and_retry(self.redis, job, result.error or "pipeline returned failure")
        except Exception as e:
            logger.exception(f"[{job_id}] Unhandled exception in pipeline")
            await mark_failed_and_retry(self.redis, job, str(e))

    async def run(self) -> None:
        await self.connect()

        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

        logger.info(
            f"Worker {settings.worker_id} started — "
            f"listening on '{settings.job_queue_key}'"
        )

        while not self._shutdown:
            try:
                response = await self.redis.brpop(
                    settings.job_queue_key,
                    timeout=settings.brpop_timeout,
                )
                if response is None:
                    continue
                _queue_name, raw_job = response
                await self.process_one(raw_job)

            except aioredis.RedisError as e:
                logger.error(f"Redis error: {e} — retrying in 3s")
                await asyncio.sleep(3)
            except Exception as e:
                logger.exception(f"Unexpected error in worker loop: {e}")
                await asyncio.sleep(1)

        logger.info(f"Worker {settings.worker_id} shut down cleanly")
        await self.disconnect()


# ── ENTRYPOINT ────────────────────────────────────────────────
if __name__ == "__main__":
    worker = Worker()
    asyncio.run(worker.run())