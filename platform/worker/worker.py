# ─────────────────────────────────────────────────────────────
# worker/worker.py — Job Consumer (Phase 2, Step 3)
# ─────────────────────────────────────────────────────────────
# What changed from Step 2:
#   - mark_completed() writes a per-worker Redis key in addition
#     to the main job state hash
#   - New key shape: jobs:workers:{job_id}:{worker_id}
#   - Aggregation endpoint in main.py reads all keys matching
#     jobs:workers:{job_id}:* and merges them
# ─────────────────────────────────────────────────────────────

from __future__ import annotations

import asyncio
import json
import logging
import signal
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
import redis.asyncio as aioredis
from pydantic_settings import BaseSettings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("worker")


class Settings(BaseSettings):
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    job_queue_key: str = "jobs:queue"
    job_state_prefix: str = "jobs:state:"
    job_worker_prefix: str = "jobs:workers:"
    worker_id: str = "worker-1"
    brpop_timeout: int = 5

    class Config:
        env_file = ".env"


settings = Settings()


class JobStatus:
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CORRUPTED = "corrupted"


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
    """
    Two writes for load_test jobs, one write for everything else.

    Write 1 — jobs:state:{job_id}
      The main state hash. Status endpoint reads this.
      Always written regardless of job type.

    Write 2 — jobs:workers:{job_id}:{worker_id}
      Per-worker contribution record. Only written for load_test.
      The aggregation endpoint scans for all keys matching
      jobs:workers:{job_id}:* and merges them into one report.

    Why keep them separate:
      The state hash is mutated by the worker as the job progresses.
      The worker key is append-only — each worker writes its own,
      never touching another worker's key. No conflicts, no overwrites.
    """
    job_key = f"{settings.job_state_prefix}{job['job_id']}"
    await redis.hset(job_key, mapping={
        "status": JobStatus.COMPLETED,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "result": json.dumps(result.data),
    })

    if job.get("job_type") == "load_test":
        worker_key = (
            f"{settings.job_worker_prefix}"
            f"{job['job_id']}:"
            f"{settings.worker_id}"
        )
        await redis.hset(worker_key, mapping={
            "worker_id": settings.worker_id,
            "job_id": job["job_id"],
            "result": json.dumps(result.data),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.info(f"[{job['job_id']}] Worker contribution written → {worker_key}")

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
def percentile(latencies: list[float], p: float) -> float:
    if not latencies:
        return 0.0
    sorted_l = sorted(latencies)
    index = min(int(len(sorted_l) * (p / 100)), len(sorted_l) - 1)
    return round(sorted_l[index], 2)


def bucket_error(exc: Exception, status_code: int | None) -> str:
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
    target_url = config["target_url"]
    request_count = config["request_count"]
    concurrency = config["concurrency"]
    timeout = config.get("timeout_seconds", 30)

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
        "_latencies": [],
    }

    semaphore = asyncio.Semaphore(concurrency)
    test_start_wall = datetime.now(timezone.utc)
    test_start_mono = asyncio.get_event_loop().time()

    async def fire_single_request(client: httpx.AsyncClient) -> None:
        async with semaphore:
            req_start = asyncio.get_event_loop().time()
            status_code = None

            try:
                response = await client.get(target_url, timeout=timeout)
                status_code = response.status_code
                elapsed_ms = (asyncio.get_event_loop().time() - req_start) * 1000
                results["_latencies"].append(round(elapsed_ms, 2))

                if response.status_code < 400:
                    results["success_count"] += 1
                else:
                    results["error_count"] += 1
                    bucket = bucket_error(Exception(f"HTTP {status_code}"), status_code)
                    results["error_breakdown"][bucket] += 1

            except Exception as exc:
                elapsed_ms = (asyncio.get_event_loop().time() - req_start) * 1000
                results["_latencies"].append(round(elapsed_ms, 2))
                results["error_count"] += 1
                bucket = bucket_error(exc, status_code)
                results["error_breakdown"][bucket] += 1
                logger.debug(f"Request failed: {type(exc).__name__}: {exc}")

    async with httpx.AsyncClient() as client:
        tasks = [fire_single_request(client) for _ in range(request_count)]
        await asyncio.gather(*tasks)

    test_end_mono = asyncio.get_event_loop().time()
    test_end_wall = datetime.now(timezone.utc)
    duration_seconds = round(test_end_mono - test_start_mono, 3)
    latencies = results.pop("_latencies")
    throughput_rps = round(request_count / duration_seconds, 2) if duration_seconds > 0 else 0.0

    results.update({
        "duration_seconds": duration_seconds,
        "throughput_rps": throughput_rps,
        "test_started_at": test_start_wall.isoformat(),
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
    match job.get("job_type"):
        case "load_test":
            return await run_load_test(job.get("config", {}))
        case "video_process":
            return await run_video_process(job.get("config", {}))
        case _:
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
                await mark_failed_and_retry(
                    self.redis, job,
                    result.error or "pipeline returned failure"
                )
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


if __name__ == "__main__":
    worker = Worker()
    asyncio.run(worker.run())