# ─────────────────────────────────────────────────────────────
# worker/worker.py — Job Consumer
# ─────────────────────────────────────────────────────────────
# Responsibilities:
#   1. Block on Redis queue waiting for jobs (brpop)
#   2. Deserialize and route job to correct pipeline
#   3. Execute the pipeline (load test or video process)
#   4. Handle failures — retry logic, state transitions
#   5. Write results back to Redis state hash
#
# What this file is NOT responsible for:
#   - Accepting HTTP traffic (no FastAPI, no uvicorn)
#   - Knowing how jobs were created (that's api/main.py)
#   - Scaling itself (that's Phase 3, the orchestrator)
# ─────────────────────────────────────────────────────────────

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
import redis.asyncio as aioredis
from pydantic_settings import BaseSettings, SettingsConfigDict

# ── LOGGING ───────────────────────────────────────────────────
# Structured logging — every log line has a consistent format.
# worker_id in every line means you can filter logs per worker
# when you have multiple workers running simultaneously.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

logger = logging.getLogger("worker")

# ── SETTINGS ──────────────────────────────────────────────────
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    job_queue_key: str = "jobs:queue"
    job_state_prefix: str = "jobs:state:"
    worker_id: str = "worker-1"
    brpop_timeout: int = 5          # seconds to block before checking shutdown flag
                                    # lower = more responsive to shutdown signals
                                    # higher = fewer Redis round trips
 
 
settings = Settings()

# ── JOB STATE TRANSITIONS ─────────────────────────────────────
# Every job moves through a defined set of states.
# The worker is responsible for driving these transitions.
# The API only ever sets QUEUED — everything after is the worker.
#
#   QUEUED → PROCESSING → COMPLETED
#                      ↘ FAILED (retry_count < max_retries → back to QUEUED)
#                      ↘ CORRUPTED (retry_count >= max_retries)
 
class JobStatus:
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CORRUPTED = "corrupted"


# ── JOB RESULT ────────────────────────────────────────────────
# A dataclass is the right tool here — this is pure data,
# no validation needed, no HTTP serialization.
# Pydantic would be overkill for internal worker data structures.
@dataclass
class JobResult:
    success: bool
    data: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


# ── REDIS STATE HELPERS ───────────────────────────────────────
# These functions are the worker's write interface to Redis.
# Each one represents a state transition with a clear name.

async def mark_processing(redis: aioredis.Redis, job: dict) -> None:
    """Job popped from queue — worker has taken ownership."""
    job_key = f"{settings.job_state_prefix}{job['job_id']}"
    await redis.hset(job_key, mapping={
        "status": JobStatus.PROCESSING,
        "worker_id": settings.worker_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
    })
    logger.info(f"[{job['job_id']}] → PROCESSING on {settings.worker_id}")


async def mark_completed(redis: aioredis.Redis, job: dict, result: JobResult) -> None:
    """Pipeline ran successfully — store results."""
    job_key = f"{settings.job_state_prefix}{job['job_id']}"
    await redis.hset(job_key, mapping={
        "status": JobStatus.COMPLETED,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "result": json.dumps(result.data),
    })
    logger.info(f"[{job['job_id']}] → COMPLETED")


async def mark_failed_and_retry(redis: aioredis.Redis, job: dict, error: str) -> None:
    """
    Pipeline failed — decide whether to retry or mark corrupted.
    This is the retry logic from the image comment:
    - increment retry_count
    - if below max_retries: push back onto queue
    - if at max_retries: mark corrupted, do not requeue
    
    Key insight: retry_count lives in the JOB PAYLOAD, not just
    the state hash. When the job goes back onto the queue, it carries
    its retry history with it. The next worker that picks it up
    knows how many times it has already failed.
    """
    job_key = f"{settings.job_state_prefix}{job['job_id']}"
    retry_count = job.get("retry_count", 0) + 1
    max_retries = job.get("max_retries", 2)
 
    if retry_count <= max_retries:
        # Update retry count in the job payload before requeueing
        job["retry_count"] = retry_count
 
        # Push back to queue — worker picks it up again later
        await redis.lpush(settings.job_queue_key, json.dumps(job))
 
        # Update state hash — status goes back to QUEUED
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
        # Exhausted all retries — equivalent to "corrupted folder" in the image
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

# ── PIPELINES ─────────────────────────────────────────────────
# Each pipeline is an async function that receives the job config
# and returns a JobResult. The worker doesn't care what's inside —
# it just calls the right one based on job_type.
# Adding a new pipeline = adding a new function + one line in route_job().

async def run_load_test(config: dict) -> JobResult:
    """
    Fires concurrent HTTP requests at target_url.
    Uses asyncio.gather for true concurrency — all requests
    are in flight simultaneously, not sequentially.
 
    This is a simplified Phase 1 version.
    Phase 2 will add: percentile latencies, error bucketing,
    per-worker result aggregation.
    """

    target_url = config["target_url"]
    request_count = config["request_count"]
    concurrency = config["concurrency"]
    timeout = config.get("timeout_seconds", 30)

    results = {
        "target_url": target_url,
        "request_count": request_count,
        "concurrency": concurrency,
        "success_count": 0,
        "error_count": 0,
        "latencies_ms": [],
        "errors": [],
    }

    # Semaphore limits how many requests are in flight at once.
    # Without this, request_count=1000 would launch 1000 coroutines
    # simultaneously — overwhelming the target and your own network.

    semaphore = asyncio.Semaphore(concurrency)

    async def fire_single_request(client: httpx.AsyncClient) -> None:
        async with semaphore:
            start = asyncio.get_event_loop().time()
            try:
                response = await client.get(target_url, timeout=timeout)
                elapsed_ms = (asyncio.get_event_loop().time() - start) * 1000
                results["latencies_ms"].append(round(elapsed_ms, 2))

                if response.status_code < 400:
                    results["success_count"] += 1
                else:
                    results["error_count"] += 1
                    results["errors"].append(f"HTTP {response.status_code}")
            except Exception as e:
                results["error_count"] += 1
                results["errors"].append(str(e))

    async with httpx.AsyncClient() as client:
        tasks = [fire_single_request(client=client) for _ in range(request_count)]
        await asyncio.gather(*tasks)

    # Compute summary stats
    latencies = results["latencies_ms"]
    if latencies:
        results["avg_latency_ms"] = round(sum(latencies) / len(latencies), 2)
        results["min_latency_ms"] = round(min(latencies), 2)
        results["max_latency_ms"] = round(max(latencies), 2)
        # p95 — sort and take the value at the 95th percentile index
        sorted_latencies = sorted(latencies)
        p95_index = int(len(sorted_latencies) * 0.95)
        results["p95_latency_ms"] = sorted_latencies[p95_index]

    # Remove raw latency list from stored results — can be thousands of floats
    results.pop("latencies_ms")
    # Keep only unique errors, capped for readability
    results["errors"] = list(set(results["errors"]))[:10]

    logger.info(
        f"Load test complete | "
        f"success={results['success_count']} "
        f"errors={results['error_count']} "
        f"avg_latency={results.get('avg_latency_ms')}ms"
    )
    return JobResult(success=True, data=results)


async def run_video_process(config: dict) -> JobResult:
    """
    Phase 1 stub — video processing pipeline placeholder.
    Real implementation (ffmpeg, HLS conversion, R2 upload)
    comes in Phase 2 when we build the video platform.
 
    The worker routing works now. The pipeline itself is TODO.
    This is intentional — you can submit video_process jobs,
    watch them move through the queue, and verify the state
    machine works before the pipeline exists.
    """
    source_path = config.get("source_path")
    resolutions = config.get("output_resolutions", ["1080p", "720p", "480p"])
 
    logger.info(f"Video process stub | source={source_path} | resolutions={resolutions}")
 
    # Simulate processing time so you can watch state transitions
    await asyncio.sleep(2)
 
    return JobResult(
        success=True,
        data={
            "source_path": source_path,
            "output_resolutions": resolutions,
            "note": "Phase 1 stub — real ffmpeg pipeline comes in Phase 2",
        }
    )
 

# ── JOB ROUTER ────────────────────────────────────────────────
# Single dispatch point. The worker calls this and doesn't
# need to know what pipeline does what internally.
async def route_job(job: dict) -> JobResult:
    job_type = job.get("job_type")
    config = job.get("config", {})
 
    if job_type == "load_test":
        return await run_load_test(config)
    elif job_type == "video_process":
        return await run_video_process(config)
    else:
        return JobResult(
            success=False,
            error=f"Unknown job_type: {job_type}"
        )
        

# ── MAIN WORKER LOOP ──────────────────────────────────────────

class Worker:
    """
    The worker is a long-running process with one loop:
      1. Block on Redis queue (brpop) — sleep until a job arrives
      2. Deserialize the job
      3. Mark it processing
      4. Route to pipeline
      5. Mark completed or handle failure/retry
      6. Go back to step 1
 
    Shutdown is handled gracefully — the worker finishes its
    current job before stopping. It does not drop a job mid-flight.
    """

    def __init__(self):
        self.redis: aioredis.Redis | None = None
        self._shutdown = False # flag checked between jobs

    async def connect(self) -> None:
        self.redis = aioredis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True
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
        """
        Called on SIGTERM or SIGINT (Ctrl+C / docker stop).
        Sets the flag — the loop exits after the current job finishes.
        This is graceful shutdown — no job is abandoned mid-flight.
        """

        logger.info(f"Worker {settings.worker_id} received shutdown signal")
        self._shutdown = True

    async def process_one(self, raw: str) -> None:
        """Deserialize and process a single job end to end."""
        try:
            job = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to deserialise job: {e} | raw={raw[:100]}")
            return # Malformed job, discard, do not retry
        
        job_id = job.get("job_id", "unknown")
        logger.info(f"[{job_id}] Picked up | type={job.get('job_type')} | retry={job.get('retry_count', 0)}")

        await mark_processing(self.redis, job=job)

        try:
            result = await route_job(job)

            if result.success:
                await mark_completed(self.redis, job=job, result=result)
            else:
                await mark_failed_and_retry(self.redis, job, result.error or "pipeline returned failure")

        except Exception as e:
            # Unexpected exception — treat as failure, apply retry logic
            logger.exception(f"[{job_id}] Unhandled exception in pipeline")
            await mark_failed_and_retry(self.redis, job, str(e))

    
    async def run(self) -> None:
        """
        This is the main loop.

        brpop is the core primitive:
        - Blocks until a job appears in the queue
        - Returns (queue_name, job_json) tuple
        - timeout = brpop_timeout means it unblocks every N seconds to check
        the _shutdown flag, then blocks again if no job

        This is more efficeint than polling with a sleep loop:
        polling -> hits Redis every N seconds regardless of queue state
        brpop -> wakes up only when a job arrives (or timeout expires)        
        
        """

        await self.connect()
        
        # Register OS signal handlers for a graceful shutdown
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

        logger.info(
            f"Worker {settings.worker_id} started — listening on '{settings.job_queue_key}'"
        )

        while not self._shutdown:
            try:
                # brpop returns None on timeout, tuple on job arrival
                response = await self.redis.brpop(
                    settings.job_queue_key,
                    timeout=settings.brpop_timeout,
                )

                if response is None:
                    # Timeout expired, no job - loop back and block again
                    continue

                _queue_name, raw_job = response
                await self.process_one(raw=raw_job)

            except aioredis.RedisError as e:
                logger.error(f"Redis error: {e} — retrying in 3s")
                await asyncio.sleep(3) # back off before reconnecting

            except Exception as e:
                logger.exception(f"Unexpected error in worker loop: {e}")
                await asyncio.sleep(1)

        logger.info(f"Worker {settings.worker_id} shut down cleanly")
        await self.disconnect()


# ── ENTRYPOINT ────────────────────────────────────────────────
if __name__ == "__main__":
    worker = Worker()
    asyncio.run(worker.run())      



