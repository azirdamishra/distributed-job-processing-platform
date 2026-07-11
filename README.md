# distributed-platform

A distributed job processing platform built with Python, Redis, and Docker. Supports multiple processing pipelines — load testing and video processing — through a shared queue and worker architecture.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Admin Dashboard                    │
│     submit jobs · monitor containers · metrics       │
└────────────────────┬────────────────────────────────┘
                     │
        ┌────────────▼────────────┐
        │      Edge API Layer      │
        │    FastAPI + autoscaler  │
        └──┬──────────┬───────────┘
           │          │
    ┌──────▼──┐   ┌───▼──────────┐
    │  Video  │   │  Load Test   │
    │ Workers │   │   Workers    │
    └──┬──────┘   └───┬──────────┘
       │              │
    ┌──▼──────────────▼──┐
    │     Redis Core      │
    │   Queue · State     │
    │   Metrics · Retry   │
    └─────────────────────┘
```

---

## Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| API | FastAPI |
| Queue | Redis 7 |
| Workers | Python + asyncio |
| HTTP client | httpx |
| Containerisation | Docker + Compose |
| Video processing | ffmpeg (Phase 2) |
| CDN | Cloudflare R2 + Workers (Phase 2) |

---

## Phases

### Phase 1 — Queue and Workers ✅

Redis job queue, stateless worker containers, retry logic, and a FastAPI producer. The foundation every subsequent phase builds on.

### Phase 2 — Processing Pipelines 🔜

Real pipeline implementations — ffmpeg HLS conversion with Cloudflare R2 upload for video, and full concurrency metrics with percentile latencies for load testing.

### Phase 3 — Autoscaling and Orchestration 🔜

An orchestrator that watches queue depth and container memory usage and programmatically scales worker count via the Docker API.

### Phase 4 — Observability and Dashboard 🔜

Live admin dashboard showing queue depth, container health, and job history across all workers.

---

## Prerequisites

- Docker Desktop

---

## Running Phase 1

```bash
git clone https://github.com/yourusername/distributed-platform
cd distributed-platform/phase1

docker-compose up --build
```

To run multiple workers:

```bash
docker-compose up --build --scale worker=3
```

---

## Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | API and Redis connectivity |
| POST | `/jobs` | Submit a job to the queue |
| GET | `/jobs/{id}` | Poll job status |
| GET | `/jobs` | Queue depth and all jobs |
| GET | `/docs` | Interactive API explorer |

---

## Usage

**Submit a load test job:**
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "load_test",
    "config": {
      "target_url": "https://httpbin.org/get",
      "request_count": 50,
      "concurrency": 5
    }
  }'
```

**Submit a video process job:**
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "video_process",
    "config": {
      "source_path": "/videos/sample.mp4",
      "output_resolutions": ["1080p", "720p", "480p"]
    }
  }'
```

**Poll job status:**
```bash
curl http://localhost:8000/jobs/{job_id}
```

**Inspect the queue directly:**
```bash
docker exec -it redis redis-cli

LLEN jobs:queue
KEYS jobs:state:*
HGETALL jobs:state:{job_id}
```

---

## Project structure

```
distributed-platform/
└── phase1/
    ├── docker-compose.yml
    ├── redis/
    │   └── redis.conf
    ├── api/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── main.py
    └── worker/
        ├── Dockerfile
        ├── requirements.txt
        └── worker.py
```