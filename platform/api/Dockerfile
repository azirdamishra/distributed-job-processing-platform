# ─────────────────────────────────────────────────────────────
# worker/Dockerfile
# ─────────────────────────────────────────────────────────────
# Structurally identical to api/Dockerfile — same layer ordering
# principle. The difference is in the CMD at the bottom.
#
# The API runs uvicorn — a server that waits for incoming requests.
# The worker runs a plain Python script — a loop that pulls from Redis.
# No HTTP server needed. No port needed.
# ─────────────────────────────────────────────────────────────

FROM python:3.11-slim

WORKDIR /app

# Dependencies before code — same caching rationale as api/Dockerfile
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# ── No EXPOSE here ────────────────────────────────────────────
# Workers receive no inbound traffic.
# They reach OUT to Redis. Nothing reaches IN to them.
# This is the pull model — no port means nothing can push to them.

# ── CMD difference from API ───────────────────────────────────
# python worker.py — just a Python script running in a loop.
# No uvicorn, no server, no port binding.
# The worker's entire interface is the Redis queue.
EXPOSE 8000

# Run the FastAPI app with Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]