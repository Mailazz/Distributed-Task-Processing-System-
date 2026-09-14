# Distributed Task Processing System

A distributed job-processing platform: an API gateway accepts jobs, a Redis
Stream acts as the message queue, and a pool of worker processes consumes and
executes jobs — with automatic retries and failure recovery if a worker dies
mid-job.

## Architecture

```
Client -> POST /jobs -> FastAPI gateway -> Redis Stream ("tasks")
                                              |
                     workers (consumer group "workers") pull jobs
                     each worker: process -> ACK on success
                     on crash: job stays "pending" -> reclaimed by
                     another worker via XCLAIM after a timeout
```

Why Redis **Streams** instead of a plain list/queue: streams give you a
built-in Pending Entries List (PEL) per consumer group. If a worker reads a
job and dies before acknowledging it, the job isn't lost — it just sits as
"pending," and any worker can later reclaim it with `XCLAIM`. That's what
gives you real automatic failure recovery instead of a queue that silently
drops in-flight work.

## Step-by-step: how this was built

1. **Job model** : each job gets a UUID and is stored as a Redis hash
   (`job:<id>`) holding status, timestamps, attempt count, and result. This
   is what `GET /jobs/{id}` reads from.
2. **API gateway** (`gateway/main.py`) : FastAPI app with `POST /jobs` (adds
   the job hash, then `XADD`s it to the stream) and `GET /jobs/{id}` (polls
   status). A `/stats` endpoint exposes queue depth and pending count.
3. **Message queue** : a Redis Stream (`tasks`) with a consumer group
   (`workers`), created once via `XGROUP CREATE`.
4. **Worker pool** (`worker/worker.py`) : each worker is its own process,
   reads new jobs with `XREADGROUP`, processes them, and `XACK`s on success.
5. **Retries** — on failure, the job is deliberately left un-ACKed. It's
   retried automatically (see step 6) rather than requiring the worker to
   manually requeue it.
6. **Failure recovery** : before reading new jobs, every worker calls
   `reclaim_stuck_jobs()`, which scans the PEL for entries idle longer than
   `CLAIM_IDLE_MS`, claims them, and either retries or marks them `failed`
   once `MAX_ATTEMPTS` is exceeded. This is what recovers work automatically
   if a worker container crashes or is killed.
7. **Containerization** : each service (`gateway/`, `worker/`) has its own
   Dockerfile; `docker-compose.yml` wires them together with Redis.
8. **Benchmarking** (`benchmark/load_test.py`) : submits N jobs concurrently,
   polls each until done, and reports throughput (jobs/sec) and latency
   percentiles.

## Running it locally

```bash
git clone <your-repo-url>
cd distributed-task-processing

# Start Redis + gateway + 1 worker
docker compose up --build
```

Check it's alive:
```bash
curl http://localhost:8000/health
```

Submit a job:
```bash
curl -X POST http://localhost:8000/jobs -H "Content-Type: application/json" \
  -d '{"payload": "hello world"}'
```

Check its status (use the `job_id` returned above):
```bash
curl http://localhost:8000/jobs/<job_id>
```

## Proving failure recovery actually works

1. Start with `SIMULATED_FAIL_RATE=0` in `docker-compose.yml` and bring the
   stack up with 2+ workers:
   ```bash
   docker compose up --build --scale worker=2
   ```
2. Submit a batch of jobs (see benchmark section below).
3. While jobs are processing, kill one worker container to simulate a crash:
   ```bash
   docker ps          # find a worker container ID
   docker kill <container_id>
   ```
4. Watch the surviving worker's logs — within `CLAIM_IDLE_MS` (default 10s)
   you should see lines like `reclaimed job <id> from a dead/stuck worker`.
   The job still completes even though the worker that originally picked it
   up never came back.
5. For a repeatable demo of this without manually killing containers, set
   `SIMULATED_FAIL_RATE=0.1` — 10% of jobs will raise an exception on
   purpose, and you can watch retries and eventual success/failure in the
   logs and via `/jobs/{id}`.

## Benchmarking throughput and latency: 1 -> 4 workers

Install the benchmark client's dependencies once:
```bash
cd benchmark
pip install -r requirements.txt --break-system-packages
```

Run the same workload against different worker counts and record the
results:

```bash
# 1 worker
docker compose up -d --build --scale worker=1
python load_test.py --jobs 500 --concurrency 50

# 2 workers
docker compose up -d --scale worker=2
python load_test.py --jobs 500 --concurrency 50

# 4 workers
docker compose up -d --scale worker=4
python load_test.py --jobs 500 --concurrency 50
```

Each run prints throughput (jobs/sec) and p50/p95 latency. Record these into
a table like:

| Workers | Throughput (jobs/sec) | p50 latency | p95 latency |
|---|---|---|---|
| 1 |45.2 |0.85s |1.3s |
| 2 |54.5 |0.95s |1.15s|
| 4 |49 | |0.85s |1.5s |

Paste your actual numbers here once you've run it — this table (plus a
simple bar chart of throughput vs. worker count) is exactly what goes in
your project README/portfolio to back up the "benchmarked throughput and
latency scaling from 1 to 4 workers" claim on your CV.

## Environment variables (worker)

| Variable | Default | Meaning |
|---|---|---|
| `REDIS_HOST` | `localhost` | Redis hostname |
| `MAX_ATTEMPTS` | `3` | Retries before a job is marked permanently failed |
| `CLAIM_IDLE_MS` | `10000` | How long a job can sit unacked before another worker reclaims it |
| `SIMULATED_FAIL_RATE` | `0.0` | Probability (0-1) a job deliberately fails, for testing recovery |

## Future Improvements

- Add a dead-letter stream for permanently failed jobs instead of just
  marking them `failed` in the hash.
- Add priority-aware consumption (currently FIFO via the stream order).
- Deploy to Kubernetes with the worker Deployment's `replicas` field driving
  scale-out, and use `kubectl scale` instead of `docker compose --scale` for
  the benchmark.
