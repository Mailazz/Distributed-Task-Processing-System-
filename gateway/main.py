import os
import uuid
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis

app = FastAPI(title="Distributed Task Processing - API Gateway")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
STREAM_NAME = "tasks"
GROUP_NAME = "workers"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def ensure_group():
    """Create the consumer group once. Safe to call on every startup."""
    try:
        r.xgroup_create(name=STREAM_NAME, groupname=GROUP_NAME, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


ensure_group()


class JobSubmission(BaseModel):
    payload: str
    priority: int = 0


@app.post("/jobs")
def submit_job(job: JobSubmission):
    job_id = str(uuid.uuid4())
    now = time.time()
    job_data = {
        "id": job_id,
        "payload": job.payload,
        "priority": job.priority,
        "status": "queued",
        "attempts": 0,
        "created_at": now,
        "started_at": "",
        "finished_at": "",
        "result": "",
        "worker_id": "",
    }
    r.hset(f"job:{job_id}", mapping=job_data)
    r.xadd(STREAM_NAME, {"job_id": job_id})
    return {"job_id": job_id, "status": "queued"}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    data = r.hgetall(f"job:{job_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Job not found")
    return data


@app.get("/stats")
def stats():
    """Useful for watching queue depth and in-flight (unacked) jobs live."""
    info = r.xinfo_stream(STREAM_NAME)
    pending = r.xpending(STREAM_NAME, GROUP_NAME)
    return {"stream_length": info.get("length"), "pending": pending}


@app.get("/health")
def health():
    return {"status": "ok"}
