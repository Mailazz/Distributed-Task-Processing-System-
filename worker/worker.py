import os
import time
import socket
import random
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
STREAM_NAME = "tasks"
GROUP_NAME = "workers"
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", 3))
CLAIM_IDLE_MS = int(os.getenv("CLAIM_IDLE_MS", 10000))  # reclaim jobs stuck longer than this
SIMULATED_FAIL_RATE = float(os.getenv("SIMULATED_FAIL_RATE", 0.0))  # for testing recovery

CONSUMER_NAME = f"{socket.gethostname()}-{os.getpid()}"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def ensure_group():
    try:
        r.xgroup_create(name=STREAM_NAME, groupname=GROUP_NAME, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def process_job(job_id: str, entry_id: str):
    r.hset(f"job:{job_id}", mapping={
        "status": "processing",
        "started_at": time.time(),
        "worker_id": CONSUMER_NAME,
    })
    r.hincrby(f"job:{job_id}", "attempts", 1)

    # Simulated work. SIMULATED_FAIL_RATE lets you deliberately trigger
    # failures to prove the retry/recovery path actually works.
    time.sleep(random.uniform(0.05, 0.3))
    if random.random() < SIMULATED_FAIL_RATE:
        raise RuntimeError("Simulated processing failure")

    r.hset(f"job:{job_id}", mapping={
        "status": "done",
        "finished_at": time.time(),
        "result": f"processed by {CONSUMER_NAME}",
    })
    r.xack(STREAM_NAME, GROUP_NAME, entry_id)


def reclaim_stuck_jobs():
    """
    This is the failure-recovery mechanism. If a worker dies (crash, OOM kill,
    node loss) after reading a job but before ACKing it, that job sits in the
    stream's Pending Entries List forever. This function periodically scans
    for entries idle longer than CLAIM_IDLE_MS, claims them for this worker,
    and either retries or marks them permanently failed.
    """
    pending = r.xpending_range(STREAM_NAME, GROUP_NAME, min="-", max="+", count=50)
    for entry in pending:
        entry_id = entry["message_id"]
        idle_ms = entry["time_since_delivered"]
        if idle_ms <= CLAIM_IDLE_MS:
            continue

        claimed = r.xclaim(
            STREAM_NAME, GROUP_NAME, CONSUMER_NAME,
            min_idle_time=CLAIM_IDLE_MS, message_ids=[entry_id],
        )
        for msg_id, fields in claimed:
            job_id = fields["job_id"]
            attempts = int(r.hget(f"job:{job_id}", "attempts") or 0)

            if attempts >= MAX_ATTEMPTS:
                r.hset(f"job:{job_id}", mapping={"status": "failed"})
                r.xack(STREAM_NAME, GROUP_NAME, msg_id)
                print(f"[{CONSUMER_NAME}] job {job_id} exceeded {MAX_ATTEMPTS} attempts -> failed")
                continue

            print(f"[{CONSUMER_NAME}] reclaimed job {job_id} from a dead/stuck worker (attempt {attempts + 1})")
            try:
                process_job(job_id, msg_id)
            except Exception as e:
                print(f"[{CONSUMER_NAME}] reclaimed job {job_id} failed again: {e}")


def main():
    ensure_group()
    print(f"[{CONSUMER_NAME}] worker started, policy=FIFO stream consumer")

    while True:
        reclaim_stuck_jobs()

        resp = r.xreadgroup(GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=1, block=2000)
        if not resp:
            continue

        for _stream, entries in resp:
            for entry_id, fields in entries:
                job_id = fields["job_id"]
                try:
                    process_job(job_id, entry_id)
                    print(f"[{CONSUMER_NAME}] completed job {job_id}")
                except Exception as e:
                    # Deliberately do NOT ack here. The job stays in the
                    # Pending Entries List and reclaim_stuck_jobs() will
                    # retry it (or fail it permanently) on a later pass.
                    print(f"[{CONSUMER_NAME}] job {job_id} failed: {e}")


if __name__ == "__main__":
    main()
