import time
import argparse
import concurrent.futures
import requests

GATEWAY_URL = "http://localhost:8000"


def submit_and_wait(i: int):
    start = time.time()
    resp = requests.post(f"{GATEWAY_URL}/jobs", json={"payload": f"task-{i}"})
    job_id = resp.json()["job_id"]

    while True:
        r = requests.get(f"{GATEWAY_URL}/jobs/{job_id}")
        status = r.json().get("status")
        if status in ("done", "failed"):
            break
        time.sleep(0.05)

    return time.time() - start, status


def run_benchmark(num_jobs: int, concurrency: int):
    start_all = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        results = list(executor.map(submit_and_wait, range(num_jobs)))
    total_time = time.time() - start_all

    latencies = sorted(latency for latency, _ in results)
    failures = sum(1 for _, status in results if status == "failed")
    p50 = latencies[len(latencies) // 2]
    p95 = latencies[int(len(latencies) * 0.95) - 1]
    throughput = num_jobs / total_time

    print(f"\n--- Benchmark result ---")
    print(f"Jobs submitted:     {num_jobs}")
    print(f"Client concurrency: {concurrency}")
    print(f"Total wall time:    {total_time:.2f}s")
    print(f"Throughput:         {throughput:.2f} jobs/sec")
    print(f"Latency p50:        {p50:.3f}s")
    print(f"Latency p95:        {p95:.3f}s")
    print(f"Failures:           {failures}")
    print(f"------------------------\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load test the job processing system")
    parser.add_argument("--jobs", type=int, default=200, help="Number of jobs to submit")
    parser.add_argument("--concurrency", type=int, default=50, help="Concurrent client threads")
    args = parser.parse_args()
    run_benchmark(args.jobs, args.concurrency)
