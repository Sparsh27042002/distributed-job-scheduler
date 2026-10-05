import redis
import time
import json
import os


class Watchdog:
    def __init__(self, redis_port=6379, queue_key="job_queue", processing_key="job_processing", stuck_threshold=15):
        redis_host = os.getenv("REDIS_HOST", "localhost")
        self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.queue_key = queue_key
        self.processing_key = processing_key
        self.stuck_threshold = stuck_threshold

    def check_for_stuck_jobs(self):
        in_progress = self.r.hgetall(self.processing_key)
        now = time.time()
        for job_id, job_json in in_progress.items():
            job = json.loads(job_json)
            age = now - job["started_at"]
            if age > self.stuck_threshold:
                if job["token"] >= 5:
                    print(f"[Watchdog] Job {job_id} exceeded max retries. Moving to dead letter queue.")
                    job["status"] = "failed"
                    self.r.hset("job_status", job_id, json.dumps(job))
                    self.r.lpush("dead_letter_queue", json.dumps(job))
                    self.r.hdel(self.processing_key, job_id)
                else:
                    job["token"] = job["token"] + 1
                    print(f"[Watchdog] Job {job_id} stuck for {age:.1f}s. Re-queueing with new token {job['token']}.")
                    self.r.hset(self.processing_key, job_id, json.dumps(job))
                    self.r.lpush(self.queue_key, json.dumps(job))

    def run(self):
        print("[Watchdog] Monitoring for stuck jobs...")
        while True:
            self.check_for_stuck_jobs()
            time.sleep(5)


if __name__ == "__main__":
    watchdog = Watchdog()
    watchdog.run()