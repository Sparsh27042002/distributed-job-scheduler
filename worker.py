import redis
import time
import json
import uuid
import os
import psycopg2


class Worker:
    def __init__(self, redis_port=6379, queue_key="job_queue", processing_key="job_processing"):
        redis_host = os.getenv("REDIS_HOST", "localhost")
        self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.queue_key = queue_key
        self.processing_key = processing_key
        self.worker_id = str(uuid.uuid4())[:8]

    def get_pg_conn(self):
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=5432,
            user="scheduler",
            password="scheduler",
            dbname="scheduler_db"
        )

    def archive_job(self, job):
        try:
            conn = self.get_pg_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO job_history (job_id, message, status, submitted_at, completed_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO UPDATE SET status = EXCLUDED.status, completed_at = EXCLUDED.completed_at
            """, (
                job["job_id"],
                job["message"],
                job["status"],
                job.get("submitted_at", job["started_at"]),
                job["completed_at"]
            ))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"[Worker {self.worker_id}] Failed to archive job to Postgres: {e}")

    def run(self):
        print(f"[Worker {self.worker_id}] Starting, waiting for jobs...")
        while True:
            result = self.r.brpop(self.queue_key, timeout=5)
            if result:
                _, job_json = result
                job = json.loads(job_json)
                job["started_at"] = time.time()
                job["worker_id"] = self.worker_id

                self.r.hset(self.processing_key, job["job_id"], json.dumps(job))
                print(f"[Worker {self.worker_id}] Picked up job: {job['job_id']} (token {job['token']})")

                job["status"] = "in_progress"
                self.r.hset("job_status", job["job_id"], json.dumps(job))

                time.sleep(2)

                current = self.r.hget(self.processing_key, job["job_id"])
                if current is None:
                    print(f"[Worker {self.worker_id}] Job {job['job_id']} was already reclaimed — discarding stale result.")
                    continue

                current_job = json.loads(current)
                if current_job["token"] != job["token"]:
                    print(f"[Worker {self.worker_id}] Job {job['job_id']} token mismatch (mine={job['token']}, current={current_job['token']}) — discarding stale result.")
                    continue

                job["status"] = "completed"
                job["completed_at"] = time.time()
                self.r.hset("job_status", job["job_id"], json.dumps(job))
                self.archive_job(job)

                print(f"[Worker {self.worker_id}] Completed job: {job['job_id']} (token {job['token']})")
                self.r.hdel(self.processing_key, job["job_id"])


if __name__ == "__main__":
    worker = Worker()
    worker.run()