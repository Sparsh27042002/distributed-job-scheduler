from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import json
import uuid
import time
import os
import psycopg2

app = FastAPI()

redis_host = os.getenv("REDIS_HOST", "localhost")
r = redis.Redis(host=redis_host, port=6379, decode_responses=True)

JOB_QUEUE = "job_queue"
JOB_STATUS = "job_status"

def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=5432,
        user="scheduler",
        password="scheduler",
        dbname="scheduler_db"
    )

def init_db():
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS job_history (
            job_id TEXT PRIMARY KEY,
            message TEXT,
            status TEXT,
            submitted_at DOUBLE PRECISION,
            completed_at DOUBLE PRECISION
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

init_db()


class JobRequest(BaseModel):
    message: str


@app.post("/jobs")
def submit_job(req: JobRequest):
    job_id = str(uuid.uuid4())[:8]
    job = {
        "job_id": job_id,
        "message": req.message,
        "token": 1,
        "status": "queued",
        "submitted_at": time.time()
    }
    r.lpush(JOB_QUEUE, json.dumps(job))
    r.hset(JOB_STATUS, job_id, json.dumps(job))
    return {"job_id": job_id, "status": "queued"}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job_json = r.hget(JOB_STATUS, job_id)
    if job_json is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return json.loads(job_json)


@app.get("/jobs")
def list_jobs():
    all_jobs = r.hgetall(JOB_STATUS)
    return [json.loads(v) for v in all_jobs.values()]


@app.delete("/jobs/{job_id}")
def cancel_job(job_id: str):
    job_json = r.hget(JOB_STATUS, job_id)
    if job_json is None:
        raise HTTPException(status_code=404, detail="Job not found")
    job = json.loads(job_json)
    if job["status"] == "completed":
        raise HTTPException(status_code=400, detail="Cannot cancel a completed job")
    job["status"] = "cancelled"
    r.hset(JOB_STATUS, job_id, json.dumps(job))
    return {"job_id": job_id, "status": "cancelled"}


@app.get("/jobs/history/all")
def job_history():
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT job_id, message, status, submitted_at, completed_at FROM job_history ORDER BY completed_at DESC")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {"job_id": r[0], "message": r[1], "status": r[2], "submitted_at": r[3], "completed_at": r[4]}
        for r in rows
    ]