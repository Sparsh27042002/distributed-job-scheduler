# Distributed Job Scheduler

> A fault-tolerant distributed task scheduling system built from first principles — implementing leader election, crash recovery, exactly-once job delivery, and persistent job history across independently containerized services.

---

## Why this exists

Most job-scheduling systems developers encounter are abstracted behind libraries like Celery or managed services like AWS Batch. This project was built deliberately without those abstractions, to understand what they're actually solving underneath: how does a system decide which node is in charge? What happens when that node dies? How do you guarantee a job isn't lost when a worker crashes mid-execution, without accidentally running it twice?

Those three questions — leader election, crash recovery, and duplicate prevention — are the core of this project, and each one is implemented from scratch, tested by deliberately breaking the system, and proven to recover correctly.

---

## Architecture

```
                    ┌─────────────────────────────────┐
                    │         REST API Gateway          │
                    │         (FastAPI :8500)           │
                    └────────────┬────────────────┬─────┘
                    Submit/Query │                │ History
                                 ▼                ▼
                           ┌──────────┐    ┌──────────────┐
                           │  Redis   │    │  PostgreSQL  │
                           │ (queues, │    │  (durable    │
                           │ live     │    │   job        │
                           │ status,  │    │   history)   │
                           │ leader   │    └──────────────┘
                           │ election)│
                           └────┬─────┘
              ┌─────────────────┼──────────────────────┐
              ▼                 ▼                       ▼
     ┌────────────────┐  ┌─────────────┐   ┌────────────────────┐
     │  Leader Node   │  │ Leader Node │   │     Watchdog       │
     │  (active —     │  │ (standby —  │   │  (monitors stuck   │
     │   pushes jobs) │  │  waiting)   │   │   jobs, re-queues) │
     └────────────────┘  └─────────────┘   └────────────────────┘
                                 │
              ┌──────────────────┴───────────────────┐
              ▼                                       ▼
     ┌─────────────────┐                  ┌─────────────────┐
     │    Worker 1     │                  │   Worker 2      │
     │ (processes jobs)│                  │ (processes jobs)│
     └─────────────────┘                  └─────────────────┘
```

**What each component actually does:**

- **Leader nodes** — two run simultaneously, racing to hold a Redis lease. Only one wins at any time and is responsible for pushing jobs onto the queue. If it dies, the other detects the expired lease and takes over within one TTL window — automatically, with no human intervention.
- **Worker nodes** — multiple run concurrently, pulling jobs from the Redis queue. Redis's atomic `BRPOP` guarantees exactly one worker gets each job.
- **Watchdog** — monitors an "in-progress" registry. Any job that's been processing longer than a threshold (worker likely crashed) gets re-queued, with an incremented fencing token to prevent a stale, slow-but-alive original worker from double-completing it.
- **Gateway** — exposes the REST API. Live job state is served from Redis for low latency. Completed job history is read from PostgreSQL for durability.

---

## What was proven in testing, not just assumed

**Leader failover:** the active leader container was `docker stop`'d mid-run. The standby detected the expired Redis lease within one TTL window (~10 seconds) and became the new leader, resuming job production with zero intervention. Workers never noticed — they just kept consuming from the same queue.

**Exactly-once delivery:** two worker containers ran simultaneously against the same queue for 50+ jobs. Zero duplicate job IDs appeared across both workers' logs. Redis's atomic `BRPOP` call popped each item exactly once.

**Crash recovery:** a worker container was killed between "picked up job" and "completed job." The watchdog detected the stuck in-progress entry after the configured threshold, re-queued it with a bumped token, and a different worker completed it. No job was permanently lost.

**Fencing token correctness:** if a "slow but alive" worker eventually finishes after its job has already been reclaimed, it reads the current token, sees a mismatch against its own, and discards its result rather than double-counting the completion.

---

## Stack

| Layer | Tech | Why |
|---|---|---|
| Coordination | Redis | Atomic `SET NX EX` for leader election, `BRPOP` for job queue |
| Live state | Redis hash | Low-latency job status reads |
| Durable history | PostgreSQL | Survives Redis restarts; queryable audit trail |
| API | FastAPI | Job submission, status polling, cancellation, history |
| Containerization | Docker + Docker Compose | 8 services, single-command deployment |
| Queue/Worker | Hand-built | See Design Decisions |

---

## Setup

**Prerequisites:** Docker Desktop

```bash
git clone https://github.com/abhisheksingh-cpu/distributed-job-scheduler.git
cd distributed-job-scheduler
docker-compose up --build
```

That single command starts all 8 services: Redis, PostgreSQL, 2 leader nodes, 2 worker nodes, 1 watchdog, and the API gateway.

**API available at:** `http://localhost:8500/docs`

---

## API endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Submit a new job |
| `GET` | `/jobs/{job_id}` | Get live status from Redis |
| `GET` | `/jobs` | List all live jobs |
| `DELETE` | `/jobs/{job_id}` | Cancel a queued/in-progress job |
| `GET` | `/jobs/history/all` | Query completed job history from PostgreSQL |

---

## Design decisions

**Why hand-build the queue and worker logic instead of using Celery?**

Celery is a legitimate production tool. It was skipped here deliberately: using it would mean configuring an abstraction over the exact distributed-systems problems this project is designed to reason through directly. Building the queue, worker pool, fencing tokens, and watchdog from scratch means being able to explain every failure mode from first principles — which is the point of the project, not just running it.

**Why Redis for leader election instead of ZooKeeper or etcd?**

ZooKeeper and etcd implement stronger consistency guarantees (Paxos/Raft-based), which is the right choice for systems that can't tolerate even brief split-brain scenarios. Redis's `SET NX EX` pattern is a simpler, widely-used approximation that's sufficient for this scheduler's requirements and more accessible to understand mechanically. The tradeoff — weaker consistency guarantees in edge cases — is a real one, acknowledged and documented, not a gap.

**Why two separate storage layers (Redis + PostgreSQL)?**

Redis is fast but not designed as a permanent record — in-memory, with configurable but not default persistence, and not ideal as an audit log. PostgreSQL is the right tool for "give me all jobs that completed between these timestamps." The two-tier design (Redis for hot, transient state; Postgres for durable history) reflects a common production pattern worth demonstrating explicitly.

**Why is the watchdog threshold a fixed time rather than heartbeats?**

A heartbeat-based approach would be strictly more accurate (detects a dead worker the moment its heartbeat stops, rather than waiting for a timeout to expire). The fixed-timeout pattern was chosen here because it's simpler to reason about and easier to test — you kill a worker, wait N seconds, and watch the watchdog fire. Switching to heartbeats is a documented next step, not an oversight.

---

## Engineering log

Real problems found and fixed during development — worth documenting because the debugging process is as much the point as the final architecture.

**Infinite watchdog re-queue loop.** With no worker running during a test, a stuck job got re-queued every 15 seconds, token incrementing to 50+ before being noticed. The system was functionally correct (it kept trying), but wasteful and never failing visibly. Fixed by adding a max-retry cap after which the job moves to a dead-letter queue — surfacing failure explicitly instead of looping forever.

**Docker Compose service-readiness race.** The gateway's `init_db()` function ran and failed because PostgreSQL's container had started but hadn't finished initializing and accepting connections. `depends_on` only guarantees container start order, not service readiness. Fixed by wrapping `init_db()` in a retry-with-backoff loop, which is the correct, production-appropriate solution to this extremely common containerization problem.

**Python stdout buffering in containers.** Worker and leader containers appeared to be running but produced zero log output. No crash, no error — just silence, while the queue was being consumed correctly the whole time. Root cause: Python's stdout is block-buffered when not attached to a TTY, so `print()` statements don't flush immediately. Fixed by setting `PYTHONUNBUFFERED=1` in the container environment. The system was working correctly the entire time; the debugging effort was purely a visibility problem, not a functional one.

**Missing port mapping on gateway container.** After one rebuild, the gateway was up and accepting connections internally but unreachable from the host browser. `docker ps` showed `8500/tcp` (internal only) instead of `0.0.0.0:8500->8500/tcp` (host-mapped). A `ports:` section had been accidentally dropped from the gateway's `docker-compose.yml` block during an edit. Fixed by restoring the mapping and running `docker-compose down` before `up` to force full container recreation.

---

## What's not done

In the interest of precision: the fencing token approach prevents double-completion but doesn't provide full exactly-once execution semantics under all possible failure modes — a job could theoretically be executed twice if both the original and replacement worker complete before either checks the token (extremely unlikely at this scale, but worth naming). True exactly-once execution under arbitrary failures requires either idempotent job handlers or two-phase commit, both of which are meaningfully more complex. This is a documented limitation, not a hidden one.

Heartbeat-based worker liveness detection would replace the fixed-timeout watchdog with faster, more precise failure detection. Horizontal autoscaling of worker nodes based on queue depth would be the natural next infrastructure addition.
