import redis
import time
import uuid
import json
import os


class LeaderElection:
    def __init__(self, redis_port=6379, lock_key="scheduler_leader", ttl=10):
        redis_host = os.getenv("REDIS_HOST", "localhost")
        self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.lock_key = lock_key
        self.ttl = ttl
        self.node_id = str(uuid.uuid4())[:8]
        self.is_leader = False

    def try_acquire_leadership(self):
        acquired = self.r.set(self.lock_key, self.node_id, nx=True, ex=self.ttl)
        if acquired:
            self.is_leader = True
            return True
        return False

    def renew_leadership(self):
        current_holder = self.r.get(self.lock_key)
        if current_holder == self.node_id:
            self.r.expire(self.lock_key, self.ttl)
            return True
        else:
            self.is_leader = False
            return False

    def push_job(self, message):
        job = {"job_id": str(uuid.uuid4())[:8], "message": message, "token": 1}
        self.r.lpush("job_queue", json.dumps(job))
        print(f"[{self.node_id}] Pushed job: {job['job_id']} (token {job['token']})")

    def run_election_loop(self):
        counter = 0
        while True:
            if not self.is_leader:
                if self.try_acquire_leadership():
                    print(f"[{self.node_id}] Became leader.")
                else:
                    print(f"[{self.node_id}] Waiting... someone else is leader.")
            else:
                if self.renew_leadership():
                    print(f"[{self.node_id}] Still leader, renewed lease.")
                    counter += 1
                    self.push_job(f"scheduled task #{counter}")
                else:
                    print(f"[{self.node_id}] Lost leadership!")
            time.sleep(self.ttl / 3)


if __name__ == "__main__":
    node = LeaderElection()
    node.run_election_loop()