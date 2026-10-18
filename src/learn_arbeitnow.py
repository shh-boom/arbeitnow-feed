"""Learn the Arbeitnow -> local file -> Kafka path by filling in each STEP.

The polished reference lives in:
  - arbeitnow_client.py
  - arbeitnow_poller.py

Do not copy those yet. Work through this file top to bottom.
Run a STEP with:  python src/learn_arbeitnow.py
(Only the active step's code under `if __name__` should run.)

Architecture (what you are building toward):

  Arbeitnow API
       |
       v
  [1] fetch page of jobs          <- you are here
       |
       v
  [2] pick fields / print
       |
       v
  [3] dedupe (remember slugs)
       |
       v
  [4] append NDJSON (local log)
       |
       v
  [5] schedule: sleep loop OR cron/hourly job
       |
       v
  [6] Kafka producer -> topic arbeitnow.jobs.raw
       |
       v
  [later] consumer / LLM / etc.

Cadence reminder:
  - "every 60 seconds while my laptop is on"  -> sleep loop in this script
  - "every hour / every day even if I log off" -> OS cron, launchd, or Airflow
    calling a one-shot "run once" function (no infinite loop)
"""

# ---------------------------------------------------------------------------
# STEP 1 — Fetch one page of jobs
# Goal: one HTTP GET, print how many jobs came back, print the first title.
# Hints: requests.get(url, timeout=10), response.json(), payload["data"]
# API:   https://www.arbeitnow.com/api/job-board-api?page=1
# ---------------------------------------------------------------------------

import requests

API_URL = "https://www.arbeitnow.com/api/job-board-api"


def step1_fetch_jobs():
    """return the list of job dicts from page 1."""
    response = requests.get(API_URL, params={"page":1}, timeout=10)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        return []
    if not isinstance(payload.get("data"), list):
        return []
    jobs = payload.get("data", [])
    return jobs


# ---------------------------------------------------------------------------
# STEP 2 — Print a few jobs (no Kafka, no files yet)
# Goal: for the first 5 jobs, print: remote/on-site | company | title | location
# ---------------------------------------------------------------------------

def step2_print_jobs(jobs, limit=5):
    """pretty-print a few jobs."""
    for job in jobs[:limit]:
        remote = job.get("remote")
        company_name = job.get("company_name") if job.get("company_name") else None
        title = job.get("title") if job.get("title") else None
        location = job.get("location") if job.get("location") else None
        print(f"{remote} | {company_name} | {title} | {location}")


# ---------------------------------------------------------------------------
# STEP 3 — Deduplicate by slug
# Goal: given jobs + a set of already-seen slugs, return only new jobs
#       and update the set (in memory only for now).
# ---------------------------------------------------------------------------

def step3_new_jobs(jobs, seen_slugs):
    """return jobs whose slug is not in seen_slugs; add them to the set."""
    new_jobs = []
    for job in jobs:
        slug = job.get("slug")
        if slug not in seen_slugs:
            new_jobs.append(job)
            seen_slugs.add(slug)
    return new_jobs

# ---------------------------------------------------------------------------
# STEP 4 — Append one job as one NDJSON line
# Goal: open data/raw/learn_jobs.ndjson in append mode, write json + "\n"
# Use a SEPARATE file from the polished poller (jobs.ndjson) so you do not
# mix learning runs with the reference output.
# ---------------------------------------------------------------------------

import json
import os

FILE_PATH = os.path.join(os.path.dirname(__file__), "../data", "raw", "learn_jobs.ndjson")

def step4_append_job(job):
    """append one job dict as a single JSON line."""
    os.makedirs(os.path.dirname(FILE_PATH), exist_ok=True)
    with open(FILE_PATH, "a", ) as f:
        f.write(json.dumps(job) + "\n")


# ---------------------------------------------------------------------------
# STEP 5 — Run once vs loop forever
# Goal: understand scheduling.
#   run_once()  -> fetch, dedupe, print, append  (good for cron hourly/daily)
#   run_loop()  -> while True: run_once(); sleep(N)  (good for "live" on laptop)
# Leave Kafka out of this step.
# ---------------------------------------------------------------------------

def step5_run_once(seen_slugs):
    """wire steps 1-4 together for a single cycle."""
    jobs = step1_fetch_jobs()
    if jobs:
        new = step3_new_jobs(jobs, seen_slugs)
        for job in new:
            # step2_print_jobs(job)
            step4_append_job(job)

# ---------------------------------------------------------------------------
# STEP 6 — Kafka producer (do this LAST, after local file works)
# Goal: for each new job, also produce a JSON message to a topic.
# We will add confluent-kafka or kafka-python + Docker Compose later.
# Do not implement this until steps 1-5 work and you ask to start Kafka.
# ---------------------------------------------------------------------------

def step6_produce_to_kafka(job):
    """LATER: send one job to Kafka. Leave as NotImplemented for now."""
    raise NotImplementedError("STEP 6: wait until we set up local Kafka together")


# ---------------------------------------------------------------------------
# Active runner — only STEP 1 for now. Uncomment later steps as you finish them.
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # --- STEP 1 only ---
    # jobs = step1_fetch_jobs()
    # print(f"got {len(jobs)} jobs")
    # if jobs:
    #     print("first title:", jobs[0].get("title"))

    # --- after STEP 1 works, uncomment STEP 2 ---
    # step2_print_jobs(jobs, limit=5)

    # --- after STEP 2, uncomment STEP 3+4+5 ---
    seen = set()
    # first = step3_new_jobs(jobs, seen)
    # print(f"first pass: slugs: {seen}")
    # second = step3_new_jobs(jobs, seen)
    # print(f"first pass: {len(first)}, second pass: {len(second)}")

    # step4_append_job(first[0])
    # print(f"appended job: {first[0]}")
    # # step5_run_once(seen)
    step5_run_once(seen)