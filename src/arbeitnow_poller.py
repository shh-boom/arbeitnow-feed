"""Poll the Arbeitnow job board API, print new jobs, and append them to NDJSON.

Each cycle:
1. Fetch page 1 of jobs (newest first).
2. Optionally keep only remote jobs when REMOTE_ONLY=1.
3. Keep only the first BATCH_SIZE jobs whose slug we have not seen before.
4. Print each new job and append it to data/raw/jobs.ndjson.
5. Remember the slugs in data/state/seen_slugs.json so restarts do not re-emit them.
6. Sleep POLL_INTERVAL_SEC and repeat.

Run with: python src/arbeitnow_poller.py
Stop with Ctrl+C.
"""

import json
import os
import time
from datetime import datetime, timezone

import requests

import arbeitnow_client

# Configuration. Environment variables override the defaults.
POLL_INTERVAL_SEC = int(os.environ.get("POLL_INTERVAL_SEC", "10"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
REMOTE_ONLY = os.environ.get("REMOTE_ONLY", "").strip() in ("1", "true", "True", "yes")

# Paths are relative to the project root (the parent of src/).
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "jobs.ndjson")
STATE_PATH = os.path.join(PROJECT_ROOT, "data", "state", "seen_slugs.json")


def load_seen_slugs():
    """Read the set of already-processed slugs from disk. Empty set on first run."""
    if not os.path.exists(STATE_PATH):
        return set()
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        try:
            contents = f.read().strip()
            if not contents:
                return set()
            return set(json.loads(contents))
        except json.JSONDecodeError:
            return set()
 


def save_seen_slugs(seen_slugs):
    """Write the set of processed slugs to disk as a sorted JSON list."""
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(sorted(seen_slugs), f)


def to_record(job):
    """Pick the fields we care about and stamp when we ingested the job."""
    return {
        "slug": job.get("slug"),
        "company_name": job.get("company_name"),
        "title": job.get("title"),
        "url": job.get("url"),
        "location": job.get("location"),
        "remote": job.get("remote"),
        "tags": job.get("tags") or [],
        "job_types": job.get("job_types") or [],
        "created_at": job.get("created_at"),
        "description": job.get("description"),
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def append_record(record):
    """Append one JSON object as a single line to the NDJSON file."""
    os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)
    with open(RAW_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def print_record(record):
    remote_label = "remote" if record.get("remote") else "on-site"
    company = record.get("company_name") or "(no company)"
    title = record.get("title") or "(no title)"
    location = record.get("location") or "(no location)"
    url = record.get("url") or "(no url)"
    print(f"{remote_label} | {company} | {title} | {location} | {url}")


def poll_once(seen_slugs):
    """Run one polling cycle. Returns the number of new jobs written."""
    jobs = arbeitnow_client.fetch_jobs(page=1)
    if REMOTE_ONLY:
        jobs = [j for j in jobs if j.get("remote")]

    new_jobs = []
    for job in jobs[:BATCH_SIZE]:
        slug = job.get("slug")
        if not slug or slug in seen_slugs:
            continue
        new_jobs.append(job)

    written = 0
    for job in new_jobs:
        slug = job["slug"]
        seen_slugs.add(slug)
        record = to_record(job)
        print_record(record)
        append_record(record)
        written += 1

    if new_jobs:
        save_seen_slugs(seen_slugs)
    return written


def main():
    seen_slugs = load_seen_slugs()
    remote_note = ", remote only" if REMOTE_ONLY else ""
    print(
        f"Polling Arbeitnow page 1 every {POLL_INTERVAL_SEC}s, batch {BATCH_SIZE}"
        f"{remote_note}, {len(seen_slugs)} slugs already seen. Ctrl+C to stop."
    )
    while True:
        try:
            written = poll_once(seen_slugs)
            print(f"-- cycle done: {written} new jobs --")
        except requests.RequestException as exc:
            print(f"warn: network error, will retry next cycle: {exc}")
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
