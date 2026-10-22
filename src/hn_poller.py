"""Poll the Hacker News API, print new stories, and append them to an NDJSON file.

Each cycle:
1. Fetch the list of IDs for the chosen feed.
2. Keep only the first BATCH_SIZE IDs that we have not seen before.
3. Fetch each new item, print it, and append it to data/raw/stories.ndjson.
4. Remember the IDs in data/state/seen_ids.json so restarts do not re-emit them.
5. Sleep POLL_INTERVAL_SEC and repeat.

Run with: python src/hn_poller.py
Stop with Ctrl+C.
"""

import json
import os
import time
from datetime import datetime, timezone
from hn_producer import produce_record

import requests

import hn_client

# Configuration. Environment variables override the defaults.
POLL_INTERVAL_SEC = int(os.environ.get("POLL_INTERVAL_SEC", "60"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
FEED = os.environ.get("FEED", "newstories")

# Paths are relative to the project root (the parent of src/).
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "stories.ndjson")
STATE_PATH = os.path.join(PROJECT_ROOT, "data", "state", "seen_ids.json")


def load_seen_ids():
    """Read the set of already-processed IDs from disk. Empty set on first run."""
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

        # return set(json.load(f))


def save_seen_ids(seen_ids):
    """Write the set of processed IDs to disk as a sorted JSON list."""
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(sorted(seen_ids), f)


def to_record(item):
    """Pick the fields we care about and stamp when we ingested the item."""
    return {
        "id": item.get("id"),
        "title": item.get("title"),
        "url": item.get("url"),
        "by": item.get("by"),
        "score": item.get("score"),
        "time": item.get("time"),
        "type": item.get("type"),
        "descendants": item.get("descendants"),
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def append_record(record):
    """Append one JSON object as a single line to the NDJSON file."""
    os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)
    with open(RAW_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def print_record(record):
    score = record["score"] if record["score"] is not None else 0
    title = record["title"] or "(no title)"
    url = record["url"] or "(no url)"
    print(f"{score:>4} | {title} | {url}")


def poll_once(seen_ids):
    """Run one polling cycle. Returns the number of new stories written."""
    ids = hn_client.fetch_story_ids(FEED)
    new_ids = [i for i in ids[:BATCH_SIZE] if i not in seen_ids]

    written = 0
    for item_id in new_ids:
        try:
            item = hn_client.fetch_item(item_id)
        except requests.RequestException as exc:
            # Leave the ID out of seen_ids so we retry it next cycle.
            print(f"warn: could not fetch item {item_id}: {exc}")
            continue

        # Mark it seen even if it was null/deleted, so we do not refetch forever.
        seen_ids.add(item_id)
        if item is None:
            continue

        record = to_record(item)
        print_record(record)
        append_record(record)
        produce_record(record)
        written += 1

    if new_ids:
        save_seen_ids(seen_ids)
    return written


def main():
    seen_ids = load_seen_ids()
    print(
        f"Polling {FEED} every {POLL_INTERVAL_SEC}s, batch {BATCH_SIZE}, "
        f"{len(seen_ids)} IDs already seen. Ctrl+C to stop."
    )
    while True:
        try:
            written = poll_once(seen_ids)
            print(f"-- cycle done: {written} new stories --")
        except requests.RequestException as exc:
            print(f"warn: network error, will retry next cycle: {exc}")
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
