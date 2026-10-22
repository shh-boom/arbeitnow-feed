"""Thin HTTP helpers for the public Hacker News API.

The HN API has no auth. It exposes two kinds of endpoints we care about:

1. List endpoints like /v0/newstories.json that return a JSON array of item IDs.
2. Item endpoints like /v0/item/{id}.json that return one item as a JSON object.

Every function here uses a timeout so a slow network can never hang the poller.
"""

import requests

BASE_URL = "https://hacker-news.firebaseio.com/v0"
TIMEOUT_SEC = 10


def fetch_story_ids(feed="newstories"):
    """Return a list of story IDs for the given feed, or [] if the response is bad.

    Valid feeds include newstories, topstories, beststories, askstories,
    showstories, and jobstories. Newest/highest-ranked IDs come first.
    """
    response = requests.get(f"{BASE_URL}/{feed}.json", timeout=TIMEOUT_SEC)
    response.raise_for_status()
    ids = response.json()
    if not isinstance(ids, list):
        return []
    return ids


def fetch_item(item_id):
    """Return one HN item as a dict, or None if the item is missing/deleted.

    Deleted or non-existent IDs return null from the API; we normalize that
    to None so the poller can skip them cleanly.
    """
    response = requests.get(f"{BASE_URL}/item/{item_id}.json", timeout=TIMEOUT_SEC)
    response.raise_for_status()
    item = response.json()
    if not isinstance(item, dict):
        return None
    return item
