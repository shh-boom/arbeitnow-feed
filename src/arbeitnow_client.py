"""Thin HTTP helpers for the public Arbeitnow job board API.

The API has no auth. One endpoint returns a page of full job objects:

    GET https://www.arbeitnow.com/api/job-board-api?page=1

The JSON shape is roughly {"data": [job, ...], "links": ..., "meta": ...}.
Each job already includes title, company, url, and the rest — unlike HN, there
is no second request per item.

Every function here uses a timeout so a slow network can never hang the poller.
"""

import requests

BASE_URL = "https://www.arbeitnow.com/api/job-board-api"
TIMEOUT_SEC = 10


def fetch_jobs(page=1):
    """Return the list of job dicts for one page, or [] if the response is bad.

    Page 1 is newest-first. This helper does not follow pagination links.
    """
    response = requests.get(BASE_URL, params={"page": page}, timeout=TIMEOUT_SEC)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        return []
    jobs = payload.get("data")
    if not isinstance(jobs, list):
        return []
    return jobs
