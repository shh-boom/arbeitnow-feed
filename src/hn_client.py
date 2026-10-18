"""Thin HTTP helpers for the public Hacker News API.

The HN API has no auth. It exposes two kinds of endpoints we care about:

1. List endpoints like /v0/newstories.json that return a JSON array of item IDs.
2. Item endpoints like /v0/item/{id}.json that return one item as a JSON object.

Every function here uses a timeout so a slow network can never hang the poller.
"""
