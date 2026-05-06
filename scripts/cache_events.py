#!/usr/bin/env python3
"""
Fetch upcoming events from Action Network and write them to a JSON cache file.

Run by GitHub Actions (which has the API key secret) so that Cloudflare Pages
can build the site without needing the API key itself.
"""

import json
import os
import sys

# Make sure we can import the plugin from the repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from plugins.action_network import fetch_events, _build_calendar

api_key = os.environ.get("ACTION_NETWORK_API_KEY", "")
if not api_key:
    print("ERROR: ACTION_NETWORK_API_KEY not set")
    sys.exit(1)

events = fetch_events(api_key)
calendar = _build_calendar(events)

data = {"events": events, "calendar": calendar}

cache_path = os.path.join(os.path.dirname(__file__), "..", "content", "events_cache.json")
with open(cache_path, "w") as f:
    json.dump(data, f, indent=2, default=str)

print(f"Cached {len(events)} upcoming events to content/events_cache.json")
