"""
Pelican plugin: fetch upcoming events from Action Network at build time.

If ACTION_NETWORK_API_KEY is set, events are fetched live from the API.
Otherwise, falls back to content/events_cache.json (a file committed to the
repo and kept fresh by the refresh-events GitHub Actions workflow). This lets
Cloudflare Pages build the site with real events without needing the API key.
"""

import calendar as cal_module
import json
import os
from datetime import date, datetime, timezone

import requests
from pelican import signals

BASE_URL = "https://actionnetwork.org/api/v2"

# Path to the committed events cache, relative to repo root
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CACHE_PATH = os.path.join(_REPO_ROOT, "content", "events_cache.json")


def _parse_dt(date_str):
    """Parse an Action Network ISO 8601 date string to an aware datetime."""
    return datetime.fromisoformat(date_str.replace("Z", "+00:00"))


def _event_to_dict(raw):
    location = ""
    borough = ""
    if raw.get("location"):
        addr = raw["location"].get("address_lines") or []
        location = addr[0] if addr else "Virtual Event"
        borough = raw["location"].get("locality", "")

    signup_url = raw.get("browser_url", "")

    date_str = raw.get("start_date", "")
    date_formatted = ""
    month_year = ""
    if date_str:
        dt = _parse_dt(date_str)
        date_formatted = dt.strftime("%-I:%M %p, %A %B %-d, %Y")
        month_year = dt.strftime("%Y-%m")
        month_label = dt.strftime("%B %Y")
    else:
        month_label = "Unknown"

    return {
        "id": raw["identifiers"][0] if raw.get("identifiers") else "",
        "name": raw.get("title", ""),
        "date": date_str,
        "date_formatted": date_formatted,
        "month_year": month_year,
        "month_label": month_label,
        "status": raw.get("status", ""),
        "description": raw.get("description", ""),
        "browser_url": raw.get("browser_url", ""),
        "signup_url": signup_url,
        "location": location,
        "borough": borough,
    }


def fetch_events(api_key):
    headers = {"OSDI-API-Token": api_key}
    events = []
    page = 1

    while True:
        resp = requests.get(
            f"{BASE_URL}/events",
            params={"page": page},
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        raw_events = data.get("_embedded", {}).get("osdi:events", [])
        total_pages = data.get("total_pages", 1)

        for raw in raw_events:
            events.append(_event_to_dict(raw))

        if page >= total_pages:
            break
        page += 1

    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    events = [
        e for e in events
        if e["status"] != "cancelled"
        and e["date"]
        and _parse_dt(e["date"]) >= today_start
    ]
    events.sort(key=lambda e: e["date"])
    return events


def _load_from_cache():
    """Load events from the committed JSON cache file."""
    try:
        with open(CACHE_PATH) as f:
            data = json.load(f)
        events = data.get("events", [])
        calendar = data.get("calendar", [])
        print(f"action_network plugin: loaded {len(events)} events from cache")
        return events, calendar
    except Exception as exc:
        print(f"action_network plugin: could not read cache — {exc}")
        return [], []


def _build_calendar(events):
    """Build a month-grid data structure for the calendar template."""
    today = date.today()

    month_set = set()
    for event in events:
        date_str = event.get("date", "")
        if date_str:
            parts = date_str[:10].split("-")
            month_set.add((int(parts[0]), int(parts[1])))

    month_set.add((today.year, today.month))

    event_lookup = {}
    for event in events:
        date_str = event.get("date", "")
        if date_str:
            parts = date_str[:10].split("-")
            key = (int(parts[0]), int(parts[1]), int(parts[2]))
            event_lookup.setdefault(key, []).append(event)

    cal = cal_module.Calendar(firstweekday=6)
    result = []

    for year, month in sorted(month_set):
        month_label = date(year, month, 1).strftime("%B %Y")
        month_year_str = f"{year:04d}-{month:02d}"

        weeks = []
        for week in cal.monthdayscalendar(year, month):
            week_cells = []
            for day in week:
                if day == 0:
                    week_cells.append({"day": 0, "in_month": False, "events": [], "is_today": False})
                else:
                    key = (year, month, day)
                    week_cells.append({
                        "day": day,
                        "in_month": True,
                        "events": event_lookup.get(key, []),
                        "is_today": date(year, month, day) == today,
                    })
            weeks.append(week_cells)

        result.append({
            "month_label": month_label,
            "month_year": month_year_str,
            "weeks": weeks,
        })

    return result


def add_events_to_context(pelican):
    api_key = os.environ.get("ACTION_NETWORK_API_KEY", "")

    if api_key:
        # API key available — fetch live (GitHub Actions build)
        try:
            events = fetch_events(api_key)
            calendar = _build_calendar(events)
            print(f"action_network plugin: fetched {len(events)} upcoming events from API")
        except Exception as exc:
            print(f"action_network plugin: ERROR fetching from API — {exc}, falling back to cache")
            events, calendar = _load_from_cache()
    else:
        # No API key — read from committed cache (Cloudflare Pages build)
        events, calendar = _load_from_cache()

    pelican.settings["ACTION_NETWORK_EVENTS"] = events
    pelican.settings["ACTION_NETWORK_CALENDAR"] = calendar


def inject_into_generator(generator):
    """Inject events directly into each generator's Jinja2 env globals.

    generator_init fires after Generator.__init__ sets up self.env, so
    env.globals are reliably available in every template render.
    """
    generator.env.globals.update({
        "ACTION_NETWORK_EVENTS": generator.settings.get("ACTION_NETWORK_EVENTS", []),
        "ACTION_NETWORK_CALENDAR": generator.settings.get("ACTION_NETWORK_CALENDAR", []),
    })


def register():
    signals.initialized.connect(add_events_to_context)
    signals.generator_init.connect(inject_into_generator)
