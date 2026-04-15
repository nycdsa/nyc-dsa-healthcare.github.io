"""
Pelican plugin: fetch upcoming events from Action Network at build time.

Requires env var ACTION_NETWORK_API_KEY. If unset, events will be empty
(safe for local dev without the key).
"""

import calendar as cal_module
import os
from datetime import date, datetime, timezone

import requests
from pelican import signals

BASE_URL = "https://actionnetwork.org/api/v2"


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
        # Convert to Eastern time for display (rough offset; DST not handled)
        date_formatted = dt.strftime("%-I:%M %p, %A %B %-d, %Y")
        month_year = dt.strftime("%Y-%m")  # used for groupby sort key
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


def _mock_events():
    """Return fake events for local development (no API key required)."""
    return [
        {
            "id": "mock:1",
            "name": "Healthcare for All Phonebank",
            "date": "2026-03-07T18:00:00+00:00",
            "date_formatted": "6:00 PM, Saturday March 7, 2026",
            "month_year": "2026-03",
            "month_label": "March 2026",
            "status": "confirmed",
            "description": "Join us for our weekly phonebank calling New Yorkers about single-payer healthcare. No experience needed — training provided!",
            "browser_url": "https://actionnetwork.org/events/mock-phonebank-march",
            "signup_url": "https://actionnetwork.org/events/mock-phonebank-march",
            "location": "zoom.us/j/mock",
            "borough": "",
        },
        {
            "id": "mock:2",
            "name": "Canvass — Bushwick",
            "date": "2026-03-14T14:00:00+00:00",
            "date_formatted": "2:00 PM, Saturday March 14, 2026",
            "month_year": "2026-03",
            "month_label": "March 2026",
            "status": "confirmed",
            "description": "Door-to-door canvassing in Bushwick to talk with neighbors about the fight for universal healthcare. Meet at Maria Hernandez Park.",
            "browser_url": "https://actionnetwork.org/events/mock-canvass-bushwick",
            "signup_url": "https://actionnetwork.org/events/mock-canvass-bushwick",
            "location": "Maria Hernandez Park",
            "borough": "Brooklyn",
        },
        {
            "id": "mock:3",
            "name": "Teach-In: How Single Payer Works",
            "date": "2026-03-21T19:00:00+00:00",
            "date_formatted": "7:00 PM, Saturday March 21, 2026",
            "month_year": "2026-03",
            "month_label": "March 2026",
            "status": "confirmed",
            "description": "A deep dive into how Medicare for All would work, what it covers, and how we win it. Featuring a healthcare policy expert and Q&A.",
            "browser_url": "https://actionnetwork.org/events/mock-teachin",
            "signup_url": "https://actionnetwork.org/events/mock-teachin",
            "location": "Brooklyn Commons",
            "borough": "Brooklyn",
        },
        {
            "id": "mock:4",
            "name": "Healthcare for All Phonebank",
            "date": "2026-04-04T18:00:00+00:00",
            "date_formatted": "6:00 PM, Saturday April 4, 2026",
            "month_year": "2026-04",
            "month_label": "April 2026",
            "status": "confirmed",
            "description": "Weekly phonebank — keep the pressure on elected officials and build our base of supporters.",
            "browser_url": "https://actionnetwork.org/events/mock-phonebank-april",
            "signup_url": "https://actionnetwork.org/events/mock-phonebank-april",
            "location": "zoom.us/j/mock",
            "borough": "",
        },
        {
            "id": "mock:5",
            "name": "Rally for NY Health Act",
            "date": "2026-04-18T12:00:00+00:00",
            "date_formatted": "12:00 PM, Saturday April 18, 2026",
            "month_year": "2026-04",
            "month_label": "April 2026",
            "status": "confirmed",
            "description": "Rally at City Hall to demand the NY Health Act pass this session. Bring signs, bring friends.",
            "browser_url": "https://actionnetwork.org/events/mock-rally",
            "signup_url": "https://actionnetwork.org/events/mock-rally",
            "location": "City Hall Park",
            "borough": "Manhattan",
        },
    ]


def _build_calendar(events):
    """Build a month-grid data structure for the calendar template."""
    today = date.today()

    # Collect all (year, month) pairs from events
    month_set = set()
    for event in events:
        date_str = event.get("date", "")
        if date_str:
            parts = date_str[:10].split("-")
            month_set.add((int(parts[0]), int(parts[1])))

    # Always include the current month
    month_set.add((today.year, today.month))

    # Build lookup: (year, month, day) -> list of events
    event_lookup = {}
    for event in events:
        date_str = event.get("date", "")
        if date_str:
            parts = date_str[:10].split("-")
            key = (int(parts[0]), int(parts[1]), int(parts[2]))
            event_lookup.setdefault(key, []).append(event)

    # Sunday-first calendar (firstweekday=6 in Python's calendar module)
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
    if not api_key:
        print("action_network plugin: ACTION_NETWORK_API_KEY not set — using mock events for local dev")
        events = _mock_events()
        pelican.settings["ACTION_NETWORK_EVENTS"] = events
        pelican.settings["ACTION_NETWORK_CALENDAR"] = _build_calendar(events)
        return

    try:
        events = fetch_events(api_key)
        pelican.settings["ACTION_NETWORK_EVENTS"] = events
        pelican.settings["ACTION_NETWORK_CALENDAR"] = _build_calendar(events)
        print(f"action_network plugin: fetched {len(events)} upcoming events")
    except Exception as exc:
        print(f"action_network plugin: ERROR fetching events — {exc}")
        pelican.settings["ACTION_NETWORK_EVENTS"] = []
        pelican.settings["ACTION_NETWORK_CALENDAR"] = []


def register():
    signals.initialized.connect(add_events_to_context)
