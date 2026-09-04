"""Pelican plugin: inject the healthcare donations dataset into templates.

Reads content/data/healthcare_donations.json (produced by the Donations Tracker
pipeline's `build_web` stage) and exposes it to Jinja as the global
HEALTHCARE_DONATIONS, mirroring the action_network plugin's pattern.
"""
import json
import os

from pelican import signals

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_PATH = os.path.join(_REPO_ROOT, "content", "data", "healthcare_donations.json")


def _load():
    try:
        with open(DATA_PATH) as f:
            data = json.load(f)
        n = len(data.get("candidates", []))
        print(f"donations plugin: loaded {n} candidates from healthcare_donations.json")
        return data
    except Exception as exc:  # noqa: BLE001
        print(f"donations plugin: could not load data — {exc}")
        return {"meta": {}, "summary": {}, "candidates": []}


def inject_into_generator(generator):
    generator.env.globals["HEALTHCARE_DONATIONS"] = _load()


def register():
    signals.generator_init.connect(inject_into_generator)
