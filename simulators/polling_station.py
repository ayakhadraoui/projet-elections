#!/usr/bin/env python
"""Simulated polling stations (architecture doc: "bureaux de vote simulés").

Each station publishes its cumulative ballot report at regular intervals via
the submission portal's URL (in local dev: directly to the ingestion API).

Flags let you inject fraud for demoing the anomaly engine:
  --fraud turnout    # final report with turnout > 100 %
  --fraud late       # publish hours after poll closure
  --fraud dup        # re-send a previous report_id
  --fraud jump       # ballots jump by a huge amount in one report

Usage (from repo root, with local docker-compose or cloud ingest running):
  python simulators/polling_station.py --stations 5
  python simulators/polling_station.py --stations 3 --interval 5 --fraud turnout
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import httpx

from shared import config as cfg

CLIENT_NAME = "simulator"


def make_station(i: int) -> dict:
    rng = random.Random(i)
    station_id = f"station-{i:03d}"
    registry = json.loads(os.environ.get("STATION_API_KEYS", "{}"))
    return {
        "station_id": station_id,
        "api_key": registry.get(station_id, f"key-{i:03d}"),
        "region": rng.choice(["Casablanca", "Rabat", "Marrakech", "Fès", "Agadir"]),
        "registered_voters": rng.randint(500, 5000),
        "ballots_cast": 0,             # cumulative counter
        "per_candidate": {"Cand. A": 0, "Cand. B": 0},
        # closed 5s ago: honest stations report within the MAX_POLL_DELAY_SECONDS window
        "polls_closed_at": datetime.now(timezone.utc) - timedelta(seconds=5),
    }


def build_report(station: dict, fraud: str | None, last_report: dict | None) -> dict:
    # ~ new ballots this round (small, plausible)
    fresh = random.randint(10, min(120, station["registered_voters"] // 4 + 1))
    if fraud == "jump":
        fresh = station["registered_voters"] // 2   # insane growth in one round
    if fraud == "turnout":
        fresh = max(fresh, int(station["registered_voters"] * 1.05 - station["ballots_cast"]))
        fresh = max(fresh, 1)

    station["ballots_cast"] += fresh
    share = random.uniform(0.35, 0.65)
    station["per_candidate"]["Cand. A"] += int(fresh * share)
    station["per_candidate"]["Cand. B"] += fresh - int(fresh * share)

    closed = station["polls_closed_at"]
    if fraud == "late":
        # this station closed its poll long ago and is only reporting now:
        # the ingestion stamps published_at=now, so the delay will exceed the limit
        closed = datetime.now(timezone.utc) - timedelta(hours=2)

    report = {
        "report_id": last_report["report_id"] if fraud == "dup" else uuid.uuid4().hex,
        "election_id": "national-2026",
        "station_id": station["station_id"],
        "region": station["region"],
        "polls_closed_at": closed.isoformat(),
        "registered_voters": station["registered_voters"],
        "ballots_cast": station["ballots_cast"],
        "candidates": dict(station["per_candidate"]),
    }
    return report


async def run_one(station: dict, interval: int, fraud: str | None, rounds: int) -> None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        url = (cfg.INGEST_URL or "http://localhost:8000").rstrip("/")
        if cfg.INGEST_API_SECRET:
            client.headers["Authorization"] = f"Bearer {cfg.INGEST_API_SECRET}"
        client.headers["X-Station-Key"] = station["api_key"]

        last_report = None
        for i in range(rounds):
            report = build_report(station, fraud if i >= 3 else None, last_report)
            try:
                r = await client.post(f"{url}/reports", json=report)
                status = r.json().get("status", r.status_code)
                cloud = r.json().get("active_cloud", "?")
                print(f"[{report['station_id']}] {status} cloud={cloud} "
                      f"ballots={report['ballots_cast']} anomaly={bool(fraud)}")
            except Exception as exc:
                # in production this is exactly what Prometheus sees: no answer
                print(f"[{report['station_id']}] INGESTION UNREACHABLE: {exc}")
            last_report = report
            await asyncio.sleep(interval)


async def main() -> None:
    stations = [make_station(i) for i in range(1, args.stations + 1)]
    await asyncio.gather(*(run_one(s, args.interval, args.fraud, args.rounds) for s in stations))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stations", type=int, default=5)
    parser.add_argument("--interval", type=int, default=10, help="seconds between reports")
    parser.add_argument("--fraud", choices=["turnout", "late", "dup", "jump"], default=None)
    parser.add_argument("--rounds", type=int, default=8)
    args = parser.parse_args()
    asyncio.run(main())
