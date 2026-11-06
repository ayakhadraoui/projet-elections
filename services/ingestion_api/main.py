"""Ingestion API — Render (PaaS). First control point of the chain.

POST /reports  (Authorization: Bearer <INGEST_API_SECRET>  |  X-Station-Key)
  1. authenticates the polling station (X-Station-Key),
  2. validates the report format (no decimals, no negatives, sane fields),
  3. dedupes by report_id,
  4. asks the Failover Controller which cloud is active,
  5. forwards the report to that cloud's Kafka Relay (or publishes locally in dev).
"""
from __future__ import annotations

import json
import logging

import httpx
import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, start_http_server
from pydantic import ValidationError as PydanticValidationError

from shared import config as cfg
from shared.kafka import publish
from shared.models import VoteReport
from shared.validation import ValidationError, is_known_station, validate_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("ingestion_api")

app = FastAPI(title="Ingestion API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # the portal on Vercel calls us; tighten in production
    allow_methods=["*"],
    allow_headers=["*"],
)

duplicates: set[str] = set()  # report_ids already accepted (small set is fine for a demo)

reports_accepted = Counter("ingestion_reports_accepted_total", "Reports forwarded")
reports_rejected = Counter("ingestion_reports_rejected_total", "Reports rejected", ["reason"])
failover_queries = Counter(
    "ingestion_failover_queries_total", "Queries to the failover controller", ["result"]
)

# which Kafka Relay URL serves which cloud ("azure"→ its tunnel URL, etc.);
# empty ⇒ local direct Kafka (dev mode)
RELAY_URLS: dict[str, str] = json.loads(cfg.env("RELAY_URLS", "{}"))


def authenticate(x_station_key: str = Header(..., description="Per-station API key")) -> str:
    """Returns the station_id the key belongs to."""
    for station_id, key in cfg.STATION_API_KEYS.items():
        if key == x_station_key:
            return station_id
    raise HTTPException(status_code=401, detail="unknown or bad station key")


def which_cloud() -> str:
    """Ask the Failover Controller which cloud is active. Never crash on failure."""
    try:
        r = httpx.get(f"{cfg.FAILOVER_URL.rstrip('/')}/active", timeout=3.0)
        active = r.json().get("active_cloud")
        failover_queries.labels(result="ok").inc()
        return active if active else cfg.DEFAULT_ACTIVE_CLOUD
    except Exception as exc:
        failover_queries.labels(result=f"error:{type(exc).__name__}").inc()
        log.warning("failover controller unreachable, using default %s", cfg.DEFAULT_ACTIVE_CLOUD)
        return cfg.DEFAULT_ACTIVE_CLOUD


def deliver(vote_report: VoteReport, active_cloud: str) -> None:
    if active_cloud in RELAY_URLS:  # production: → kafka relay over HTTPS
        r = httpx.post(
            f"{RELAY_URLS[active_cloud].rstrip('/')}/vote_reports",
            headers={"Authorization": f"Bearer {cfg.INGEST_API_SECRET}"},
            json=vote_report.model_dump(mode="json"),
            timeout=10.0,
        )
        r.raise_for_status()
    else:  # dev/local: publish straight into local Kafka
        publish(cfg.TOPIC_VOTE_REPORTS, key=vote_report.station_id,
                payload=vote_report.model_dump())


@app.post("/reports", status_code=202)
def receive_report(
    report: dict,
    station_id: str = Depends(authenticate),
) -> dict:
    # a caller may only act *for the station it authenticated*
    report.setdefault("station_id", station_id)
    if report["station_id"] != station_id:
        raise HTTPException(status_code=403, detail="station id mismatch")

    # --- 1. shape ---
    try:
        vote_report = VoteReport.model_validate(report)
    except PydanticValidationError as exc:
        reports_rejected.labels(reason="malformed").inc()
        raise HTTPException(status_code=422, detail="malformed report") from exc

    # --- 2. format rules ---
    try:
        validate_report(vote_report)
    except ValidationError as exc:
        reports_rejected.labels(reason="invalid").inc()
        raise HTTPException(status_code=422, detail=exc.reason) from exc

    # --- 3. dedup ---
    if vote_report.report_id in duplicates:
        reports_rejected.labels(reason="duplicate").inc()
        raise HTTPException(status_code=409, detail="duplicate report_id")

    # --- 4. where does this vote go? ---
    active = which_cloud()
    if active != cfg.PRIMARY_CLOUD:
        log.warning("failover decision: primary is not %s, routing to %s",
                    cfg.PRIMARY_CLOUD, active)
    try:
        deliver(vote_report, active)
    except Exception as exc:
        reports_rejected.labels(reason="delivery_failed").inc()
        raise HTTPException(status_code=502, detail=f"kafka relay unreachable: {exc}") from exc

    duplicates.add(vote_report.report_id)
    reports_accepted.inc()
    return {"status": "accepted", "active_cloud": active, "report_id": vote_report.report_id}


if __name__ == "__main__":
    start_http_server(9700)
    uvicorn.run(app, host="0.0.0.0", port=8000)
