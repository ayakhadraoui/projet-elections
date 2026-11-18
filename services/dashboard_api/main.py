"""Dashboard API — Render (PaaS). Serves the public dashboard.

Two modes, chosen automatically by environment:
- LOCAL (docker-compose dev): `/live` reads PostgreSQL directly, `/anomalies`
  consumes the Kafka topic live.
- CLOUD (Render): `/live` is proxied from the ACTIVE cloud's Kafka Relay
  (Failover Controller decides), `/anomalies` is proxied from the Cloudflare
  Worker's KV storage. No DB port and no Kafka port are ever exposed.

Endpoints: GET /live · GET /elections/{id} · GET /anomalies · GET /health
"""
from __future__ import annotations

import json
import logging
import threading

import httpx
import psycopg2
import psycopg2.extras
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import start_http_server

from shared import config as cfg
from shared.kafka import consume

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("dashboard_api")

app = FastAPI(title="Dashboard API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

RELAY_URLS: dict[str, str] = json.loads(cfg.env("RELAY_URLS", "{}"))
ANOMALIES_SOURCE_URL = cfg.env("ANOMALIES_SOURCE_URL")
ANOMALIES_SOURCE_TOKEN = cfg.env("ANOMALIES_SOURCE_TOKEN")
CLOUD_MODE = bool(RELAY_URLS)  # Render has relays configured → cloud mode


def db():
    return psycopg2.connect(cfg.DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def active_cloud() -> str:
    try:
        r = httpx.get(f"{cfg.FAILOVER_URL.rstrip('/')}/active", timeout=3.0)
        return r.json().get("active_cloud") or cfg.DEFAULT_ACTIVE_CLOUD
    except Exception:
        return cfg.DEFAULT_ACTIVE_CLOUD


# --------------------------------------------------------------------------
# POSTGRES (local mode)
# --------------------------------------------------------------------------
def live_from_postgres(election_id: str) -> dict:
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT region, candidate, votes, ballots_cast
               FROM region_results WHERE election_id = %s
               ORDER BY region, votes DESC""",
            (election_id,),
        )
        regions: dict[str, dict] = {}
        total: dict[str, int] = {}
        ballots = 0
        for row in cur.fetchall():
            regions.setdefault(row["region"], {"region": row["region"], "results": {}})
            regions[row["region"]]["results"][row["candidate"]] = int(row["votes"])
            total[row["candidate"]] = total.get(row["candidate"], 0) + int(row["votes"])
            ballots = max(ballots, int(row["ballots_cast"] or 0))
    return {"election_id": election_id, "regions": list(regions.values()),
            "total": total, "ballots_cast": ballots}


# --------------------------------------------------------------------------
# KAFKA RELAY (cloud mode) — the failover choice decides which relay we ask
# --------------------------------------------------------------------------
def live_from_relay(election_id: str) -> dict:
    cloud = active_cloud()
    url = f"{RELAY_URLS.get(cloud, RELAY_URLS[cfg.DEFAULT_ACTIVE_CLOUD]).rstrip('/')}/live"
    r = httpx.get(url, params={"election_id": election_id}, timeout=10.0)
    r.raise_for_status()
    body = r.json()
    body["source_cloud"] = body.get("source_cloud", cloud)
    return body


def _anomalies_cached() -> dict:
    """Cloud mode: the worker's KV list (one HTTP call per dashboard request)."""
    try:
        r = httpx.get(
            f"{ANOMALIES_SOURCE_URL.rstrip('/')}/anomalies",
            headers={"X-List-Token": ANOMALIES_SOURCE_TOKEN}, timeout=10.0,
        )
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.warning("anomaly source unreachable: %s", exc)
        return {"count": 0, "anomalies": []}


@app.get("/health")
def health() -> dict:
    if CLOUD_MODE:
        return {"status": "ok", "mode": "cloud"}
    with db() as conn:
        conn.cursor().execute("SELECT 1")
    return {"status": "ok", "mode": "local"}


@app.get("/live")
def live(election_id: str = "national-2026") -> dict:
    if CLOUD_MODE:
        return live_from_relay(election_id)
    return live_from_postgres(election_id)


@app.get("/elections/{election_id}")
def election(election_id: str) -> dict:
    return live(election_id)


# --------------------------------------------------------------------------
# anomalies
# --------------------------------------------------------------------------
ANOMALIES: list[dict] = []
_ANOMALIES_LOCK = threading.Lock()
_consumer_started = False


def _consume_anomalies_forever() -> None:
    global _consumer_started
    if _consumer_started:
        return
    _consumer_started = True
    log.info("anomaly consumer thread starting…")
    for message in consume(cfg.TOPIC_ANOMALIES, group_id="dashboard-anomalies"):
        with _ANOMALIES_LOCK:
            ANOMALIES.insert(0, message)
            del ANOMALIES[50:]


@app.get("/anomalies")
def anomalies() -> dict:
    if CLOUD_MODE:
        return _anomalies_cached()
    with _ANOMALIES_LOCK:
        return {"count": len(ANOMALIES), "anomalies": ANOMALIES[:50]}


if __name__ == "__main__":
    threading.Thread(target=_consume_anomalies_forever, daemon=True).start()
    start_http_server(9701)
    uvicorn.run(app, host="0.0.0.0", port=8001)
