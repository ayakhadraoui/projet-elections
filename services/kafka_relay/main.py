"""Kafka Relay — small HTTPS entry point on each IaaS cloud (Azure + Oracle).

Why: Render (PaaS) cannot join the Tailscale mesh, and we never expose Kafka
publicly. So each cloud runs this tiny relay, published to the internet
**exclusively through a Cloudflare Tunnel** (no open ports, TLS everywhere).

The Ingestion API (Render) asks the Failover Controller which cloud is active,
then POSTs validated reports to that cloud's relay:

    POST /vote_reports     {"reports": [ {...VoteReport...}, ... ]}
    headers: Authorization: Bearer <INGEST_API_SECRET>

MirrorMaker 2 then keeps the two Kafka clusters synchronized.
"""
from __future__ import annotations

import logging

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from prometheus_client import Counter, start_http_server
from pydantic import ValidationError as PydanticValidationError
from typing import Any

from shared import config as cfg
from shared.kafka import publish
from shared.models import VoteReport

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("kafka_relay")

app = FastAPI(title="Kafka Relay", version="1.0.0")
bearer = HTTPBearer()

published = Counter("relay_reports_published_total", "Reports published into Kafka", ["topic"])
rejects = Counter("relay_rejects_total", "Invalid payloads refused", ["reason"])


def auth(credentials: HTTPAuthorizationCredentials = Depends(bearer)) -> None:
    if not credentials or credentials.credentials != cfg.INGEST_API_SECRET:
        raise HTTPException(status_code=401, detail="bad relay secret")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "cloud": cfg.PRIMARY_CLOUD}


@app.post("/vote_reports", status_code=202)
def vote_reports(
    reports: list[dict],
    _: None = Depends(auth),
) -> dict:
    accepted = 0
    for raw in reports:
        try:
            vote = VoteReport.model_validate(raw)
        except PydanticValidationError as exc:
            rejects.labels(reason="malformed").inc()
            raise HTTPException(status_code=422, detail="malformed report") from exc
        publish(cfg.TOPIC_VOTE_REPORTS, key=vote.station_id, payload=vote.model_dump())
        published.labels(topic=cfg.TOPIC_VOTE_REPORTS).inc()
        accepted += 1
    return {"accepted": accepted, "cloud": cfg.PRIMARY_CLOUD}


@app.get("/live")
def live(election_id: str = "national-2026") -> dict:
    """Aggregated results from THIS cloud's PostgreSQL (localhost, private)."""
    import psycopg2
    import psycopg2.extras

    with psycopg2.connect(cfg.DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor) as conn:
        with conn.cursor() as cur:
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
    return {
        "election_id": election_id,
        "regions": list(regions.values()),
        "total": total,
        "ballots_cast": ballots,
        "source_cloud": cfg.PRIMARY_CLOUD,
    }


if __name__ == "__main__":
    start_http_server(9730)
    uvicorn.run(app, host="0.0.0.0", port=8300)
