"""Aggregator — Azure VM (primary), exact same job on Oracle (standby).

Consumes validated vote reports from Kafka and stores each station's latest
cumulative snapshot into PostgreSQL (`station_reports`). Region totals are a
SQL view: `region_results` (see infra/sql/init.sql). PostgreSQL is therefore the
source of truth of the public dashboard — exactly the role the architecture doc
assigns to this component.

Snapshots are upserts keyed by station_id, so replays (failover resync) are safe.
"""
from __future__ import annotations

import logging

import psycopg2
from psycopg2.extras import Json

from shared import config as cfg
from shared.kafka import consume

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("aggregator")


def db_connect():
    return psycopg2.connect(cfg.DATABASE_URL)


UPSERT_SQL = """
INSERT INTO station_reports
    (election_id, station_id, region, registered_voters,
     ballots_cast, candidates, published_at, polls_closed_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (station_id)
    DO UPDATE SET
        election_id        = EXCLUDED.election_id,
        region             = EXCLUDED.region,
        registered_voters  = EXCLUDED.registered_voters,
        ballots_cast       = EXCLUDED.ballots_cast,
        candidates         = EXCLUDED.candidates,
        published_at       = EXCLUDED.published_at,
        polls_closed_at    = EXCLUDED.polls_closed_at
WHERE EXCLUDED.published_at >= station_reports.published_at
"""


def handle(report: dict) -> None:
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(
            UPSERT_SQL,
            (
                report["election_id"],
                report["station_id"],
                report["region"],
                report["registered_voters"],
                report["ballots_cast"],
                Json(report["candidates"]),
                report["published_at"],
                report["polls_closed_at"],
            ),
        )
    log.info(
        "stored snapshot station=%s region=%s ballots=%s",
        report["station_id"], report["region"], report["ballots_cast"],
    )


if __name__ == "__main__":
    log.info("aggregator started, consuming %s", cfg.TOPIC_VOTE_REPORTS)
    for report in consume(cfg.TOPIC_VOTE_REPORTS, group_id="aggregator"):
        try:
            handle(report)
        except Exception:
            log.exception("failed to aggregate report %s", report.get("report_id"))
