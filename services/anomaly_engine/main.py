"""Anomaly engine — Azure VM (IaaS). "The Referee".

Consumes vote reports from Kafka, applies fraud rules, publishes anomalies to
Kafka and pushes them to the notification webhook (Cloudflare Worker → Telegram).

Rules (per architecture doc):
  duplicate_report   — same report_id published twice by a station
  turnout_above_100  — ballots_cast / registered_voters above the allowed ratio
  late_submission    — votes published long after the poll closed
  impossible_jump    — a sudden huge vote transfer between two consecutive
                       reports of the same station (input error, not fraud)
  totals_mismatch    — candidate votes exceed ballots cast in the report
"""
from __future__ import annotations

import logging

from datetime import datetime, timezone

from shared import config as cfg
from shared.kafka import consume, publish

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("anomaly_engine")

seen_report_ids: set[str] = set()
last_seen: dict[str, dict] = {}  # station -> previous report summary, for jump detection


def as_anomaly(kind: str, report: dict, details: str) -> None:
    payload = {
        "kind": kind,
        "election_id": report["election_id"],
        "station_id": report["station_id"],
        "region": report["region"],
        "report_id": report["report_id"],
        "details": details,
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }
    publish(cfg.TOPIC_ANOMALIES, key=report["station_id"], payload=payload)
    log.warning("ANOMALY %s station=%s %s", kind, report["station_id"], details)
    if cfg.ANOMALY_WEBHOOK_URL:  # Cloudflare Worker → Telegram
        try:
            import httpx

            httpx.post(cfg.ANOMALY_WEBHOOK_URL, json=payload, timeout=5.0)
        except Exception:
            log.exception("failed to reach the notification webhook")


def check_duplicate(report: dict) -> None:
    if report["report_id"] in seen_report_ids:
        as_anomaly("duplicate_report", report, "this report_id was already published")
    else:
        seen_report_ids.add(report["report_id"])


def check_turnout(report: dict) -> None:
    registered = max(1, report["registered_voters"])
    ratio = report["ballots_cast"] / registered
    if ratio > cfg.MAX_TURNOUT_RATIO:
        as_anomaly(
            "turnout_above_100",
            report,
            f"turnout {ratio:.0%} of registered voters (a station cannot vote more than it has)",
        )


def check_late(report: dict) -> None:
    closed = datetime.fromisoformat(report["polls_closed_at"]).replace(tzinfo=timezone.utc)
    published = datetime.fromisoformat(report["published_at"]).replace(tzinfo=timezone.utc)
    delay = (published - closed).total_seconds()
    if delay > cfg.MAX_POLL_DELAY_SECONDS:
        as_anomaly("late_submission", report, f"published {delay:.0f}s after poll closure")


def check_totals(report: dict) -> None:
    total_votes = sum(report["candidates"].values())
    if total_votes > report["ballots_cast"]:
        as_anomaly(
            "totals_mismatch",
            report,
            f"candidate votes ({total_votes}) exceed ballots cast ({report['ballots_cast']})",
        )


def check_jump(report: dict) -> None:
    prev = last_seen.get(report["station_id"])
    last_seen[report["station_id"]] = {"ballots_cast": report["ballots_cast"]}
    if not prev:
        return
    delta = report["ballots_cast"] - prev["ballots_cast"]
    if delta <= 0:
        return  # new ballot days / corrections can legitimately lower the counter
    if delta > cfg.MAX_VOTE_JUMP:
        as_anomaly(
            "impossible_jump",
            report,
            f"ballots jumped by {delta} since the previous report (input error?)",
        )


RULES = [check_duplicate, check_turnout, check_late, check_totals, check_jump]


def run() -> None:
    log.info("anomaly engine started, watching %s", cfg.TOPIC_VOTE_REPORTS)
    for report in consume(cfg.TOPIC_VOTE_REPORTS, group_id="anomaly-engine"):
        for rule in RULES:
            try:
                rule(report)
            except Exception:
                log.exception("rule %s failed on %s", rule.__name__, report.get("report_id"))


if __name__ == "__main__":
    run()
