"""Pydantic models shared between services."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class VoteReport(BaseModel):
    """One report from one polling station when its poll closes."""

    report_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    election_id: str = "national-2026"
    station_id: str
    region: str
    # poll closing time, published report time
    polls_closed_at: datetime
    published_at: datetime = Field(default_factory=utcnow)
    registered_voters: int
    ballots_cast: int
    # candidate name -> vote count (all integers, no decimals, no negatives)
    candidates: dict[str, int]

    @property
    def turnout_ratio(self) -> float:
        return self.ballots_cast / self.registered_voters if self.registered_voters else 0.0


class Anomaly(BaseModel):
    """Message published on the `anomalies` topic / sent to Telegram."""

    anomaly_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    election_id: str
    station_id: str
    region: str = ""
    kind: str                 # duplicate_report | turnout_above_100 | late_submission | ...
    details: str
    detected_at: datetime = Field(default_factory=utcnow)
    report_id: Optional[str] = None


class AggregatedResults(BaseModel):
    """Snapshot published on `aggregated_results` and served by the dashboard API."""

    election_id: str
    region: str
    generated_at: datetime = Field(default_factory=utcnow)
    registered_voters: int
    ballots_cast: int
    results: dict[str, int]   # candidate -> total votes in this region
