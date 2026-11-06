"""Format-only validation used by the ingestion API (first control point).

Business/fraud checks (turnout > 100 %, duplicates, late votes ...) belong to the
anomaly engine, not here: ingestion only guarantees well-formed, authenticated input.
"""
from __future__ import annotations

from typing import Iterable

from .models import VoteReport


class ValidationError(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def validate_report(report: VoteReport) -> None:
    """Raises ValidationError on any format problem."""
    r = report
    if not r.region.strip():
        raise ValidationError("region must not be empty")
    if not r.candidates:
        raise ValidationError("candidates must not be empty")

    numbers = [r.registered_voters, r.ballots_cast, *r.candidates.values()]
    for v in numbers:
        if isinstance(v, bool) or not isinstance(v, int):
            # pydantic would coerce "12.5"→error anyway; this guards raw inputs
            raise ValidationError("all counts must be plain integers, decimals are forbidden")
        if v < 0:
            raise ValidationError("negative vote counts are forbidden")

    if r.registered_voters <= 0:
        raise ValidationError("registered_voters must be positive")

    if r.published_at < r.polls_closed_at:
        raise ValidationError("published_at must not be earlier than polls_closed_at")


def is_known_station(station_id: str, api_key: str, registry: dict[str, str]) -> bool:
    expected = registry.get(station_id)
    return bool(expected) and expected == api_key
