"""Tests for the format-validation logic of the ingestion API."""
import pytest
from datetime import datetime, timedelta, timezone
from pydantic import ValidationError as PydanticValidationError

from shared.models import VoteReport
from shared.validation import ValidationError, validate_report, is_known_station


def base(**overrides) -> dict:
    payload = {
        "report_id": "abc123",
        "election_id": "national-2026",
        "station_id": "station-001",
        "region": "Rabat",
        "polls_closed_at": datetime.now(timezone.utc),
        "published_at": datetime.now(timezone.utc),
        "registered_voters": 1000,
        "ballots_cast": 900,
        "candidates": {"A": 480, "B": 420},
    }
    payload.update(overrides)
    return payload


def test_valid_report():
    validate_report(VoteReport.model_validate(base()))


def test_negative_votes_rejected():
    with pytest.raises(ValidationError):
        validate_report(VoteReport.model_validate(base(candidates={"A": -5, "B": 905})))


def test_negative_ballots_rejected():
    with pytest.raises(ValidationError):
        validate_report(VoteReport.model_validate(base(ballots_cast=-1)))


def test_float_count_rejected():
    with pytest.raises(PydanticValidationError):  # coerce failure → malformed
        validate_report(VoteReport.model_validate(base(candidates={"A": 12.5, "B": 887})))


def test_published_before_closure_rejected():
    now = datetime.now(timezone.utc)
    with pytest.raises(ValidationError):
        validate_report(
            VoteReport.model_validate(
                base(polls_closed_at=now, published_at=now - timedelta(hours=2))
            )
        )


def test_empty_region_rejected():
    with pytest.raises(ValidationError):
        validate_report(VoteReport.model_validate(base(region="  ")))


def test_station_registry():
    registry = {"station-001": "key-001"}
    assert is_known_station("station-001", "key-001", registry)
    assert not is_known_station("station-001", "wrong", registry)
    assert not is_known_station("station-999", "key-999", registry)
