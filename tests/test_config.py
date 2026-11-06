"""Config sanity checks (values used across services)."""
from shared import config as cfg


def test_topics():
    assert cfg.TOPIC_VOTE_REPORTS
    assert cfg.TOPIC_ANOMALIES
    assert cfg.TOPIC_AGGREGATED


def test_cloud_names():
    assert cfg.PRIMARY_CLOUD_NAME and cfg.STANDBY_CLOUD_NAME
    assert cfg.PRIMARY_CLOUD_NAME != cfg.STANDBY_CLOUD_NAME
