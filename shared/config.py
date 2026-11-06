"""Environment/config loading shared by all services."""
from __future__ import annotations

import json
import os
from functools import lru_cache


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


KAFKA_BROKERS = env("KAFKA_BROKERS", "localhost:9092").split(",")
TOPIC_VOTE_REPORTS = env("KAFKA_TOPIC_VOTE_REPORTS", "vote_reports")
TOPIC_AGGREGATED = env("KAFKA_TOPIC_AGGREGATED", "aggregated_results")
TOPIC_ANOMALIES = env("KAFKA_TOPIC_ANOMALIES", "anomalies")

DATABASE_URL = env("DATABASE_URL", "postgresql://election:election@localhost:5432/elections")

# simulators → ingestion API
INGEST_URL = env("INGEST_URL", "http://localhost:8000")

# ingestion API
STATION_API_KEYS: dict[str, str] = json.loads(env("STATION_API_KEYS", "{}"))
INGEST_API_SECRET = env("INGEST_API_SECRET", "dev-secret")
FAILOVER_URL = env("FAILOVER_URL", "http://localhost:8002")
DEFAULT_ACTIVE_CLOUD = env("DEFAULT_ACTIVE_CLOUD", "azure")
PRIMARY_CLOUD = env("PRIMARY_CLOUD", "azure")

# failover controller
PROMETHEUS_URL = env("PROMETHEUS_URL", "http://localhost:9090")
PRIMARY_CLOUD_NAME = env("PRIMARY_CLOUD_NAME", "azure")
STANDBY_CLOUD_NAME = env("STANDBY_CLOUD_NAME", "oracle")
CONTROLLER_PORT = int(env("CONTROLLER_PORT", "8002"))

# metrics (prometheus_client) — each service uses a different port
METRICS_PORT = int(env("METRICS_PORT", "9700"))

# dashboard api
FRONTEND_ORIGIN = env("FRONTEND_ORIGIN", "*")

# anomaly engine
ANOMALY_WEBHOOK_URL = env("ANOMALY_WEBHOOK_URL")

# tuning knobs for the anomaly rules (per-doc: fraud & input-error rules)
MAX_TURNOUT_RATIO = float(env("MAX_TURNOUT_RATIO", "1.02"))    # > 102 % turnout -> anomaly
MAX_POLL_DELAY_SECONDS = int(env("MAX_POLL_DELAY_SECONDS", "30"))   # published > 30s after close
MAX_VOTE_JUMP = int(env("MAX_VOTE_JUMP", "2000"))              # insane ballot growth between reports
