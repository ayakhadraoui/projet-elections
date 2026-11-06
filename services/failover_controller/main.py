"""Failover Controller — Google Cloud (IaaS), inside the Failover Manager.

Knows the state of both clouds from the Prometheus alert feed (Prometheus
already scrapes the metrics ports of both machines), and answers
GET /active → {"active_cloud": "azure" | "oracle"}.

Decision logic:
  - no AzureDown alert firing  → azure (primary)
  - AzureDown firing (panne)  → oracle (Prometheus bound the plan to define it)
  - Prometheus unreachable    → last known state, then default primary
"""
from __future__ import annotations

import logging
import time

import httpx
import uvicorn
from fastapi import FastAPI
from prometheus_client import Counter, Gauge, start_http_server

from shared import config as cfg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("failover_controller")

app = FastAPI(title="Failover Controller", version="1.0.0")

active_cloud_state = Gauge("failover_active_cloud", "1=primary,2=standby")
decisions = Counter("failover_decisions_total", "Failover decisions", ["active_cloud"])

_last_decision: tuple[float, str] = (0.0, cfg.PRIMARY_CLOUD_NAME)


def query_alerts(client: httpx.Client) -> dict | None:
    try:
        r = client.get(f"{cfg.PROMETHEUS_URL}/api/v1/query",
                       params={"query": "ALERTS{alertstate=\"firing\"}"}, timeout=3.0)
        r.raise_for_status()
        return r.json().get("data", {}).get("result") or []
    except Exception as exc:
        log.warning("prometheus unreachable: %s", exc)
        return None


def decide() -> str:
    # Local-dev switch: without real cloud metrics, Prometheus "sees" nothing and
    # would wrongly fail over; STANDALONE mode short-circuits this.
    if cfg.env("FAILOVER_STANDALONE") in ("1", "true", "yes"):
        return cfg.PRIMARY_CLOUD_NAME

    alert_names = set()
    series = query_alerts(httpx)
    if series is not None:  # Prometheus answered
        for alert in series:
            labels = alert.get("metric", {})
            name = labels.get("alertname", "")
            alert_names.add(name)
        azure_down = any(
            a.startswith(cfg.PRIMARY_CLOUD_NAME + "Down") or a == "AzureDown"
            for a in alert_names
        )
        azure_overloaded = any(
            "overload" in a.lower() or a == "AzureOverloaded" for a in alert_names
        )
        if azure_down or azure_overloaded:
            return cfg.STANDBY_CLOUD_NAME
        return cfg.PRIMARY_CLOUD_NAME
    # Prometheus is down: fall back on the last decision (< grace period) else primary
    recent = time.time() - _last_decision[0] < 30
    return _last_decision[1] if recent else cfg.PRIMARY_CLOUD_NAME


@app.get("/active")
def active() -> dict:
    global _last_decision
    choice = decide()
    if choice != _last_decision[1]:
        log.warning("FAILOVER DECISION: %s → %s", _last_decision[1], choice)
        decisions.labels(active_cloud=choice).inc()
    _last_decision = (time.time(), choice)
    active_cloud_state.set(2 if choice == cfg.STANDBY_CLOUD_NAME else 1)
    return {"active_cloud": choice, "decided_at": time.time(), "last_decision": _last_decision[1]}


if __name__ == "__main__":
    start_http_server(9702)
    uvicorn.run(app, host="0.0.0.0", port=cfg.CONTROLLER_PORT)
