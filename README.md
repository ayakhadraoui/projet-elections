# Projet Élections — Real-Time Multi-Cloud Electoral Platform

Real-time electoral results platform with automatic anomaly (fraud) detection and
multi-cloud failover. Cloud Computing course project.

**Team:** Diae EL Mokhtari · Aya Khadraoui · Hind EL YOUBI · Ibtissam EL ASSLOUJ

Target architecture (`projet-elections.pdf`):

| Type | Cloud | Role |
|------|-------|------|
| PaaS | Vercel | Submission portal + public dashboard |
| PaaS | Render | Ingestion API + Dashboard API |
| IaaS | Azure | **Primary cloud** — Kafka brokers, aggregator, anomaly engine, PostgreSQL |
| IaaS | GCP | Supervision (Prometheus/Grafana) + Failover Manager (controller + MirrorMaker 2 + Patroni) |
| IaaS | Oracle | **Standby cloud**, continuously synchronized |
| FaaS | Cloudflare Workers | Anomaly notifications → Telegram |
| Network | Tailscale | Encrypted overlay for all IaaS VMs, nothing exposed publicly |

## Repository layout

```
portal/                    → Vercel: submission portal (public-entry point for polling stations)
dashboard/                 → Vercel: public dashboard (live results + fraud alerts)
services/
  ingestion_api/           → Render: authenticates stations, validators, dedupe, transmits to the Kafka Relay of the active cloud
  kafka_relay/             → Azure & Oracle VM: HTTPS entry to Kafka (via Cloudflare Tunnel), per-cloud
  dashboard_api/           → Render: reads PostgreSQL, consumes the anomalies, serves the portal
  aggregator/              → Azure VM: Kafka consumer → aggregates by region → PostgreSQL
  anomaly_engine/          → Azure VM: Kafka consumer → fraud rules → publishes anomalies
  failover_controller/     → GCP VM: decides which cloud is active, GET /active
  notification_worker/     → Cloudflare Worker: anomaly webhook → Telegram group
shared/                    → common models, validation, Kafka helpers
simulators/                → simulated polling stations (Python scripts)
infra/                     → Prometheus rules, Patroni + MirrorMaker 2 configs, SQL schema
docs/                      → step-by-step setup & deployment guides
```

## Quick start (local, no cloud needed)

Everything runs locally with Docker:

```
docker compose up -d
pip install -r requirements.txt
python -m services.ingestion_api.main          # starts API on :8000
python -m services.aggregator.main
python -m services.anomaly_engine.main
python simulators/polling_station.py --count 10 --interval 5
```

Then explore:
- Ingestion API: http://localhost:8000/docs
- Prometheus: http://localhost:9090 · Grafana: http://localhost:3001 (admin/admin)
- Dashboard JS (open Vercel APIs locally): set `DASHBOARD_API_URL=http://localhost:8001`

See `docs/01-local-development.md`.

## Production deployment

Follow the docs in order:

1. `docs/02-cloud-setup.md` — accounts, VMs (Azure / Oracle / GCP), Tailscale
2. `docs/03-sync-failover.md` — PostgreSQL Patroni replication + Kafka MirrorMaker 2
3. `docs/04-deployment.md` — Render / Vercel / Cloudflare Workers
4. `docs/05-demo.md` — the failover demo scenario

## Tech

Python 3.11+, FastAPI, kafka-python-ng, psycopg2, Prometheus client, Docker.
