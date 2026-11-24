# Projet Élections — Real-Time Multi-Cloud Electoral Platform

Real-time electoral results platform with automatic anomaly (fraud) detection and
multi-cloud failover. Cloud Computing course project — ESI.

**Team:** Diae EL Mokhtari · Aya Khadraoui · Hind EL YOUBI · Ibtissam EL ASSLOUJ

## The project in one sentence

Polling stations publish their counted votes into the cloud, the public watches
live results and fraud alerts on a dashboard, and the whole system keeps running
even when one cloud provider goes down.

## Official architecture

| Provider | Service type | Components |
|---|---|---|
| **Vercel** | PaaS | Public dashboard (live results + fraud alerts) |
| **Render** | PaaS | Ingestion API (authenticates & validates stations) + Dashboard API (feeds the dashboard) |
| **Oracle Cloud** | IaaS | Kafka cluster (brokers) + PostgreSQL **primary** + Aggregator |
| **Microsoft Azure** | IaaS | Anomaly Engine ("the referee") + PostgreSQL **replica** (auto-promoted on failover) + supervision (Prometheus + Grafana) |
| **Cloudflare** | FaaS | Notification worker: anomaly → Telegram group |
| **Tailscale** | network | Encrypted overlay between all VMs — **no public ports anywhere** |

Data flows: stations → ingestion API → Kafka → (aggregator → PostgreSQL) and
(anomaly engine → Kafka → dashboard API + Cloudflare worker). PostgreSQL is
replicated across the two IaaS clouds; Kafka brokers live on both clouds so the
surviving cloud keeps the pipeline alive during the failover demo.

## Deployment status

| Component | Where | State |
|---|---|---|
| VM `azure-1` (Ubuntu 24.04, B2als v2) | Azure, Belgium Central | ✅ deployed |
| Tailscale mesh (`100.80.90.119`) | azure-1 | ✅ connected |
| Kafka 3.7.1 (KRaft, systemd) | azure-1 | ✅ 3 topics live |
| PostgreSQL 16 + schema | azure-1 | ✅ end-to-end tested |
| kafka_relay, aggregator, anomaly engine (systemd) | azure-1 | ✅ running |
| node_exporter + Prometheus + Grafana + dashboards | azure-1 | ✅ running |
| Oracle VMs (Kafka primary brokers, PostgreSQL primary) | Oracle Cloud | ⏳ account verification |
| Ingestion API + Dashboard API | Render | 🔜 next |
| Public dashboard | Vercel | 🔜 next |
| Notification worker | Cloudflare Workers | 🔜 next |

## Repository layout

```
portal/                    → station-facing submission portal (static page)
dashboard/                 → public dashboard (static page, deployed on Vercel)
services/
  ingestion_api/           → Render: authenticates stations, validators, dedupe,
                             routes validated votes to the active cloud's relay
  kafka_relay/             → per-VM HTTPS entry to Kafka (via Cloudflare Tunnel)
  dashboard_api/           → Render: reads PostgreSQL, consumes anomalies,
                             serves the public dashboard
  aggregator/              → Kafka consumer → aggregates by region → PostgreSQL
  anomaly_engine/          → Kafka consumer → fraud rules → anomalies topic
  failover_controller/     → decides which cloud is active (GET /active)
  notification_worker/     → Cloudflare Worker: anomaly webhook → Telegram
shared/                    → common models, validation, Kafka helpers, config
simulators/                → simulated polling stations (Python scripts)
infra/                     → Prometheus config & rules, SQL schema, Grafana
                             provisioning
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
python simulators/polling_station.py --stations 5 --interval 2
```

Then explore:
- Ingestion API: http://127.0.0.1:8000/docs
- Results: http://127.0.0.1:8001/elections/national-2026
- Anomalies: http://127.0.0.1:8001/anomalies
- Prometheus: http://localhost:9090 · Grafana: http://localhost:3001 (admin/admin)

Demo fraud injection:

```
python -m simulators.polling_station --stations 2 --rounds 4 --fraud turnout
```

See `docs/01-local-development.md`.

## Production deployment

1. `docs/02-cloud-setup.md` — accounts, VMs, Tailscale
2. `docs/03-sync-failover.md` — replication & failover
3. `docs/04-deployment.md` — Render / Vercel / Cloudflare Workers
4. `docs/05-demo.md` — the failover demo scenario

## Tech

Python 3.11+, FastAPI, kafka-python, psycopg2, Prometheus, Grafana, Docker.
