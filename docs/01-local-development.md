# 01 — Local development (everything on one machine)

Goal: **prove the whole pipeline works before spending a minute on clouds.**

## 1. Prerequisites

- Python 3.11+ (`pip install -r requirements.txt`)
- Docker + Docker Compose
- (Windows/macOS: `export`-style vars are loaded via `.env` if you use direnv,
  or just set them manually)

## 2. Start the infrastructure

```bash
docker compose up -d          # Kafka + PostgreSQL + Prometheus + Grafana
```

Wait ~1 min then check:

- Kafka topics created: `docker compose logs kafka-init` → `vote_reports`, `aggregated_results`, `anomalies`
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3001 (admin/admin)

## 3. Start the services (4 terminals, from repo root)

```bash
# terminal 1 — ingestion API (http://localhost:8000/docs)
python -m services.ingestion_api.main

# terminal 2 — the failover controller (answers "which cloud is active")
python -m services.failover_controller.main          # :8002

# terminal 3 — the aggregator (writes PostgreSQL)
python -m services.aggregator.main

# terminal 4 — the anomaly engine (detects fraud)
python -m services.anomaly_engine.main

# terminal 5 — dashboard API (http://localhost:8001/live)
python -m services.dashboard_api.main
```

## 4. Simulate polling stations

```bash
python simulators/polling_station.py --stations 5
```

- `curl http://localhost:8001/live` → results accumulate
- open `portal/index.html` in a browser and submit a report manually too
- open `dashboard/index.html` in a browser (it fetches `:8001/live`)

## 5. Trigger the anomaly rules

```bash
python simulators/polling_station.py --stations 2 --interval 3 --fraud turnout
python simulators/polling_station.py --stations 2 --interval 3 --fraud dup
python simulators/polling_station.py --stations 2 --interval 3 --fraud late
python simulators/polling_station.py --stations 2 --interval 3 --fraud jump
```

Each run must produce visible lines `ANOMALY <kind> ...` in the anomaly engine
and corresponding entries in the dashboard's 🚨 table.

## 6. Verify everything

```bash
python -m pytest tests -q
docker compose exec postgres psql -U election -c "SELECT region, candidate, votes FROM region_results LIMIT 10;"
docker compose exec postgres psql -U election -c "SELECT kind, station_id FROM anomalies_log LIMIT 10;"
curl http://localhost:8002/active
```

## Check out data flows

- watch Kafka streams: `docker compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --from-beginning --topic anomalies`
- Grafana: add panels on `up{job="..."}`
