# 04 — Deployment of the PaaS & FaaS services

Once docs/03 is green (the IaaS heart works), wire in the three platform
services: Render, Vercel, Cloudflare Workers.

## The one design decision of this phase

Render (PaaS) **cannot join the Tailscale mesh**. Never expose Kafka or
PostgreSQL publicly. Solution used in this codebase:

1. each IaaS VM already runs the **Kafka Relay** behind a Cloudflare Tunnel
   (`POST /vote_reports` for ingestion, plus a `GET /live` that reads the
   cloud-local PostgreSQL and returns aggregated results — the dashboard data
   travels as HTTPS/JSON, no DB port ever opens);
2. the **Cloudflare Worker** (notifications) additionally keeps the last ~50
   anomalies in a **Cloudflare KV** namespace and exposes a public
   `GET /anomalies` (authenticated by a token) — that is how anomaly history
   reaches Render without a Kafka consumer;

So the flow:  Render → (HTTPS/CF Tunnel) → IaaS. Everything else is private.

## 1. Render — the two APIs

Repo on GitHub; Render deploys from it. Two web services, **root dir `/`**:

| Service | Build | Start command | Health check |
|---|---|---|---|
| Ingestion API | `pip install -r requirements.txt` | `uvicorn services.ingestion_api.main:app --host 0.0.0.0 --port $PORT` | `/` returns 404 → set `GET /health` |
| Dashboard API | idem | `uvicorn services.dashboard_api.main:app --host 0.0.0.0 --port $PORT` | idem |

Add `GET /health` endpoints — done in both services (`/health`).

Environment variables:

```
# Ingestion API
FAILOVER_URL=https://failover.yourdomain          # CF Tunnel → GCP VM :8002
RELAY_URLS={"azure":"https://azure-relay.yourdomain","oracle":"https://oracle-relay.yourdomain"}
PRIMARY_CLOUD=azure          # only to color the warning logs
INGEST_API_SECRET=<long random>
STATION_API_KEYS={"station-001":"key-001", ...}

# Dashboard API (cloud mode)
RELAY_URLS=<same json>
FAILOVER_URL=<same>
ANOMALIES_SOURCE_URL=https://election-notifications.<account>.workers.dev
ANOMALIES_SOURCE_TOKEN=<same token as the Worker>
```

## 2. Vercel — portal & dashboard

Two static projects (no build step): one with root `portal/`, one root `dashboard/`.
Set (before deploy or via Vercel config) in each HTML (we check both):

```html
<script>
  window.INGEST_API_URL = "https://your-render-ingestion.onrender.com";  // portal only
  window.DASHBOARD_API_URL = "https://your-render-dashboard.onrender.com"; // dashboard
</script>
```

## 3. Cloudflare Workers — Telegram + anomaly KV

```bash
cd services/notification_worker
npx wrangler login
npx wrangler kv namespace create ANOMALIES          # copy the id into wrangler.toml
npx wrangler secret put TELEGRAM_BOT_TOKEN
npx wrangler secret put TELEGRAM_CHAT_ID
npx wrangler secret put LIST_TOKen                  # shared secret for GET /anomalies
npx wrangler deploy
```

The worker code (`index.js`) handles:
- `POST /notify` — anomaly engine pushes anomalies (→ Telegram + insert into KV)
- `GET /anomalies` — public read (token-protected) for the Render dashboard API.

## 4. The dependency graph (who calls who)

- polling stations/portal (Vercel) → Ingestion API (Render) - HTTPS ✓
- ingestion → `/active` (failover controller, GCP, CF Tunnel) ✓
- ingestion → Kafka Relay of the *chosen* cloud (CF Tunnel) ✓
- relay → local Kafka (+ MirrorMaker 2 keeps both clouds synchronized) ✓
- aggregator & anomaly engine → local Kafka + local PostgreSQL (Patroni) ✓
- anomaly engine → Worker `POST /notify` → Telegram + KV ✓
- dashboard API → Relay `/live` (active cloud) + Worker `GET /anomalies` ✓
- dashboard (Vercel) → dashboard API (Render) ✓

## 5. Order of deployment (recommended)

1. Cloudflare Tunnels (2 relay hostnames + 1 failover hostname) → bare URLs first
2. Cloudflare Worker (+ KV) — quick, independent
3. Render (2 services, env vars point at those fixed URLs)
4. Vercel (last: URLs of the Render services)
