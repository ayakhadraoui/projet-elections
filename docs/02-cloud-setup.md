# 02 — Cloud accounts & the three IaaS VMs

The whole project lives on free/trial tiers. Split the work between team members
(ideally 4 people, one per account, so the extra credit covers the whole team).

## 1. Accounts to create (day 1, parallel)

| Cloud | Plan used | Account | Quotas to know |
|---|---|---|---|
| Azure | for Students | $100 credit, ARM supports Ubuntu | 2 vCPU max on free |
| Oracle | Always Free | 2 VMs (arm64 or x86), always and forever free | total 4 ARM vCPUs |
| GCP | Free Tier | e2-small (VM created, we keep only IaaS here) | e2-small ≈ $13/mo without credit |
| Vercel | Hobby | static/GitHub auto-deploy | personal |
| Render | Free (with credits) | 2 web services (PostgreSQL remains on VM!) | sleep after 15 min |
| Cloudflare | free | wrangler | 100 k req/day |
| Tailscale | free Personal | up to 100 devices | • |
| Telegram | free | create the group; note the chat_id | • |

## 2. The three VMs (Ubuntu 22.04/24.04 LTS)

Suggested sizing (the free tier may need compromises — document the real values
in this doc once created):

| VM | Cloud | role | suggested size |
|---|---|---|---|
| `azure-1` | Azure | Kafka primary + Aggregator + Anomaly engine + PostgreSQL (primary) | B2s (2 vCPU, 4 GB) |
| `oracle-1` | Oracle | standby: exactly the same as azure-1 | Standard.A1.Flex 1 oCPU / 4 GB |
| `gcp-1` | GCP | Prometheus + Grafana + Failover Controller + etcd Patroni | e2-small |

Azure & Oracle: **do not open any public port** (NSG / no ingress). GCP too:
only the GCP VM needs no public ingress either; Prometheus scrapes through
Tailscale, Grafana is accessed later via a Cloudflare tunnel or a Tailscale HTTP.

### 2.1 Bootstrap each VM

```bash
# on all three VMs
sudo apt update && sudo apt -y upgrade && sudo apt -y install python3-venv docker.io curl
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up --hostname azure-1         # / oracle-1 / gcp-1
tailscale ip -4                              # ← note the 100.x.y.z of each VM, they will be everywhere
```

Then Azure & Oracle get the identical "workload" stack (to allow the swap):

```bash
# postgresql
sudo apt -y install postgresql etcd          # etcd only on GCP in fact: see 03
# kafka (KRaft, no zookeeper)
wget https://dlcdn.apache.org/kafka/3.7.1/kafka_2.13-3.7.1.tgz
tar -xzf kafka_2.13-3.7.1.tgz && sudo mv kafka_2.13-3.7.1 /opt/kafka
```

The real config (server.properties, supervisor services, postgres PATRONI) is
detailed in `03-sync-failover.md`: **do not start Kafka as an isolated unit, it
will come along with Patroni + MirrorMaker**.

GCP VM additionally gets: prometheus, grafana, etcd, the failover controller
service, node_exporter of the 2 other clouds? **No** — node_exporter runs on
Azure/Oracle; the GCP VM only scrapes.

## 3. Tailscale checks (blocked = the main pitfall)

From each VM, before going further:

```bash
# in both directions, all 3 pairs:
ping 100.<oracle>...     # from azure: ping oracle & gcp
ping 100.<azure>...      # from oracle: ping azure  & gcp
ping 100.<gcp>...        # from gcp:    ping azure & oracle
telnet 100.<x.y.z> 5432  # etc. the future service ports
```

- if the telnet is blocked → `tailscale up --hostname=... --accept-routes` and
  check the ACLs in the Tailscale admin; the whole suite depends on this.

## 4. Telegram & the notification group

1. `@BotFather` → `/newbot` → note the token
2. in the admin group, add the bot
3. `curl https://api.telegram.org/bot<TOKEN>/getUpdates` → note the `chat_id`
4. `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` go to the Cloudflare Worker env

## 5. What to record now, in docs/02-cloud-vm-inventory.md

- the 3 Tailscale IP addresses
- the DNS names / console identifiers of each VM
- PostgreSQL roles & passwords used (not in git! use a private doc)
