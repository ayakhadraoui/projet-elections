# 03 — The heart: replication (Patroni + MirrorMaker 2) and the Failover

The most risky part of the project. Do it **as soon as the VMs are up** — do not
postpone it. Everything below happens on the three IaaS VMs through Tailscale.

## 0. Prerequisites

- 3 VMs running, Tailscale OK, the `100.x.y.z` IPs go in all commands
- PostgreSQL not running (`systemctl is-active postgresql` → stop it, Patroni wants
  to launch it alone)

## 1. etcd — Patroni's DC repository (GCP VM)

```bash
sudo apt -y install etcd
# /etc/default/etcd (or systemd drop-in):
#   ETCD_LISTEN_CLIENT_URLS="http://0.0.0.0:2379"
#   ETCD_ADVERTISE_CLIENT_URLS="http://<GCP_TS_IP>:2379"
sudo systemctl enable --now etcd
```

## 2. Patroni (Azure AND Oracle — same file, only `name` & IPs change)

```bash
sudo apt -y install python3-pip && sudo pip3 install 'patroni[etcd]'
sudo mkdir -p /etc/patroni
cp infra/patroni/patroni.yml.example /etc/patroni/patroni.yml
# → replace <NODE_NAME>, <THIS_TS_IP>, passwords (see <<= config reference)
patroni /etc/patroni/patroni.yml        # in the foreground for the first debugging
```

Systemd service to look at it for an automatic restart:

```ini
# /etc/systemd/system/patroni.service
[Unit] Description=Patroni
[Service] ExecStart=/usr/local/bin/patroni /etc/patroni/patroni.yml
Restart=always
```

### Cluster validation

```bash
curl -s http://<AZURE_TS_IP>:8008/cluster | head    # 1 leader + 1 standby
psql -h <AZURE_TS_IP> -U postgres -c "SELECT pg_is_in_recovery();"   # "f" (primary)
psql -h <ORACLE_TS_IP> -U postgres -c "SELECT pg_is_in_recovery();"  # "t" (replica)
```

### Manual failover test (do it now, it is free)

1. `systemctl stop patroni` on Azure
2. `curl http://<ORACLE_TS_IP>:8008/cluster` → after ~10 s Oracle is `leader`
3. start Patroni on Azure → it comes back as **standby** (resync handled automatically)

## 3. Kafka (KRaft) — identical on Azure & Oracle

`/opt/kafka/config/server.properties`, the essential values (identical on both sides
except `node.id` and `advertised.listeners`):

```properties
process.roles=broker,controller
node.id=1                          # 1 on azure / 3? (nb: any distinct value on each side)
controller.quorum.voters=1@<OWN_TS_IP>:9093
listeners=PLAINTEXT://<OWN_TS_IP>:9092,CONTROLLER://<OWN_TS_IP>:9093
advertised.listeners=PLAINTEXT://<OWN_TS_IP>:9092
default.replication.factor=2       # within a cloud, no disk unit = single node
```

Start + creation of the 3 topics (local): `kafka-topics.sh --create --topic vote_reports ...`

```bash
sudo systemd-run --unit=kmafka /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
```

## 4. MirrorMaker 2 (on the GCP VM — Failover Manager)

Materialized `infra/mirrormaker/mm2.properties` with the proper IPs, then:

```bash
/opt/kafka/bin/connect-mirror-maker.sh infra/mirrormaker/mm2.properties
# check:
/opt/kafka/bin/kafka-topics.sh --bootstrap-server <ORACLE_TS_IP>:9092 --list
#   → you must see the mirrored topics (azure.vote_reports, azure.anomalies, ...)
```

Attention: the mirroring being bidirectional, the consumed topics become on
`<remote>.<topic>`: **the aggregator services must handle the two name schemes** or
(in simple demo) consume their local topic + the mirrored topics of the other cloud.
Provide in `shared.config`: `KAFKA_TOPIC_PREFIXES` = `"" | "azure. | oracle."`.

## 5. Kafka Relay + Cloudflare Tunnel (Render → IaaS, without exposing a port)

Each VM hosts `services/kafka_relay/main.py` (port 8300) and goes through:

```bash
curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared
cloudflared tunnel --url http://localhost:8300      # the manual version, for the 1st test
# then a permanent tunnel + config, e.g.: azure vote relay : azure-relay.adomain / oracle-relay.adomain
```

In the ingestion API on Render, set `RELAY_URLS = {"azure": "https://azure-relay...",
"oracle": "https://oracle-relay..."}`.

## 6. The Prometheus stack (on the GCP VM)

- install `prometheus`, `grafana`, plus on each VM Azure/Oracle `node_exporter` & `postgres_exporter`
- `infra/prometheus/prometheus.yml`: replace `<AZURE_TS_IP>`/`<ORACLE_TS_IP>`
- load the rules `infra/prometheus/rules/alerts.yml` (the Failover Controller consumes them)
- start `<FAILOVER CONTROLLER>`: systemd python service `services/failover_controller/main.py`

### Total validation

```bash
curl http://<GCP_TS_IP>:8002/active          # {"active_cloud":"azure"}
# kill Patroni + Kafka on Azure 10 s:
curl http://<GCP_TS_IP>:8002/active          # {"active_cloud":"oracle"}   ← the demo, already!
```

## 7. Checklist (each must be green before going to docs/04)

- [ ] Patroni: automatic leader election, tested in both directions
- [ ] MirrorMaker 2: the 3 topics are vivacious on both sides
- [ ] Kafka Relay of each cloud o(pens a Cloudflare Tunnel), auth tested with curl
- [ ] The `/active` answers "azure", and "oracle" when Azure is down
- [ ] Grafana shows the CPU of the 2 VMs + kafka response times + replication lag
