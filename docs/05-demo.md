# 05 — Demo scenario (the one that sells the project)

Rehearse this end-to-end **at least twice** before the presentation.
All commands can be run from any machine on the Tailscale mesh.

## Setup (before the jury arrives)

- services running on the 3 VMs (systemd), Render awake (ping them 10 min before:
  free tier sleeps), simulator scripts ready
- Grafana dashboard pre-open in a browser tab, with:
  panel 1 `up{job=~"azure-.*"}`, panel 2 `up{job=~"oracle-.*"}`, panel 3 replication lag
- the Telegram group on screen
- the public dashboard in a second tab

## Scenario (≈ 8 minutes)

### 1. Nominal election (2 min)
```bash
python simulators/polling_station.py --stations 5 --interval 4
```
→ live dashboard is updating, Grafana green, nothing on Telegram.

### 2. A cheating station (2 min)
```bash
python simulators/polling_station.py --stations 2 --interval 4 --fraud turnout
```
→ Telegram message arrives instantly + line in the dashboard's 🚨 table.
Repeat a few types: `dup`, `late`, `jump`.

### 3. The failover — the demonstration (3 min)
On the **Azure VM**: `sudo systemctl stop patroni kafka` (or even
`sudo shutdown now` for maximum drama).

- Grafana: `up{job="azure-node"}` goes to 0 → alert `AzureDown` firing
- ingestion on Render switches: `curl https://<render-ingestion>/health` → logs show
  the routing towards `oracle-relay`
- the simulator keeps its rhythm: **no vote is lost** (the volume does not drop)
- `curl https://<failover>/active` → `oracle`

### 4. Return to the primary (1 min)
`sudo systemctl start patroni` on Azure → mirror 2 resync → `/active` says `azure` again.

## Zero data loss: the proof to show

```bash
# before the kill: note the total bulletins
curl -s http://<render-dash>/live | jq .ballots_cast
# ... during the failover, the simulator continues ...
# after the return:
curl -s http://<render-dash>/live | jq .ballots_cast
#     → the counter has increased by exactly the number of votes sent DURING the outage
```

Grafana screenshot + this counter = your "evidence of no data loss".

## Exit checklist (things that break in a demo)

- [ ] Render services awake (they sleep after 15 min)
- [ ] CF tunnels alive (`cloudflared` on the VMs, systemd restart=always)
- [ ] Telegram token/chat_id valid (a test `POST /notify`)
- [ ] Tailscale up on each VM
- [ ] simulator IPs/keys consistent with `STATION_API_KEYS`
- [ ] Grafana pre-open, Prometheus retention OK
