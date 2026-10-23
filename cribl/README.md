# SIEM outage buffer for Cribl Stream, on LaserData

When a SIEM goes down, the events Cribl Stream routes to it wait in one shared Apache Iggy log instead of
on each Worker's disk, every other Destination keeps receiving in milliseconds, and fresh events reach the
SIEM first when it is back. It sits behind a Webhook Destination, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Cribl_SIEMOutageBuffer.pdf`](LaserData_Cribl_SIEMOutageBuffer.pdf)

## Why this use case

Cribl's own docs and blog describe where a Destination's backlog lives today and what it costs:

- **On the Worker's disk.** Persistent queues are "specifically on-disk storage" and operate "at the Worker
  Process level". On Cribl.Cloud, "Cribl automatically allocates up to 1 GB of disk space per Destination, per
  Worker Process … You cannot change this limit", and "If the queue fills up, Cribl Stream blocks data flow".
  For host loss, only shared S3 or NFS storage "allows surviving Nodes to recover and drain data that would
  otherwise be lost". ([Persistent Queues, Cribl Stream 4.20](https://docs.cribl.io/stream/persistent-queues/))
- **Sized as a project.** Cribl's PQ sizing post works through 25 Worker Nodes with 900 GB of SSD each for
  queuing, about 21 TB in all, against ~35 TB a day (about 175k EPS) to 120 Splunk indexers: "Cribl can handle
  about 14 hours of Splunk downtime", with a suggested drain rate of about 5% of EPS.
  ([How Does Persistent Queuing Work Inside Cribl Stream?](https://cribl.io/blog/cribl-persistent-queuing/), 2023)
- **One stuck Destination can stop the rest.** "If any Destination is configured to Block and enters
  backpressure, Cribl Stream blocks delivery for that event across all Destinations that receive the same input
  data … the Source will stop sending to all connected Destinations." (same docs page)
- **Replay for a new Destination is a batch job** from object storage, with Collectors over a time range.
  ([Telemetry replay with Cribl Stream and Cribl Lake](https://cribl.io/blog/master-telemetry-replay-with-cribl-stream-and-cribl-lake/))

So this kit puts one Route's security events in one log and lets each Destination read it at its own pace.
Worker Processes write each event once, keyed by the sending host, with a per-host sequence. The SIEM, Cribl
Lake and a new SIEM in a migration each read every partition directly from their own offsets (no consumer
group, so no rebalances). When the SIEM is down, its offsets stay put and its events wait in the log, sized by
retention and replicated, not on a Worker's disk; Cribl Lake keeps receiving and the Workers never see
backpressure. When the SIEM is back, its live reader jumps to the end of the log, so fresh events reach it in
milliseconds at once, and a second reader drains the held range behind them at a rate you set. A new SIEM
starts the same way: live from the end, the retained log backfilled behind it. Every Destination checks each
host's sequence across both readers, so "nothing lost" is a counted number.

Cribl Stream keeps its Sources, Routes, Pipelines and every Destination. There is no Apache Iggy Destination in
Cribl Stream today; until there is, a Webhook Destination and the small receiver in this kit
(`cribl_webhook.py`) carry the events in.

## Run it

Needs Docker and Python 3.10+. Defaults: one Worker Node writing 7,000 events a second (the PQ post's 175k EPS
over 25 nodes) from 2,000 sending hosts on 4 Worker Processes, 16 partitions, 72 hours of retention.

```
docker compose up -d                                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                        # apache-iggy 0.9.0, aiohttp
python destination.py --dest cribl_lake &              # Cribl Lake: reads from its own offsets
python destination.py --dest splunk_lb --outage-at 60 --outage-for 300 --drain-rate 10000 &
                                                       # the SIEM: down from 60 s for 5 min, then fresh events first
python workers.py                                      # one Worker Node writing the Route's events once
python destination.py --dest new_siem --backfill       # later, in another terminal: a new SIEM backfills behind live
```

Each Destination prints a line a second (wrapped here):

```
[splunk_lb] destination down: delivered 0/s  waiting 15,248 in the log (5.8 MB), offsets held
            down for 19.1 s  lost 0
[splunk_lb] destination back: 16,030 events (6.1 MB) to drain behind live at 1,500/s; fresh events go first
[splunk_lb]    800 events/s live to splunk_lb  event-to-destination p50 5.5ms  p99 9.7ms  p99.9 10.5ms
            PASS (<100ms)  over budget 0  lost 0  repeats dropped 0
            | drained 1,489/s behind live, 1,031 waiting (0.4 MB), oldest 29.4 s old
[splunk_lb] backlog drained: 16,030 events in 10.7 s, fresh events first throughout, 0 events lost
```

## What CI shows

`ci.sh` runs on a real Apache Iggy 0.9.0 broker on every change: one Worker Node at 800 events a second from
600 hosts on 8 partitions; Cribl Lake and the SIEM reading from their own offsets; the SIEM down from its
second 25 to 45, then fresh events first while ~16,000 held events drain behind them at 1,500 a second; a new
SIEM joining at second ~50 and backfilling ~38,400 events behind live; and one gzip NDJSON request through
`cribl_webhook.py`. It passes only if every Destination holds a 100 ms p99 for live events in 95% of its
steady-state seconds, the outage happens, both drains finish and no event is lost.

In three green runs on the final code, every Destination passed every steady-state second: p50 4.2 to
6.2 ms, p99 8.4 to 13.3 ms and p99.9 9.2 to 14.2 ms (medians per second), worst second 14 to 49 ms. The SIEM's
p99 for fresh events while it drained its held range was 8.7 to 13.0 ms, against 8.4 to 12.7 ms otherwise. It
drained ~16,000 held events in 10.7 s and the new SIEM backfilled ~38,400 in 25.6 s, 0 events lost in every run.

What the numbers mean: they time each event from the Worker's send in `workers.py` to arrival at
`destination.py`, through Python clients, on a shared 2-vCPU GitHub runner with the broker and four Python
processes on one box, at reduced scale. They don't include Cribl Stream itself, the Webhook hop or the SIEM's
indexing. The broker's own tail latency and cost are measured separately (`bench/PLAN.md`).

## Shadow a Route from your Cribl Stream

```
python cribl_webhook.py --port 10080 --token <secret>   # writes each request into cribl_stream/security_events
python destination.py --dest splunk_lb                  # one per Destination you want to compare
```

In Cribl Stream, add a **Webhook** Destination: URL `http://<receiver>:10080/`, method POST, format NDJSON,
Auth token `<secret>`. Attach it to the Route you want to shadow with **Final** off (or an Output Router rule
with Final off), so your existing Destinations still get every event. Set its **Backpressure behavior** to
**Drop** for the trial: per Cribl's docs, a Destination set to Block that backs up stops the Source for every
Destination sharing those events. The receiver answers 200 only after Apache Iggy has the events and 503 if it
can't take them, which the Webhook Destination retries. It keys by `host` and keeps `_time`, `sourcetype`
and `_raw`; run one receiver per Worker Group.

## What Apache Iggy 0.9 has today, and what it doesn't

- **Replication:** Viewstamped Replication clusters, shipped September 2026; three nodes tolerate one failure.
- **Delivery:** at-least-once. The per-host sequence makes repeats safe to drop; Cribl's HTTP Destinations can
  also emit duplicates, per its docs.
- **Retention:** per-topic message expiry and maximum size, so the outage window is a setting, not disk sizing per
  Worker. Tiered storage to object storage is not in 0.9; days of a large Route's events sit on broker disks.
- **Cribl Stream integration:** no native Apache Iggy Source or Destination yet. The Webhook receiver works today;
  a native Destination would be the joint work.
- **Kafka API:** a Kafka-compatible gateway is in progress, not in 0.9, so Cribl Stream's Kafka Destination can't
  write to Iggy directly.
- **Hosting:** LaserData Cloud runs as a managed service on AWS or GCP, or inside your own AWS or GCP account
  (BYOC). On Azure, run Apache Iggy self-hosted on your own VMs; LaserData supports it.

## Files

| File | What it does |
|---|---|
| `workers.py` | One Worker Node: hosts pinned to Worker Processes, each event written once, keyed by host |
| `destination.py` | One Destination: live from its own offsets, outage, fresh-first drain, backfill, loss check |
| `cribl_webhook.py` | Receiver for a Cribl Stream Webhook Destination (NDJSON or JSON Array, gzip) |
| `common.py` | Stream, topic, retention, partition function |
| `bench/PLAN.md` | The benchmark plan, rules first |
| `ci.sh` | The CI test above |

Contact: kranti@laserdata.com · [laserdata.com](https://laserdata.com)
