# Streaming exports of finished views for Mux Data, on LaserData

Each streaming export gets its own Apache Iggy topic, so a customer's throttled or unreachable
Kinesis stream or Pub/Sub topic delays only that customer's views, which then backfill from the last
exported view with nothing lost. It runs beside Kafka and Kafka Connect, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.mux-kit
- **One slide:** [`LaserData_Mux_StreamingExports.pdf`](LaserData_Mux_StreamingExports.pdf)

## Why this use case

Mux's docs describe the product: "Mux Data supports streaming exports of video views to an Amazon
Kinesis Data Stream or Google Cloud Pub/Sub topic", views are sent as they complete and are available
"within about one minute after the view ends", customers should "handle multiple or duplicate records
for each view ID", and a view "can be updated after it has been exported" (Export raw Mux data). Each
destination belongs to the customer: their stream, their IAM role for Mux's AWS account, their
"Pub/Sub Publisher" grant.

Mux's engineering blog describes what runs it. Finished views come out of Go processors that read
beacons from Kafka, keyed by customer and view ID ("We process millions of video views each day",
2019), and the exports run on Kafka Connect in distributed mode on Kubernetes ("Kafka Connect: The
Magic Behind Mux Data Realtime Exports", 2020). The same post names the pain: "the default behavior
in Kafka Connect is to not retry writes on a worker task", "The only way to resolve this is by
restarting the individual failed tasks", Kinesis gives "spurious write-failures that immediately
succeed upon retry", and rebalances can make the autoscaler show "massive overprovisioning and
thrashing in response to a moderate increase in load".

This kit gives each streaming export its own lane. The finished-view writer appends every finished
view, and every later update to one, to the topic of its environment's export, keyed by view ID, with
a version and a sequence per export partition. Export workers read their exports' partitions
directly, each at its destination's write limit (1,000 records a second per Kinesis shard; a set limit
for Pub/Sub), from their own offsets (no consumer group and no sink tasks, so no rebalances and nothing
to restart). A write that fails spuriously is retried in place by the worker that owns the export.
When a customer's stream is unreachable, that export's offset stays at the last exported view and its
views wait on its own topic; when the stream is back, it backfills from that offset at its limit
until it has caught up. Every export's backlog is read from the log itself every second. Collectors,
Kafka, the processors, ClickHouse and the Monitoring Dashboard stay as they are.

## Run it

Needs Docker and Python 3.10+. Defaults: 150 environments with a streaming export each (long-tail:
a few large media customers produce most views; odd-numbered environments export to Kinesis, even
ones to Pub/Sub; the largest 5% on 4 partitions and a 4-shard stream), about 2,500 finished views a
second, 3% of records updating an exported view, 1.2 KB per view record, Kinesis limited to 1,000
records/s per shard and Pub/Sub to 5,000/s, 1% of destination writes failing once, and 50 MB/s of
raw player beacons next door. View volumes are illustrative; Mux has not published them.

```
docker compose up -d                                      # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                           # apache-iggy 0.9.0
python exporter.py --shard 0 --shards 2 --outage-for 120 &   # export workers, two shards;
python exporter.py --shard 1 --shards 2 --outage-for 120 &   # env-0001-kinesis's stream is down from 60 s for 2 min
python views.py &                                         # finished and updated views to every export
python beacon_flood.py                                    # raw player beacons next door
```

Each export shard prints a line a second, plus one for any export in trouble:

```
[exp-1]  1,112 views/s exported by 20 exports  view-to-exporter p50    6.8ms  p99   14.8ms  p99.9   15.8ms  PASS (<100ms)  behind 0/20 healthy exports  inside 1-min promise 100.00%  lost 0  repeats dropped 0  updates 39  writes retried 8
[exp-1] destination down: env-0001-kinesis exported 0/s  backlog 4,171 on its own topic, offset held at the last exported view  down for 14.0 s
[exp-1] backfilling at destination limit: env-0001-kinesis exported 2,000/s (limit 4,000/s)  backlog 2,468 on its own topic  oldest waiting  15.0 s
[exp-1] env-0001-kinesis caught up: backlog cleared 18.0 s after its trouble began, oldest wait 15.0 s, 0 views past the one-minute promise, 0 views lost
```

The business numbers: `behind`, how many healthy exports had any view over budget that second;
`inside 1-min promise`, the share of views handed to the export worker within a minute of being
written (the lane's part of Mux's one-minute promise); `lost`, views missing from any export (a gap in
an export partition's sequence; it must stay 0); `repeats dropped`, duplicates removed (the lane is
read at least once); `updates`, newer versions of views already exported, which customers keep by
`view_id`; `writes retried`, spurious destination failures retried in place; and for an export whose
destination is down or backfilling, its backlog on its own topic, its oldest wait and how long it took
to catch up.

**What the numbers mean.** The kit times each view from the moment `views.py` writes it to the moment its export worker has read it, through Python clients, on whatever machine you run it on. On a shared 2-vCPU GitHub runner, with the broker and four Python processes on one box, at about 1,500 finished views a second across 40 export topics with 4 MB/s of raw beacons next door, three CI runs on the final code held p50 5.1 to 11.6 ms, p99 12.9 to 21.1 ms and p99.9 13.7 to 23.2 ms (medians per second) over 69 to 70 steady-state seconds per shard, with no second over the 100 ms budget (worst second 15.6 to 51.8 ms) and no view lost. While env-0001-kinesis's stream was down for 15 s and the export then backfilled at its 4,000 records/s limit, the other exports on its shard kept a median per-second p99 of 9.8 to 19.5 ms (13.6 to 21.3 ms otherwise); its backlog peaked at 4,171 views on its own topic, its oldest view waited 15.0 s, and it caught up 16.0 to 18.0 s after its stream went down, with 0 views past the one-minute promise and 0 lost. Each run retried 782 to 857 spurious write failures in place and exported about 3,100 updated views. That is the end-to-end lane through this demo code, not the broker alone; it does not include view finalization in the processors or the write to the customer's stream. The broker's own tail latency, how many export topics a cluster carries comfortably, and cost per billion views are measured with `iggy-bench` on dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `views.py` | Finished views from long-tail environments, one topic per streaming export (`env-0001-kinesis` …), keyed by view ID, with VideoView-style fields, a version (3% are updates of exported views) and a sequence per export partition. |
| `exporter.py` | Export workers, sharded by export. Read every export partition directly from their own offsets, with a write limit per destination (Kinesis per shard, Pub/Sub), retry spurious failures in place; `--outage*` takes one customer's stream away. Drop repeats, count sequence gaps as lost, count views past the minute, read backlogs from the log. |
| `beacon_flood.py` | Raw player beacons on one topic per SDK family, for the noisy-neighbour test. |
| `kafka_bridge.py` | Copies finished views from your Kafka topic into per-export topics, read-only, as consumer group `iggy-shadow`, with an `exports.json` of which environments export where. |
| `common.py` | Names, destinations, environment sizes, partitions, the one-minute promise and the Kinesis shard limit, shared by every script. |
| `bench/PLAN.md` | The benchmark we run with you, export-topic count and cost per billion views included; rules fixed before results. |

## Three tests

1. **A customer's stream down for an hour:** `--outage-for 3600` on shard 1 (env-0001-kinesis, the
   largest export, unreachable for an hour, about 1.3 million views at the defaults). Pass: every
   other export keeps p99 under 100 ms and `behind` stays at 0; env-0001-kinesis backfills at its
   four-shard limit afterwards with `0 views lost`. Its views are late by the length of the outage,
   which its own stream caused; nobody else's are.
2. **A live event ends:** `--views-per-s 25000` on `views.py` (ten times the default, as viewers leave
   at once). Pass: exports under their limits keep p99 under budget; exports at their Kinesis limit
   drain their own backlog and stay inside the minute once the burst passes; `lost` stays 0. Add
   `--readers 4` to the shards if one Python reader is the bottleneck.
3. **Restart an export shard:** stop one and start it again. The kit keeps offsets in memory, so it
   reads its exports from the first retained offset again: those views are counted as replayed, apart
   from live latency, and would reach the customer a second time with the same `view_id`, as Mux's
   docs already allow. In production a worker stores its offset with the broker after each write the
   customer's stream acknowledges, and resumes from the last exported view. Pass: back under budget
   within seconds of reaching the live edge, `lost 0`.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --kafka-topics <finished views topic> --config exports.json
python exporter.py --envs 0                      # read every export topic the bridge created
```

The bridge joins the Kafka cluster that holds finished views as its own consumer group,
`iggy-shadow`, from the latest offset, and never writes to Kafka: the collectors, the processors, the
Kafka Connect sinks and every existing consumer are untouched. Map your record shape in `to_view()`
(decode `VideoView` protobuf there if that is what the topic carries): the kit needs the environment,
the view ID and a version. Which topic the sinks read today is not public; point the bridge at it.

## What Apache Iggy 0.9 has today, and what it doesn't

Today: replicated clusters (Viewstamped Replication, new in September 2026), direct partition reads
with your own offsets, consumer offsets stored on the broker, message expiry and size caps per topic
(a hold window per export), at-least-once delivery (the same contract Mux gives customers: dedupe on
`view_id`), official SDKs for Go, Rust, Java, Python, C# and Node.js, so the workers can stay in Go.
Not yet: a Kafka API gateway (in progress), so the lane is fed with an Iggy SDK and the trial uses
`kafka_bridge.py`; transactions or exactly-once, so this lane is for exports, not for billing; and
published numbers for tens of thousands of topics per node, which the evaluation measures.

LaserData Cloud runs as a managed service on AWS or GCP, or inside your own AWS or GCP account
(BYOC). On Azure, run Apache Iggy self-hosted on your own VMs; LaserData supports it.

## Next step

If the numbers are interesting: a 2–4 week evaluation on mirrored finished views for a set of
exports, beside Kafka Connect, priced per billion views. No advantage, no migration.

kranti@laserdata.com · laserdata.com
