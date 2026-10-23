# Real-time Data Connector streams for Samsara, on LaserData

Each stream subscription gets its own Apache Iggy topic, so a customer's broker outage delays only
that stream, which then backfills from its last streamed datapoint with nothing lost. It runs
beside Kinesis, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_Samsara_DataStreams.pdf`](LaserData_Samsara_DataStreams.pdf)

## Why this use case

Samsara's developer docs describe the shape. Data Connectors "push fleet telemetry, events, and
entity changes directly to your message broker or streaming platform": the customer's own Kafka,
AWS MSK, Azure Event Hubs or Google Cloud Pub/Sub. The Kafka Connector Reference says "Each stream
subscription enabled will send messages for one (1) entity to the designated Kafka topic", that
"Data is streamed as it is updated on the Samsara cloud", that "GPS data updates every 5 seconds
when the vehicle is on", and that after an outage the subscription "will pickup from the last
streamed datapoint, until it's caught back up to current data", with "Data recovery: up to 6 hours".
Ridership events use "the route ID as the Kafka partition key" so each route's events arrive in order.

So the connector depends on many brokers Samsara does not run, each behind its own public endpoint,
credentials and limits, and Samsara already promises what happens when one goes away. The
Infrastructure team's 2026 job post states the goal in general terms: "Improve the scalability,
capacity management, isolation, recovery, and cost efficiency" and "build safeguards that prevent
localized failures from escalating".

This kit gives each stream subscription its own lane. The datapoint writer appends every updated
datapoint (GPS, engine states, fuel levels, temperatures, and events such as `geofenceEntry`,
`routeStopArrival` and `severeSpeedingStarted`) to the topic of its org's subscription for that
entity, keyed by vehicle ID, with a unique id and a per-vehicle sequence. Connector workers read
their subscriptions' partitions directly, each at its destination's write limit, from their own
offsets (no consumer group, so no rebalances). When a customer's broker is unreachable, that
subscription's offset stays at the last streamed datapoint and its datapoints wait on its own topic,
whose retention is the 6-hour recovery window; when the broker is back, it backfills from that
offset at its limit until it has caught up. Every subscription's backlog is read from the log itself
every second. Kinesis, the ingestion services, the time-series store and the devices stay as they are.

## Run it

Needs Docker and Python 3.10+. Defaults: 200 orgs with long-tail fleet sizes and 150,000 vehicles,
40% of them on (GPS every 5 s; every 5 minutes when off), 457 stream subscriptions (every org
streams GPS, every second org events, every third engine states, every fourth fuel levels, every
fifth temperatures; the largest fleets on 4 partitions), about 14,000 datapoints a second, a 5,000
datapoints/s write limit per destination, and 50 MB/s of raw device uploads next door.

```
docker compose up -d                                       # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                            # apache-iggy 0.9.0
python connector.py --shard 0 --shards 2 --outage-for 120 &   # connector workers, two shards;
python connector.py --shard 1 --shards 2 --outage-for 120 &   # org-0002-gps's broker is down from 60 s for 2 min
python datapoints.py &                                     # updated datapoints to every stream subscription
python ingest_flood.py                                     # raw device uploads next door
```

Each connector shard prints a line a second, plus one for any subscription in trouble:

```
[conn-1]  1,871 datapoints/s streamed by 42 subscriptions  datapoint-to-connector p50    9.7ms  p99   25.5ms  p99.9   28.1ms  PASS (<100ms)  behind 0/44 healthy subscriptions  lost 0  repeats dropped 0
[conn-1] broker down: org-0002-gps streamed 0/s  backlog 3,718 on its own topic, offset held at the last streamed datapoint  down for 14.0 s of the 6 h recovery window
[conn-1] backfilling at destination limit: org-0002-gps streamed 600/s (limit 1,200/s)  backlog 3,382 on its own topic  oldest waiting  15.0 s
[conn-1] org-0002-gps caught up: backlog cleared 21.1 s after its trouble began, oldest wait 15.0 s inside the recovery window, 0 datapoints lost
```

The business numbers: `behind`, how many healthy subscriptions had any datapoint over budget that
second; `lost`, datapoints missing from any subscription (a gap in a vehicle's sequence; it must stay
0); `repeats dropped`, duplicates removed (the lane is read at least once); and for a subscription
whose broker is down or backfilling, its backlog on its own topic, how long it has been down
against the 6-hour window, its oldest wait and how long it took to catch up.

**What the numbers mean.** The kit times each datapoint from the moment `datapoints.py` writes it to the moment its connector worker has read it, through Python clients, on whatever machine you run it on. On a shared 2-vCPU GitHub runner, with the broker and four Python processes on one box, at about 2,800 datapoints a second from 40,000 vehicles in 40 orgs across 92 subscription topics with 4 MB/s of raw uploads next door, two CI runs on the final code held p50 11 to 18 ms, p99 26 to 36 ms and p99.9 28 to 39 ms (medians per second) over 68 to 69 steady-state seconds per shard, with no second over the 100 ms budget (worst second 30 to 46 ms) and no datapoint lost. While org-0002-gps's broker was down for 15 s and the subscription then backfilled at a 1,200 datapoints/s limit, the other subscriptions on its shard kept a median per-second p99 of 25 to 35 ms (27 to 36 ms otherwise); its backlog peaked at 3,715 to 3,718 datapoints on its own topic, its oldest datapoint waited 15.0 s, and it caught up 19.0 to 21.1 s after its broker went down, inside the recovery window, with 0 datapoints lost. That is the end-to-end lane through this demo code, not the broker alone, and it does not include the write to the customer's broker. The broker's own tail latency, how many subscription topics a cluster carries comfortably, and cost per billion datapoints are measured with `iggy-bench` on dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `datapoints.py` | Updated datapoints (GPS every 5 s while on, engine states, fuel levels, temperatures, events) from long-tail fleets, one topic per stream subscription (`org-0002-gps` …), keyed by vehicle ID, with a unique id and a per-vehicle sequence. |
| `connector.py` | Data Connector workers, sharded by subscription. Read every subscription partition directly from their own offsets, with a write limit per destination; `--outage*` takes one customer's broker down. Drop repeats, count sequence gaps as lost, read backlogs from the log, check the recovery window. |
| `ingest_flood.py` | Raw device uploads on one topic per device family, for the noisy-neighbour test. |
| `kinesis_bridge.py` | Copies datapoints from a Kinesis data stream into per-subscription topics, read-only, with boto3 and a `subscriptions.json` of which entities each org streams. |
| `common.py` | Names, entities, fleet sizes, partitions and the 6-hour retention, shared by every script. |
| `bench/PLAN.md` | The benchmark we run with you, subscription-topic count and cost per billion datapoints included; rules fixed before results. |

## Three tests

1. **A long broker outage:** `--outage-for 3600` on both shards (org-0002-gps's broker down for an
   hour, about 3 million datapoints at the defaults). Pass: every other subscription keeps p99 under
   100 ms and `behind` stays at 0; org-0002-gps backfills at its limit afterwards, inside the
   recovery window, with `0 datapoints lost`.
2. **More subscriptions:** `--orgs 2000` on `datapoints.py` and both shards (about 4,570 subscription
   topics). Pass: p99 holds; the broker's CPU and open files grow with traffic, not with topic count alone.
3. **Restart a connector shard:** stop one and start it again. The kit keeps offsets in memory, so it
   reads its subscriptions from the first retained offset again: those datapoints are counted as
   replayed, apart from live latency, and would reach the customer a second time with the same id,
   as an at-least-once stream allows. In production a connector stores its offset with the broker
   after each write the customer's broker acknowledges, and resumes from the last streamed datapoint.
   Pass: back under budget within seconds of reaching the live edge, `lost 0`.

## Shadow your own traffic

```
pip install boto3
python kinesis_bridge.py --stream <your Kinesis stream> --config subscriptions.json
python connector.py --orgs 0                     # read every subscription topic the bridge created
```

The bridge reads each shard with GetRecords from LATEST, once a second per shard (one of the
shard's five reads a second), and never writes to Kinesis: the devices, the ingestion services, the
Data Connectors and every existing consumer are untouched. Map your record shape in
`to_datapoint()`: the kit needs the org, the entity, the vehicle ID and the record id. Which stream
the Data Connectors read today is not public; if it is Kafka, swap the Kinesis client for a
read-only consumer group and keep the rest.

## What Apache Iggy 0.9 has today, and what it doesn't

Today: replicated clusters (Viewstamped Replication, new in September 2026), direct partition reads
with your own offsets, consumer offsets stored on the broker, message expiry and size caps per
topic (the 6-hour window per subscription), at-least-once delivery (dedupe by id and per-vehicle
sequence), official SDKs for Go, Rust, Java, Python, C# and Node.js. Not yet: a Kafka API
gateway (in progress), so the connector still writes to customers' Kafka with a Kafka client and the
trial uses `kinesis_bridge.py`; and published numbers for tens of thousands of topics per node,
which the evaluation measures.

## Next step

If the numbers are interesting: a 2–4 week evaluation on mirrored Data Connector traffic for a set
of subscriptions, beside Kinesis, priced per billion datapoints. No advantage, no migration.

kranti@laserdata.com · laserdata.com
