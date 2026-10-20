# Real-time Currents connectors for Braze, on LaserData

Every Currents connector gets its own Apache Iggy topic, so a partner outage delays only that
connector, which resumes from its own offset with nothing lost. It runs beside Kafka, so nothing
else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_Braze_CurrentsConnectors.pdf`](LaserData_Braze_CurrentsConnectors.pdf)

## Why this use case

Braze's own sources describe the shape. The Currents team's job post calls Currents "a robust
Kafka-based event pipeline handling tens of billions of messages daily that our customers leverage
to analyze user behavior in near real-time". Braze's 2018 talk on real-time data export (Preston
Thompson, Senior Software Engineer) shows Ruby apps producing to "30 different events types, one
topic each", keyed by user ID, and a routing app writing "one topic per integration" from a
configuration of which events each integration exports. Its list of problems is per partner:
"Partner downtime", "Rate limiting", "Invalid credentials", "Retries are difficult", plus
"Rebalance loops", and "we must limit number of partitions per connector".

And Braze's status page for 31 July 2026 ("EU02 Currents Data Latency") describes the failure in
its own words: "multiple Currents connectors failing to gracefully restart after routine
maintenance, but appearing in a healthy state in our monitoring system. Those specific servers
build significant backlogs while in this state." Up to ~5% of connectors were delayed for about
five hours; "No data was lost".

So this kit gives each Currents connector its own lane. The router writes every engagement event
to the topic of each connector configured for that event type, keyed by user ID, with the event
`id` and a per-user sequence. Each connector reads its own partitions directly, keeps its own
offset (no consumer group, so no rebalances) and reads at its partner's rate limit. When a partner
goes down, its events wait on its own topic and the offset stays put; when the partner is back,
the connector drains from that offset. Every connector's backlog is read from the log itself every
second, not from the connector's own health report. Kafka keeps the event-type topics, the
routing inputs and everything else.

## Run it

Needs Docker and Python 3.10+. Defaults: 150 apps with long-tail sizes and 255 connectors (every
app exports to storage, every second app to an analytics partner, every fifth to a CDP; the largest
apps with 4 partitions per connector), 12,000 engagement events a second before fan-out, 20 million
active users, a 5,000 events/s partner rate limit per connector, and 50 MB/s of raw events on the
event-type topics next door.

```
docker compose up -d                                       # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                            # apache-iggy 0.9.0
python connector.py --shard 0 --shards 2 --outage-for 120 &   # connector writers, two shards;
python connector.py --shard 1 --shards 2 --outage-for 120 &   # app-0002-storage's partner is down from 60 s for 2 min
python router.py &                                         # engagement events to every configured connector
python firehose.py                                         # raw events on the event-type topics next door
```

Each connector shard prints a line a second, plus one for any connector in trouble:

```
[conn-0]  2,171 events/s exported by 34 connectors  event-to-connector p50   14.2ms  p99   31.0ms  p99.9   33.4ms  PASS (<100ms)  behind 0/34 healthy connectors  lost 0  repeats dropped 0
[conn-0] partner down: app-0002-storage read 0/s  backlog 4,475 on its own topic, offset held  down for 14.0 s
[conn-0] at partner rate limit: app-0002-storage read 783/s (limit 1,500/s)  backlog 4,012 on its own topic  oldest waiting  15.0 s
[conn-0] app-0002-storage caught up: backlog cleared 21.0 s after its trouble began, 0 events lost
```

The business numbers: `behind`, how many healthy connectors had any event over budget that second;
`lost`, events missing from any connector (a gap in a user's sequence; it must stay 0); `repeats
dropped`, duplicates removed (delivery is at least once, as Currents documents); and for a
connector whose partner is down or throttling, its backlog on its own topic, its oldest wait and how
long it took to catch up.

**What the numbers mean.** The kit times each event from the moment `router.py` routes it to the
moment its connector has read it, through Python clients, on whatever machine you run it on. In CI
on a shared 2-vCPU GitHub runner, with the broker and four Python processes on one box, at 3,000
engagement events a second (about 4,500 a second after routing) across 68 connector topics with
4 MB/s of raw events next door, both shards held p50 13 to 15 ms, p99 32 ms and p99.9 35 ms (medians
per second) over 68 steady-state seconds each, with no second over the 100 ms budget (worst second
39 ms) and no event lost. While app-0002-storage's partner was down for 15 s and the connector then
drained at a 1,500 events/s rate limit, the other connectors on its shard kept a median per-second
p99 of 31.6 ms (32.1 ms otherwise); app-0002-storage's backlog peaked at 4,475 events on its own
topic, its oldest event waited 15.0 s, and it caught up 21.0 s after its partner went down, with 0
events lost. That is the end-to-end lane through this demo code, not the broker alone. The broker's
own tail latency, how many connector topics a cluster carries comfortably, and cost per billion
events are measured with `iggy-bench` on dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `router.py` | Engagement events (Push Notification Send, Email Delivery, Purchase, …) routed to every connector configured for the event type, one topic per connector (`app-0002-storage` …), keyed by user ID, with the event `id` and a per-user sequence. |
| `connector.py` | Currents connector writers, sharded by connector. Read every connector partition directly from their own offsets, with a partner rate limit per connector; `--outage*` takes one partner down. Drop repeats, count sequence gaps as lost, read backlogs from the log. |
| `firehose.py` | Raw events on one topic per event type, for the noisy-neighbour test. |
| `kafka_bridge.py` | Copies events from your Kafka event-type topics into per-connector topics, read-only, with confluent-kafka and a `connectors.json` of which events each connector exports. |
| `common.py` | Names, event types, connector kinds, app sizes and partitions, shared by every script. |
| `bench/PLAN.md` | The benchmark we run with you, connector-topic count and cost per billion events included; rules fixed before results. |

## Three tests

1. **A long partner outage:** `--outage-for 1800` on both shards (app-0002-storage's partner down for
   30 minutes). Pass: every other connector keeps p99 under 100 ms and `behind` stays at 0;
   app-0002-storage catches up at its rate limit afterwards with `0 events lost`.
2. **More connectors:** `--apps 1000` on `router.py` and both shards (about 1,700 connector topics).
   Pass: p99 holds; the broker's CPU and open files grow with traffic, not with topic count alone.
3. **Restart a connector shard:** stop one and start it again. The kit keeps connector offsets in
   memory, so it reads its connectors from the first retained offset again: those events are counted
   as replayed, apart from live latency, and would reach the partner a second time with the same
   `id`, as Currents' at-least-once delivery allows. In production a connector stores its offset with
   the broker after each file is written and resumes from there.
   Pass: back under budget within seconds of reaching the live edge, `lost 0`.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <broker:9092> --kafka-topics <your event-type topics> --config connectors.json
python connector.py --apps 0                     # read every connector topic the bridge created
```

The bridge joins as its own consumer group, `iggy-shadow`, and only reads: the producers, the
routing app, the export pipeline and every existing consumer are untouched. Map your record shape
in `to_event()`: the kit needs the app, the user ID, the event type and the event id.

## What Apache Iggy 0.9 has today, and what it doesn't

Today: replicated clusters (Viewstamped Replication, new in September 2026), direct partition reads
with your own offsets, consumer offsets stored on the broker, message expiry and size caps per
topic, at-least-once delivery (dedupe by event `id`, as Currents documents today). Not yet: a Kafka
API gateway (in progress), so Kafka clients don't connect to Iggy directly and the trial uses
`kafka_bridge.py`; and published numbers for thousands of topics per node, which the evaluation
measures.

## Next step

If the numbers are interesting: a 2–4 week evaluation on mirrored Currents traffic for a set of
connectors, alongside Kafka, priced per billion events. No advantage, no migration.

kranti@laserdata.com · laserdata.com
