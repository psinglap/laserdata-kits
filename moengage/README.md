# Real-time Smart Trigger events for MoEngage, one topic per brand, on LaserData

Every brand's trigger events reach the campaign evaluator in milliseconds, even while another brand
runs a flash sale. Each workspace gets its own Apache Iggy topic and its own rate, beside your Kafka
clusters, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/Q7WHXiENSMgBdKye4XdZjx
- **One slide:** [`LaserData_MoEngage_SmartTriggers.pdf`](LaserData_MoEngage_SmartTriggers.pdf)

## Why this use case

MoEngage's engineering blog describes the problem in its own words. The real-time ingestion post
(Vishal Surana, Technical Architect, Dec 2024) lists, under future work: "The use of Kafka makes it
difficult to implement rate-limiting policies as we have thousands of customers whose data is
distributed among the partitions of a topic and Kafka itself can't support one topic per client at
our scale." The Kafka restructuring post (Oct 2023) puts "time-critical actions/notifications" and
"a transactional OTP" on the Kafka-automation cluster, "a few million events per minute" with a
"single-digit seconds" end-to-end SLA, and describes 600-partition topics, 100K+ partitions on 8
brokers and ~90% broker CPU. The reliability post (Apr 2025) uses one brand's flash sale as the
example of what must not hurt the other brands. And MoEngage's status page shows Smart Trigger
campaigns and Flows delayed whenever ingestion lags, "as it consumes the data from the ingestion
pipeline and apply trigger/flows conditions" (24 Mar 2026; also 9 Apr and 14 May 2026).

So this kit gives the events that live Smart Trigger campaigns and Flows wait on their own lane,
with one topic per workspace. The evaluator reads each workspace at its own quota: a brand that
bursts past it queues on its own topic, and every other brand is evaluated as if nothing happened.
Kafka, Samza and Flink keep ingestion, the unified topic, CDC and everything else.

## Run it

Needs Docker and Python 3.10+. Defaults: 200 workspaces with long-tail sizes (the largest 10 with 4
partitions), 10,000 trigger events a second, 5 million active users, a 5,000 events/s quota per
workspace, and 50 MB/s of enriched ingestion next door.

```
docker compose up -d                                  # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                       # apache-iggy 0.9.0
python trigger_eval.py --shard 0 --shards 2 &         # the Smart Trigger evaluator, two shards
python trigger_eval.py --shard 1 --shards 2 &
python workspaces.py --flash-for 120 &                # brands' trigger events; ws-0002's flash sale 60 s in
python ingest_flood.py                                # the ingestion firehose next door
```

Each evaluator prints a line a second, plus one for any workspace held at its quota:

```
[eval-1]  5,000 trigger events/s from 100 workspaces  event-to-evaluator p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  over budget 0/99 workspaces  fired …/s  within 1 s 100.00%
[eval-1] held at quota: ws-0002 read 5,000/s (quota 5,000/s)  backlog … on its own topic  oldest waiting … s
[eval-1] ws-0002 back inside its quota: backlog cleared … s after it was first held
```

The business numbers: `within 1 s`, the share of campaign triggers ("Added To Cart" → push, "OTP
Requested" → SMS, "Purchase Completed" → WhatsApp, all sent Immediately) fired within a second of
the event; `over budget`, how many brands inside their quota had any event over budget that second;
and for a brand over its quota, its backlog, its oldest wait and how long it took to clear.

**What the numbers mean.** The kit times each trigger event from the moment `workspaces.py` creates
it to the moment the evaluator has it, through Python clients, on whatever machine you run it on. On a
shared 2-vCPU GitHub runner, with the broker and four Python processes on one box, at 3,000 trigger
events a second across 40 workspace topics with 4 MB/s of ingestion next door, four CI runs held
p50 4.8 to 13.2 ms, p99 13.8 to 24.6 ms and p99.9 14.9 to 27.8 ms (medians per second) over 66 steady-state
seconds per shard, with no second over the 100 ms budget (worst second 33 ms), and every campaign
trigger fired within 1 s. While ws-0002 ran a flash sale of 4,000 extra events a second for 10 s
against a 1,500/s quota, the other workspaces on its shard kept a median per-second p99 of 13.9 to 25.9 ms
(13.7 to 23.7 ms without the sale); ws-0002's backlog peaked at 27,197 to 27,472 events on its own topic,
its oldest event waited 18.5 to 18.9 s, and the backlog cleared 35.8 to 36.0 s after it was first held.
That is the end-to-end lane through this demo code, not the broker alone. The broker's own tail latency,
the number of workspace topics a cluster carries comfortably, and cost per billion events are measured
with `iggy-bench` on dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `workspaces.py` | Brands' trigger events, one topic per workspace (`ws-0001` …), keyed by user with a per-user sequence. Long-tail workspace sizes; `--flash-*` runs one brand's flash sale. |
| `trigger_eval.py` | The Smart Trigger evaluator, sharded by workspace. Reads every workspace partition directly, with a per-workspace token bucket; drops repeats; fires matching campaigns to `trigger-fires`. |
| `ingest_flood.py` | The ingestion firehose: enriched events on the `unified-events` topic, for the noisy-neighbour test. |
| `kafka_bridge.py` | Copies trigger events from an existing Kafka topic into per-workspace topics, read-only, with kafka-python. |
| `common.py` | Names, campaigns, workspace sizes and partitions, shared by every script. |
| `bench/PLAN.md` | The benchmark we run with you, workspace-topic count and cost per billion events included; rules fixed before results. |

## Three tests

1. **A flash sale:** `python workspaces.py --flash-rate 50000 --flash-for 600` (ws-0002 at 10× its quota).
   Pass: every other workspace keeps p99 under 100 ms, `over budget` stays at 0, and ws-0002's backlog
   clears at its quota once the sale ends.
2. **More brands:** `--workspaces 1000` on `workspaces.py` and both evaluators.
   Pass: p99 holds; the broker's CPU and open files grow with traffic, not with topic count alone.
3. **Restart an evaluator shard:** stop one and start it again. It reads its workspaces from the
   first retained offset and skips every event written before it started, so nothing is fired twice;
   those events are counted as replayed, apart from live latency.
   Pass: back under budget within seconds of reaching the live edge.

## Shadow your own traffic

```
pip install kafka-python
python kafka_bridge.py --bootstrap <broker:9092> --kafka-topic <the topic your trigger evaluation reads>
python trigger_eval.py --workspaces 0               # read every ws-* topic the bridge created
```

The bridge joins as its own consumer group, `iggy-shadow`, and only reads: producers, the Samza and
Flink jobs and every existing consumer are untouched. It keeps only the event names live campaigns
trigger on (`--events`). Map your record shape in `to_event()`: the kit needs the workspace, the user
ID and the event name.

## Next step

If the numbers are interesting: a 2–4 week evaluation on mirrored trigger events for a set of
workspaces, alongside Kafka, priced per billion events. No advantage, no migration.

kranti@laserdata.com · laserdata.com
