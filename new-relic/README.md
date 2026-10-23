# Real-time pipeline watermarks for New Relic alerts, on LaserData

Every time-wave generation emitted into a cell, and every tally of its synthetic points reaching the end of the
pipeline, reach a watermark service in milliseconds, keyed by cell and source pipeline, so an alert window closes
only on complete data and a stalled cell is flagged while it is still stalled. It runs beside Kafka, on a log
outside the cells it measures, so ingest, every cell's Kafka, Flink, NRDB and the alerting pipeline stay where
they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_NewRelic_TimeWaveWatermarks.pdf`](LaserData_NewRelic_TimeWaveWatermarks.pdf)

## Why this use case

From New Relic's own engineering posts, docs and talks:

- Data is processed asynchronously before it is stored and evaluated by the alerting pipeline; delayed data
  "could be missing from alerting query results", which "can cause false positive or negative alerts",
  including loss-of-signal. Time waves are "known sets of synthetic data points" written regularly, and "the
  arrival of all data points for a generation forms the watermark", scoped by source pipeline and by cell.
  Lag measured from the traffic itself has a blind spot: "Blockages in the pipeline are not observed until the
  blockage has cleared" (Amy Boyle, "Understanding lag in a streaming pipeline", December 2023).
- Each cell has its own Kafka cluster, ingestion service, pipeline services and NRDB cluster. When a Kafka
  broker in one cell became unresponsive, the recovery ended with "data collection and alerting [...] unusable
  for many customers in our US region for multiple hours" (Nic Benders, August 2021).
- More than 100 Kafka clusters by 2022 (Anton Rodriguez, Confluent Streaming Audio, April 2022); ingest at
  hundreds of GB/s across regions and clouds with Flink, Kafka and S3 with Paimon (Current 2024).
- Event flow, the default alert aggregation, "aggregates a window of data when the first data point arrives in
  a subsequent window"; default window 1 minute, delay 2 minutes (streaming alerts docs).

Kafka stays the right home for the telemetry itself. This kit puts only the time waves' bookkeeping on one
small lane, outside the cells, and turns it into a watermark per cell pipeline. It prints the lane's p99 every
second and checks every watermark and behind flag against the truth.

## Run it

Needs Docker and Python 3.10+. Defaults: 40 cells, 4 source pipelines (metrics, events, logs, spans) with 64
Kafka partitions each, a generation every second (one synthetic point per partition, 10,240 points a second,
tallied every 20 ms), 2% of tallies written twice, and a quarter of one cell's Kafka partitions stalling for
two minutes, 90 s in. The kit uses one-second windows and a two-second event-flow delay, so a run shows hundreds
of windows; yours default to one and two minutes.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python watermark.py &                # the watermark service evaluators ask
python waves.py                      # 40 cells, every watermark checked
```

They print a line a second:

```
[watermark] 2,600 msgs/s  wave-to-watermark p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  cell pipelines 160
            behind now …  generations in flight …  watermarks sent …  repeats ignored …
[waves]     cell pipelines 160 (stalled 4)  | event flow alone: windows closed …, on partial data …
            | gated by watermark: closed …, early 0, after data complete p50 …ms
            | behind flags right … (detected max …s after the stall)  wrong 0  missed 0
```

`waves.py` knows when every point arrived, so it checks every message the watermark service sends: did a
watermark ever claim a generation complete while points were still in flight (early), was every stalled cell
pipeline flagged behind and how soon, and was anything flagged that was not stalled? It also counts the
windows event flow alone would have closed on partial data. Those are the business numbers.

**What the numbers mean.** The kit times each generation record and tally from the moment `waves.py` creates
it to the moment `watermark.py` has applied it, through Python clients, on whatever machine you run it on. On a
shared 2-vCPU CI runner with the broker and all scripts on one box, at about 590 messages/s (12 cells × 4
pipelines × 32 partitions), two green runs held p50 3.5 to 4.2 ms, p99 6.3 to 7.6 ms and p99.9 6.8 to 8.2 ms
(median of 63 steady-state seconds each), with no second over the 50 ms budget. 3 MB/s of backlog drain on the
topic next door moved p99 by 2 to 3%. With a quarter of the partitions in 2 cells stalled for 20 s, event flow
alone closed 150 to 152 of about 3,360 windows on partial data (about 1,150 points still in Kafka); gated by the
watermark, about 3,500 windows closed and none early, a median of 12 to 13 ms (at most 24 ms) after their data
was complete, including the check's own read. All 8 stalled cell pipelines were flagged behind 3.2 to 3.5 s
after the stall began (overdue limit 3 s), none wrongly; about 790 tallies written twice were all ignored. That
is the end-to-end lane through this demo code, not the broker alone. The broker's own tail latency is measured
with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `waves.py` | Time-wave emitters and cell pipelines: a record per generation and tallies of arrived points on `wave-lane`, keyed by cell/pipeline; a broker stall and repeated tallies on purpose. Checks every watermark and behind flag against the truth and counts what event flow alone would do. |
| `watermark.py` | The watermark service: complete-through generation per cell pipeline, behind flags naming the missing Kafka partitions, rebuilt from the lane on start; publishes to `cell-watermarks`. |
| `backlog_drain.py` | A neighbour on the same broker: a recovered cell's telemetry draining (16 KB records) on `backlog-drain`. |
| `kafka_bridge.py` | Writes generations and tallies from your Kafka topics as a read-only consumer group. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A broker goes unresponsive:** `python waves.py --stall-cells 1 --stall-share 0.25 --stall-at 90
   --stall-for 300`. Pass: every stalled cell pipeline is flagged within the overdue limit plus a second, with
   the right partitions; nothing else is flagged; no watermark is early.
2. **The backlog drains next door:** `python backlog_drain.py --mbps 100` while both run. Pass: the lane's
   p99 moves by less than 10% and no window closes early. The script prints the rate it actually reached; one
   Python process may not reach 100 MB/s on a laptop.
3. **Restart the watermark service:** stop `watermark.py` and start it again; it replays the retained lane
   (15 minutes) and rebuilds every watermark. Pass: it catches up within seconds and no later watermark is
   early.

## Shadow your own waves

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --arrival-topic <time-wave-arrivals> \
  --emit-topic <time-wave-generations> -X security.protocol=SSL
```

The bridge only reads, as its own consumer group `iggy-shadow`; the pipeline services and the alerting
pipeline are untouched. Map your records in `to_wave()`. If your generator does not record emitted generations
on a topic, a few lines in the generator can write `{k, g, x}` to `wave-lane` directly. Run the bridge next to
the watermark service, outside the cells.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one cell's time waves, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
