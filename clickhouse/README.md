# An Apache Iggy source for ClickHouse ClickPipes, on LaserData

Log and event streams on Apache Iggy land in ClickHouse exactly once, with the rules ClickPipes already documents
for its exactly-once mode, and are queryable in about half a second. A ClickHouse outage or a killed pipe costs no row
and duplicates none. Nothing in ClickHouse changes; Iggy is one more source next to the Kafka-protocol brokers.

This kit lets your team check that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs, and ClickHouse 25.8.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.clickhouse-kit
- **One slide:** [`LaserData_ClickHouse_IggySource.pdf`](LaserData_ClickHouse_IggySource.pdf)

## Why this use case

- **ClickPipes' streaming sources are all Kafka-protocol brokers:** Apache Kafka, Confluent Cloud, Redpanda, MSK,
  Event Hubs and WarpStream, with Google Managed Kafka in preview (Kafka ClickPipes reference, 2026).
- **Its exactly-once mode is documented precisely:** a high-water mark per partition, pending offset ranges, and
  replayed blocks that reuse deterministic deduplication tokens, so ClickHouse drops a block it has already
  written, within the table's dedup window (Kafka ClickPipes best practices, 2026).
- **Iggy users already write their own pipe into ClickHouse.** Production Apache Iggy users (see Apache Iggy's
  Powered By page) read Iggy with manual offset commits and an "already seen" check before each ClickHouse insert.
- **Iggy's own 0.9 ClickHouse sink isn't exactly-once.** Its docs say the runtime "doesn't replay failed sink
  batches, so end-to-end at-least-once delivery isn't guaranteed", and retries resend a batch "without an
  `insert_deduplication_token`".

This kit is the missing piece: an Iggy source that follows ClickPipes' exactly-once rules, as the reference a
ClickPipes connector or a joint Iggy plus ClickStack stack would follow. The data is OpenTelemetry logs in an
`otel_logs` table shaped like the one ClickStack reads, because that is where most streaming rows into ClickHouse
come from. Your own engineers published what happens to a log pipeline when ClickHouse pushes back (in-memory queues
running out of memory within minutes; a disk WAL taking about 4 hours to drain 1 TiB; "Ensuring reliable
OpenTelemetry ingestion at scale", Aug 2026). This kit runs the same kind of outage.

## How the pipe works

Every 0.5 s (`--flush-s`), `iggy_pipe.py`:

1. reads each partition of `otel-logs` directly from its high-water mark (up to 5,000 rows per partition);
2. records the block it is about to insert in its own Iggy topic, `pipe-state`: the offset range per partition, the
   high-water marks, and a token derived from the ranges;
3. inserts the whole block as one `INSERT ... FORMAT JSONEachRow` with that token as `insert_deduplication_token`;
4. only then moves the high-water marks.

If ClickHouse is down, it retries the same block with the same token until it lands. It holds nothing in memory, and
the events wait in Iggy. If the pipe dies, a restart reads the last `pipe-state` record and re-inserts the block that
was in flight with its original token, so ClickHouse drops it if it had landed. Each row carries `IggyPartition`
and `IggyOffset`, so any gap or duplicate traces back to an offset. One insert per half second keeps the insert
rate well inside ClickHouse's guidance; set `--flush-s 1` for one per second.

## Run it

Needs Docker and Python 3.10+. Defaults: 20,000 events/s from 5,000 sources (pods of a database fleet across six
regions: servers, keepers, proxies, operators; illustrative), 16 partitions, one insert every 0.5 s, and a 1 s budget
for event-to-ClickHouse freshness.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090, ClickHouse 25.8 on :8123
pip install -r requirements.txt      # apache-iggy 0.9.0 (the ClickHouse client is stdlib HTTP)
python iggy_pipe.py &                # Iggy -> otel_logs, exactly once
python collectors.py                 # OpenTelemetry gateway collectors -> Iggy
python verify.py                     # every event in ClickHouse exactly once?
```

They print a line a second:

```
[pipe]  … rows/s in 2 inserts  event-to-ClickHouse p50 …ms p99 …ms p99.9 …ms  PASS (<1000ms)  held in Iggy …
[pipe]  totals: inserted …  dropped by ClickHouse as repeats …  written twice 0  insert retries …
        replayed on restart …  outage … s
[pipe]  ClickHouse unavailable for 10 s: block of … rows retried with the same token; … events held in Iggy
[verify] events in Iggy …  rows in ClickHouse …  distinct …  missing 0  duplicates 0  sequence gaps 0
```

Freshness runs from the collector's timestamp to ClickHouse acknowledging the insert, when the rows are queryable.
Seconds while ClickHouse is down, and while the pipe drains what Iggy held, print as `catching up` and are not
counted as live latency.

**What the numbers mean.** CI runs the kit on a shared 2-vCPU GitHub runner with Iggy, ClickHouse and all scripts on
one box, at reduced scale: 2,000 events/s from 2,000 sources for 80 s with a 3× spike for 8 s, ClickHouse stopped for
12 s mid-run, the pipe killed with SIGKILL and restarted, and every 20th block sent twice as if its acknowledgement
had been lost. In three green runs, event-to-ClickHouse freshness was p50 261 to 264 ms, p99 503 to 507 ms and p99.9
511 to 513 ms (medians per second), with all 145 steady-state seconds inside the 1 s budget and no second's p99 above
521 ms. ClickHouse was unavailable for 14 to 15 s (including its restart); up to 29,120 events waited in Iggy, and the
pipe was live again 2 to 3 s after ClickHouse came back. Each restart found its block in flight (995 to 1,005 rows)
had already landed, and ClickHouse dropped the re-insert; 7,981 to 8,015 repeated rows were dropped per run and none
was written twice. `verify.py` found all 192,000 events exactly once in every run: missing 0, duplicates 0, sequence
gaps 0 across 2,000 sources. That is the end-to-end pipe through this demo's Python code, not the broker alone. Most of
the freshness is the 0.5 s insert interval, which is a ClickHouse insert-rate choice; the Iggy hop itself is a few ms.
The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `iggy_pipe.py` | The source: direct partition reads, block recorded in `pipe-state`, one INSERT per cycle with a deterministic dedup token, retry-until-landed, restore on start. Prints freshness, events held in Iggy and the exactly-once counters. |
| `collectors.py` | OpenTelemetry gateway collectors writing log events into Iggy, keyed by source, each source numbering its events. `--spike-x` for a deploy wave or an incident. |
| `verify.py` | Compares Iggy with ClickHouse: missing rows, duplicates, and per-source sequence gaps. |
| `kafka_shadow.py` | Mirrors a Kafka topic your ClickPipe already reads into Iggy, read-only, as consumer group `iggy-shadow`. |
| `common.py` | Topics, the `otel_logs` DDL, a small stdlib ClickHouse HTTP client. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **ClickHouse goes away:** `docker compose stop clickhouse`, wait a minute, `docker compose start clickhouse`.
   Pass: the pipe retries one block with one token, every event waits in Iggy, the backlog drains, and
   `verify.py` reports missing 0 and duplicates 0.
2. **The pipe dies mid-insert:** `kill -9` the pipe at any moment and start it again.
   Pass: it restores from `pipe-state`, re-inserts the block in flight with its original token, and `verify.py`
   reports duplicates 0. `--repeat-every 20` adds a lost acknowledgement every 20 blocks.
3. **A 10× spike:** `python collectors.py --spike-x 10 --spike-at 60 --spike-for 60`.
   Pass: freshness p99 stays under 1 s, or the pipe catches up within a minute after the spike.

## Shadow a topic your ClickPipe already reads

```
pip install confluent-kafka
python kafka_shadow.py --bootstrap <brokers> --kafka-topic <topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username=… -X sasl.password=…
python iggy_pipe.py --ch-url https://default:<password>@<service>.clickhouse.cloud:8443 --database iggy_shadow
```

The shadow only reads, as its own consumer group (`iggy-shadow`); the ClickPipe's consumer group, offsets and your
producers are untouched. Create the `iggy_shadow` database first, so the pipe never writes your tables. Freshness is
then measured from the Kafka record timestamp, next to what your ClickPipe shows for the same topic.

## What Apache Iggy 0.9 doesn't have yet

Iggy does not speak the Kafka protocol in 0.9. A Kafka-compatible gateway is in progress, and once it ships, an
Iggy topic could be read by today's Kafka ClickPipes unchanged. Until then, Iggy needs its own source (this kit, or
a native ClickPipes connector built on the same rules). Iggy's own ClickHouse sink connector exists but is not
exactly-once today; the rules in this kit are what it needs. Delivery from producers into Iggy is at-least-once; this
kit's exactly-once is from Iggy into ClickHouse, which is the hop ClickPipes owns. Replicated clusters (Viewstamped
Replication) shipped in September 2026. LaserData Cloud runs as a managed service on AWS or GCP, or inside your own
AWS or GCP account (BYOC). On Azure, run Apache Iggy self-hosted on your own VMs; LaserData supports it.

## Next step

If the numbers are interesting: a 2–4 week joint evaluation. We run an Iggy source into a ClickHouse Cloud service
on one mirrored topic, beside the ClickPipe that reads it today, and decide together whether it becomes a ClickPipes
source. No advantage, no migration.

kranti@laserdata.com · laserdata.com
