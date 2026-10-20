# Burst-proof ingest buffering for SigNoz, on LaserData

Every OTLP batch your collectors accept lands in one durable log in milliseconds, and ClickHouse
writers drain it at their own pace. Bursts and ClickHouse stalls delay data by seconds and lose none.
It runs between your collectors and ClickHouse, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs, and a real ClickHouse.

- **One slide:** [`LaserData_SigNoz_IngestBuffer.pdf`](LaserData_SigNoz_IngestBuffer.pdf)

## Why this use case

SigNoz's 2026 job posts for its cloud and platform teams name the goal: "Scaling the ingest path —
making it robust to bursts while maintaining data freshness" on a "petabyte-scale observability
system". The collector's GitHub history names the constraints:

- Agents reconnecting after a 30-minute collector outage maxed a 4-core server and slowed queries
  and alerts ([signoz#5956](https://github.com/SigNoz/signoz/issues/5956)). The answer there: the
  Kafka exporter and receiver "exist to aid with such a system", but a queue "won't be the default
  mode as it would unnecessarily complicate setup".
- "If your main concern is an outage of the final backend destination please set up a proper queue
  that guarantees the delivery of data without loss"
  ([#182](https://github.com/SigNoz/signoz-otel-collector/issues/182)), where Ankit Nayan called it
  something that "should be part of recommended OSS settings too".
- "We should only mark messages as consumed after receiving confirmation from ClickHouse that the
  data has been successfully written" ([#351](https://github.com/SigNoz/signoz-otel-collector/issues/351)).

So the buffer has to be durable, commit after ClickHouse confirms, absorb bursts without pushing back
on agents, and be light enough to switch on by default. Apache Iggy is one binary with no ZooKeeper
or KRaft quorum. This kit runs that buffer and prints its freshness every second.

## Run it

Needs Docker and Python 3.10+. Defaults: 200 tenants, 1,000 export batches/s × 100 log records,
one insert every 250 ms, and a 1 s receive-to-queryable budget.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090, ClickHouse on :8123
pip install -r requirements.txt      # apache-iggy 0.9.0, clickhouse-connect
python writer.py &                   # drains otlp_logs into ClickHouse, commits after insert
python collectors.py                 # 200 tenants, 100,000 records/s into the log
```

Both print a line a second:

```
[writer]  100,000 rows/s  receive-to-queryable p50 …ms  p99 …ms  p99.9 …ms  PASS (<1000ms)
          queue p99 …ms  inserts 4/s  waiting in writer … rows  oldest unwritten …s
[collectors] 1,000 batches/s  100,000 records/s  accept p50 …ms  p99 …ms  accepted … records
```

Run the collectors with `--duration 120` and then `python verify.py`: it waits for the writer to
drain and counts every accepted record in ClickHouse. Start from `docker compose down && docker
compose up -d` between runs, since the per-tenant sequence restarts at 1.

**What the numbers mean.** Receive-to-queryable runs from the moment `collectors.py` builds a
batch to the moment ClickHouse confirms the insert that contains it, through Python clients. In CI
(a shared 2-vCPU GitHub runner, with Apache Iggy, ClickHouse and every script on one box, 10,000
records/s), with the first writer killed by SIGKILL at 30 s and ClickHouse frozen for 12 s: 1,100,000
records accepted, 1,100,000 in ClickHouse, none lost; collector accept p99 median 2.8 ms over all
106 s, freeze included, never 50 ms or more; writer receive-to-queryable about p50 169 ms and p99
312 ms (58 of 58 steady-state seconds inside 1 s), most of it the 250 ms insert interval by design;
the 132,500 rows that waited during the freeze drained in one second. That is the end-to-end path
through this demo code, not the broker alone. The broker's own tail latency is measured with
`iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `collectors.py` | The receive side of a gateway collector fleet. Each export batch goes to the `otlp_logs` topic, keyed by tenant, with a per-tenant sequence; the agent is released when Apache Iggy acknowledges. |
| `writer.py` | A ClickHouse writer: one consumer group, one insert every `--flush-ms`, offsets stored only after ClickHouse confirms. Stops reading above `--max-pending` rows when ClickHouse stalls. |
| `verify.py` | Counts every accepted record in ClickHouse (distinct tenant, batch, index); replays after a crash fold away in a ReplacingMergeTree. |
| `reconnect_flood.py` | Agents that could not reach the collectors for 30 minutes flush everything at once. |
| `kafka_bridge.py` | Copies an existing `otlp_logs` Kafka topic (OTLP proto) into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests

1. **Agents reconnect after an outage:** `python reconnect_flood.py --agents 20 --minutes 30` while
   both run. Pass: collector accept p99 does not move; the writer drains at ClickHouse's pace and
   live freshness returns under 1 s on its own.
2. **ClickHouse stops confirming inserts:** `docker compose pause clickhouse; sleep 30; docker compose
   unpause clickhouse`. Pass: agents are still acknowledged in milliseconds; the writer pauses reading
   and drains afterwards without a restart.
3. **A writer crashes:** `kill -9` the writer and start another. Pass: it resumes from the last
   confirmed batch and `verify.py` finds every accepted record.
4. **Burst at the collectors:** `python collectors.py --burst 5 --burst-at 60 --burst-for 60`.
   Pass: accept p99 stays flat; freshness is set by how fast the writers insert.

Tests 2 and 3 run in CI on every push (`ci.sh`).

## Shadow your own traffic

```
pip install confluent-kafka opentelemetry-proto
python kafka_bridge.py --bootstrap <brokers> --kafka-topic otlp_logs \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads, as its own consumer group `iggy-shadow`; the exporter, your SigNoz collectors
and every other consumer are untouched. It decodes each OTLP `LogsData` payload into the kit's batch
shape; adapt the tenant and service mapping in `to_batches()`.

## What Apache Iggy 0.9 doesn't have yet

- No OpenTelemetry Collector exporter or receiver for Iggy. It would be a Go pair modelled on
  `signozkafkaexporter` / `signozkafkareceiver`, on Iggy's official Go SDK; the kit uses Python
  stand-ins.
- No Kafka API in 0.9 (a gateway is in progress), so the existing Kafka receiver cannot point at Iggy.
- At-least-once delivery; the writer's commit-after-insert and the ReplacingMergeTree key make replays
  harmless.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored `otlp_logs` topic, beside your
current ingest path. No advantage, no migration.

kranti@laserdata.com · laserdata.com
