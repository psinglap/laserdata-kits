# Real-time witness ingest for Postman Insights, on LaserData

Every batch your Insights Agents upload reaches witness_assembler in milliseconds, and a batch that never
arrived is named, by agent and number. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Postman_InsightsWitnesses.pdf`](LaserData_Postman_InsightsWitnesses.pdf)

## Why this use case

From Postman's own public code. The Insights Agent flushes its upload batch every 5 seconds (or at 30 MB)
and POSTs it, with its `client_id`, to `/v2/agent/services/{service}/learn/{session}/async_reports`; on HTTP
429 it warns that "Postman Insights will generate partial results", and a failed upload is logged and
cleared (`trace/report_buffer.go`, postman-insights-agent). In September 2026 the shared library added
per-stage timestamps "so that end-to-end latency can be broken down by stage": `witness_uploaded`, then
`asw_witness_received` ("batch received by async_witnesses"), `asw_kafka_published` ("published to Kafka"),
`asm_kafka_received` ("witness_assembler consumed it") and `asm_ch_inserted` ("inserted into ClickHouse")
(`api_schema/witness.go`, observability-shared-libs, INS-240).

The same months of agent pull requests are about accounting: "The back end drops 13% of identity-service
witnesses in prod and 33% in lower as `missing_status_code`" across 278 pods (#200, Aug 2026); upload
outcome counts close "the last unmeasured link between `witness_paired` and backend ingestion", until "the
funnel now closes arithmetically" (#206, Sep 2026); without them, "the batch that never arrived [is]
invisible on both sides" (code comment). A 2025 job post puts Insights at "billions of requests a week on
our dogfooding data alone".

Kafka and ClickHouse are a sound home for Insights, and this kit leaves them alone. It runs one hop, from
async_witnesses to witness_assembler, on Apache Iggy: one message per upload batch, keyed by service, with
the agent's batch number on it, so the assembler counts a retried publish once and names any batch that
never arrived. The agent waits up to 5 seconds to fill a batch, so milliseconds on this hop are not what a
user of the Insights views feels; the case is the accounting, the tail under load, and a number you can
put next to your own `asw_kafka_published` → `asm_kafka_received` interval.

## Run it

Needs Docker and Python 3.10+. Defaults: 300 Insights Agents on 60 services, a batch from each every 5
seconds (60 batches a second), 5,000 witnesses a second (about 3 billion a week), 0.5% of uploads lost and
0.5% of batches published twice.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python assembler.py &                # witness_assembler on the lane
python async_witnesses.py            # 300 agents' uploads onto the lane
```

They print a line a second:

```
[witness_assembler]    60 batches/s   5,000 witnesses/s  publish-to-assembler p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
[funnel] assembled … batches, … witnesses from 300 agents  missing batches …  duplicates absorbed …  agents closed …
         |  60 services, … endpoints  this second: … requests, … 5xx, API p90 … ms
[funnel] never arrived: cli_00042 batch 119, …
[async_witnesses]    60 batches/s from 300 agents    5,000 witnesses/s (steady)  publish p50 …ms  p99 …ms  lost uploads …  resent …
```

**What the numbers mean.** The kit times each batch from the moment `async_witnesses.py` publishes it to
the moment `assembler.py` has applied it, through Python clients, on whatever machine you run it on. On a
shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (500 agents on 40
services, 2,500 witnesses a second in 100 batches a second, twice the witnesses from second 35 to 50, 1% of
uploads lost, 1% of batches published twice), two runs held publish-to-assembler p50 3.6–3.7 ms and p99
6.6–6.9 ms (52 of 52 steady-state seconds inside 50 ms in each), and the publish itself p99 1.0–1.2 ms.
At about 100 batches a second, p99.9 is the slowest batch of the second and reads the same as p99. With
2 MB/s of agent telemetry on the same broker, the hop's p99 moved by 0.1 ms or less; during the busy hour
it was 6.6–7.0 ms. Each run published about 6,640 batches and 200,000 witnesses, and the assembler
assembled every one exactly once, named all 53–65 lost uploads and nothing else, and absorbed all 63–67
duplicate publishes. That is the hop through this demo code, not the broker alone. The broker's own tail
latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `async_witnesses.py` | The receive side. Plays the agents (each watching one service, flushing every 5 s, spread over the interval) and publishes each upload batch to `witness-batches`, keyed by service, with `client_id`, the agent's batch number and its witnesses. Some uploads are lost before the lane, some batches are published twice. |
| `assembler.py` | witness_assembler on the lane. Reads the 16 partitions directly, applies each agent's batches in order, counts repeats once, names missing batches, and keeps per-service endpoint counts (requests, 5xx, API p90). Prints the hop's p99 and the funnel every second. |
| `telemetry_flood.py` | A heavy agent-telemetry stream (capture diagnostics, packet counts) on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies the topic async_witnesses publishes to into Iggy as its own consumer group (`iggy-shadow`), for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A busy hour:** `python async_witnesses.py --peak 3 --peak-at 60 --peak-for 120`.
   Pass: publish-to-assembler p99 under 50 ms, and the funnel still closes.
2. **Agent telemetry next door:** `python telemetry_flood.py --mbps 20` while the lane runs.
   Pass: the witness lane's p99 moves by less than 10%.
3. **Restart the assembler:** stop `assembler.py` and start it again; it replays the lane, rebuilds each
   agent's last batch number and prints "catching up" until a whole second of batches is inside the budget.
   Pass: it catches up within seconds, and its funnel counts match the run before the restart.

## Design notes

- The batch number is the one change this needs outside the lane: today's `UploadReportsRequest` carries
  `client_id` but no number. With it, at-least-once delivery is enough: a repeat changes nothing, and a jump
  names exactly what never arrived. Apache Iggy 0.9 does not offer exactly-once, and this hop doesn't need it.
- The kit's witness is a compact `[endpoint, status, latency ms]` stand-in for the witness proto, so the
  assembler can show what the Insights overview counts. The real assembler's pairing and ClickHouse insert
  are out of scope; `asm_ch_inserted` stays where it is.
- Services, endpoints and the traffic mix are illustrative, except `identity-service`, which appears in
  Postman's PR #200.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --topic <the topic async_witnesses publishes to>
python assembler.py
```

The bridge reads as its own consumer group (`iggy-shadow`), so the agents, async_witnesses,
witness_assembler and ClickHouse are untouched. Map your record in `to_batch()`: the record shape on Kafka
is not public, so the field names are flags. Until uploads carry a batch number, the bridge numbers each
agent's batches in the order Kafka delivers them, so missing-batch naming covers only what reached Kafka;
the latency comparison with your own stage timestamps holds either way.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored witness topic, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
