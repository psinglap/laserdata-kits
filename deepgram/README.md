# Real-time usage metering for Deepgram, on LaserData

Every live stream's usage reaches the per-project usage summary in milliseconds, counted once even when it is
sent twice, even at peak. It runs beside your billing pipeline, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/9gA8sbg2EU28SUQEMSExWF
- **One slide:** [`LaserData_Deepgram_StreamingUsage.pdf`](LaserData_Deepgram_StreamingUsage.pdf)

## Why this use case

From Deepgram's billing job post (Billing & Analytics Software Engineer, Console & Analytics team, Apr 2026):
the role owns "the pipeline that ingests, validates, and processes usage messages from Deepgram's inference and
voice agent services", and must "ensure events are captured with sub-second reliability at scale, with
idempotency and exactly-once semantics to guarantee billing accuracy". Usage rolls "into usage summaries—by
customer, product, time window, and pricing dimension" for "real-time dashboards and end-of-period billing
runs", with "automated reconciliation between usage events, aggregated summaries, billing records, and payment
processor data". The post prefers Rust and lists Kafka, SQS and Pub/Sub as a plus.

From Deepgram's pricing page (read Oct 2026): streaming speech-to-text from $0.0048 a minute (Nova-3,
pay-as-you-go), with "true per-second billing". And from Adam Sypniewski's post on the engine: it was rewritten
in Rust for control over memory and CPU, with no garbage collector.

Per-second billing on live WebSocket streams is a steady flow of small usage messages for as long as each stream
stays open. This kit runs that one lane on Apache Iggy: each live stream's billable audio so far, keyed by
project, into per-minute summaries by project, model and API key. It prints the lane's latency every second,
and the billing numbers: audio metered, closed streams reconciled to the millisecond, retries absorbed, and what
a meter without dedupe would have over-billed.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 engine servers, 20,000 live streams across 5,000 projects, one
usage message per stream a second, streams of 90 s on average, and 0.5% of messages sent twice as retries.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python meter.py &                    # the meter, fed by the usage lane
python engine.py                     # 8 engine servers, 20,000 live streams
```

They print a line a second:

```
[meter]  20,000 usage msgs/s  usage-to-summary p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  summaries sent …
[usage] minute …: audio metered … h ($… at list)  projects …  streams reconciled …/…  retries absorbed …
        without dedupe +… s (+$…, …%)
[engine] 20,000 usage msgs/s (steady)  live streams 20,000 on 8 servers  opened …/s closed …/s  audio served … s/s  resent …
```

**What the numbers mean.** The kit times each usage message from the moment `engine.py` sends it to the moment
`meter.py` has added it to the summary, through Python clients, on whatever machine you run it on. On a shared
2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (3,000 live streams over 500
projects on 4 servers, about 3,080 usage messages/s, 20 s mean stream length, 1% of messages sent twice), it held
usage-to-summary p50 11 ms, p99 22 ms and p99.9 24 ms (62 of 62 steady-state seconds inside 50 ms). With 2 MB/s
of request logs on the same broker, the lane's p99 went from 21.6 ms to 22.0 ms. All 10,475 streams that closed
reconciled to the millisecond; 2,129 retries were absorbed, where a meter that adds every message would have
billed 2,128 extra seconds of audio (0.96% of the audio served). That is the end-to-end lane through this demo
code, not the broker alone. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on
dedicated machines. CI run: https://github.com/psinglap/laserdata-kits/actions/runs/39475217895

| File | What it is |
|---|---|
| `engine.py` | A fleet of engine servers holding live streams. Every second each stream publishes its billable audio so far (the resulting total, with a per-stream sequence) to `usage`, keyed by project, and its last message carries `fin`. A share of messages is sent twice, as retries. |
| `meter.py` | The meter. Adds only what is new for each stream, ignores retries and replays, sums by minute, project, model and API key, reconciles every closed stream, and publishes changed per-project totals to `usage-summaries` every 250 ms. Prints the lane's p99 and the billing numbers. |
| `log_flood.py` | A heavy request-log stream on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your usage messages from a Kafka-protocol topic into Iggy as a read-only consumer group, for a shadow run on real traffic. Notes for SQS and Pub/Sub inside. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A surge of new streams:** `python engine.py --peak 3 --peak-at 60 --peak-for 120`.
   Pass: usage-to-summary p99 under 50 ms, every closed stream reconciles, no retry billed.
2. **Request logs next door:** `python log_flood.py --mbps 20` while both run.
   Pass: the usage lane's p99 moves by less than 10%.
3. **Restart the meter:** stop `meter.py` mid-minute and start it again. It replays the usage topic and rebuilds
   every stream's total and every summary.
   Pass: it catches up within seconds, the replay adds nothing twice, closed streams still reconcile.

## Shadow your traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers:9092> --topic <usage-topic> \
  --id-field request_id --project-field project_id --duration-field duration
```

It reads as its own consumer group, `iggy-shadow`, so the engine, the usage pipeline, metering and billing are
untouched. If usage rides SQS, shadow from a second queue subscribed to the same SNS topic (receiving from your
queue would take messages from your pipeline); on Pub/Sub, add a second subscription.

## What Apache Iggy 0.9 has, and what it doesn't

- Replication: Viewstamped Replication clusters, shipped September 2026; three nodes tolerate one failure.
- Delivery: at-least-once, not exactly-once. Exactly-once billing here comes from the message shape (the stream's
  total with a sequence, applied idempotently), and the kit proves it with retries on purpose.
- Kafka API: a Kafka-compatible gateway is in progress and not in 0.9, which is why the shadow run uses a bridge.
- SDKs: Rust, TypeScript/Node.js, Go, Python, Java, C#.

Contact: kranti@laserdata.com · laserdata.com
