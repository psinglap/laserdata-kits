# Real-time streaming aggregation for Last9, on LaserData

Every raw sample reaches the streaming aggregator that owns its shard key in milliseconds, and every timeslice
closes exact, counted once even when a sample is sent twice. It runs beside your gateways and storage, so
nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Last9_StreamingAggregation.pdf`](LaserData_Last9_StreamingAggregation.pdf)

## Why this use case

From Piyush Verma's post "How we tame High Cardinality by Sharding a stream" (Aug 2023): "Gateways communicate
with receivers that distribute the data evenly across Storage nodes before it is stored", and "extremely high
cardinality data can be sent to Streaming Aggregators, which provide an almost 100x cardinality boost." The shard
key is cluster plus metric name, with an optional per-metric `shard_label` ("Choosing the right shard_label can
massively boost the reliability and performance of streaming aggregates"), because "all the data the function
needs has to live in the same place as the function." The post also warns that modulo hashing remaps keys when
nodes are added.

From the Streaming Aggregation docs: PromQL rules run before storage, with `sum`, `max` and `sum2` ("Sum, but
for counters and reset awareness"), and an aggregate is "limited to a cardinality of 3M timeseries per hour".
From the Aug 2023 changelog: the gateway is "backed by durable embedded queues", with "improvements in handling
streaming aggregation pipeline back pressure". From the Altinity x Last9 webinar (May 2026): "1M
samples/second" and "traffic variations from 100k to 300Mn per minute".

That path, receivers to streaming aggregators, is a keyed, ordered fan-in where the tail decides how soon a
timeslice can close, and where a late copy of a counter sample corrupts an aggregate. This kit runs that one
lane on Apache Iggy: every raw sample, keyed by (cluster, metric, service), into the aggregator that owns it,
running `sum2 by (cluster, service, le) (http_requests_duration_seconds_bucket[timeslice])`. It prints the lane's
latency every second, and the aggregation numbers: each timeslice checked against the gateway's own count, the
fold from series in to series out, retries absorbed, counter resets handled, late samples, and what an
aggregator without a sequence check would have over-counted.

## Run it

Needs Docker and Python 3.10+. Defaults: 4 clusters, 20 services with 10 pods each, 10 routes and 12 buckets
per pod (96,000 histogram series) scraped every 15 s, about 6,400 samples/s, 60 s timeslices with 2 s grace,
0.5% of samples sent again and 0.05% counter resets.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python aggregator.py &               # the streaming aggregator, fed by the lane
python gateway.py                    # 4 clusters, 96,000 series
```

They print a line a second:

```
[aggregator]   6,400 samples/s  sample-to-aggregator p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  aggregates sent …
[timeslice] last closed … +2,0… ms after its end: series in 96,000 -> out 960 (100x fewer)  exact …/…
            retries absorbed …  counter resets …  late 0  without seq check +… (…%)
[gateway]   6,400 samples/s (steady)  live series 96,000 in 4 clusters  resets …  resent …  timeslices reported …
```

**What the numbers mean.** The kit times each sample from the moment `gateway.py` sends it to the
moment `aggregator.py` has applied it, through Python clients, on whatever machine you run it on. On a shared
2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (9,216 series in 4 clusters scraped
every 5 s, about 1,860–1,870 samples/s, 10 s timeslices, 1% of samples sent again as copies of the last ~10 s,
0.1% counter resets, a rollout doubling the pods to ~3,690 samples/s from second 30 for 15 s, and 2 MB/s of raw
logs from second 20), three green runs on the final code held sample-to-aggregator p50 5.8–6.0 ms, p99
10.6–11.1 ms and p99.9 12.1–12.6 ms (62 of 62 steady-state seconds inside 50 ms in each). The aggregator reads the
16 sample partitions directly, on two connections. The raw logs moved the lane's median p99 by 0.3–0.6 ms;
the rollout on top raised it 1.7–2.2 ms. In every run every closed timeslice (6 per run) matched the gateway's
own count exactly, with 1,577–1,603 retries absorbed, 129–168 counter resets handled and 0 late samples; the
fold was 36x (9,216 series in, 256 out), 72x during the rollout. An aggregator without the sequence check would
have read the late copies as counter resets and reported the same aggregates 677–708% high: that figure depends
on how late the copies are and how large the counters, so read it as the direction, not a forecast. That is the
end-to-end lane through this demo code, not the broker alone. The broker's own tail latency is measured with
`iggy-bench` in `bench/PLAN.md`, on dedicated machines. CI runs:
https://github.com/psinglap/laserdata-kits/actions/runs/39867101382,
https://github.com/psinglap/laserdata-kits/actions/runs/39867124474,
https://github.com/psinglap/laserdata-kits/actions/runs/39867137845

| File | What it is |
|---|---|
| `gateway.py` | Gateways and receivers for several clusters. Every series is scraped on its interval and each sample (the counter's cumulative value, with a per-series sequence) goes to `samples`, keyed by shard key. Late copies and counter resets on purpose; `--peak` runs a rollout. Publishes its own count of what each group gained per timeslice to `window-truth`. |
| `aggregator.py` | The streaming aggregator. Reads its partitions directly, applies `sum2` with the sequence check, closes each timeslice after the grace, publishes one resulting value per group to `aggregates`, and checks each timeslice against the gateway's count. |
| `log_flood.py` | A heavy raw-log stream on its own topic, for the isolation test. |
| `otlp_tee.py` | An OTLP/HTTP (JSON) receiver for a collector's second exporter: writes one histogram's buckets into the lane, read-only for the existing path, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A rollout's cardinality spike:** `python gateway.py --peak 2 --peak-at 60 --peak-for 120`.
   Pass: sample-to-aggregator p99 under 50 ms, every timeslice through the rollout exact.
2. **Raw logs next door:** `python log_flood.py --mbps 20` while both run.
   Pass: the lane's p99 moves by less than 10%.
3. **Restart the aggregator:** stop `aggregator.py` mid-timeslice and start it again. It replays the samples
   topic and rebuilds every series' last value and the open timeslice.
   Pass: it catches up within seconds, the replay adds nothing twice, the open timeslice still closes exact.

## Shadow your traffic

Add a second exporter to a collector that already sends to Last9:

```
exporters:
  otlphttp/iggy-shadow:
    endpoint: http://<this-host>:4319
    encoding: json
service:
  pipelines:
    metrics:
      exporters: [<your existing exporter>, otlphttp/iggy-shadow]
```

```
python otlp_tee.py --port 4319 --cluster prod-00 --metric http.server.request.duration
python aggregator.py --timeslice-s 60
```

If the tee stops, only the shadow stops. Compare the kit's aggregates with what your streaming aggregation
produces for the same rule.

## What Apache Iggy 0.9 has, and what it doesn't

- Replication: Viewstamped Replication clusters, shipped September 2026; three nodes tolerate one failure.
- Delivery: at-least-once, not exactly-once. Counting once here comes from the message shape (the counter's
  value with a per-series sequence, applied idempotently), and the kit proves it with late copies on purpose.
- Partitions: the count is set per topic; size it for the aggregator fleet, and aggregators move partitions
  between them without remapping keys.
- Kafka API: a Kafka-compatible gateway is in progress and not in 0.9; this path speaks remote write and OTLP,
  so the shadow run uses an OTLP tee.
- SDKs: Rust, Go, TypeScript/Node.js, Python, Java, C#.

Contact: kranti@laserdata.com · laserdata.com
