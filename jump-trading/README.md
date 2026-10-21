# Real-time link health alerts for Jump Trading, on LaserData

Every RSSI, SNR and BER sample from a network link reaches the baseline and alert rules, and every degraded or
recovered alert reaches the network console, on-call and the status board, in milliseconds and in order per
link, while the link is still degraded. It runs beside Redpanda, and nothing on the trading path changes.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_JumpTrading_LinkHealth.pdf`](LaserData_JumpTrading_LinkHealth.pdf)

## Why this use case

From Redpanda's Jump Trading case study (written by Alex Davies, CTO, 2023): "Our primary use cases are telemetry
and log files", with "system measurements, market information, and other telemetry" from "tens of thousands of
nodes publishing data", at "billions of messages daily" and demand of "millions of messages per second across
thousands of nodes", and "50 milliseconds for P95 and 150 milliseconds for P99". Jump started with Kafka and "did
not like having a Java application in such a critical role"; the gaps it found in earlier systems included tail
latency and "tolerance of slow consumers". From Jump's Network Engineer (Wireless) posting (2026): the team runs
"the telemetry pipelines that collect and baseline link performance (RSSI, SNR, BER)" and "the alerting that fires
off them".

A link alert is most useful while the link is still degraded: the console and the status board then describe
the network as it is, and a short burst is seen while it happens rather than after the link has recovered. This
kit runs that one lane on Apache Iggy: link samples keyed by link into the baseline and alert rules, degraded and
recovered alerts out to the network console, on-call and the status board. It prints both hops' latency every
second and counts the degraded alerts that reached the console while the link was still degraded. Because a
link's samples stay in order in one partition, each link is judged against its own learned baseline, the way the
posting describes.

Jump has not published which pipeline carries its link telemetry, its alert latency, or its link count and
sample interval; the kit's figures for those are placeholders. This kit is one monitoring lane beside Redpanda.
It does not touch market data or order flow.

## Run it

Needs Docker and Python 3.10+. Defaults: 1,000 links with a sample every 20 ms each (50,000 samples a second), a
fade every 10 minutes per link on average, and illustrative fade durations: 30% short bursts of 80–400 ms, the
rest 3–90 s (`links.fade_seconds`; set them to yours).

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python link_rules.py &               # the baseline and alert rules, fed by the link health lane
python links.py                      # 1,000 links: samples out, alerts in
```

They print a line a second:

```
[link-rules]  50,000 samples/s  sample-to-rules p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
              alerts sent … (degraded …, recovered …)  links degraded now …  stall gc …ms loop …ms
[links]  50,000 samples/s (steady)  fades started …  degraded alerts at the console …, while still degraded …
         (short bursts …; at 150 ms it would be …)  recovered alerts …  fade-to-consumers n … p50 …ms p99 …ms
```

"At 150 ms it would be …" counts the same degraded alerts whose fade lasted longer than 150 ms: what an alert
that took 150 ms (the p99 Jump published for its telemetry pipeline) would still have found degraded. Set
`--compare-ms`. "Stall" is the longest garbage-collection pause and event-loop stall in that process that second,
so a slow second can be traced rather than guessed at.

**What the numbers mean.** CI_NUMBERS

| File | What it is |
|---|---|
| `links.py` | A network of links. Each publishes RSSI, SNR and BER to `link-samples` every 20 ms, keyed by link, with its sample sequence; links fade and recover. Three consumers (network console, on-call, status board) read `link-alerts`; the console checks whether each degraded alert's link is still degraded. |
| `link_rules.py` | The rules: a per-link SNR baseline learned from clean samples, DEGRADED on BER above the limit or SNR well below baseline, RECOVERED after five clean samples. `rules()` is where yours go. Prints the lane's p99. |
| `slow_reader.py` | A slow consumer on the lane itself (a research capture or backfill), for the slow-consumer test. |
| `log_flood.py` | Log files shipped from hosts, on their own topic, for the isolation test. |
| `redpanda_bridge.py` | Copies your link telemetry topic from Redpanda into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `common.py` | Topics, partitioning by link, thresholds, illustrative clear-sky levels per link. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Storm:** `python links.py --storm 10 --storm-at 60 --storm-for 120`.
   Pass: sample-to-rules p99 under 50 ms, and every degraded alert reaches the console while its link is degraded.
2. **Slow consumer and log shipping next door:** `python slow_reader.py --rate 1000` and
   `python log_flood.py --mbps 50` while both run.
   Pass: the lane's p99 moves by less than 10% while the slow reader falls further behind.
3. **Restart the rules:** stop `link_rules.py` and start it again; it resumes from its stored offset (stored once a
   second), relearns baselines, and still sends alerts for samples from the last 30 seconds.
   Pass: it catches up within seconds, and no alert reaches a consumer twice as a new alert.

## Shadow your own traffic

```
pip install confluent-kafka
python redpanda_bridge.py --bootstrap <brokers> --topic <link-telemetry-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-256 -X sasl.username=... -X sasl.password=...
```

The bridge only reads; the collectors, every producer and everything that reads Redpanda today are untouched.
Map your record in `to_sample()`: the kit needs the link ID, RSSI, SNR and BER. Run it next to your brokers.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and every Kafka-API
client stays on Redpanda. Delivery is at-least-once (samples carry their sequence and alerts name their episode,
so repeats are harmless). Replicated clusters (Viewstamped Replication) shipped in September 2026. Storage is
local disk per node; there is no tiered storage to S3 as Jump uses on Redpanda, which is why this is one lane,
not the telemetry history. Official SDKs cover Rust, Go, Java, Python, C# and Node; there is no official C++ SDK.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored link telemetry topic, alongside Redpanda.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
