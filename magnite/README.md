# Real-time DSP appetite for traffic shaping at Magnite, on LaserData

Every change in which DSPs are bidding on which supply reaches every traffic-shaping node in
milliseconds, in order, and a shaping pod that starts or is rescheduled rebuilds the whole table from the
log. It runs beside Kafka, so the exchange, SpringServe, Spark Structured Streaming and everything else
stay where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/N5Y6LyouHnL9i2XevZNkFT
- **One slide:** [`LaserData_Magnite_TrafficShaping.pdf`](LaserData_Magnite_TrafficShaping.pdf)

## Why this use case

From Magnite's own job posts:

- The Demand Tech team "builds traffic-shaping technology that prioritizes high-value ad requests at
  scale", writing "low-latency software that routes high volumes of ad requests" with a target of
  "p99 latencies <= 20ms", across about 2 trillion daily transactions. "A 1% improvement in ad request
  prioritization" affects about 20 billion transactions a day. Core development is in Rust and Scala, with
  Kafka, Spark Structured Streaming and Kubernetes (Engineer II, Demand Tech, 2026).
- Traffic shaping "uses machine learning to identify the ad requests most valuable to process", to
  maximize publisher revenue while using infrastructure efficiently, at "trillions of requests per day"
  (Associate Product Manager, Traffic Shaping, 2026).
- Infrastructure runs multi-node Kafka clusters next to Aerospike and Hadoop (Senior Big Data Engineer,
  2026).

To send a request only where it is worth sending, a shaping node needs to know where demand is right now:
which DSPs are bidding on which supply, and when one stops or starts. The exchange already sees every DSP
response. This kit carries that one signal, the DSP appetite lane, to every shaping node on Apache Iggy and
prints its p99 every second, next to the cost of being late: callouts sent to a DSP that had already
stopped bidding on that segment. The log adds rebuilds: a shaping pod that starts or is rescheduled reads
the last minute of the log and has every pair, including ones that have not changed for minutes.

We don't know how your shaper gets its demand signal today; the kit's "1 s behind" figure is a
comparison, not a claim about your system.

## Run it

Needs Docker and Python 3.10+. Defaults: 60 DSPs × 5,000 supply segments (300,000 pairs), 20,000
appetite changes a second (200 of them flips, where a DSP stops or starts bidding on a segment), and
shaping nodes that each shape 200,000 ad requests a second. All illustrative: set them to yours.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python exchange.py &                 # the exchange side: appetite changes, one writer per partition
python shaper.py --name shaper-1 &   # a traffic-shaping node
python shaper.py --name shaper-2     # and another: each reads every change
```

Each prints a line a second:

```
[shaper-1] 20,000 changes/s  exchange-to-shaper p50 …ms  p99 …ms  p99.9 …ms  PASS (<20ms)  flips 200
           wasted callouts …/s (… per 1M)  1 s behind …/s  bids held back …/s  gaps 0  pairs 300,000
[exchange] 20,000 appetite changes/s  flips 200  … writes  write ack p50 …ms  p99 …ms
```

- **wasted callouts:** requests this node sent to a DSP after it stopped bidding on that segment, before
  the node knew (latency × the node's request rate on that pair), per second and per million callouts;
  **1 s behind:** the same flips if every one reached the node a full second late. A table refreshed once
  a second would be about 0.5 s stale on average, so would waste about half that.
- **bids held back:** requests not sent to a DSP that had started bidding again, times its bid rate.
- **gaps:** a missing sequence number for a pair; must stay 0.

**What the numbers mean.** The kit times each change from the moment the exchange side saw it (including
its wait for the next write) to the moment `shaper.py` has applied it, through Python clients, on whatever
machine you run it on. In CI, on a shared 2-vCPU GitHub runner with the broker, the exchange writers, two
shaping nodes and the auction log all on one box, at 2,000 changes/s over 12,000 pairs (20 DSPs × 600
segments), two green CI runs held a median per-second p50 of
3.6 to 4.1 ms, p99 6.5 to 7.7 ms and p99.9 7.3 to 9.0 ms on each node; 62 of 63 steady-state
seconds passed the 20 ms budget in each run, and the one slow second (21 to 39 ms) came as the restarted
node replayed the log on the same two CPUs. A 4× auction-log burst next door (to 4 MB/s) moved the median
p99 by less than 0.05 ms. About 48 flips a second cost a median of 3.9 to 4.5 wasted callouts a second
(15 to 18 per million callouts) at 20,000 requests/s per node; had every flip arrived a full second later,
about 1,030 to 1,070 (a once-a-second refresh, about 0.5 s stale on average, would be about half that). The restarted node rebuilt all 12,000 pairs from the last 20 s of the log (about
51,600 changes) in 0.46 to 0.64 s; 10 s of live changes alone would have covered 57% of them. No sequence
gaps.
That is the end-to-end lane through this demo code, not the broker alone; the broker's own tail latency is
measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `exchange.py` | The exchange side: per (DSP, segment) pair, the recent bid rate and whether the DSP is bidding, written on every change with a per-pair sequence; flips when a DSP stops or starts; a rolling snapshot of every pair once a minute. One writer per partition, each writing every 2 ms. |
| `shaper.py` | A traffic-shaping node: reads every partition with its own offsets, keeps the live table, counts callouts per request rate, wasted callouts and bids held back, checks sequence gaps. Rebuilds from the last `--replay-s` seconds of the log on start. |
| `auction_log.py` | The auction log (requests, callouts, responses, clearing price; ~0.9 KB records) on its own topic, with a burst, for the isolation test. |
| `kafka_bridge.py` | Reads your DSP response topic from Kafka as its own consumer group (`iggy-shadow`) and writes appetite changes into Iggy, for a shadow run on real responses. |
| `ci.sh` | The CI run: two shaping nodes, a 4× auction-log burst next door, a node restart; pass only on 95% PASS seconds per node, zero gaps, flips counted and a full rebuild. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests

1. **Peak rate:** `python exchange.py --rate 100000 --flips 1000`.
   Pass: exchange-to-shaper p99 stays under 20 ms and `gaps` stays 0.
2. **More shaping nodes:** start a third and a fourth `shaper.py`.
   Pass: the first node's p99 moves by less than 10%; every node reports the same change count.
3. **Reschedule a node:** stop `shaper.py` and start it again.
   Pass: `rebuilt from the last 75 s of the log … appetite for 300,000 of 300,000 pairs` within seconds,
   and the line after it is PASS. Compare with the `without the log` line.
4. **The auction log next door:** `python auction_log.py --mbps 20 --x 4` while the rest runs.
   Pass: the lane's p99 moves by less than 10%.

## Shadow your own DSP responses

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <broker>:9092 --kafka-topic <your DSP response topic> \
  --dsp-field dsp_id --segment-fields publisher_id,format,geo --bid-field bid
python shaper.py --name shadow --replay-s 0
```

The bridge is one more Kafka consumer group, so the exchange, producers and existing consumers are
untouched. Map your record in `to_response()`. It turns responses into appetite changes (a moving bid rate
per pair, and a flip after `--off-after-s` without a bid); the latency `shaper.py` prints then starts at
the bridge, not at the exchange.

## What Apache Iggy 0.9 has today, and what it doesn't

- **Replication:** Viewstamped Replication clusters, new in September 2026. Three nodes tolerate one
  failure. It is young; the evaluation's failover row is there to test it.
- **Delivery:** at-least-once. The per-pair sequence makes repeats harmless and gaps visible.
- **Replay:** polling from an offset or a timestamp, which is how a node rebuilds; per-topic expiry. There
  is no log compaction, which is why the exchange side writes a rolling snapshot.
- **Kafka API:** a Kafka-compatible gateway is in progress and not in 0.9. The lane does not need it; the
  bridge reads Kafka as a consumer group, and your Kafka stays.
- **Clients:** official SDKs for Rust, Go, Java, Python, C# and Node.js. The Rust SDK fits your Rust
  shaping services; Scala can use the Java SDK.
- **Footprint:** a single Rust binary, Apache-licensed: thread-per-core, no garbage collector.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored DSP response topic, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
