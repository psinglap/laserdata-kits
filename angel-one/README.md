# Real-time chart candles for Angel One, on LaserData

Every price tick reaches the candle builders on AWS in milliseconds, so the live candle and the
1-minute candle are ready long before today's ~1 s, and a DR builder rebuilds the open candles
from the log. It runs beside Kafka and MirrorMaker, so the price capture, the AWS Kafka, the
candle consumer, Aurora and the chart API stay where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_AngelOne_ChartCandles.pdf`](LaserData_AngelOne_ChartCandles.pdf)

## Why this use case

From "Charting at Angel One" (Abhin Hattikudru, Angel One tech blog on Medium, June 2023):

- Charts are "designated as a Tier 1 service. Any issues with chart formation, immediately
  affects our customers." Candles must be calculated "as soon as the price changes happen (within a
  couple of seconds)" and served "in the order of thousands of TPS during peak load".
- "Our charting solution involves an On Premise cluster for collecting the prices, a Kafka Mirror
  maker based replication to AWS". The cross-geo hop runs with `acks=-1`,
  `max.in.flight.requests.per.connection=1`, larger batches and compression.
- A tick is a scrip code, a timestamp, a price and the total volume for the day. A candle is an
  upsert (high = max, low = min, open from the earliest tick, close from the latest), "in theory
  orderless". 1-minute candles are built from the stream; higher timeframes from 1-minute candles.
- They meter "the time of the last tick processed against the system time": p50 ~550 ms, p95
  ~960 ms, p99 ~1,000 ms on a typical day, and are "working on several other projects to improve
  the ingestion delays". They plan AWS Hyderabad as "a Active disaster recovery site for charts".

The tick path into the candle builders is the one real-time stream Angel One has described in
detail, with its own delay numbers and its own wish to cut them. This kit runs that one lane on
Apache Iggy, uses your tick fields and your candle math, and prints the same delay you meter today,
every second. What the log adds: a builder that restarts, or a DR builder in another region,
replays from the start of the open candle and closes exactly the same candles.

## Run it

Needs Docker and Python 3.10+. Defaults: 40,000 scrips at 20,000 ticks a second (placeholders:
Angel One has not published its tick rate; set yours), each capture writer writing once a
millisecond, 1-minute candles.

```
docker compose up -d                          # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt               # apache-iggy 0.9.0
python capture.py &                           # the price capture, one writer per partition
python candles.py --name candles-mumbai &     # a candle builder
python candles.py --name candles-dr           # a DR builder: replays the open candle, then runs live
```

Each prints a line a second, and a line per closed candle:

```
[candles-mumbai] 20,000 ticks/s  tick-to-candle p50 …ms  p95 …ms  p99 …ms  p99.9 …ms  PASS (<25ms)
                 on the chart within 1 s 100.000%  store upserts …/s  late 0  gaps 0
[candles-mumbai] candle 09:15:00 closed: 38,912 scrips, digest 1f86f06676dc, final …ms after it ended
[candles-dr]     rebuilt the open candles from 09:16:00 in …s: … ticks replayed, … candles open
[capture]        20,000 ticks/s  … writes  write ack p50 …ms  p99 …ms
```

**What the numbers mean.** The kit stamps each tick when the capture engine produced it (including
its wait for the next write) and meters it against the builder's clock once the candle has been
updated, the same measure Angel One uses today, through Python clients, on whatever machine you run
it on. On a shared 2-vCPU GitHub runner, with the broker, the capture, two builders and the
order-event script on one box, at 1,000 ticks/s across 8,000 scrips with 15-second candles, three
green CI runs held a median per-second p50 of 1.9 to 2.3 ms, p95 3.0 to 3.5 ms, p99 3.4 to 4.0 ms and
p99.9 3.8 to 4.5 ms, with 0 of 69 steady-state seconds over the 25 ms budget; the worst second reached
13 to 20 ms. Every closed candle was final 3.3 to 6.5 ms after it ended (median per run 3.5 to 5.1 ms).
A DR builder started 30 s in rebuilt the open candle from the log (3,200 to 5,200 candles, 5,900 to
13,900 ticks) and was timing live ticks within 2 s; it then closed the same candles as the first
builder, digest for digest, on every candle both closed (3, 3 and 4 candles). A 10× order-event burst
next door moved the median p99 by 0.06 to 0.2 ms. No late ticks, no sequence gaps. That is the lane through this demo code on one box,
not the on-premise to AWS link, which adds its own round trip in production. The broker's own tail
latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `capture.py` | The price capture: one writer per partition, scrips spread by `part()`, a write once per ms. Each tick carries the resulting price and total volume plus a per-scrip sequence. `--burst` for the 09:15 open. |
| `candles.py` | A candle builder: reads every partition with its own offsets, applies Angel One's upsert math, closes a candle as soon as every partition has passed its end, counts the upserts a store would take every 100 ms. `--replay period` rebuilds the open candle from the log on start. |
| `orders.py` | Order lifecycle events shaped like a SmartAPI order-book row, on their own topic, with a burst, for the isolation test. Default rate from Angel One's September 2026 order count. |
| `kafka_bridge.py` | Reads your tick topic as Kafka consumer group `iggy-shadow` (on-prem or AWS side) and writes it into Iggy, for a shadow run on real ticks. |
| `ci.sh` | The CI run: 1,000 ticks/s, 15 s candles, a builder from the start, a DR builder from 30 s in, a 10× order burst; pass only on 95% PASS seconds per builder, no late ticks, no gaps, and identical candles from both builders. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests that match your peaks

1. **The 09:15 open.** `python capture.py --burst 5 --burst-at 60 --burst-for 30`. Pass: p99 under
   25 ms, `late` and `gaps` 0.
2. **A second and third builder.** Start more `candles.py` (a new timeframe, a watchlist tagger).
   Pass: the first builder's p99 moves by less than 10%, and closed candles carry the same digest.
3. **DR takeover.** Stop a builder mid-candle and start it again with `--replay period`. Pass: it
   catches up within seconds and its next closed candle matches the other builder's digest.
4. **Order events next door.** `python orders.py --rate 800 --x 10`. Pass: the candle lane's p99
   moves by less than 10%.

## Then try it on your ticks

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <your-kafka>:9092 --kafka-topic <your tick topic>
python candles.py --name shadow --replay period
```

The bridge is one more consumer group on your tick topic; the capture, MirrorMaker and the candle
consumer are untouched. Map your fields in `to_tick()`. Compare the shadow builder's delay and
candles with the ones your consumer writes to Aurora.

## What Apache Iggy 0.9 has today, and what it doesn't

- Replication: Viewstamped Replication clusters, new in September 2026; three nodes tolerate one failure.
- Delivery: at-least-once; the per-scrip sequence makes repeats harmless and gaps visible.
- Replay: polling from an offset or a timestamp, which is how a builder rebuilds the open candle.
- No cross-region replication and no MirrorMaker equivalent: the lane crosses the on-prem to AWS
  link once (capture writes to Iggy in AWS, or builders read across the link). A Hyderabad DR
  builder reads the same lane or a second lane fed by a relay.
- No Kafka API yet (a gateway is in progress): the candle builder uses an Iggy client (Go, Rust,
  Java, Python, C#, Node.js). Kafka stays for everything else.
