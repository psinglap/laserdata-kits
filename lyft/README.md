# Real-time demand forecasts for Lyft pricing, on LaserData

Every forecast frame, for every geohash, reaches Rider Pricing and Driver Earnings complete in milliseconds,
even on a faster pricing loop. It runs beside MSK and Flink, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Lyft_GeohashForecasts.pdf`](LaserData_Lyft_GeohashForecasts.pdf)

## Why this use case

From Lyft Engineering's own posts:

- *Real-Time Spatial Temporal Forecasting @ Lyft* (May 2025): demand and supply forecasts "at geohash-6 level for
  every 5 minute interval in the next hour", run "every minute", for "4M geohashes per minute, per signal". The
  models' results are synced "to a Kafka topic … subscribed by downstream services (Rider Pricing, Driver
  Earnings, etc.) who … want to consume them in real-time." Stack: Beam on Flink, MSK, Kinesis, DynamoDB,
  ClickHouse.
- *Rerouting the Stream* (August 2026): pricing and routing jobs are the business-critical part of a fleet of
  hundreds of Flink jobs, deploys cost minutes of downtime before blue/green, and next comes "faster feedback
  loops for marketplace pricing."

So one narrow lane decides how fresh every price is: the forecast topic between the models and the services that
price rides and set driver incentives. At once a minute, delivery is a small share of the loop. At a faster loop
it is not: at a 5 s cadence, every 50 ms a frame spends in transit prices 1% of quotes on the previous forecast.
This kit runs that one lane on Apache Iggy and prints, every second, its p99 at each reader and the share of
quotes priced on a superseded forecast.

## Run it

Needs Docker and Python 3.10+. Defaults: 300 regions × 400 geohash-6 cells (120,000 cells, 12 demand and 12
supply values each) in a frame every 5 s, a concert letting out in one region 20 s in, and Rider Pricing quoting
500 rides a second.

```
docker compose up -d                          # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt               # apache-iggy 0.9.0
python forecaster.py &                        # a forecast frame for every region every 5 s
python marketplace.py --role earnings &       # Driver Earnings: bonus-zone cells from each frame
python marketplace.py --role pricing          # Rider Pricing: quotes, judged against the newest sealed frame
```

The readers print a line a second:

```
[rider-pricing]    900 forecast msgs/s  chunk-to-pricing p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
                 frames ready 300 (seal-to-ready p99 …ms)  quotes 500  judged …: on a superseded forecast,
                 live lane …%  1 s poll …%
[driver-earnings] … chunk-to-earnings p99 …ms  frames ready 300  bonus-zone cells …
```

`--cadence-s 60` runs today's once-a-minute loop; `--cells` and `--regions` scale the frame (at `--cadence-s 60`,
300 regions × 1,000 cells is 300,000 cells a minute; Lyft's post cites 4M geohashes a minute per signal, which
needs the benchmark rig, not one laptop).

**What the numbers mean.** The kit times each message from its creation in `forecaster.py` to its arrival in a
`marketplace.py` reader, and each frame from its seal to the moment the reader has the whole frame parsed and in
use, through Python clients, on whatever machine runs it. On a shared 2-vCPU GitHub runner with the broker and
all four scripts on one box (40 regions × 150 cells every second, 160 messages a second per reader in one burst
per frame, 2 MB/s of analytics events next door), the steady-state medians over three green
runs were chunk-to-reader p50 4.2 to 7.6 ms and p99 18.1 to 27.5 ms on each reader, seal-to-ready p99 20.1 to
30.4 ms, and every steady-state second inside the 50 ms budget (65 of 65 and 53 of 53 per reader); the worst
second's p99 was 22.1 to 43.8 ms. The restarted reader rebuilt all 40 regions from the last 3 s of the log in
0.01 s. Of about 13,000 quotes per run, 0.75 to 1.42% were priced on a superseded forecast from the lane, against
9.0 to 14.1% from a copy refreshed every 0.2 s (at a 1 s cadence; the share scales with delivery time over
cadence). That is the end-to-end lane through demo code at reduced scale, not the broker alone; the broker's own
tail is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `forecaster.py` | Stand-in for the forecasting models: a frame per region every `--cadence-s`, chunks of cells then a seal, all of a region in one partition. A concert spike in one region. |
| `marketplace.py` | A reader, `--role pricing` or `--role earnings`. Reads every partition directly, switches a region to a frame only when it is complete, rebuilds the latest frame of every region from the log on start. Pricing judges every quote against the newest sealed frame, next to a copy polled every `--poll-s`. |
| `analytics_flood.py` | A heavy neighbour: analytics events on their own topic, the input side of feature generation. |
| `msk_bridge.py` | Copies an existing forecast topic on MSK into Iggy as a read-only consumer group (`iggy-shadow`), grouping per-geohash records into chunks and seals. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A faster loop:** `python forecaster.py --cadence-s 1` (and `--quotes-per-s 2000` on pricing).
   Pass: chunk-to-reader p99 under 50 ms on both readers; quotes on a superseded forecast stay under 2%.
2. **Halloween next door:** `python analytics_flood.py --mbps 50` while everything runs.
   Pass: the forecast lane's p99 moves by less than 10%.
3. **A reader deploy:** stop `marketplace.py --role earnings` and start it again.
   Pass: it rebuilds the latest frame of every region from the log within seconds, before it uses a single one.

## Shadow your own traffic

```
pip install confluent-kafka
python msk_bridge.py --bootstrap <msk-brokers> --topic <forecast topic> --cells-per-run <cells in a region's run> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads and never commits an offset; the models, Rider Pricing, Driver Earnings and every other
consumer are untouched. The forecast topic's record format is not public, so map your fields in `to_messages()`:
the kit needs region, forecast run, geohash, and the demand and supply arrays.

## What Apache Iggy 0.9 doesn't have yet

No Kafka API (a gateway is in progress), so readers use the Iggy SDK and the trial uses the bridge.
At-least-once delivery (the frame number per region makes it safe). Replicated clusters shipped in September 2026.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored forecast topic, beside MSK. No advantage,
no migration.

kranti@laserdata.com · laserdata.com
