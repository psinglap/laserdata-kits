# Real-time position updates for Groww Holdings, on LaserData

Every fill's new position reaches the Holdings service in milliseconds, so the refresh right after a
trade shows the stock, even at the 9:15 open. It runs beside your Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/34FzTVXuMgT7Y4PdaU64XY
- **One slide:** [`LaserData_Groww_PositionUpdates.pdf`](LaserData_Groww_PositionUpdates.pdf)

## Why this use case

From Groww's own Engineering Blog (Aug 2026, "Holding Revamp Went Live. Then Reality Check Hit
Hard"): after an order executes, the app refreshes Holdings immediately, which leaves "only about
500 ms for the latest Position update to reach the Holdings service". Position updates were taking
"around 1.5 seconds (P99) to propagate through the CDC pipeline", and newly bought stock was missing
from the refresh. Fixing the CockroachDB changefeed brought it to about 281 ms, roughly 220 ms of
headroom at P99. The post's lesson: "Low consumer lag doesn't guarantee low end-to-end latency."

The Holdings page peaks at the 9:15 open, up to 1M requests a minute (Jul 2026 post), and Groww has
seen Kafka publish tails over 700 ms on its order-update topics (Jun 2025 post). This kit runs the
position path on its own lane, keyed by user, timed from the fill to the Holdings view, and checks
the outcome a user feels: does the refresh 500 ms after a fill already show it?

## Run it

Needs Docker and Python 3.10+. Defaults: 1,000,000 trading users, 2,000 instruments, 3,000 fills a
second with a 5× burst for the first minute (the open). Groww does not publish fills per second, so
set the rates to yours.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python holdings.py &                   # the Holdings service, and the app's refresh check
python oms_fills.py &                  # OMS fills, with the 9:15 burst
python dataplatform_flood.py --at 20   # a heavy DataPlatform stream next door
```

Each prints a line a second:

```
[oms] 15,000 fills/s (market open)  positions held …
[holdings] 15,000 position updates/s  fill-to-holdings p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  ETags invalidated …
[app] post-trade refreshes 15,000  missing the new fill …% (Holdings refreshed 500 ms after the fill)
```

`missing the new fill` is the business number: `holdings.py` plays the app, schedules a Holdings
refresh 500 ms after every fill (from its order update), and checks whether the Holdings view
already holds that fill. A slow or congested lane shows up there first.

**What the numbers mean.** The kit times each position from the moment `oms_fills.py` creates the
fill to the moment `holdings.py` has applied it, through Python clients, on whatever machine you run it
on. On a shared GitHub runner, with the broker and three Python processes on one box, five green CI
runs at 500 fills a second (2.5× for 15 s as the open) held p50 12.0 to 12.7 ms, p99 23.3 to 25.3 ms
and p99.9 25.1 to 26.8 ms (median per second over 63 steady-state seconds each, no second over the
100 ms budget). Through the open burst (1,250 fills a second) the median per-second p99 was 24.2 to
33.9 ms, worst 46.4 ms. Of about 42,300 post-trade refreshes checked in each run, none missed the new
fill. A 1 MB/s DataPlatform stream ran next door for 34 s. One earlier run at 1,000 fills a second
with a 3× burst failed: during the burst the single Python process reading both the lane and the order updates took in
about 1,800 position updates a second while 3,000 arrived, and fell seconds behind, so CI runs at the
lower rate; on a laptop or a larger machine, raise the rates. That is the end-to-end lane through
this demo code, not the broker alone; the broker's own tail latency is measured with `iggy-bench` on
dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `oms_fills.py` | OMS fills: each one's resulting position (net quantity, average price, per-position sequence) on `position-updates`, and the order update on `order-updates`, both keyed by user. `--open-burst` plays 9:15. |
| `holdings.py` | The Holdings service: applies positions by sequence, invalidates the user's ETag, times fill-to-holdings; and the app's refresh check. |
| `dataplatform_flood.py` | A bulk, latency-tolerant stream on its own topic on the same broker. |
| `kafka_bridge.py` | Copies the Kafka topic your Holdings service reads positions from into Iggy, as a read-only consumer group, for a shadow run on real traffic. Unwraps a changefeed's `{"after": …}` envelope. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **The 9:15 open:** `python oms_fills.py --fills-per-s 3000 --open-burst 5 --open-for 60`.
   Pass: fill-to-holdings p99 under 100 ms through the burst, and under 0.01% of refreshes missing the fill.
2. **DataPlatform on the same broker:** `python dataplatform_flood.py --mbps 20 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart Holdings:** stop `holdings.py` and start it again. It replays the day's positions, prints
   "catching up", then times live fills again. Pass: no holding applied twice, back under budget within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <cluster>-kafka-bootstrap:9092 --kafka-topic <your-position-topic> \
  -X security.protocol=SASL_PLAINTEXT -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=<user> -X sasl.password=<password>
```

The bridge only reads; the changefeed, the Holdings service and every existing consumer are untouched.
Map your column names in `to_msg()`: the kit needs `k` (user:instrument), `u`, `seq`, `qty` and `avg`.
In shadow mode the clock starts when the bridge reads the record.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored position-update topic, beside
your Strimzi Kafka on GKE. No advantage, no migration.

kranti@laserdata.com · laserdata.com
