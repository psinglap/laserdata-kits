# Real-time surge windows for Uber, on LaserData

Every rider and driver event reaches the surge window that owns its hexagon in milliseconds, so late events stop
dropping out of the multiplier, and both regions close every window with the same counts. It runs beside Kafka
and Flink, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.uber-kit
- **One slide:** [`LaserData_Uber_SurgeWindow.pdf`](LaserData_Uber_SurgeWindow.pdf)

## Why this use case

Uber runs one of the largest Kafka estates in the world: "trillions of messages and multiple petabytes of data per
day", with a consumer proxy serving "over 1,000 consumer services" (Introducing uForwarder, Feb 2026). This kit
does not ask Uber to move any of that. It picks one workload Uber has described in its own words, where the tail
of one hop decides whether the numbers are right.

From "Real-time Data Infrastructure at Uber" (Fu and Soman, SIGMOD 2021): surge "is essentially a streaming
pipeline for computing the pricing multipliers per hexagon-area geofence based on the trip data, rider and driver
status in a time window". It "ingests streaming data from Kafka, runs a complex machine-learning based algorithm
in Flink, and stores the result in a sink key-value store". It "favors data freshness and availability over data
consistency. The late-arriving messages do not contribute to the surge computation and the pipeline must meet a
strict end-to-end latency SLA requirement on the calculation per time window", and it "uses the Kafka cluster
configured for higher throughput but not lossless guarantee". In every region a Flink job computes the pricing;
its state is too large to replicate, so "Given that the input to the Flink job from aggregate Kafka is consistent
across all regions, the output state converges."

From "Building Scalable Streaming Pipelines for Near Real-Time Features" (Uber Engineering, Aug 2021): demand and
supply are counted as distinct riders and drivers per H3 hexagon, per product, in one-minute windows; about 120k
events/s of Kafka input, about 5M hexagons in 1,500 cities, 45 demand events per hexagon per minute on average.

That input, demand and supply into the per-hexagon window, is a keyed fan-in where the tail decides whether an
event makes its window. This kit runs that one lane on Apache Iggy: every event keyed by hexagon into the surge
window that owns it, in two regions reading the same log. It prints the lane's latency every second, and the
window numbers: each window checked against the marketplace's own distinct counts, late events, how long after
its end each window's last event arrived, cells surging, windows rebuilt from the log after a restart, and
windows where both regions agree.

## Run it

Needs Docker and Python 3.10+. Defaults: 4 cities x 1,000 hexagons, 45 demand events per hexagon per minute
(3,000/s), 1,500 drivers per city reporting every 4 s (about 900 available/s), 60 s windows with 1 s grace.

```
docker compose up -d                                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                        # apache-iggy 0.9.0
python surge_window.py --region region-a --primary &   # region A's surge worker, publishes multipliers
python surge_window.py --region region-b &             # region B's, the same log, computed independently
python marketplace.py                                  # 4 cities, 4,000 hexagons
```

They print a line a second:

```
[region-a]  3,900 events/s  event-to-window p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  multipliers sent …
[window] region-a closed … +1,0… ms after its end: … cells, riders …, drivers …, surging … (max …x)
         last event +… ms after its end  exact …/…  late 0  rebuilt 0  regions agree …/…
[marketplace]  3,900 events/s (steady)  demand 3,000  supply …  4 cities x 1,000 hexagons  windows reported …
```

**What the numbers mean.** The kit times each event from the moment `marketplace.py` sends it to the moment
`surge_window.py` has applied it to its window, through Python clients, on whatever machine you run it on. On a
shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (2 cities x 400 hexagons,
about 850 events/s, 10 s windows with 0.5 s grace, a stadium spike multiplying demand 30x in 19 hexagons from
second 30 for 15 s, about 1,250 events/s, and 2 MB/s of app analytics from second 20), three green runs on the
final code held event-to-window p50 2.9–5.5 ms, p99 5.5–10.0 ms and p99.9 6.1–11.5 ms (64 to 65 of 65 steady-state
seconds inside 50 ms). Each region's worker reads the 16 event partitions directly, on two connections. The
analytics stream moved the lane's median p99 by 0.4 ms; the spike on top by 0.1–0.3 ms more. In every run every
window region A closed (7–8 per run) matched the marketplace's own distinct counts exactly, 0 events arrived after
their window closed, the last event of each full window arrived within 5 ms of the window's end against a 500 ms
grace, cells surged up to the 3.0x cap, and both regions agreed on all 8 windows compared. Region B's worker was
stopped at second 38 and started at second 45: it replayed about 41,400 earlier events in its first second, caught
up in two, rebuilt 5–6 windows from the log, all exact, and held 31 of 31 live seconds inside 50 ms. The one slow
second (p99 62 ms, in the first run) came at about the moment region B's restarted worker replayed those events on
the same two vCPUs. That is the end-to-end lane through this demo code, not the broker alone. The broker's own
tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.
CI runs:
https://github.com/psinglap/laserdata-kits/actions/runs/39875711943,
https://github.com/psinglap/laserdata-kits/actions/runs/39875972990,
https://github.com/psinglap/laserdata-kits/actions/runs/39876198938

| File | What it is |
|---|---|
| `marketplace.py` | Rider demand and driver supply for several cities. Every event goes to `surge-events`, keyed by hexagon, with its event time; `--spike` runs a stadium emptying. Publishes its own distinct counts per window to `window-truth`. |
| `surge_window.py` | One region's surge worker. Reads every partition directly, counts distinct riders and drivers per hexagon and product per window, closes each window after the grace, computes a placeholder multiplier, publishes a fingerprint to `window-digests` (and, with `--primary`, every multiplier to `surge-multipliers`), and checks each window against the marketplace's count and the other region. |
| `analytics_flood.py` | A heavy app-analytics stream on its own topic, for the isolation test. |
| `kafka_bridge.py` | Read-only shadow from your existing demand and supply topics (consumer group `iggy-shadow`). |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

The multiplier in `common.py` is a placeholder (a cell's riders per driver against its city's ratio), so the kit
can show which cells surge; your model stays yours. The lane's job is the counts it is fed.

## Three tests

1. **A stadium empties:** `python marketplace.py --spike 30 --spike-at 60 --spike-for 300`.
   Pass: event-to-window p99 under 50 ms, 0 late events, every window exact, the spike's cells surging.
2. **Analytics next door:** `python analytics_flood.py --mbps 20` while all run.
   Pass: the lane's p99 moves by less than 10%.
3. **Restart a region's worker:** stop region B's `surge_window.py` mid-window and start it again. It replays
   `surge-events` and rebuilds every window it missed and the one it was holding.
   Pass: it catches up within seconds, every rebuilt window is exact, and both regions agree on every window.

## Shadow your traffic

```
python kafka_bridge.py --bootstrap <brokers> --demand-topic <demand topic> --supply-topic <supply topic> \
  --hex-field hexagon --product-field product --id-field id --time-field ts_ms
python surge_window.py --region shadow --window-s 60 --grace-s 1
```

The bridge is its own consumer group, so producers, the surge pipeline and every existing consumer are untouched;
if it stops, only the shadow stops. Compare the worker's distinct counts per hexagon and window with what your
pipeline computed for the same windows.

## What Apache Iggy 0.9 has, and what it doesn't

- Replication: Viewstamped Replication clusters, shipped September 2026; three nodes tolerate one failure.
  Replication is within one cluster. There is no cross-region replication in 0.9: the kit's two regions read one
  broker, which models two independent computations over the same input, not a multi-region deployment.
- Delivery: at-least-once. The counts here are distinct riders and drivers, so a second delivery of an event
  cannot change them.
- Partitions: the count is set per topic; size it for the worker fleet, and workers move partitions between them
  without remapping hexagons.
- Kafka API: a Kafka-compatible gateway is in progress and not in 0.9, so the shadow run uses a bridge, and
  Flink's Kafka source cannot read the lane directly yet.
- SDKs: Rust, Go, TypeScript/Node.js, Python, Java, C#.

Contact: kranti@laserdata.com · laserdata.com
