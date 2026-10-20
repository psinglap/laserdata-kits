# Real-time kitchen state for Zomato, on LaserData

Every accepted order and every Food Order Ready tap reaches the food preparation time (FPT) model
in milliseconds, even at New Year's Eve peak. It runs beside Confluent Cloud, so nothing else has
to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_Zomato_KitchenState.pdf`](LaserData_Zomato_KitchenState.pdf)

## Why this use case

From Zomato's own engineering posts:

- FPT is "an important component of Estimated Delivery Time (EDT)", predicted "for each order in
  real-time", and it tells delivery partners "the right time to reach a restaurant"
  ([2021](https://www.zomato.com/blog/elements-of-scalable-machine-learning),
  [2022](https://www.zomato.com/blog/predicting-fpt-optimally/)).
- The model reads each kitchen's "running orders at time T, at max 5 running orders" and its "last 5
  completed orders", and the Food Order Ready button made those preparation times true
  ([2020](https://www.zomato.com/blog/food-preparation-time/)).
- "Real-time features are computed via event streams published on Apache Kafka, and are processed in
  real-time by stream processing engine Apache Flink" into a Redis feature store (2021).
- Kafka peaked at 450M+ messages a minute and orders at 8,400 a minute on NYE 2023
  ([2024](https://www.zomato.com/blog/a-tale-of-scale-behind-the-scenes-at-zomato-tech-for-nye-2023)).
- Eternal moved logging and benchmark replay to WarpStream because they are latency-insensitive, and
  keeps latency-sensitive workloads on Confluent Cloud
  ([WarpStream case study, Aug 2026](https://www.warpstream.com/blog/eternal-case-study)).

So the stream that decides how good each prediction can be is the per-restaurant kitchen state:
if a kitchen accepted an order or marked one ready a moment before a checkout, and the model has not
seen it yet, the FPT is computed on a queue that no longer exists. This kit runs that one lane on
Apache Iggy, prints its p99 every second, and counts the predictions made on a stale kitchen view.

## Run it

Needs Docker and Python 3.10+. Defaults: 150,000 kitchens, 140 checkouts a second (the NYE 2023
peak of 8,400 orders a minute), 120,000 orders already cooking, kitchen time in real minutes.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python kitchens.py &                 # every kitchen: accept, cook, Food Order Ready, pickup
python fpt.py                        # feature view + checkouts with an FPT prediction each
```

Add `--minute 1` to `kitchens.py` to run kitchen time 60 times faster, so orders cook in seconds.
Both print a line a second:

```
[fpt]      420 kitchen events/s  event-to-feature-view p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  FPT predictions 140
[kitchens] 420 kitchen events/s  predictions checked 140/s  made on a stale kitchen view …%  …
```

**What the numbers mean.** The kit times each kitchen change from the moment `kitchens.py` creates
it to the moment `fpt.py` has applied it to the feature view, through Python clients, on whatever
machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at
about 900 kitchen events/s (300 checkouts/s, kitchen time 60× faster), it holds about p50 12 ms and
p99 23 ms; p99 rose to about 36 ms while a 3.5 MB/s benchmark replay ran on the same broker and the
same two vCPUs. A median 0.3% of predictions per second were made on a stale kitchen view. That is
the end-to-end lane through this demo code, not the broker alone. The broker's own tail latency is
measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `kitchens.py` | Every kitchen: accepts orders, cooks them (slower beyond its slots), Food Order Ready, pickup. Publishes each change to `kitchen-state`, keyed by restaurant, with a sequence and the resulting state. Checks every prediction against the truth. |
| `fpt.py` | The FPT feature view, kept from `kitchen-state`, and checkouts that each get a prediction from it. `predict()` is a stand-in for your model. |
| `replay_flood.py` | A benchmark replay on its own topic, for the isolation test. |
| `confluent_cloud_bridge.py` | Copies an existing Confluent Cloud topic into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `ci.sh` | The CI run: reduced scale, a 20 s replay next door, pass only on 95% PASS seconds and checked predictions. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **NYE rush:** `python fpt.py --rush 3 --rush-at 60 --rush-for 120`.
   Pass: event-to-feature-view p99 under 100 ms, and stale-view predictions under 0.5%.
2. **A benchmark replay next door:** `python replay_flood.py --mbps 50` while both run.
   Pass: the kitchen lane's p99 moves by less than 10% (on dedicated machines, not one laptop).
3. **Rebuild the feature view:** stop `fpt.py` and start it again; it replays the topic from the start.
   Pass: every kitchen is back in the view within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python confluent_cloud_bridge.py --bootstrap <bootstrap-server> --kafka-topic <order-state-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=<api-key> -X sasl.password=<api-secret>
```

The bridge only reads; producers, Flink jobs and existing consumers are untouched. Map your record
shape in `to_msg()`: the kit needs `r` (restaurant id), `seq`, `ev`, and the resulting kitchen state.

## What Apache Iggy 0.9 does not do yet

No Kafka wire protocol (a gateway is in progress), so the bridge copies rather than redirects;
delivery is at-least-once, so readers drop repeats by sequence, as `fpt.py` does; clustering is new
(Sep 2026).

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored order-state topic, alongside
Confluent Cloud. No advantage, no migration.

kranti@laserdata.com · laserdata.com
