# Real-time store feedback for DoorDash dispatch, on LaserData

Every Dasher arrival, pickup and wait at the counter reaches the dispatch coordinator in
milliseconds, before its next run, even on Super Bowl Sunday. It runs beside Kafka, so nothing
else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_DoorDash_DispatchFeedback.pdf`](LaserData_DoorDash_DispatchFeedback.pdf)

## Why this use case

From DoorDash's own engineering posts:

- The new dispatch platform's RL agents "observe localized, delayed marketplace feedback — such as
  merchant congestion and courier utilization — to dynamically shift the optimizer's tradeoff
  between delivery speed and batching efficiency at the individual store level". A stateful
  coordinator keeps "the global state of the logistics network" for every dispatch run
  ([Oct 2026](https://careersatdoordash.com/blog/how-doordash-built-a-next-generation-real-time-dispatch-platform/)).
- "the real-time signal on dasher wait at the restaurant and number of orders placed are among the
  top 10 most important" ETA features
  ([2020](https://careersatdoordash.com/blog/supercharging-doordashs-marketplace-decision-making-with-real-time-knowledge/));
  delivery events become "real time features like recent average wait times for restaurants"
  ([2022](https://careersatdoordash.com/blog/building-scalable-real-time-event-processing-with-kafka-and-flink/)).
- Iguazu (Kafka and Flink) carries "hundreds of billions of events per day", produced with
  asynchronous writes, replication factor two and "a reasonable linger time between 50ms to 100ms"
  for batching (2022).
- The streaming team, running Kafka and Event Bus for DoorDash, Wolt and Deliveroo at "trillions of
  events per day", is "re-architecting toward diskless, object-store-backed streaming to cut cost",
  weighing "write-path latency versus cost"
  ([job post, 2026](https://job-boards.greenhouse.io/doordashusa/jobs/8049133)).

So the stream inside the dispatch loop is the per-store feedback: if a Dasher arrived at a store a
moment before a dispatch run, and the coordinator has not seen it yet, the agent decides that
store's orders on a counter that no longer exists. This kit runs that one lane on Apache Iggy,
prints its p99 every second, and counts the dispatch decisions made on stale store feedback, and
how many of those the newer feedback would have flipped between speed and batching.

## Run it

Needs Docker and Python 3.10+. Defaults: 200,000 stores in 2,000 markets, 125 orders a second
(970 million orders in Q2 2026), 200,000 Dashers on shift, a dispatch run every second, marketplace
time in real minutes. Store, market and Dasher counts are illustrative; set yours.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python dashers.py &                  # every store's Dashers, assigned to delivered
python dispatch.py                   # coordinator: feedback view + dispatch runs
```

Add `--minute 1` to `dashers.py` to run marketplace time 60 times faster, so a delivery takes
seconds, and `--why` to `dispatch.py` to split each second's latency into Dasher event to broker and
broker to coordinator. Both print a line a second:

```
[dispatch]   370 feedback events/s  event-to-coordinator p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  dispatch runs 1  decisions 125  …
[dashers]    370 feedback events/s  decisions checked 125/s  made on stale store feedback …%  flipped speed/batch …%  …
```

**What the numbers mean.** The kit times each Dasher event from the moment `dashers.py` creates it
to the moment `dispatch.py` has applied it to the coordinator's view, through Python clients, on
whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one
box, at about 860 feedback events/s (20,000 stores, 300 orders/s, a dispatch run every 500 ms,
marketplace time 60× faster), the steady state held p50 11.1 ms, p99 22.0 ms and p99.9 24.2 ms, with
55 of 55 seconds under the 100 ms budget. While a 4 MB/s client analytics flood ran on the same
broker and the same two vCPUs, p99 moved from 22.0 ms to 22.3 ms. In a typical second, 1 of 300
dispatch decisions (0.33%) was made on store feedback that had already changed; the worst second was
4 of 300, and at most 2 of 300 would have flipped between speed and batching. That is the end-to-end
lane through this demo code, not the broker alone. The broker's own tail latency is measured with
`iggy-bench` in `bench/PLAN.md`, on dedicated machines.
([CI run](https://github.com/psinglap/laserdata-kits/actions/runs/39478089715))

| File | What it is |
|---|---|
| `dashers.py` | Every store's Dashers: assigned (now for speed, or held back to batch), arrived, waiting at the counter, picked up, delivered. Publishes each change to `store-feedback`, keyed by store, with a sequence and the resulting feedback. Checks every dispatch decision against the truth. |
| `dispatch.py` | The coordinator: the feedback view, kept from `store-feedback`, and a dispatch run every `--run-ms` over the new orders. `policy()` in `common.py` is a stand-in for your RL agent. |
| `analytics_flood.py` | Client analytics on their own topic, for the isolation test. |
| `kafka_bridge.py` | Reads your Dasher and delivery events from Kafka as a read-only consumer group, keeps each store's feedback and writes it to Iggy, for a shadow run on real traffic. |
| `ci.sh` | The CI run: reduced scale, a 20 s analytics flood next door, pass only on 95% PASS seconds and checked decisions. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Super Bowl rush:** `python dispatch.py --rush 3 --rush-at 60 --rush-for 120`.
   Pass: event-to-coordinator p99 under 100 ms, and stale-feedback decisions under 0.5%.
2. **Client analytics next door:** `python analytics_flood.py --mbps 50` while both run.
   Pass: the feedback lane's p99 moves by less than 10% (on dedicated machines, not one laptop).
3. **Coordinator failover:** stop `dispatch.py` and start it again; it replays the topic from the
   start and prints "catching up" until a whole second of live feedback is inside the budget.
   Pass: every store is back in the view within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <broker:9092> --kafka-topic <dasher-events-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads; producers, the REST proxy, Flink jobs and existing consumers are untouched.
Map your record in `to_event()`: the kit needs the store id, the event (assigned, arrived or
picked up), the delivery id and, if you have it, the market's utilization. Events in the Iguazu
envelope need your Protobuf deserializer there.

## What Apache Iggy 0.9 does not do yet

No Kafka wire protocol (a gateway is in progress), so the bridge copies rather than redirects;
delivery is at-least-once, so readers drop repeats by sequence, as `dispatch.py` does; clustering
is new (Sep 2026).

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored delivery-event topic,
alongside Kafka. No advantage, no migration.

kranti@laserdata.com · laserdata.com
