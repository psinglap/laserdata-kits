# Real-time charging, theft and crash alerts for Ather Energy, on LaserData

Every charging, LockSafe and crash event from every connected scooter reaches the alert service in
milliseconds, in order per scooter, even while thousands of scooters reconnect after a network drop and flush
their backlog. It runs beside your MQTT broker and Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_AtherEnergy_RiderAlerts.pdf`](LaserData_AtherEnergy_RiderAlerts.pdf)

## Why this use case

From Ather's own engineering blog:

- "Ather's self-managed cloud broker handles 40 billion messages per day and the various applications process
  anywhere between 3000 to 5000 messages per second for customer use-cases, in real-time." The broker gateway
  and Kafka have to handle a "sudden surge of bikes connecting to the cloud at scale" and "network erraticity"
  (Vishwas Bhat, *An Overview of Engineering (Cloud!) Challenges at Ather*, Feb 2022).
- "With a drop in network connectivity and sudden re-connections, there is a significant back-pressure on the
  Kafka and the only option to handle such erratic load of data bursts on the platform is to manually scale the
  applications and data notes by 10X." Today that is handled with processes and scale tests (Vishwas Bhat and
  Hithesh Bhat, *Delivering one lakh ride stats everyday!*, Feb 2022).
- On the scooter, Cloud Connect "saves the data locally until it receives an acknowledgement from the cloud".
  The real-time pipeline processes 5 million records a second at peak and drives "critical alert monitoring",
  the real-time state of charge on the app and "notifications on the charging status" (Vignesh G, *The Ather
  Dataverse*, Sep 2023: 150 billion messages a day from 1.6 lakh vehicles; Kafka, Java, Kubernetes, Spark,
  ClickHouse and more).
- The fleet is now past half a million scooters, and AtherStack 7 added Crash Alerts, LockSafe and ParkSafe
  (Annual Report 2025-26).

So a small, rider-facing slice (charging started and complete, movement while parked, crashes) shares a
pipeline with a very large one, and a reconnect burst is the moment they collide. This kit runs that slice as
its own lane on Apache Iggy and keeps the reconnect backlog off it:

- **Route by the scooter's clock.** Fresh events (and every crash) go on `rider-events`; events older than
  `FRESH_S` (120 s, illustrative) go on `reconnect-backlog`, which ride history files at its own pace. A flush of
  ten minutes of buffered data never queues ahead of a live alert.
- **Drop resends by sequence.** A scooter that never got its acknowledgement sends the packet again; the alert
  service has already applied that sequence, so the rider's phone buzzes once.
- **No stale news.** An event that is history by the time it arrives is filed, not pushed. A crash is the
  exception: it is always sent, marked as delayed.

Ather has not published alert latency targets; the kit's 50 ms budget is for the cloud lane only.

## Run it

Needs Docker and Python 3.10+. Defaults: 500,000 scooters with a rider-facing event every 100 s each (about
5,000 events/s, the top of Ather's published 3,000 to 5,000 a second for customer use cases), 1% resent, and a
reconnect storm at 60 s: 5% of the fleet (25,000 scooters) back online within 10 s after a 10-minute outage.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python alert_service.py &            # the alert service, fed by the rider events lane
python ride_history.py &             # files the reconnect backlog
python scooters.py                   # 500,000 scooters: events out, alerts in, checked
```

They print a line a second:

```
[alert-service]  5,000 events/s  event-to-service p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
                 alerts sent … (charging started …, complete …, parked movement …, crash …, delayed …)
                 late events filed, not pushed …  repeats dropped …
[scooters]       5,000 events/s (steady)  alerts expected …  at the app …, on time (<1000 ms) …
                 missing …  pushed twice …  stale pushed …  late crash sent …  to backlog …
                 event-to-app p50 …ms p99 …ms
[ride-history]   filed …/s  repeats dropped …  behind the newest by … s  oldest event … min old
```

`scooters.py` knows every event it sent, so its app push reader checks each alert against the truth: did every
fresh alert-worthy event reach the app, once, within a second; was any event pushed twice; was any event older
than the fresh window pushed as news. That is the rider's side of a slow or noisy lane.

**What the numbers mean.** CI_NUMBERS

| File | What it is |
|---|---|
| `scooters.py` | Connected scooters riding, parking, charging, moving while parked and (rarely) crashing, an event every `--event-s` s each with the scooter's clock, sequence and whole state, routed by age (`common.route()`). Plays resends, a reconnect storm, and the notifiers (app push, SMS gateway, support console); checks every alert against the truth. |
| `alert_service.py` | The alert service: drops repeats by sequence, files late events, sends charging, LockSafe and crash alerts to `rider-alerts`. `rules()` is where yours go. Prints the lane's p99. |
| `ride_history.py` | Files the reconnect backlog into each scooter's history at its own pace, dropping resends. |
| `telemetry_flood.py` | A neighbour on the same broker: full sensor packets on `vehicle-telemetry`, with a burst at reconnect. |
| `kafka_bridge.py` | Copies your vehicle events topic from Kafka into the lane as a read-only consumer group, routing by the scooter's clock. |
| `common.py` | Topics, partitioning by scooter, event kinds, the fresh window and the routing rule. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Reconnect storm:** `python scooters.py --storm-share 0.2 --offline-min 30 --flush-s 10` (a fifth of the
   fleet back within 10 s, each with 30 minutes buffered). Pass: event-to-service p99 under 50 ms through the
   storm, no alert pushed twice, no stale event pushed, and ride history files the whole backlog.
2. **The full sensor stream next door:** `python telemetry_flood.py --mbps 100 --burst-x 3 --burst-at 60
   --burst-for 10` while the others run. Pass: the lane's p99 moves by less than 10%. The script prints the rate
   it actually reached; one Python process may not reach 100 MB/s on a laptop.
3. **Restart the alert service:** stop `alert_service.py` and start it again; it resumes from its stored offset
   (stored once a second) and still sends alerts for events from the last 30 seconds. Pass: it catches up within
   seconds, and no alert reaches the app twice.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --kafka-topic <vehicle-events-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads, as its own consumer group `iggy-shadow`; the scooters, the MQTT broker, every producer
and the stream processors are untouched. Map your record in `to_event()`: the kit needs the scooter ID, the
event kind, the scooter's own timestamp and, if the record carries it, the packet sequence. Run it next to
your brokers.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and the MQTT broker,
Kafka, Spark and everything else stay as they are. Delivery is at-least-once (events carry their sequence and
alerts name their event, so repeats are harmless). Replicated clusters (Viewstamped Replication) shipped in
September 2026. Storage is local disk per node, not object storage: that keeps the tail in milliseconds, and it
is why this is one lane, not the whole telemetry history. Apache Iggy does not speak MQTT; the scooters keep
talking to your broker.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored vehicle event topic, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
