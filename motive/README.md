# Real-time cargo door and temperature alerts for Motive, on LaserData

Every door change and reading from an Asset Gateway reaches the alert rules, and every alert reaches the driver
and the back office, in milliseconds and in order per asset, while the door is still open. It runs beside Kafka
and Flink, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Motive_CargoAlerts.pdf`](LaserData_Motive_CargoAlerts.pdf)

## Why this use case

From Motive's Equipment Monitoring job posts: the team designs and operates "the ingestion and stream-processing
pipelines (Kafka, Flink) that turn this raw sensor telemetry into accurate real-time locations, geofence events,
trips, and alerts", from "Asset Gateways, BLE environmental and door sensors, and Beacons", for "hundreds of
thousands of IoT devices" (Staff Software Engineer, Backend w/Kafka, 2026). From Motive's Door and Environmental
Sensor launch: "real-time door open/close alerts tied to precise location and time", and rules "so operations
teams are notified if a door opens outside a customer location, yard, or other approved geofence" (May 2026).
From the Reefer Monitoring page: sensors "capture and transmit data every minute", and drivers and back office
"get notified the moment the temperature exceeds thresholds".

A door alert is worth most while the door is open: the driver can close it or explain it, the back office can call
before the load is touched. Most door openings at a stop are short. This kit runs that one lane on Apache Iggy:
door changes and readings keyed by asset into the alert rules, alerts out to the driver app, the fleet dashboard
and webhooks. It prints both hops' latency every second and counts the door alerts that reached the driver app
while the door was still open. Because an asset's events stay in order in one partition, the rules can also name
the door event behind a temperature excursion, the link Motive's product team describes between door,
temperature and humidity.

Motive has not published its alert latency or where its alert rules run; this kit is one lane beside Kafka and
Flink, not a replacement for them.

## Run it

Needs Docker and Python 3.10+. Defaults: 300,000 assets with a reading a minute each (about 5,000 readings a
second), a door opening every two hours per asset on average, 5% of openings outside an approved geofence, and
illustrative door durations: most 20–90 s, some 2–15 min at a dock (`fleet.door_minutes`; set them to yours).

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python alert_rules.py &              # the alert rules, fed by the cargo sensor lane
python fleet.py                      # 300,000 assets: events out, alerts in
```

They print a line a second:

```
[alert-rules]  5,000 events/s  event-to-rules p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  door changes …
               alerts sent … (door outside geofence …, temperature …, cause named …)  stall gc …ms loop …ms
[fleet]  5,000 events/s (steady)  door changes …  doors opened outside a geofence …
         door alerts at the driver app …, while the door was still open … (at 60 s it would be …)
         temperature alerts …  event-to-notifiers n … p50 …ms p99 …ms  stall gc …ms loop …ms
```

"At 60 s it would be …" counts the same door alerts whose door stayed open longer than 60 seconds: what a lane
that delivered the alert a minute after the door opened (one environmental-reading interval) would still have
caught while the door was open. Set `--compare-s`. "Stall" is the longest garbage-collection pause and event-loop
stall in that process that second, so a slow second can be traced rather than guessed at.

**What the numbers mean.** The kit times each event from the moment `fleet.py` creates it to the moment `alert_rules.py` has evaluated it, and each alert from the gateway event that raised it to all three notifiers, through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (120,000 assets, a door opening every 30 minutes per asset, about 2,200 events/s and 3 notifiers), two green runs held event-to-rules p50 11.1–12.6 ms, p99 21.6–24.9 ms and p99.9 23.9–27.8 ms (59 of 59 steady-state seconds inside 50 ms in each), and event-to-notifiers (both hops) p50 13.8–15.5 ms and p99 24.8–29.2 ms. With 2 MB/s of dead-zone backfill on the same broker, the lane's p99 moved by under 1 ms (21.4 to 21.6 ms, 24.7 to 25.4 ms). Of 187 and 212 door-outside-geofence alerts, every one reached the driver app while the door was still open; with the same illustrative door durations, an alert that took 60 s would have arrived after the door had shut for 79 (42%) and 86 (41%) of them. That is the end-to-end loop through this demo code, not the broker alone. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. The rules in the kit are stand-ins that take no time; yours add their own.

| File | What it is |
|---|---|
| `fleet.py` | A fleet of trailers and reefers. Each gateway publishes door changes as they happen and a reading a minute to `cargo-sensor-events`, keyed by asset, with the asset's whole current state and event sequence. Three notifiers (driver app, fleet dashboard, webhooks) read `cargo-alerts`; the driver app checks whether each door alert's door is still open. |
| `alert_rules.py` | The alert rules: a door opened outside every approved geofence, and cargo temperature out of range with the door event named as the likely cause. `rules()` is where yours go. Prints the lane's p99. |
| `backfill_flood.py` | Readings buffered in dead zones, uploaded on reconnect, on their own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your sensor events topic from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `common.py` | Topics, partitioning by asset, approved geofences (`sites()`), set points. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Morning dispatch:** `python fleet.py --peak 5 --peak-at 60 --peak-for 120`.
   Pass: event-to-rules p99 under 50 ms, and every door alert reaches the driver app while its door is open.
2. **Dead-zone backfill next door:** `python backfill_flood.py --mbps 50` while both run.
   Pass: the cargo sensor lane's p99 moves by less than 10%.
3. **Restart the alert rules:** stop `alert_rules.py` and start it again; it resumes from its stored offset (stored
   once a second), and still sends alerts for events from the last 30 seconds.
   Pass: it catches up within seconds, and no alert reaches a notifier twice as a new alert.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --kafka-topic <sensor-events-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads; the gateways, ingestion, every producer and the Flink jobs are untouched. Map your record in
`to_event()`: the kit needs the asset ID, door state, cargo temperature and set point, and position. Put your
approved geofences in `common.sites()`. Run it inside your AWS account, next to your brokers.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and the Flink jobs and
everything else stay on Kafka. Delivery is at-least-once (events carry their sequence and alerts name their event,
so repeats are harmless). Replicated clusters (Viewstamped Replication) shipped in September 2026. Storage is local
disk per node, not object storage: that keeps the tail in milliseconds, and it is why this is one lane, not the
whole telemetry history. There is no Ruby SDK; Go and Kotlin services can use the Go and Java SDKs.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored sensor topic, alongside Kafka and Flink.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
