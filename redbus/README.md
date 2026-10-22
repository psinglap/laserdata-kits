# Real-time boarding-point alerts for redBus, on LaserData

Every bus GPS packet becomes an ENTRY, HALT or EXIT at the boarding point, and a "your bus is arriving" alert
for every passenger waiting there, in milliseconds, even on festival nights. It runs beside YourBus's Kafka, so
the GPS vendor integrations, the consumers and the live-tracking page stay where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_redBus_BoardingPoints.pdf`](LaserData_redBus_BoardingPoints.pdf)

## Why this use case

From redBus's own engineering blog:

- "Everyday about 150 K customers travel with a ticket bought on redBus in about 40 K bus trips. We collect real
  time GPS data from the buses". YourBus turns the GPS feed into live location, "ETA for boarding and dropping
  points", "Contextual notifications to passengers through SMS, push notification and automated calls", "Booking
  window extension for delayed services" and a "Feed for real time settlement of delay insurances".
- The tracking chain runs on Kafka: "We use SVC_DOJ as partitioning key so that all events for a journey are
  processed in order"; "We replay the messages for simulation and debugging purposes." A "Real time GPS scanner"
  "scans the 'Change data set' every second" and pushes the latest packets of active journeys to Kafka; separate
  consumers map GPS, "detect ENTRY/HALT/EXIT events of a bus at a BP", decide the action "with respect to each
  passenger", and dispatch notifications. "GPS data: 20K/min from 15K services"; "Kafka cluster: 3 nodes";
  "130 K messages/min ... across 15 topics. This includes 30 K process state updates and 100 K log messages."
  (redBus Bus Tracking system, Pradhan Boccsa, February 2022)
- In 2026 the cost of a late answer still lands on support: "When a traveler is stranded at a bus stop at
  midnight, unsure if their bus is coming, the last thing they need is to wait in a chat queue". "Live tracking
  failures" and "boarding point confirmation" are among the journey-critical issues that get a phone call.
  (From Chat to Talk, April 2026)

So whether a traveller hears that the bus is at their boarding point while it is still there depends on how
quickly a GPS packet becomes an ENTRY event and that event becomes an alert, across every hop of the chain. This
kit runs that one lane on Apache Iggy, on its own, keyed by SVC_DOJ as YourBus keys it, and prints its p99 every
second.

## Run it

Needs Docker and Python 3.10+. Defaults follow the 2022 post: 15,000 journeys tracked at once, a GPS packet every
45 s per bus (about 20K packets a minute), leaving Bengaluru, Hyderabad and Chennai through their boarding points
(Electronic City, Silk Board, Madiwala, Majestic, Hebbal; Miyapur, Kukatpally, Ameerpet; Koyambedu, Guindy,
Tambaram and others), halting 2–8 minutes at each, with 0–10 passengers boarding per point.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python tracker.py &                  # boarding-point tracker: ENTRY, HALT, EXIT and passenger alerts
python buses.py                      # buses on the road: assignments and GPS packets, and the truth check
```

Both print a line a second:

```
[tracker]    333 packets/s  packet-to-tracker p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  journeys 15,000
             ENTRY … HALT … EXIT …  passenger alerts …
[buses]      333 packets/s  journeys tracked 15,000  bp events checked …  from the right packet …%  late …%
             missed …  passengers alerted …  not alerted …  packet-to-alert p99 …ms
```

`buses.py` runs the same boarding-point rules on every packet it sends, so it knows which packet caused each
ENTRY, HALT and EXIT. It checks every event `tracker.py` publishes (from the right packet, late, or missed) and
every passenger alert (did each passenger boarding there get one, and how long after the packet was created did
it exist). A missed or late alert is a traveller at the bus stop who does not know the bus is already there. Add
`--why` to `tracker.py` to split the latency into packet-to-broker and broker-to-tracker.

**What the numbers mean.** The kit times each GPS packet from the moment `buses.py` creates it to the
moment `tracker.py` has applied it, through Python clients, on whatever machine you run it on. On a shared 2-vCPU
CI runner with the broker and all scripts on one box, with 6,000 journeys sending a packet every 2 s (about 9× the
2022 rate per service) rising to 9,000 journeys from second 20 (a 1.5× surge; journeys already on the road finish
their route, so the lane stayed at about 4,500 packets/s for the rest of the run) and road time running 15× so
buses reach their boarding points inside the minute, three runs held packet-to-tracker p50 7.0 to 11.0 ms, p99
11.6 to 18.2 ms and p99.9 12.6 to 19.8 ms (median of 62 steady-state seconds each), with no second over the 50 ms
budget. Each run checked 19,800 to 20,100 boarding-point events: every one came from the right packet, none late,
none missed. 26,700 to 27,800 passenger alerts per run, none missing, with packet-to-alert p99 of 17.0 to 28.3 ms
(two hops: packet to tracker, then alert to reader). A 10× log burst on a neighbouring topic (to about 1 MB/s,
generated by a Python script on the same two CPUs) did not move the lane's p99 (11.7 to 11.5 ms, 12.9 to 12.6 ms,
18.5 to 18.1 ms). That is the end-to-end lane through this demo code, not the broker alone, and it does not
include the GPS vendor or the mobile network. The broker's own tail latency is measured with `iggy-bench` in
`bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `buses.py` | Buses on live journeys: each journey's assignment (boarding points in route order, passengers per point), then a GPS packet every `--every` seconds keyed by SVC_DOJ, with a per-journey sequence and the position itself. Drives to each boarding point, halts, leaves town; a new departure takes its place. Checks every event and alert against the truth. |
| `tracker.py` | The boarding-point tracker: reads the lane's 16 partitions directly, runs the distance-to-BP rules on every packet, publishes ENTRY, HALT and EXIT to `bp-events` with the packet they came from, and one alert per passenger to `passenger-alerts` on ENTRY. Rebuilds every journey from the retained lane on restart. |
| `common.py` | Topics, the SVC_DOJ key and partition function, boarding points per city, and the ENTRY/HALT/EXIT rules both sides use. |
| `log_flood.py` | A neighbour on the same broker: tracking-consumer log lines at a steady rate, then 10× for a minute. |
| `kafka_bridge.py` | Copies your GPS topic from Kafka into Iggy as consumer group `iggy-shadow`, read-only, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Festival night:** `python buses.py --peak 3 --peak-at 60 --peak-for 600` (three times the journeys tracked
   for ten minutes). Pass: packet-to-tracker p99 under 50 ms, no boarding-point event missed, no passenger left
   without an alert.
2. **A log burst next door:** `python log_flood.py --x 10` while both run (100K log lines a minute, then 10×).
   Pass: the lane's p99 moves by less than 10%. The script prints the rate it actually reached.
3. **Replay:** stop `tracker.py` and start it again; it rebuilds every journey from the retained lane (24 h by
   default, for simulation and debugging too). Pass: it catches up within seconds, and events keep coming from
   the right packet.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --kafka-topic <topic the GPS scanner writes> [--assign-topic <assignments>]
```

The bridge is a plain Kafka consumer in its own group, `iggy-shadow`, from the latest offset: the GPS vendor
integrations, the scanner, and every consumer you run today are untouched. Map your record shape in `to_packet()`
(SVC_DOJ, sequence or device timestamp, lat, lon) and, for boarding-point events, your validated assignments in
`to_assign()`. With real packets, `tracker.py` reports packet-to-tracker latency straight away; compare its
ENTRY, HALT and EXIT with your BP detection's.

## What Apache Iggy 0.9 doesn't do yet

No Kafka wire protocol (a gateway is in progress), so the WebSocket API and existing consumers would read the
lane through the Iggy SDKs or stay on Kafka. Delivery is at-least-once; the kit carries a per-journey sequence
so a repeat or an older packet is ignored. Clustering with replication is new (September 2026).

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored GPS topic, alongside Kafka. No advantage,
no migration.

kranti@laserdata.com · laserdata.com
