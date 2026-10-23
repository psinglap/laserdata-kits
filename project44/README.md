# Real-time carrier position sharing for project44, on LaserData

Every truck position reaches the customer whose shipment is on board, in milliseconds, and never after the share
ends: positions and shipment changes travel in order per vehicle on one lane, into a share gate that applies
project44's own data-sharing rule. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.project44-kit
- **One slide:** [`LaserData_project44_PositionSharing.pdf`](LaserData_project44_PositionSharing.pdf)

## Why this use case

From project44's own words:

- "project44 sources data from tens of thousands of different sources ... This data is ingested, transformed, and
  then sent to the core tracking systems for processing using Kafka." The ingestion pipeline "enables carriers to
  control with-whom and where their data is shared", granting "access to vehicle data only for the time that the
  shipper had goods on the vehicle". (Troy Mass, "System Architecture at project44", project44 TechBlog, 2022)
- Sharing starts when the right vehicle enters "the outer geofence around the loading place (usually a 50-km
  radius)" inside the time window, and "stops as soon as the vehicle departs after final delivery"; "Carriers can
  share their whole fleet 24/7"; "Any data project44 receives from the vehicles which haven't been assigned to any
  shipment is not shared." (project44 carrier help centre, "How Data Is Shared")
- "Our use of Kafka at Project44 has continued to expand ... to the point where a large portion of our product
  depends on it", with rising "storage, network, and CPU costs" and a plan "to scale down some of our largest Kafka
  clusters". (Ralph Brendler, "Experiments in Kafka Compression", project44 TechBlog, 2023)
- "Manage and improve our Kafka and Strimzi streaming infrastructure, supporting high-throughput event pipelines
  that are foundational to project44's real-time data capabilities"; "Lead cost optimization initiatives".
  (Senior Software Engineer, DevOps/SRE, Foundations Engineering, job posting, 2026)

The share decision is a per-vehicle state machine with two inputs: positions, and changes to the shipments on that
vehicle (assigned, stopped, moved to another tractor). This kit runs that one lane on Apache Iggy: positions and
shipment changes keyed by vehicle into a share gate, shared positions, "tracking started" and "completed" out to the
customer push, the visibility screens and the ETA service. Because a vehicle's events stay in order in one
partition, a stop is always applied before the next position, so no position reaches a customer after their
shipment has left the truck.

project44 has not published how the share decision is implemented today, how many positions a second it handles,
or whether positions and shipment changes share a topic; this kit is one lane beside Kafka, not a replacement for
it, and its defaults are illustrative.

## Run it

Needs Docker and Python 3.10+. Defaults: 100,000 connected trucks (whole fleets shared), a position a minute each
(about 1,700 a second), 1,300 customers, shipments assigned with a time window, opened at the 50 km pick-up
geofence and completed on leaving the delivery geofence (1 km in the kit; the real radius is not published) or
after a 3-hour dwell, and 2% of shipments stopped mid-route. All illustrative; set them to yours.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python share_gate.py &               # the share gate, fed by the position share lane
python fleet.py                      # 100,000 trucks: positions and shipment changes out, updates in, all checked
```

They print a line a second:

```
[share-gate]  1,700 events/s  position-to-gate p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  positions …
              shared …  withheld …  tracking started …  completed …  assigned …  stopped …  updates sent …
              open shipments …  stall gc …ms loop …ms  slowest event->broker … ms, broker->gate … ms
[fleet]  1,700 positions/s (steady)  shipments assigned …  tracking started …  completed …  stopped …
         positions customers were entitled to …: shared with the right customer …, leaks …, missed …, late …
         withheld … (a gate 5 s late on stops: … sent after the shipment left the truck)
         position-to-customer n … p50 …ms p99 …ms  stall gc …ms loop …ms
```

`fleet.py` applies the same rule (`common.step`) to the true state of every truck, so it knows which customer is
entitled to each position. It checks every update the gate publishes: did each entitled customer get it (missed
otherwise), did any customer get a position it was not entitled to at that moment (a leak), and did any arrive
after its 5-second audit window (late). "Withheld" counts positions from trucks with no open shipment. "A gate 5 s
late on stops" counts the positions a gate that learned of stops five seconds late, for example through a separate
changelog, would have sent to a customer whose shipment had already left the truck (set `--compare-s`). "Stall" is
the longest garbage-collection pause and event-loop stall in that process that second, and "slowest event->broker"
and "broker->gate" split that second's slowest event between the send and the read.

**What the numbers mean.** The kit times each event from the moment `fleet.py` creates it to the moment
`share_gate.py` has applied it, and each shared position from its creation to all three notifiers, through Python
clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one
box, at about 1,530 events a second (12,000 trucks, a position every 8 s, driving sped up to 20 km between
positions so shipments open and close inside the minute, and stops raised to 10% of shipments so there are enough
to judge), three runs held position-to-gate p50 4.2–4.4 ms, p99 7.9–8.1 ms and p99.9 8.8–9.1 ms, with all 182
steady-state seconds inside 100 ms, and position-to-customer (both hops) p50 6.9–7.2 ms and p99 11.9–12.2 ms.
Each run opened 4,230–4,258 shipments, completed 1,290–1,325 and stopped 408–427 mid-route; of 70,838 to 71,153
positions customers were entitled to, every one reached the right customer, with no leak, none missed and none
late, and 20,478–20,833 positions from trucks with no open shipment were withheld. A gate 5 s late on stops would
have sent 329–347 positions per run to a customer whose shipment had already left the truck. With 2 MB/s of a
72-hour replay on the same broker, the lane's p99 moved by −0.3 to +0.2 ms. That is the end-to-end loop through
this demo code, not the broker alone. The broker's own tail latency is measured with `iggy-bench` in
`bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `fleet.py` | Connected trucks driving to pick-ups and deliveries. Shipment changes and positions go to `fleet-positions`, keyed by vehicle, with the truck's event sequence. Three notifiers (customer push, visibility screens, ETA service) read `shipment-updates`; the customer push checks every update against the truth. |
| `share_gate.py` | The share gate: opens a shipment at the pick-up outer geofence inside its window, shares every position until the truck leaves the delivery geofence, a dwell timeout or a stop. Reads every partition directly; prints the lane's p99. |
| `common.py` | Topics, partitioning by vehicle, the sharing rule (`step`), sites. |
| `replay_flood.py` | A 72-hour replay of other topics, on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your normalized position and shipment topics from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Morning dispatch peak:** `python fleet.py --peak 3 --peak-at 60 --peak-for 120` (three times the position rate
   for two minutes). Pass: position-to-gate p99 under 100 ms, no leak, none missed.
2. **A 72-hour replay next door:** `python replay_flood.py --mbps 50` while both run (your own Collaborative
   Visibility stress test rewound offsets by 72 hours). Pass: the lane's p99 moves by less than 10%.
3. **Restart the gate:** stop `share_gate.py` and start it again; it reads the retained lane from the start,
   rebuilds every truck's open shipments, and still sends updates for positions from the last 30 seconds. Pass: it
   catches up within seconds, and no position leaks (re-sent copies of positions already delivered show as late).

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <kafka-bootstrap> --kafka-topic <normalized-position-topic> \
  --kafka-topic <shipment-change-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads, in its own consumer group `iggy-shadow`; the carrier integrations, the ingestion pipeline,
the tracking systems and every producer and consumer group on Kafka are untouched. Map your record in
`to_event()`: the kit needs the vehicle and a position, an assignment (shipment, customer, pick-up and delivery
coordinates, window start) or a stop. Compressed topics are read as they are. Run it in your GCP project, next to
the Strimzi brokers on GKE.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and the ingestion pipeline,
the tracking systems, Collaborative Visibility and everything else stay on Kafka. Delivery is at-least-once (events
carry the vehicle's sequence and updates name the position they came from, so repeats are harmless). Replicated
clusters (Viewstamped Replication) shipped in September 2026. Storage is local disk per node, not object storage:
that keeps the tail in milliseconds, and it is why this is one lane, not a home for three days of replayable
history. Regional data locks (carriers can keep data in a region) mean one cluster per region, not one global
cluster.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored position topic, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
