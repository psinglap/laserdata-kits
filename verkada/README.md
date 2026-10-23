# Real-time alarm triggers and disarms for Verkada, on LaserData

Every camera detection, access-control disarm, arm and panic press for a camera partition reaches the alarm state
service in milliseconds and in order per partition, so a disarm entered inside the entry delay always cancels the
alarm, and every raised alarm reaches the monitoring agents and the response workflows at once. It runs beside
Kafka and Temporal, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Verkada_AlarmLane.pdf`](LaserData_Verkada_AlarmLane.pdf)

## Why this use case

From Verkada's Alarms job post: the team will "design and build low-latency, high-availability backend services
that orchestrate alarm detection and response", with "scalable data systems (e.g. Kafka, Redis)" and Postgres
(Senior Software Engineer, Alarms, 2026). From the Virtual Guard post: "event-driven systems that trigger
automated actions such as alerts, verification, and escalation", with "Kafka, Temporal, Postgres".
From Verkada's help center: the entry delay is the "number of seconds to wait after a trigger event occurs before
raising an alarm"; a single badge at a configured door disarms its partitions; and "for camera partitions, arming
and disarming occur through the cloud, allowing access events to disarm the alarm site."

So for a camera partition, two product lines meet on one key in the cloud: a camera sees a person (person
detection, line crossing, loitering) and starts the entry delay; a badge, Face Unlock or Mobile NFC at a door
disarms the partition. If the disarm reaches the alarm state after the delay has run out, an alarm goes to a
monitoring agent for a person who had already disarmed: contacts get called, and with Immediate Dispatch the
police are called first. Most people disarm with time to spare. The ones who disarm in the last seconds are
the ones a stalled consumer, a rebalance or a backed-up topic turns into false alarms.

This kit runs that one lane on Apache Iggy: detections, disarms, arms and panic presses keyed by alarm partition
into the alarm state service, raised alarms out to the responders. It prints the lane's latency every second and
counts the arrivals that disarmed in time, the false alarms raised for them, and how many a lane 2 seconds slower
would have raised.

Verkada has not published how its alarm state is wired, where the entry delay is evaluated, its alarm volumes or
its latency; this kit is one lane beside Kafka and Temporal, not a replacement for them.

## Run it

Needs Docker and Python 3.10+. Defaults: 200,000 camera partitions (two per site), half armed at start, a 30 s
entry delay, a detection a minute per disarmed partition (about 1,700 events a second), 20 arrivals a second at
armed partitions, 15% of them disarming in the last 3 s of the delay, 0.2 intrusions and 0.05 panic presses a
second. All illustrative: set them to yours with the flags in `sites.py`.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python alarm_state.py &              # the alarm state service, fed by the camera partition lane
python sites.py                      # 200,000 camera partitions: events out, alarms in
```

They print a line a second:

```
[alarm-state]  1,700 events/s  event-to-state p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  detections …
               entry delays started …, cancelled by a disarm … (closest … s before running out),
               disarms applied after the delay ran out …  alarms sent … (intrusion …, panic …)
               countdowns running …  stall gc …ms loop …ms  slowest event->broker … ms, broker->state … ms
[sites]  1,700 events/s (steady)  arrivals …, disarmed in time … (in the last 3 s of the delay …)
         false alarms raised … (with a lane 2 s slower …)  intrusion alarms …  panic alarms …  missing …
         due-to-responders n … p50 …ms p99 …ms  stall gc …ms loop …ms
```

"False alarms raised" counts alarms that reached the monitoring agents for an arrival that disarmed before its
entry delay ran out: only the lane can cause those. "With a lane 2 s slower" counts the same arrivals whose disarm
came less than 2 seconds before the delay ran out, judged at the same moment, so it is what a lane that delivered
the disarm 2 seconds later would have turned into false alarms. Set `--compare-s`. "Missing" counts intrusion and
panic alarms not at the monitoring agents 2 seconds after they fell due. "Due-to-responders" times each alarm from
the moment it fell due (the delay ran out, or the panic press) to each responder.

**What the numbers mean.** The kit times each event from the moment `sites.py` creates it to the moment
`alarm_state.py` has applied it, and each alarm from the moment it fell due to all three responders, through
Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts
on one box, at reduced scale (30,000 camera partitions, a 15 s entry delay, about 1,450 events/s, 15 arrivals a
second and 3 responders), three green runs in a row on the final code held event-to-state p50 3.6–6.1 ms, p99 6.4–11.3 ms and p99.9 7.1–12.6 ms (all 181 steady-state seconds inside 50 ms), and due-to-responders (from the moment an alarm fell due to all three responders) p50 4.3–5.7 ms and p99 4.4–6.4 ms. With 2 MB/s of camera analytics on the same broker, the lane's p99 moved by at most 0.4 ms. Of 897 to 922 arrivals per run that disarmed in time (122 to 131 of them in the last 3 seconds of the delay), none got an alarm; with the same illustrative disarm timing, a lane 2 seconds slower would have raised 72 to 89 false alarms per run (7.9–9.7% of arrivals). All 27 intrusion and 13 panic alarms per run reached the monitoring agents, none missing. That is the end-to-end loop through this demo code, not the broker alone.
The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. The alarm
logic in the kit takes no time; yours adds its own, and the path from the camera and the door reader to Kafka adds
its own before any of this.

| File | What it is |
|---|---|
| `sites.py` | Camera partitions at alarm sites. Cameras publish detections, doors publish access-granted disarms, arms and panic presses go to `camera-partition-events`, keyed by alarm partition, with the partition's event sequence. Three responders (monitoring agents, notifications, response workflows) read `alarms-raised`; the monitoring agents check each alarm against what the site really did. |
| `alarm_state.py` | The alarm state service: armed or not, and the entry-delay countdown, per partition; raises intrusion alarms when a countdown runs out and panic alarms at once. Prints the lane's p99. |
| `analytics_flood.py` | People and vehicle analytics metadata from cameras, on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your alarm partition events from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `common.py` | Topics, partitioning by alarm partition, event and alarm kinds. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Morning opening:** `python sites.py --peak 5 --peak-at 60 --peak-for 120`.
   Pass: event-to-state p99 under 50 ms, and no arrival that disarmed in time gets an alarm.
2. **Camera analytics next door:** `python analytics_flood.py --mbps 50` while both run.
   Pass: the camera partition lane's p99 moves by less than 10%.
3. **Restart the alarm state service:** stop `alarm_state.py` and start it again; it resumes from its stored offset
   (stored once a second), rebuilds every partition's state and running countdowns from the log, and still raises
   alarms that fell due in the last 30 seconds.
   Pass: it catches up within seconds, no intrusion alarm goes missing, and no alarm reaches a responder twice as a
   new alarm.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --kafka-topic <camera-trigger-topic> --kafka-topic <access-event-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads; cameras, access control, Command, every producer, the Temporal workflows and the monitoring
console are untouched. Map your record in `to_event()`: the kit needs the alarm partition ID, the event kind, the
event's time and the partition's entry delay. Run it inside your AWS account, next to your brokers.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and everything else stays
on Kafka. Delivery is at-least-once (events carry their partition's sequence and alarms name the event that
caused them, so repeats are harmless). Replicated clusters (Viewstamped Replication) shipped in September 2026; a
life-safety path should run replicated, and the evaluation measures failover. Storage is local disk per node, not
object storage: that keeps the tail in milliseconds, and it is why this is one lane, not video or the whole
analytics history. Go services can use the Go SDK. LaserData runs inside your own AWS account (BYOC) or
self-hosted, which matters for FedRAMP workloads.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored alarm event topic, alongside Kafka and
Temporal. No advantage, no migration.

kranti@laserdata.com · laserdata.com
