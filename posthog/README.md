# Real-time event streams for PostHog's Live tab, on LaserData

Every captured event goes to a small live lane keyed by project token, so a project's events sit on one
partition, in order. The livestream pod that serves a Live view reads only the partitions of the projects
its viewers have open, straight from the log, in milliseconds: no pod reads the whole firehose for a few
watched projects, no event is passed between pods through Redis, and a newly opened view is filled with the
project's last minute from the lane. It runs beside MSK and WarpStream, so capture, ingestion and ClickHouse
stay where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_PostHog_LiveEvents.pdf`](LaserData_PostHog_LiveEvents.pdf)

## Why this use case

From PostHog's own monorepo and docs:

- "Hog 3000 powers live event stream on PostHog": a Go service that reads Kafka and streams events to the
  browser over Server-Sent Events ([livestream README](https://github.com/PostHog/posthog/blob/master/livestream/README.md)).
- "livestream consumes the event firehose from MSK"; about two thirds of its fetches crossed AZs, around
  $220 a day across prod-us and prod-eu ([PR #111332](https://github.com/PostHog/posthog/pull/111332), October 2026).
- "Livestream spends most of its CPU parsing events that no one is watching" ([PR #111908](https://github.com/PostHog/posthog/pull/111908),
  October 2026).
- Every event is published to a Redis channel per project token "whether or not any viewer is subscribed";
  "most publishes reach zero receivers while still crossing availability zones", and under load "events for
  tokens that do have viewers get dropped" ([PR #109338](https://github.com/PostHog/posthog/pull/109338), October 2026).
- Synchronous Redis publishes "capped throughput at ~20K events/sec" ([#50877](https://github.com/PostHog/posthog/pull/50877),
  March 2026); "consistently seeing old events" ([#46071](https://github.com/PostHog/posthog/pull/46071), January 2026);
  a silent consumer stall left the Live tab at zero users online ([#60425](https://github.com/PostHog/posthog/pull/60425), May 2026).
- Live promises "Seconds" of latency over "the last few minutes", for launches, campaigns and deploys
  ([PostHog docs: Live](https://posthog.com/docs/web-analytics/live)).

The firehose is partitioned for ingestion, not for viewers. This kit partitions a small copy by project, so
the pod with the viewer reads the viewer's project itself. It prints the lane's p99 every second and checks
every watched project's events by sequence number.

## Run it

Needs Docker and Python 3.10+. Defaults: 100,000 projects sending 50,000 events a second, skewed so a few big
projects send most of them; a 256-partition lane; 60 Live views open on the node, with one viewer leaving and
another project opening every 2 s; and one project's launch going from 200 to 10,000 events a second for a
minute, 30 s in.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python live.py &                     # a livestream pod with 60 Live views open
python capture.py &                  # 100,000 projects, 50,000 events/s
python launch.py                     # one project's launch, 30 s in
```

They print a line a second:

```
[live]    4,210 msgs/s  capture-to-live p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  views 61
          partitions read 52/256  read …/s (other projects …)  missed 0 (total 0)  dup 0
          users online … (launch …, … ev/s)  opened 1 filled 1 with … events (oldest 59.8s)
[capture] 50,000 events/s  projects sending … of 100,000  partitions written 256/256
[launch]  10,000 events/s for phc_launch_demo (LAUNCH)
```

`live.py` checks every watched project's sequence numbers, so an event that never reached the view is
counted as missed and a repeat as a duplicate. It also prints how much of the firehose it had to read (the
other projects that share its viewers' partitions), users online per watched project, and how full each new
view was when it opened. Those are the business numbers.

**Measured in CI** (GitHub Actions, shared 2-vCPU runner, Apache Iggy 0.9.0 in Docker, Python clients, reduced
scale: 5,000 projects at 2,500 events/s, 64 partitions, 41 views open, one view swapped every 2 s, and the launch
project going from 100 to 1,500 events/s for 25 s): capture-to-live p50 8.3 ms, p99 17.6 ms, p99.9 19.3 ms
(medians of the per-second figures over 64 steady seconds, all PASS against 100 ms). During the launch, the
p99 stayed at 17.4 ms. No watched event missed or shown twice; all 41,502 launch events shown; 38 views opened
and filled from the lane with their last 60 s (44,153 events). The node read 33 of 64 partitions, 66% of the
firehose, half of it for other projects sharing those partitions; more partitions or fewer views per pod
lower that share. These are end-to-end times through Python clients on a shared runner, mostly the reader's
polling round over its partitions; they show the shape, not the engine's floor.

| File | What it is |
|---|---|
| `capture.py` | Capture for every project: one compact record per event (token, sequence, distinct_id, event, path, time) on `live-events`, keyed by token, one batch per partition per tick. |
| `live.py` | A livestream pod: reads only its viewers' partitions directly, shows and checks each watched event, counts users online, opens and closes views, fills new views from the lane. |
| `launch.py` | One project's launch: its traffic jumps to 10,000 events/s on its one partition. |
| `kafka_bridge.py` | Writes the lane from your MSK events topic as a read-only consumer group, using the `token` and `distinct_id` headers capture already writes. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A launch:** `python launch.py --peak 10000 --at 30 --duration 60`. Pass: every launch event is shown,
   its p99 stays inside the budget, and the other views' p99 does not move.
2. **Busy afternoon:** `python live.py --churn-every 0.5 --viewers 200`. Pass: no watched event missed or
   shown twice, and every opened view filled with its last minute.
3. **Restart a pod:** stop `live.py` and start it again. It stores no offsets on the broker; it starts each
   watched partition at the time of the open and fills views from the lane. Pass: views are live again within
   a second or two.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <msk-brokers> --kafka-topic <events-topic> \
  --partitions 0-31 --client-rack use1-az1 -X security.protocol=SSL
```

The bridge only reads, as its own consumer group `iggy-shadow`, the topic livestream reads today
(`LIVESTREAM_CONSUMERS_EVENT_TOPIC`); capture, ingestion and livestream are untouched. Map your record in
`to_live()`. In production, capture would write the lane itself with the Rust SDK, and the bridge goes away.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible API (in progress), so the events topic, ingestion and ClickHouse's Kafka engine tables stay
on Kafka. Delivery is at-least-once; the per-project sequence number makes repeats harmless. Storage is local
disk, which is why this is a ten-minute live lane, not your event history. Replicated clusters shipped in
September 2026.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one region's Live traffic, alongside MSK.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
