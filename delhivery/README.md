# Real-time load checks for Delhivery's hub docks, on LaserData

Every load scan and plan revision at a hub or gateway dock reaches the load check, and every misload reaches the
loader's handheld, in milliseconds and in order per trip, before the next bag is scanned. It runs beside MSK, so
nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Delhivery_DockScans.pdf`](LaserData_Delhivery_DockScans.pdf)

## Why this use case

From Delhivery's own words:

- "The data comes from shipment tracking, order tracking, GPS, biometrics, handheld devices, sorters, weights,
  clients, and facilities. It moves through various systems and services using several real-time and batch
  pipelines." On Amazon MSK: 500+ topics, 110+ producers, 300+ consumers, 55+ Kafka Streams apps, 10,000–12,000
  messages a second in and 50,000–55,000 out. Before MSK, "We had to allocate two developers on a constant basis to
  manage our Apache Kafka infrastructure and maintain uptime." (Akash Deep Verma, AWS Big Data Blog, July 2020)
- "Launched automated dock, yard, and trailer management to ensure faster unloading and loading of trucks in
  hubs." 47 automated sortation centres and 129 gateways, a rated automated sort capacity of 10.4 million shipments a
  day, and more than 1 billion Express Parcel shipments in FY26. (Annual Report FY26)
- 6.2 million shipments "shipped on a single day" in September 2025. (NSE filing, 3 October 2025)
- 800+ data-lake pipelines "ingest 60k messages per second" from CDC connectors through Amazon MSK to S3.
  (AWS Storage Blog, November 2024)

Loading is the moment a wrong bag becomes a wrong trailer. A misload flagged while the bag is still at the door
costs a few seconds; one found at the destination costs a re-route and a late shipment. This kit runs that one
lane on Apache Iggy: plan revisions, load scans and seals keyed by trip into the load check, misload alerts out to
the loader's handheld, the dock supervisor and the yard view. Because a trip's events stay in order in one
partition, a plan revision is always applied before the scans that follow it, so the check never judges a bag
against a plan that has already changed.

Delhivery has not published how misloads are caught today, or whether dock scans are on MSK now; this kit is one
lane beside MSK, not a replacement for it.

## Run it

Needs Docker and Python 3.10+. Defaults: 1,000 docks loading at once, two loaders each, a scan every 3–5 s per
loader (up to 500 load scans a second), 80–200 bags and loose shipments per trailer, 10% of trips revised by
linehaul planning while loading, loaders who keep loading by the old plan for 5–30 s, and 0.1% of bags from the
wrong cart. All illustrative; set them to yours.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python dock_check.py &               # the load check, fed by the dock scan lane
python docks.py                      # 1,000 docks: scans out, alerts in, every alert checked against the truth
```

They print a line a second:

```
[dock-check]  500 events/s  scan-to-check p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  load scans …
              plan revisions …  seals …  misloads flagged …  alerts sent …  open trips …
              stall gc …ms loop …ms  slowest event->broker … ms, broker->check … ms
[docks]  500 scans/s (steady)  seals …  plan revisions …  misloads …
         flagged at the loader's handheld …: before the loader's next scan …, before the seal …
         (a lane 5 s behind: before the next scan …)  false alerts …  missed …
         scan-to-handheld n … p50 …ms p99 …ms  stall gc …ms loop …ms
```

`docks.py` knows every misload: a bag from the wrong cart, or one loaded by a plan linehaul planning has just
revised. It checks every alert the load check publishes: was it a real misload, did it reach the loader's handheld
before that loader's next scan (the bag is still at the door) and before the seal, and is any misload never flagged?
"A lane 5 s behind" counts the misloads a lane delivering the alert five seconds late would still have caught before
the loader's next scan (set `--compare-s`). "Stall" is the longest garbage-collection pause and event-loop stall in
that process that second, and "slowest event->broker" and "broker->check" split that second's slowest event between
the send and the read.

**What the numbers mean.** The kit times each event from the moment `docks.py` creates it to the moment
`dock_check.py` has checked it, and each alert from the load scan that raised it to all three notifiers, through
Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on
one box, at about 920 events a second (2,000 docks, a scan every 2–4 s, loading time compressed to 12–30 bags a
trailer so trailers turn over inside the minute, and plan revisions and wrong carts raised so misloads happen often
enough to judge), three runs held scan-to-check p50 2.7–5.8 ms, p99 5.4–10.7 ms and p99.9 6.2–12.3 ms, with all
182 steady-state seconds inside 100 ms, and scan-to-handheld (both hops) p50 5.3–8.5 ms and p99 8.1–13.4 ms. Of
1,011 to 1,096 misloads per run, every one reached the loader's handheld before that loader's next scan and before
the seal, with no false alert and none missed; a lane 5 s behind would have caught 13–17% of them before the next
scan (only the loader's last bag before a seal). With 2 MB/s of CDC changes on the same broker, the lane's p99
moved by 0.4–1.1 ms. That is the end-to-end loop through this demo code, not the broker alone. The broker's own
tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. The rule in the kit is a
stand-in that takes no time; yours adds its own.

| File | What it is |
|---|---|
| `docks.py` | Docks at hubs and gateways, each loading one outbound trailer at a time. Plans, load scans and seals go to `dock-scans`, keyed by trip, with the trip's event sequence. Three notifiers (loader's handheld, dock supervisor, yard view) read `dock-alerts`; the handheld checks every alert against the truth. |
| `dock_check.py` | The load check: a bag whose destination is not on the trip's current plan is a misload. `check()` is where your rules go. Reads every partition directly; prints the lane's p99. |
| `cdc_flood.py` | Change data capture into the data lake, on its own topic, for the isolation test. |
| `msk_bridge.py` | Copies your dock scan topic from MSK into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `common.py` | Topics, partitioning by trip, gateways and destinations. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Festive peak:** `python docks.py --peak 3 --peak-at 60 --peak-for 120` (three times the scan rate for two
   minutes). Pass: scan-to-check p99 under 100 ms, and every misload reaches the handheld before the loader's next
   scan.
2. **CDC next door:** `python cdc_flood.py --mbps 50` while both run (a connector re-snapshotting a large table).
   Pass: the dock scan lane's p99 moves by less than 10%.
3. **Restart the load check:** stop `dock_check.py` and start it again; it reads the retained lane from the start,
   rebuilds every open trip's plan, and still sends alerts for scans from the last 30 seconds. Pass: it catches up
   within seconds, and no misload is missed.

## Shadow your own traffic

```
pip install confluent-kafka
python msk_bridge.py --bootstrap <msk-brokers> --kafka-topic <dock-scan-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads, in its own consumer group `iggy-shadow`; the handheld apps, the scan services, every
producer and every consumer group on MSK are untouched. Map your record in `to_event()`: the kit needs the trip ID
and a plan (its destinations), a load scan (bag, destination, scanner) or a seal. Run it inside your AWS account in
ap-south-1, next to your brokers.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and the Kafka Streams apps,
the CDC pipeline and everything else stay on MSK. Delivery is at-least-once (events carry the trip's sequence and
alerts name their scan, so repeats are harmless). Replicated clusters (Viewstamped Replication) shipped in September
2026. Storage is local disk per node, not object storage: that keeps the tail in milliseconds, and it is why this is
one lane, not the data lake's feed.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored dock scan topic, alongside MSK.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
