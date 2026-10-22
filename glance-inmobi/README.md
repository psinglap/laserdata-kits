# Real-time frequency caps and dismissals for Glance lock-screen ads, on LaserData

Every impression and dismiss beacon reaches every ad server in milliseconds, before that phone asks for its
next prefetch refill, even in a sale's first hour. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.glance-inmobi-kit
- **One slide:** [`LaserData_GlanceInMobi_AdState.pdf`](LaserData_GlanceInMobi_AdState.pdf)

## Why this use case

From Glance's own job posts and published case studies:

- The ad platform serves "the lock screen and mobile surfaces" that 100M+ people see daily. Ad requests are
  "decided in milliseconds on a low-end Android phone". The new ad monetization team will support ads that are
  prefetched and sometimes shown offline, "with caching, expiry, pacing, and delayed impression beacons", own
  per-user frequency and ad load across all demand sources, and balance ad load against dismiss rate. The serving
  stack is Java and Spring Boot, with event pipelines on Kafka, Spark or Flink (Staff Engineer, Ad Monetization,
  Glance, 2026).
- Glance runs Kafka on Confluent as the "central nervous system" for user interaction events from about 200 million
  active users, who unlock their phones "at least 150 times a day" (Arvind Jayaprakash, SVP Technology, Confluent
  blog, Aug 2023).
- Glance serves over 300 million users across 450 million devices, and moving to "a Kafka-based event-driven
  architecture with intelligent retries and asynchronous operations resulted in 4x throughput improvement"
  (Google Cloud customer blog, July 2025).

A phone syncs: it uploads the beacons it stored (the ads it showed, the ones the user swiped away) and, a mobile
round trip later, asks for its next prefetch refill. The refill can land on any ad server. If that server has not
yet applied the sync's beacons, it puts a campaign past its cap, or the advertiser the user just dismissed, back on
the lock screen for the next unlocks. Some phones were offline and their beacons are minutes old already; the lane
cannot win that time back, so it must not add to it. This kit runs that one lane on Apache Iggy, prints its latency
every second, and prints stale placements: ads placed after a beacon that rules them out had already reached the
collector.

## Run it

Needs Docker and Python 3.10+. Defaults: 3,000 phone syncs a second over 500,000 users, 1% of them heavy users
making 30% of the syncs, 1–3 impressions per sync with 6% dismissed, 25% of syncs from phones that were offline,
40 campaigns capped at 3 per user per 60-second window (a compressed day), 3 ads per refill, 3 ad servers.

```
docker compose up -d                       # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt            # apache-iggy 0.9.0
python adserver.py --name ad-1 --pod 0 &   # three ad servers, each reading every beacon
python adserver.py --name ad-2 --pod 1 &   # and its own share of refill requests
python adserver.py --name ad-3 --pod 2 &
python beacons.py                          # the beacon collector and its phones
```

Each ad server prints a line a second:

```
[ad-1] beacons  4,700/s  beacon-to-ad-server p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  refills 1,000  ads placed 3,000
       skipped at cap …  dismissed …  stale placements 0  this run … of … (…%)  gaps 0
```

**What the numbers mean.** The kit times each beacon from the moment the collector receives it to the moment an
ad server has applied it, through Python clients, on whatever machine you run it on. CI result on a shared 2-vCPU
runner, reduced scale (one collector at 800 phone syncs a second over 50,000 users, about 1,700 beacons a second,
3 ad servers taking about 270 refills a second each, 20-second windows), ranges over three green runs
([39855575823](https://github.com/psinglap/laserdata-kits/actions/runs/39855575823),
[39856089826](https://github.com/psinglap/laserdata-kits/actions/runs/39856089826),
[39856495760](https://github.com/psinglap/laserdata-kits/actions/runs/39856495760)): beacon-to-ad-server
**p50 3.1–6.3 ms, p99 5.5–11.1 ms, p99.9 6.0–12.9 ms** (medians of 64–65 steady-state seconds per server, worst
second p99 14.5–42.8 ms), every steady-state second inside the 50 ms budget. With 2 MB/s of interaction events on the
same broker, p99 moved by 0.3–0.6 ms. Each ad server skipped about 1,900 campaigns at their cap and about 1,800 for a
dismissed advertiser over the run, and **1 to 6 ads in ~59,000 (0.002–0.010%) were stale placements**; those are a
heavy user syncing again while the previous refill request is still on its way. A restarted ad server rebuilt its
view from the log in 0.3–0.6 s (about 16,000 users, 56,000 beacons) before taking refills, with no sequence gaps. The
same load with every ad server applying beacons 2 seconds late (`--hold-ms 2000`): **2.6–2.9% stale, about 1,000 to
1,100 ads in 38,000**, some 300 times more. That is the end-to-end lane through this demo code, not the broker alone;
the broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `beacons.py` | The beacon collector with its phones. Publishes every impression and dismiss as a running total per user and campaign or advertiser, with a sequence, keyed by user; sends each phone's refill request a mobile round trip later to a random ad server. |
| `adserver.py` | One ad server. Reads every partition of the lane directly to keep every user's caps and dismissals current, fills its share of refills with ads still under cap from advertisers not dismissed, and counts stale placements. Rebuilds its view from the log before taking refills. |
| `interactions_flood.py` | A heavy content interaction feed on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your beacon topic from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `common.py` | Topics, keys, partitioning and the record shape. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A sale's first hour:** `python beacons.py --peak 5 --peak-at 60 --peak-for 120`.
   Pass: beacon-to-ad-server p99 under 50 ms, and stale placements under 0.1% of ads placed.
2. **Interaction events next door:** `python interactions_flood.py --mbps 50` while the lane runs.
   Pass: the lane's p99 moves by less than 10%.
3. **Restart an ad server:** stop one and start it again; it reads the lane from the start of the previous cap
   window, rebuilds every user's caps and dismissals, then takes refills.
   Pass: it rebuilds in seconds, with no sequence gaps and nothing counted twice.

What-if: `--hold-ms 2000` on every ad server applies beacons only once they are 2 seconds old, like a pipeline that
runs in micro-batches. Watch the stale placements.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <broker> --kafka-topic <beacon-topic> \
  --user-field <user-field> --event-field <event-field> --campaign-field <campaign-field> \
  --advertiser-field <advertiser-field> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=... -X sasl.password=...
python adserver.py --name shadow           # reads the lane; no refills arrive, so it only keeps the view
```

The bridge only reads; collectors, producers and existing consumers are untouched. Map your record in
`to_beacon()`: the kit needs a user or device ID (whatever you cap on), the event type, the campaign and the
advertiser. When the Java ad servers read the lane directly, Apache Iggy has an official Java SDK.

## What Apache Iggy 0.9 has today, and what it doesn't

- Replication: Viewstamped Replication clusters, shipped September 2026.
- Delivery: at-least-once; the running total with a sequence makes it safe.
- Kafka API: a Kafka-compatible gateway is in progress and not in 0.9, which is why the trial uses
  `kafka_bridge.py` and why interaction events, revenue events and Spark and Flink jobs stay on Kafka.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored beacon topic, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
