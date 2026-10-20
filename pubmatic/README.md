# Real-time household frequency caps for PubMatic CTV, on LaserData

Every CTV impression reaches every ad server in milliseconds, before that household's next ad request,
even in a live-sports break. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_PubMatic_HouseholdFrequency.pdf`](LaserData_PubMatic_HouseholdFrequency.pdf)

## Why this use case

From PubMatic's own job posts and releases: the CTV ad server operates "at a scale of 1 trillion+ requests
per day and managing millions of requests per second" (Staff Software Engineer, CTV Ad Server, 2026); the
performance team will "implement precise cross-device graph integrations, household-level frequency capping,
and server-to-server attribution systems" for CTV and video (Senior Performance Advertising Engineer, 2026);
ad pods already offer "competitive separation, frequency, and ad deduplication management" (PubMatic
Enhances CTV Platform, June 2023). Streaming data runs on "Kafka and Spark Streaming" (Engineering Manager,
Data & Analytics, 2026).

A household's ad requests do not all land on one server: the TV and the phone in the same room, the next
break, another data centre. The cap only holds if the impression one server just served has reached the
server that answers the household next. This kit runs that one lane on Apache Iggy, prints its latency
every second, and prints over-cap impressions (served after the household had already seen the campaign
`--cap` times) every cap window.

## Run it

Needs Docker and Python 3.10+. Defaults per ad server: 5,000 ad slots a second over 200,000 households, 1% of
them heavy, multi-screen viewers making 30% of the requests, 40 campaigns capped at 3 per household per
60-second window (a compressed day).

```
docker compose up -d                       # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt            # apache-iggy 0.9.0
python adserver.py --name ad-1 &           # three CTV ad servers,
python adserver.py --name ad-2 &           # each reading every impression
python adserver.py --name ad-3
```

Each prints a line a second, and a tally when a cap window closes:

```
[ad-1]  5,000 slots filled/s (steady)  from other servers 10,000/s  exposure-to-ad-server p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  capped …  unfilled …  gaps 0
[cap] window … closed: impressions …  household-campaign pairs at cap …  over-cap impressions … (…%)
```

**What the numbers mean.** The kit times each impression from the moment one `adserver.py` serves it to the
moment another has applied it, through Python clients, on whatever machine you run it on. CI result on a shared
2-vCPU runner, reduced scale (3 ad servers at 1,500 slots/s each over 50,000 households, 15-second windows,
[run 39467852226](https://github.com/psinglap/laserdata-kits/actions/runs/39467852226)): exposure-to-ad-server
**p50 6.7 ms, p99 12.9 ms, p99.9 15 ms** (medians of 66 steady-state seconds, worst second p99 39.5 ms), 66 of 66
seconds inside the 50 ms budget. With 2 MB/s of log-level data on the same broker, p99 was 13.3 ms against 12.9 ms
before. About 2,500 household-campaign pairs reached their cap in each window, and **1 to 4 impressions in
~67,000 (0.001–0.006%) went past a cap**. A restarted ad server rebuilt its cap view from the log in 1.5 s
(122,936 impressions, 41,201 households) before serving, with no sequence gaps. The same run with every server
applying the others' impressions 2 seconds late (`--hold-ms 2000`): **0.78–0.79% over cap, about 530 impressions
a window**, some 200 times more. That is the end-to-end lane through this demo code, not the broker alone; the
broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `adserver.py` | One CTV ad server. Fills ad slots with the first campaign each household is still under its cap for, publishes every impression (its own running count per household and campaign, with a sequence) keyed by household, and reads every partition of the lane directly to apply every other server's impressions. Rebuilds its view from the log before serving. |
| `loglevel_flood.py` | A heavy log-level feed on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your impression topic from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `common.py` | Topics, keys, partitioning and the record shape. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A live-sports break:** `python adserver.py --name ad-1 --peak 5 --peak-at 60 --peak-for 120` (same on each server).
   Pass: exposure-to-ad-server p99 under 50 ms, and over-cap impressions under 0.1%.
2. **Log-level data next door:** `python loglevel_flood.py --mbps 50` while the servers run.
   Pass: the lane's p99 moves by less than 10%.
3. **Restart an ad server:** stop one and start it again; it reads the lane from the start of the previous
   cap window, rebuilds every household's counts, then serves.
   Pass: it rebuilds in seconds, with no sequence gaps and nothing counted twice.

What-if: `--hold-ms 2000` on every server applies the others' impressions only once they are 2 seconds old,
like a pipeline that runs in micro-batches. Watch the over-cap tally.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <broker> --kafka-topic <impressions-topic> \
  --household-field <household-field> --campaign-field <campaign-field> --server-field <host-field> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
python adserver.py --name shadow --rate 0  # reads the lane only
```

The bridge only reads; ad servers, producers and existing consumers are untouched. Map your record in
`to_impression()`: the kit needs a household ID (or the device IFA, if that is what you cap on), the campaign
or line item, and the ad server. When the ad servers write to the lane directly, Apache Iggy has official Go
and Rust SDKs.

## What Apache Iggy 0.9 has today, and what it doesn't

- Replication: Viewstamped Replication clusters, shipped September 2026.
- Delivery: at-least-once; the running count with a sequence makes it safe.
- Kafka API: a Kafka-compatible gateway is in progress and not in 0.9, which is why the trial uses
  `kafka_bridge.py` and why Spark Streaming, log-level data and reporting stay on Kafka.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored impression topic, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
