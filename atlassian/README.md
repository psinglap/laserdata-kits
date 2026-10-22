# Real-time Forge usage alerts for Atlassian, on LaserData

Every Forge function's usage reaches its app's allowance check, and every 50%, 75%, 90% and 100% crossing
reaches the alert service, in milliseconds, counted once, even at peak. It runs beside StreamHub, so nothing
else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.atlassian-kit
- **One slide:** [`LaserData_Atlassian_ForgeUsageAlerts.pdf`](LaserData_Atlassian_ForgeUsageAlerts.pdf)

## Why this use case

From "Engineering the Forge billing platform for reliability and scale" (Puneet Patwari, Principal Software
Engineer, Apr 2026): Forge usage flows from Forge services through StreamHub into the Usage Tracking Service
(UTS), which "ingests events from multiple producers in near real time" and deduplicates and orders them,
then on to Plato (Databricks, Flink, StarRocks) and Commerce for invoices. Volume is "around 300M usage events
per day, expected to exceed 600M within 6 months", and 1 billion in a year. The goals are "no double
counting", with idempotency keys "like a `usageUniqueId`" so "replays and retries don't create extra usage",
and limits that work, because a bug in enforcement can "let runaway usage accumulate without corresponding
revenue". "Today, Forge provides fixed alerting when an app's usage crosses 50%, 75%, 90%, and 100%."

From the Forge developer community (Aug 2026): developers asked for per-app budgets and caps, and an Atlassian
engineer replied that "Budget based notifications is something the dependent platform doesn't support yet"
(tracked as FRGE-1847).

And from "Scaling StreamHub" (Prakash Kava, 2026): StreamHub moved from Kinesis to MSK with tiered storage
and now takes about 150 billion events a day, with a local delivery SLO under 2 seconds. That is the right
home for the bulk of Atlassian's events, and this kit leaves it alone.

An alert is a loop with two hops: an invocation's usage has to reach the check that owns the app's allowance,
and the crossing has to reach the service that tells the developer. This kit runs that one loop on Apache
Iggy, prints both hops' latency every second, and prints how many alerts a counter without deduplication
would have sent early.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 runtime hosts, 5,000 apps, 4,000 invocations a second (about
350M usage events a day, today's Forge volume), Forge Functions duration at $0.000025 per GB-second, and
60-second billing cycles (a compressed cycle with allowances scaled to it, so apps cross their levels while
you watch).

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python tracker.py &                  # the allowance tracker, fed by the usage lane
python notifier.py &                 # the alert service, fed by the tracker's crossings
python runtime.py                    # 8 Forge runtime hosts: every invocation's usage out
```

They print a line a second:

```
[tracker]   4,000 usage events/s  usage-to-tracker p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  alerts sent …
[usage] cycle …: … GB-s on … apps  past 50% …  75% …  90% …  100% …  budget …/…  overage $…
        retries absorbed …  without dedupe +… GB-s (…%), early alerts …
[alerts] alerts …/s  usage-to-alert n … p50 …ms p99 …ms  tracker-to-alert p50 …ms p99 …ms  cycle …: 50% … 75% … 90% … 100% …
[runtime]   4,000 invocations/s (steady) on 8 hosts  Forge Functions duration … GB-s/s  resent …
```

**What the numbers mean.** The kit times each usage record from the moment `runtime.py` creates it to the moment
`tracker.py` has applied it, and each crossing from the usage record that caused it to `notifier.py`,
through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and
all scripts on one box, at reduced scale (3,000 invocations/s, 1,000 apps, 4 runtime hosts, 20 s cycles,
1% of records sent twice), two runs held usage-to-tracker p50 5.0–5.7 ms and p99 8.9–10.2 ms (62 of 62
steady-state seconds inside 50 ms in each), usage-to-alert p50 7.5–8.1 ms and p99 12.3–12.8 ms, and
tracker-to-alert p50 2.3–2.4 ms and p99 4.4–4.5 ms. With 2 MB/s of product analytics events on the same
broker, the lane's p99 moved by 0.4 ms or less. In each full 20 s cycle, 206–237 of 1,000 apps passed 100%
of their allowance and 48–57 apps with a budget reached it; 2,102–2,134 retried records were absorbed per
run, where a counter without dedupe would have counted 0.96–1.05% more GB-seconds and sent up to 203–219
alerts a cycle before the app reached the level. That is
the end-to-end loop through this demo code, not the broker alone. The broker's own tail latency is
measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `runtime.py` | A fleet of Forge function runtime hosts. Every invocation publishes the host's running GB-seconds for that app this cycle (the resulting total, with a sequence and a usageUniqueId) to `usage`, keyed by app. 0.5% of records are sent twice, as retries are. |
| `tracker.py` | The allowance tracker. Sums the latest total from every host per app, so retries and replays add nothing, checks it against the app's allowance and publishes each 50/75/90/100% crossing (and 125% for apps with a budget) once per cycle. Prints the lane's p99, retries absorbed and early alerts avoided. |
| `notifier.py` | The alert service. Reads every crossing, acts on each (cycle, app, level) once, and prints both hops' latency. |
| `analytics_flood.py` | A heavy product-analytics stream (clicks and impressions) on its own topic, for the isolation test. |
| `sqs_bridge.py` | Copies Forge usage events from your own StreamHub subscription (an SQS queue) into Iggy, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A busy hour:** `python runtime.py --peak 3 --peak-at 60 --peak-for 120`.
   Pass: usage-to-tracker p99 under 50 ms, and no usage counted twice.
2. **Product analytics next door:** `python analytics_flood.py --mbps 20` while the lane runs.
   Pass: the usage lane's p99 moves by less than 10%.
3. **Restart the tracker:** stop `tracker.py` and start it again; it replays the usage topic and rebuilds
   every app's total from the running totals, without double counting. The alert service drops the
   crossings it has already acted on.
   Pass: it catches up within seconds, and no app gets the same alert twice.

## Design notes

- Running totals with a sequence make at-least-once delivery idempotent: a repeated or replayed record
  changes nothing. Apache Iggy 0.9 does not offer exactly-once, and this lane doesn't need it: it only
  decides when to alert. Billable usage, invoices and the deterministic cold-to-hot path stay on UTS,
  Plato and Commerce.
- Cycles here are compressed and aligned to the clock, with allowances scaled to them. In production the
  cycle is the calendar month and the allowance is 200,000 GB-seconds per app (`--cycle-s 0
  --allowance-gbs 200000`).
- The kit tracks one meter, Forge Functions duration. Other counters (Key-Value Store, Logs, SQL requests)
  fit the same shape; gauges such as SQL data stored need their GB-hours derived first, as UTS does.

## Shadow your own traffic

```
pip install boto3
python sqs_bridge.py --queue-url <your shadow subscription's SQS queue> --meter <functions duration meter>
python tracker.py --cycle-s 0 --allowance-gbs 200000
```

Declare one more StreamHub subscription for the Forge usage events, delivered to an SQS queue of your own.
The bridge reads and deletes only from that queue; the Forge services, StreamHub, UTS, Plato and Commerce
are untouched. Map your record in `to_usage()`: the kit needs the app, a source, the usageUniqueId and the
GB-seconds of the event.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored usage subscription, alongside
StreamHub. No advantage, no migration.

kranti@laserdata.com · laserdata.com
