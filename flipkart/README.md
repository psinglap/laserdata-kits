# Real-time ad spend and budget stops for Flipkart Ads, on LaserData

Every billable view and click reaches BCAP, charged once, and every budget stop reaches every ad server, in
milliseconds. It runs beside Flink and Pub/Sub, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Flipkart_AdSpend.pdf`](LaserData_Flipkart_AdSpend.pdf)

## Why this use case

From Flipkart Ads' own post ("High-Risk, High-Scale: Guaranteeing Ad Budget Precision at 1 Million
Events/Second", Flipkart Tech Blog, January 2026): "the latency between an ad impression/click and a budget
deduction represents a direct financial risk. If the system lags, advertisers overspend; if it blocks, revenue is
lost." "If spend aggregation incurs even minute-level latency ("processing lag"), we risk serving ads even after
the budget is exhausted." The pipeline takes "a firehose of ~1 million events per second", chose "at-least-once
semantics" on purpose, dedupes on a canonical impression ID plus "a 1-minute bucket derived from the event
timestamp", drops events "arriving more than one hour after their timestamp", and its budget controller, BCAP,
"utilizes the propagated watermark progress to determine the maximum acceptable lag". A batch layer settles the
final charges.

Spend control is a loop with two hops: a billable event has to reach BCAP, and BCAP's stop has to reach every ad
server before its next ad. Whatever the loop takes, ads keep serving on a campaign that has no budget left. This
kit runs that one loop on Apache Iggy, with your dedupe, lateness and watermark rules, prints both hops' latency
every second, and prints the over-burn in rupees. Flink, the batch settlement and Pub/Sub stay as they are.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 ad servers, 10,000 campaigns, 10,000 billable events a second (CPM
campaigns pay ₹60–400 per 1,000 viewable impressions, CPC campaigns ₹2–14 per click), 60-second budget days (a
compressed day, so budgets run out while you watch), and a dirty firehose: 0.5% of events sent twice with the ID
in another source's format, 2% arriving minutes late, 0.1% more than an hour late.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python bcap.py &                     # BCAP, fed by the billable-events lane
python ad_servers.py                 # 8 ad servers: billable events out, budget stops in
```

They print a line a second:

```
[bcap]  10,000 billable events/s  billable-to-BCAP p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  watermark lag …ms  stops sent …
[budget] day …: charged ₹…  campaigns stopped …/10,000  over-burn ₹… (…% of spend, … events)  repeats dropped … (₹…)  stale dropped …
[ads]   10,000 billable events/s (steady)  ₹…/s  resent …  withheld by stops …  stop-to-ad-servers n … p50 …ms p99 …ms
```

**What the numbers mean.** The kit times each billable event from the moment `ad_servers.py` sends it to the
moment `bcap.py` has applied it, and each stop from `bcap.py` to every ad server, through Python clients, on
whatever machine you run it on. CI result on a shared 2-vCPU runner with the broker and all scripts on one box,
reduced scale (about 3,000 billable events/s over 2,000 campaigns, 4 ad servers, 20-second budget days; ranges
over two green runs, [run 39837768596](https://github.com/psinglap/laserdata-kits/actions/runs/39837768596) and
[run 39838034920](https://github.com/psinglap/laserdata-kits/actions/runs/39838034920)): billable-to-BCAP
**p50 3.5–7.7 ms, p99 6.2–14.3 ms, p99.9 6.9–15.9 ms** (medians of 62 steady-state seconds per run; worst second
p99 8.7–15.4 ms), inside the 50 ms pass line in 124 of 124 steady-state seconds; stop-to-ad-servers **p50 2.5–3.5
ms, p99 4.2–6.1 ms** to all four ad servers. With 2 MB/s of UI telemetry on the same broker, the lane's p99 did
not move (6.3 and 14.4 ms before and after). In each full budget day about ₹73,000–75,000 was charged and up to
367–373 of 2,000 campaigns reached their budget and were stopped; **over-burn was ₹12–74 a day, 0.016–0.102% of
spend**, highest on the slower runner, which is the point: the loop's tail sets the over-burn. BCAP dropped
1,049–1,076 repeated events (₹1,349–1,387 that would otherwise have been charged twice) and 211–213 events sent
more than an hour after their event time. The watermark lag (BCAP's slowest partition) had a median of 23–30 ms
and a worst second of 289–390 ms, well inside the 2-second fail-safe line. That is the end-to-end loop through
this demo code, not the broker alone; the broker's own tail latency is measured with `iggy-bench` in
`bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `ad_servers.py` | Ad serving and event collection. Every billable view or click goes to `billable-events`, keyed by campaign, with repeats, late and stale events mixed in, and a watermark mark on every partition every 100 ms. Each ad server reads `budget-stops` on its own connection and stops serving a campaign when told. |
| `bcap.py` | The budget controller. Dedupes on canonical impression ID plus 1-minute bucket, drops events more than an hour late, adds spend per campaign and day, stops each campaign at its budget, and goes fail-safe when its slowest partition falls behind. Prints the lane's p99 and over-burn in ₹. |
| `telemetry.py` | The rest of the UI telemetry firehose on its own topic, for the isolation test. |
| `shadow_bridge.py` | Copies your billable events from Pub/Sub (its own subscription) or Kafka (its own consumer group) into Iggy, read-only, for a shadow run on real traffic. |
| `common.py` | Stream, topics, keys, prices, your dedupe and lateness rules. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |
| `docker-compose.yml` | Apache Iggy 0.9.0. |
| `ci.sh` | The CI smoke test on a real broker. |

## Three tests that match your peaks

1. **Big Billion Days opening hour:** `python ad_servers.py --peak 7 --peak-at 60 --peak-for 120` (your 2023
   sale ran 7× BAU transactional data). Pass: billable-to-BCAP p99 under 50 ms, and over-burn under 0.1% of
   spend.
2. **The firehose next door:** `python telemetry.py --mbps 50` while both run. Pass: the lane's p99 moves by
   less than 10%.
3. **Restart BCAP:** stop `bcap.py` and start it again; it replays the lane and rebuilds every campaign's spend
   for the day, charging each event once. Pass: it catches up within seconds and stopped campaigns stay stopped.

## Shadow your own traffic

```
# from Pub/Sub: gcloud pubsub subscriptions create iggy-shadow --topic=<ads-events-topic> --message-retention-duration=1h
python shadow_bridge.py --source pubsub --project <gcp-project> \
  --campaign-field <campaign> --id-field <impression-id> --time-field <event-time-ms> --kind-field <view|click> --cost-field <cost-inr>
# or from Kafka
python shadow_bridge.py --source kafka --bootstrap <broker> --kafka-topic <ads-events-topic> ... -X security.protocol=SASL_SSL ...
python bcap.py --day-s 86400 --budgets-csv budgets.csv     # lines of campaign,budget_inr
```

The bridge only reads; collectors, the Flink speed layer, the batch settlement and every existing subscriber are
untouched. Map your record in `to_event()`. Compare the shadow BCAP's spend and stops per campaign with yours.
Ad serving and collectors on the JVM can write to the lane directly with Apache Iggy's official Java SDK.

## What Apache Iggy 0.9 has today, and what it doesn't

Replication (Viewstamped Replication clusters) shipped in September 2026. Delivery is at-least-once, the same
choice your pipeline made; the canonical-ID dedupe makes repeats harmless, and the server can also deduplicate by
message ID. A Kafka-compatible gateway is in progress and not in 0.9, and there is no Pub/Sub API, which is why the
trial uses `shadow_bridge.py` and why your ingestion, Flink jobs and settlement stay where they are. The kit keeps
three minutes of dedupe keys in memory; your Flink state keeps the full hour, and a production BCAP on this lane
would too.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored billable-events stream, alongside Pub/Sub.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
