# Real-time in-flight spend for Pinterest ad budgets, on LaserData

Every ad insertion's spend reaches every ad server's budget view in milliseconds, so ad groups stop at their
daily budget instead of serving free ads. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Pinterest_InflightSpend.pdf`](LaserData_Pinterest_InflightSpend.pdf)

## Why this use case

From Pinterest's own engineering blog: "At Pinterest, we use Kafka Streams API to provide inflight spend data
to thousands of ads servers in mere seconds", because "overdelivery occurs when free ads are shown for
out-of-budget advertisers". "Inflight spend is the cost of ads insertions that haven't yet been charged", and
the serving rule is "If actual_spend + inflight_spend > daily_budget, don't show ad for this advertiser", with
in-flight spend = price × impression_rate × action_rate. The requirements were "tens of thousands of events per
second", fan-out "to more than 1,000 consumer machines" and an end-to-end delay under 10 seconds, met with a
10-second window store per ad group (Using Kafka Streams API for predictive budgeting, Pinterest Engineering,
2017). In 2022 budgets were still enforced "through our budgeting, pacing, and bidding systems", with Flexible
Daily Budgets allowing "overspend each day, up to a certain limit" (Flexible Daily Budgeting at Pinterest).
Pinterest has not published whether the 2017 design is still the one in production.

An ad group with budget left is a candidate on every ad server at once. When one server's insertion uses the
last of the day's budget, every other server keeps inserting that ad group until the spend reaches its budget
view, and each of those insertions is a free ad. This kit runs that one lane on Apache Iggy, prints its
latency every second, and prints spend, ad groups at budget and overdelivery every budget day.

## Run it

Needs Docker and Python 3.10+. Defaults per ad server: 2,000 auctions a second over 20,000 ad groups, a small
share of ad groups winning most auctions, up to 4 candidates per auction; daily budgets with room for 5 to 100
insertions of each ad group, over a 60-second budget day (a compressed day); in-flight spend = price ×
impression_rate × action_rate, impression-billed at $2–20 CPM or click-billed at $0.30–2.00 CPC; 0.5% of
insertions delivered twice.

```
docker compose up -d                                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                        # apache-iggy 0.9.0
python ad_server.py --name ads-1 &                     # three ad servers, each with its own
python ad_server.py --name ads-2 &                     # budget view, each reading every
python ad_server.py --name ads-3                       # insertion's spend
```

Each prints a line a second, and a tally when a budget day closes:

```
[ads-1] 2,000 insertions/s  from other servers 4,000/s  spend-to-budget-view p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  out of budget …  unfilled …  repeats dropped …  gaps 0
[day] … closed: spend $…  ad groups at budget …  overdelivery $… (…% of spend, … insertions past budget)
```

An ad server inserts an ad group only if actual plus in-flight spend plus this insertion fits the day's budget
(your rule, with the insertion's own cost included), so any spend past a budget can only come from an insertion
on another server that has not reached this one yet. Overdelivery is a direct measure of the lane.

**What the numbers mean.** The kit times each insertion's spend from the moment one `ad_server.py` publishes it
to the moment another has applied it to its budget view, through Python clients, on whatever machine you run
it on. CI result on a shared 2-vCPU runner, reduced scale (3 ad servers at 400 auctions/s each over 2,000 ad
groups, 20-second budget days; ranges over two green runs,
[run 39703166763](https://github.com/psinglap/laserdata-kits/actions/runs/39703166763) and
[run 39703209729](https://github.com/psinglap/laserdata-kits/actions/runs/39703209729)): spend-to-budget-view
**p50 2.8–3.2 ms, p99 5.2–6.0 ms, p99.9 5.8–6.5 ms** (medians of 66 steady-state seconds per server; worst
second p99 23.5–101.4 ms), inside the 50 ms budget in 263 of 264 steady-state seconds. Each budget day about
$226 of spend (about 24,000 insertions) went through and 185–202 ad groups reached their budget; across six full
days, **2 insertions went past budget, $0.01 of overdelivery, at most 0.005% of spend in any day**. Each server
dropped 292–324 repeated insertions by event ID, with no sequence gaps. With 2 MB/s of insertion logs on the
same broker, p99 moved from 5.6 to 5.7 ms in both runs. A restarted ad server rebuilt its budget view from the
log in 0.23–0.24 s (2,000 ad groups, 27,000–33,000 insertions) before serving. The same run with every server
applying the others' spend 2 seconds late (`--hold-ms 2000`): **1.6–1.7% of spend past budget, 396–401 free
insertions in a full day**, more than the 1% your post called the optimized case. That is the end-to-end lane
through this demo code, not the broker alone; the broker's own tail latency is measured with `iggy-bench` in
`bench/PLAN.md`, on dedicated machines.

## Three tests that match your peaks

1. **More ad servers.** `for n in 4 5 6; do python ad_server.py --name ads-$n & done` adds servers, each
   reading the whole lane. Pass: spend-to-budget-view p99 stays under 50 ms on every server, and overdelivery
   stays under 0.1% of spend.
2. **Insertion logs next door.** `python insertion_logs.py --mbps 50` writes insertion and engagement logs to
   their own topic on the same broker. Pass: the lane's p99 moves by less than 10%.
3. **Restart an ad server.** Stop one and start it again. It reads the lane from the start of the previous
   budget day, rebuilds every ad group's spend, then serves. Pass: it rebuilds in seconds, with no sequence
   gaps and no spend counted twice.

And one what-if: `--hold-ms 2000` makes each server apply the others' spend only once it is 2 seconds old, a
spend view still faster than the 10-second target in your post.

## Try it on your traffic

`kafka_bridge.py` joins the Kafka topic your ad servers write insertion events to (the input of your in-flight
spend aggregator), as its own consumer group `iggy-shadow`. It computes each insertion's in-flight spend (a
field you already carry, or price × impression_rate × action_rate), drops a repeat of an insertion ID it has
already seen, and writes running totals per ad group and day into Apache Iggy. It only reads, so ads serving,
the aggregator, the spend system and every other consumer are untouched. Map your record in `to_insertion()`.

```
python kafka_bridge.py --bootstrap <broker> --kafka-topic <ad-insertions-topic> \
  --adgroup-field <adgroup-field> --id-field <insertion-id-field> --spend-field <inflight-spend-field>
python ad_server.py --name shadow --rate 0 --day-s 86400
```

Compare the shadow reader's per-ad-group totals with your aggregator's output topic. Apache Iggy has an
official Java SDK, so the ad servers can write to the lane directly once the shadow run looks right.

## Files

| File | What it is |
|---|---|
| `ad_server.py` | One ad server: auctions under daily budgets, its insertions' spend out, everyone's in |
| `insertion_logs.py` | A heavy neighbour: insertion and engagement logs on their own topic |
| `kafka_bridge.py` | Shadow your insertion topic into the lane, read-only |
| `common.py` | Stream, topics, keys, record shape, the in-flight spend formula |
| `bench/PLAN.md` | The benchmark plan against Kafka, rules fixed first |
| `docker-compose.yml` | Apache Iggy 0.9.0 |
| `ci.sh` | The CI smoke test on a real broker |

## What Apache Iggy 0.9 has today, and what it doesn't

Replication (Viewstamped Replication clusters) shipped in September 2026. Delivery is at-least-once; the
running total with a sequence makes repeats harmless. A Kafka-compatible gateway is in progress and not in 0.9,
which is why the trial uses `kafka_bridge.py` and why your Kafka clusters, the spend system and every other
consumer stay on Kafka.

Contact: kranti@laserdata.com · laserdata.com
