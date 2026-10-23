# Real-time order groups across Kalshi's shards, on LaserData

Every fill on every matching engine shard counts toward one rolling 15-second limit per order group, and
every trigger reaches every shard in milliseconds. It runs beside the engines and Kafka, so nothing else has
to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.kalshi-kit
- **One slide:** [`LaserData_Kalshi_OrderGroups.pdf`](LaserData_Kalshi_OrderGroups.pdf)

## Why this use case

From Kalshi's API docs on order groups: they "provide automatic order cancellation when a contracts limit is
reached within a rolling 15-second window. When an order group is triggered, all resting orders in that group
are canceled and no new orders can be placed until the group is reset." A Reset clears the triggered state and
the rolling count. Since August 2026 a user can hold up to 100,000 order groups (API changelog).

From Kalshi's docs on exchange sharding: "Kalshi splits trading across multiple matching engines to scale
capacity." Combos run on shard 1, crypto and commodities on shard 2, tennis, baseball and basketball on shard 3,
and everything else on shard 0, rolled out from August to October 2026. And in the FAQ: "Order groups do not
function across exchange instances." Margin order groups are "bound to a single exchange_index".

A market maker quoting one NBA game rests orders on its single-game markets (Sports) and on the combos that
include it (Combos). Since sharding, each shard counts only its own fills, so the maker either gives every shard
the full limit and can fill a multiple of the exposure it wanted in 15 seconds, or splits the limit and trips
early on one shard while the other sits quiet.

A cross-shard order group is a loop with two hops: a fill on any shard has to reach the tracker that holds the
group's rolling count, and the tracker's trigger has to reach every shard the group trades on. Whatever the
loop takes, other shards keep filling the group's orders past its limit. This kit runs that one loop on Apache
Iggy, prints both hops' latency every second, and prints the contracts filled past each trigger.

Kalshi's infrastructure and data roles name Kafka (with Flink) for real-time data, on AWS with Kubernetes; the
lane sits beside it, and `kafka_bridge.py` shadows a fills topic read-only for a trial.

## Run it

Needs Docker and Python 3.10+. Defaults: 4 shards (Default, Combos, Crypto, Sports), 20,000 order groups
resting orders on two or three shards each, 8,000 fills a second at 1–49 contracts each, and groups reset 5 s
after a trigger (standing in for the market maker's Reset call).

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python tracker.py &                  # the order group tracker, fed by the fills lane
python engine.py                     # four matching engine shards: fills out, triggers in
```

They print a line a second:

```
[tracker]  8,000 fills/s  fill-to-tracker p50 …ms  p99 …ms  p99.9 …ms  PASS (<20ms)  states sent …
[groups] triggered … (no single shard over the limit …)  reset …  triggered now …/20,000  contracts …
         past the trigger … (in flight to the tracker …, trigger in flight to a shard …; …% of contracts)
[engine]  8,000 fills/s (steady)  … contracts/s  by shard: default …  combos …  crypto …  sports …
         refused, group triggered …  groups triggered now …  state-to-shards n … p50 …ms p99 …ms
```

**What the numbers mean.** The kit times each fill from the moment `engine.py` creates it to the moment
`tracker.py` has applied it, and each group state from `tracker.py` to every shard, through Python clients, on
whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (1,200 fills/s over 2,000 groups and four shards, with a 2x burst to 2,400 and 2 MB/s of trade export next door), three green runs held fill-to-tracker p50 2.5–4.3 ms, p99 4.6–7.7 ms and p99.9 5.1–8.4 ms (every steady-state second inside the
20 ms budget, 62–63 per run; worst single second p99 5.4–13.6 ms), and state-to-shards p50 2.4–3.0 ms and p99
4.3–5.7 ms on four shard readers. The trade export and the burst each moved the lane's p99 by 0.7–1.4 ms. Per run,
2,409–2,454 groups were triggered and reset, 92–93% of them with no single shard over the limit, and contracts
filled past a trigger were 0.006–0.011% of contracts. CI runs:
https://github.com/psinglap/laserdata-kits/actions/runs/39888493713,
https://github.com/psinglap/laserdata-kits/actions/runs/39888685304,
https://github.com/psinglap/laserdata-kits/actions/runs/39888882234 That is the end-to-end loop through this demo code, not the broker alone.
The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

"No single shard over the limit" counts triggers that a per-shard group with the same limit would not have fired.
In the kit a group's fills split evenly across its shards, so most triggers are of this kind; the real share
depends on your makers' mix, which is what a shadow run on your fills measures.

| File | What it is |
|---|---|
| `engine.py` | Four matching engine shards in one process. Every fill in an order group publishes the shard's running total of contracts for that group (with a sequence) to `order-group-fills`, keyed by order group. Each shard reads `order-group-states` on its own connection and refuses fills for triggered groups. |
| `tracker.py` | The order group tracker. Keeps each group's contracts over the rolling 15 seconds across every shard, triggers it when the total exceeds the limit, resets it, and publishes each state. Prints the lane's p99, the triggers no single shard could see, and contracts past the trigger. |
| `trade_export.py` | A heavy stream of public trades and book changes for the data lake on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your fills topic from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **The final minutes of an NFL Sunday:** `python engine.py --burst 4 --burst-at 60 --burst-for 120`.
   Pass: fill-to-tracker p99 under 20 ms, and contracts past the trigger under 0.05% of contracts.
2. **A trade export next door:** `python trade_export.py --mbps 50` while both run.
   Pass: the order group lane's p99 moves by less than 10%.
3. **Kalshi's order group maximum:** `python tracker.py --groups 100000 --fills-per-s 15000 --readers 4` and
   `python engine.py --groups 100000 --fills-per-s 15000` on a machine with 8 or more cores.
   Pass: fill-to-tracker p99 and state-to-shards p99 under 20 ms.

## Design notes

- Running totals with a sequence per (shard, group) make at-least-once delivery idempotent: a repeated or
  replayed fill changes nothing. Apache Iggy 0.9 does not offer exactly-once, and this lane doesn't need it.
- The window is by fill time, as Kalshi documents: contracts over the last 15 seconds, triggered when the total
  exceeds the limit; a Reset clears the state and the count. Fills that arrive after a trigger are counted as
  past the trigger, split into fills that were on their way to the tracker and fills made while the trigger was
  on its way to a shard.
- A cross-shard brake is one hop slower than a check inside one engine. It is meant to sit beside the per-shard
  order groups, not replace them; the contracts-past-the-trigger number shows what the hop costs.
- Group IDs, limits, markets and fill sizes are illustrative. In production the limits come from your order
  group store, and the group states from the tracker are what the engines act on.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers:9092> --topic <fills-topic> \
  --group-field order_group_id --shard-field exchange_index --count-field count
```

The bridge only reads, as its own consumer group `iggy-shadow`, and never commits; the matching engines, the
services that produce fills and every other consumer are untouched. Map your record in `to_fill()`: the kit needs
the order group ID, the exchange index the fill matched on and the contracts filled.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored fills feed, beside the matching engines.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
