# Real-time customer loan aggregates for Affirm checkout, on LaserData

Every new loan, payment and dispute reaches the customer's aggregate in the online feature store in
milliseconds, so the checkout version check finds it current. It runs beside Kafka and Flink, so
nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.affirm-kit
- **One slide:** [`LaserData_Affirm_CustomerAggregates.pdf`](LaserData_Affirm_CustomerAggregates.pdf)

## Why this use case

From Affirm's Tech Blog post "Precompute First" (Machine Learning and Decision Platform, Oct 2026):
the lending system of record sees every event that changes a returning customer's aggregates (a new
loan, a manual payment, a dispute opening or closing), keeps a monotonically increasing counter per
customer, and publishes the user-level aggregate as an event that the feature platform stores. At
checkout, the stored value is served if its counter still matches the system of record's; if the
counter has moved, the read recomputes from the customer's full history. The post says the pipeline
"targets under five minutes from loan event to updated feature value" and that roughly one read in
forty pays for a recompute.

The counter moves exactly when a customer has just done something, so the latency of that one
stream decides how often a checkout soon after a loan event takes the expensive branch, and how tight
a freshness bound a fraud rule can ask for. This kit runs that one lane on Apache Iggy, keeps Affirm's
five-outcome comparison (first, newer, older, duplicate, schema up-versioned) on both paths, and prints
the lane's p99 every second.

## Run it

Needs Docker and Python 3.10+. Defaults: 500,000 returning customers, 400 loan events a second
(5% of them a first-time customer's first loan), a quarter of them followed by a checkout from the same
customer 50 ms to 5 minutes later, 200 other checkouts a second, and 1% of aggregates delivered twice.
Affirm does not publish loan-event rates; set yours.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python features.py &                 # the store's consumer and the checkout reads
python lending.py                    # the system of record: loan events, aggregates, checkouts
```

Both print a line a second:

```
[features]    400 changes/s  change-to-store p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  customers stored …
              write path: first … newer … schema-up … duplicate … older …
[checkout] reads …  within 300 s of a loan event …: current …%  recompute …%  unavailable …%  |  no recent change …
[lending]    400 loan events/s (steady)  first loans …  redelivered …  checkouts: after a loan event …  returning …
```

**What the numbers mean.** The kit times each aggregate from the moment `lending.py` creates it to the
moment `features.py` has stored it, through Python clients, on whatever machine you run it on. A checkout
counts as current if the store already held the system of record's counter when the checkout started.
On a shared 2-vCPU CI runner with the broker and all scripts on one box (20,000 returning customers,
400 loan events a second, 3× for 15 s, 1 MB/s of backfill next door), three runs held p50 3.6 to 5.1 ms,
p99 6.7 to 9.1 ms and p99.9 7.2 to 9.8 ms, with no failing second; the peak's median p99 was 8.1 to 12.3 ms
and its worst second 8.4 to 15.0 ms. Of 15,120 to 15,332 checkouts per run that started within five
minutes of the customer's last loan event, 99.91 to 99.96% found the stored aggregate current and 0.04 to
0.09% needed a recompute (checkouts that started within milliseconds of another loan event for
that customer). 327 to 354
duplicate and 9 to 20 older deliveries per run were dropped, and none replaced a newer value. That is the
end-to-end lane through this demo code, not the broker alone, and the checkout mix is synthetic, so the
shares are not comparable with production. The broker's own tail latency is measured with `iggy-bench`
in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `lending.py` | The lending system of record: loan events, a counter per customer, the resulting user-level aggregate on `user-aggregates`, and checkouts on `checkout-reads` carrying the record's counter. Redelivers 1% of aggregates. |
| `features.py` | The feature platform's consumer (stores an aggregate only when `compare()` says first, newer or schema up-versioned) and the checkout version check (current, recompute or unavailable). |
| `common.py` | `compare()`, the five-outcome function used on both paths; topics, keys and partitions. |
| `backfill.py` | A heavy history backfill on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your aggregate topic from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Black Friday and Cyber Monday:** `python lending.py --peak 5 --peak-at 60 --peak-for 120`.
   Pass: change-to-store p99 under 50 ms, and at least 99.9% of checkouts within five minutes of a loan event current.
2. **A history backfill next door:** `python backfill.py --mbps 50` while both run.
   Pass: the lane's p99 moves by less than 10%.
3. **Rebuild the store:** stop `features.py` and start it again; it replays the lane from the start.
   Pass: it catches up within seconds, and no redelivered aggregate replaces a newer one.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <kafka:9092> --kafka-topic <user-aggregate-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads; the system of record, the feature platform's consumer and every other consumer are
untouched. Map your record shape in `to_aggregate()`: the kit needs the customer, the counter and the
schema version. If the topic is Avro or Protobuf behind Schema Registry, decode it there: Apache Iggy 0.9
has no schema registry of its own.

## What Apache Iggy 0.9 doesn't have yet

At-least-once delivery, not exactly-once (your counter and comparison already handle that); no Kafka API
yet (the gateway is in progress); no schema registry; replication clusters are new (September 2026).

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored aggregate topic, alongside
Kafka. No advantage, no migration.

kranti@laserdata.com · laserdata.com
