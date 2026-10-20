# Real-time card state for Robinhood's stand-in authorizer, on LaserData

Every balance and card-status change reaches every stand-in replica in milliseconds, so its decisions match the
primary's, even at the market open. It runs on its own broker beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Robinhood_CardState.pdf`](LaserData_Robinhood_CardState.pdf)

## Why this use case

From Robinhood's own engineering blog ("Building a Resilient Card Transaction System", October 2022): Robinhood
has 2 seconds to answer its payments processor, or the transaction is declined. Beside the primary authorizer,
which pulls account state on demand, it runs a lightweight stand-in authorizer built on a different stack, with
its own database, to avoid shared failures. The stand-in answers from a cache of "the latest state of each
cardholder's account", kept current by "subscribing to asynchronous updates broadcast over Kafka streams".

So the stand-in is exactly as right as its cache is fresh. A purchase right after cash moves into stocks, a
transfer, or a card lock is decided on whatever the cache holds at that moment. Robinhood's engineers put one
Kafka hop at 20–50 ms (diginomica, 2023). This kit runs that one feed on Apache Iggy, on a broker the primary does
not depend on, and prints its p99 every second with the stand-in decisions that disagree with the primary.

## Run it

Needs Docker and Python 3.10+. Defaults: 500,000 card accounts, 3,000 account changes a second, and 500
authorization requests a second plus purchases that follow a change.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python ledger.py &                   # every account's state, and the primary authorizer
python standin.py --name standin-1 & # a stand-in replica
python standin.py --name standin-2   # another; each holds every account
```

They print a line a second:

```
[standin-1]  3,000 changes/s  change-to-cache p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  decided approved …  declined …  expired 0
[ledger]  3,000 changes/s (steady)  requests …  stand-in decisions compared …  on stale state …
          disagreed with the primary … (approved … declined …)  a cache 0.5s behind would disagree on …
```

@@RESULTS@@

| File | What it is |
|---|---|
| `ledger.py` | Spendable balance and card status per account, published to `card-state` with a per-account sequence and the resulting state. Plays the processor (authorization requests) and the primary authorizer, and checks every stand-in decision against the primary's. `--open` multiplies the rate with cash moving into and out of stocks. |
| `standin.py` | A stand-in replica: reads every partition of the lane directly into its cache, rebuilds from the log on start, decides requests from the cache, and keeps the same cache 0.5 s behind for comparison. |
| `settlement_backfill.py` | A heavy, latency-tolerant stream (clearing and settlement records) on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies the account-update topic your stand-in reads today into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

The follow-up purchases are an assumption of the kit, not a Robinhood figure: 30% of balance and status changes
are followed 0.15–2 s later by a purchase on the same account, sized so only the new state decides it right.
Change `--follow-share` and `--follow-delay` to match what you see.

## Three tests

1. **The market open:** `python ledger.py --open 3 --open-at 60 --open-for 120`.
   Pass: change-to-cache p99 under 50 ms on every replica, and decisions on stale state near zero through the open.
2. **Settlement backfill at full volume:** `python settlement_backfill.py --mbps 50` while it runs.
   Pass: the lane's p99 moves by less than 10%.
3. **Restart a replica:** stop one `standin.py` and start it again; it rebuilds from the log before deciding.
   Pass: every account rebuilt within seconds, and its decisions agree with the primary from the first one.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --topic <account-updates> -X security.protocol=SASL_SSL …
```

The bridge reads as its own consumer group, `iggy-shadow`, and never commits offsets; producers, the stand-in and
every other consumer are untouched. Map your record shape in `to_change()`: the kit needs `k` (account), `seq`,
`avail` (cents) and `status`.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so Kafka clients can't read the lane; the stand-in uses the
Apache Iggy SDK. Delivery is at-least-once; the per-account sequence makes replays and retries safe. Replicated
clusters shipped in September 2026, which is why the evaluation kills a leader on purpose.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored account-update topic, beside Kafka and the
primary authorizer. No advantage, no migration.

kranti@laserdata.com · laserdata.com
