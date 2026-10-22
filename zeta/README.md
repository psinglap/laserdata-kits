# Real-time account spend for Zeta's Tachyon feature store, on LaserData

Each card approval and reversal reaches the feature store in milliseconds, so the next authorization
checks category limits against current spend. It runs beside your Kafka, in shadow first, so nothing
else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.zeta-kit
- **One slide:** [`LaserData_Zeta_AccountSpend.pdf`](LaserData_Zeta_AccountSpend.pdf)

## Why this use case

From Zeta's own pages:

- Tachyon's Real-Time Transaction Feature Store can "track real-time spend for each account based on
  attributes like MCC, category, location, merchant, or card", and its Real-Time Authorization Decision
  Engine enables "spend rules across family add-on cards", letting primary cardholders set "spend limits
  on categories" (zeta.tech, Zeta Tachyon credit processing page).
- Tachyon is "event-driven and real-time", "emitting 100s of real-time messages" that "power real-time
  decision engines" (zeta.tech, Real-Time APIs and Events in Zeta Tachyon).
- "Apache Kafka is used as a message broker", and "Atropos is a message-based integration bus based on
  Kafka developed by Zeta" (Zeta engineering blog, Sep 2021, "Zeta's Robust Tech Stack").

A category limit on an add-on card is only as good as the spend the decision engine reads on that
card's next authorization. If the last approval has not reached the feature store, a second or third
bill a few seconds later goes through over the limit. If a reversal has not reached it, a card that
had room is declined. This kit runs account spend on its own lane, keyed by account, timed from the
authorization to the store, and checks the outcomes the card team cares about: over-limit approvals
and wrong declines.

We do not know how Tachyon's feature store is fed today, so this is a lane to evaluate in shadow,
beside whatever feeds it now, never in the authorization path until you decide otherwise.

## Run it

Needs Docker and Python 3.10+. Defaults: 25 million accounts with 0 to 3 family add-on cards each, 250
authorizations a second (about ten times the 2 million a day Zeta reported for 2024), 2% of them in
add-on card runs of 2 to 7 bills one to three seconds apart that cross a ₹10,000 daily category limit,
3% of approvals reversed 2 to 20 seconds later, and a 4× peak for the first minute. Zeta does not
publish per-second peaks, limits or add-on shares, so set them to yours.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python feature_store.py &              # the feature store, and the decision engine's check
python switch.py &                     # authorizations, add-on runs, reversals and the peak
python events_flood.py --at 20         # a heavy event-catalog stream next door
```

Each prints a line a second:

```
[switch]   1,000 authorizations/s (peak)  in add-on runs …  declined at the category limit …  reversals …  checks on repeat accounts …
[store]    1,000 spend updates/s  auth-to-store p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  reversals …  accounts in store …
[check] repeat-account checks … (in add-on runs …)  missing the last change …%  declined at the limit …  over-limit approvals … (Rs … over)  wrong declines …
```

`over-limit approvals` is the business number. When an account whose spend changed in the last 10
minutes authorizes again, `switch.py` sends a check carrying the sequence of the account's last
change, the amount, the add-on card's category limit and the true spend. `feature_store.py` takes the
spend it held when the authorization arrived, decides against the limit, and compares with the
decision on the true spend. An approval the true spend would decline is an over-limit approval; a
decline the true spend would approve (a reversal not yet applied) is a wrong decline. A slow or
congested lane shows up there first, and most of all in add-on runs.

**What the numbers mean.** The kit times each spend update from the moment `switch.py` decides the
authorization to the moment `feature_store.py` has applied it, through Python clients, on whatever
machine you run it on. On a shared GitHub runner (2 vCPU), with the broker and three Python processes
on one box and the store reading the 16 partitions directly on two connections, three green CI runs at
500 authorizations a second (5% in add-on runs, 3% of approvals reversed, 2× for 15 s as the peak)
held p50 3.8 to 5.4 ms, p99 6.8 to 9.5 ms and p99.9 7.3 to 10.7 ms (median per second over 61 to 62
steady-state seconds each, no second over the 50 ms budget). Through the peak (1,000 a second) the
median per-second p99 was 7.6 to 11.4 ms, worst 11.9 ms. Of about 5,300 to 5,400 checks on repeat
accounts in each run (about 1,450 of them in add-on runs), none missed the account's last change;
862 to 885 bills were declined at the category limit, with no over-limit approvals and no wrong
declines, while 1,076 to 1,093 reversals were applied. A 1 MB/s catalog stream ran next door for 34 s.
That is the end-to-end lane through this demo code at reduced scale, not the broker alone; the
broker's own tail latency is measured with `iggy-bench` on dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `switch.py` | The authorization switch: decides each authorization against true spend, publishes each approval's or reversal's resulting spend per category for the account on `account-spend`, and a check on `auth-checks` for each repeat account's next authorization. `--run-share`, `--reversal-share`, `--limit-rupees` and `--peak` shape the load. |
| `feature_store.py` | The transaction feature store: applies spend by sequence, times authorization-to-store, and verifies every check against the category limit. |
| `events_flood.py` | A bulk, latency-tolerant event-catalog stream (notifications, data lake) on its own topic on the same broker. |
| `kafka_bridge.py` | Reads your authorization outcomes from Kafka as a read-only consumer group, keeps the same per-account spend and publishes it to the lane, for a shadow run on real traffic. |
| `common.py` | Topics, partitioning by account, the spend fold shared by the switch and the bridge. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **The peak:** `python switch.py --auths-per-s 250 --peak 4 --peak-for 60`.
   Pass: authorization-to-store p99 under 50 ms through the peak, and zero over-limit approvals.
2. **The event catalog on the same broker:** `python events_flood.py --mbps 20 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart the store:** stop `feature_store.py` and start it again. It replays the lane, prints
   "catching up", then times live authorizations again. Pass: no approval or reversal counted twice
   (spend is a resulting value with a sequence), back under budget within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <broker>:9092 --kafka-topic <your-authorization-outcomes-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=<user> -X sasl.password=<password>
```

The bridge only reads; the switch, every producer and every existing consumer are untouched. Map your
field names in `to_outcome()` and your MCC groups in `MCC_GROUPS`: the kit needs the account, the
card, the MCC or category, the amount in paise, approved or reversed, and a reference. In shadow mode
the clock starts when the bridge reads the record. Everything runs inside your AWS account; no
cardholder data leaves it.

## What Apache Iggy 0.9 has today, and what it doesn't

- Has: a durable, replayable log with per-partition order, TCP/QUIC/HTTP/WebSocket transports,
  consumer groups, and a Python SDK (used here).
- Doesn't yet: a Kafka wire-protocol gateway (in progress), so Kafka clients don't talk to it
  directly; delivery is at-least-once, so the kit carries a per-account sequence and resulting values;
  clustering is new (Sep 2026), so a regulated rollout starts with a single-tenant shadow.

## Next step

If the numbers are interesting: a 2–4 week design-partner evaluation on one tenant's mirrored
authorization outcomes, self-hosted in your AWS account beside your Kafka, in shadow. No advantage,
no migration.

kranti@laserdata.com · laserdata.com
