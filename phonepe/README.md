# Real-time payment velocity features for PhonePe risk checks, on LaserData

Each payment's new velocity counts reach the risk check in milliseconds, so the check on a payer's
next payment counts the last one. Self-hosted beside your Kafka, in shadow first, so nothing else
has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_PhonePe_VelocityFeatures.pdf`](LaserData_PhonePe_VelocityFeatures.pdf)

## Why this use case

From PhonePe's own posts:

- The real-time fraud model for P2P transfers declines fraud during the payment. Most of its inputs
  were turned into real-time aggregates, and maintaining those real-time variables was "the first
  major challenge" of moving from batch to real time (PhonePe Tech Blog, Jul 2022, "Building
  PhonePe's Real-Time Transaction Model to prevent Frauds: A Journey").
- Fraud checks run "within a few milliseconds during transactions" on over 31 crore transactions a
  day, and Guardian's Action Intelligence "aggregates user events in real time, including device
  usage, transaction frequency, and activity" (PhonePe blog, Feb 2025, "PhonePe's Guardrails").
- Events reach Kafka through a client that spools to disk and an IngestionService. That path is
  built for resilience: analytics p99 is under 3 s. A direct path exists for "critical,
  time-sensitive applications" (PhonePe Tech Blog, Oct 2023, "The Kafka Edge").

A velocity count is only useful if it is there before the payer's next payment. In a scam or
account-takeover run, that next payment can come one to three seconds later. This kit runs the
velocity counts on their own lane, keyed by payer, timed from the payment to the store, and checks
the outcome the risk team cares about: did the check on the next payment count the last one?

We do not know how Guardian's aggregates are fed today, so this is a lane to evaluate in shadow,
beside whatever feeds them now, never in the authorization path until you decide otherwise.

## Run it

Needs Docker and Python 3.10+. Defaults: 20 million payers, 4,200 payments a second (about 360
million a day), 2% of payments in bursts of 2 to 7 payments one to three seconds apart, and a 3×
peak for the first minute. PhonePe does not publish per-second peaks or burst shapes, so set them
to yours.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python risk.py &                       # the aggregate store, and the next-payment check
python payments.py &                   # payments, with payer bursts and the peak
python events_flood.py --at 20         # a heavy analytics stream next door
```

Each prints a line a second:

```
[payments]  12,600 payments/s (peak)  in bursts …  risk checks on repeat payers …  payers tracked …
[risk]  12,600 velocity updates/s  payment-to-store p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  payers in store …
[check] repeat-payer checks … (in bursts …)  missing the previous payment …% (in bursts …)  velocity rule hits …
```

`missing the previous payment` is the business number. When a payer who paid in the last 10
minutes starts another payment, `payments.py` sends a risk check carrying the sequence of the
payer's previous payment. `risk.py` asks whether its store had counted that payment by the time the
check started. A slow or congested lane shows up there first, and most of all in bursts.
`velocity rule hits` applies an example rule (5 or more payments in 60 s) to show the counts in use.

**What the numbers mean.** The kit times each velocity update from the moment `payments.py`
completes the payment to the moment `risk.py` has applied it, through Python clients, on whatever
machine you run it on. On a shared GitHub runner (2 vCPU), with the broker and three Python
processes on one box, a green CI run at 500 payments a second (5% in bursts, 2× for 15 s as the
peak) held p50 12.0 ms, p99 23.4 ms and p99.9 25.0 ms (median per second over 63 steady-state
seconds, no second over the 50 ms budget). Through the peak (1,000 payments a second) the median
per-second p99 was 23.3 ms, worst 24.2 ms. Of 5,604 risk checks on repeat payers (1,668 of them in
bursts), none missed the payer's previous payment, and the example rule fired 762 times. A 1 MB/s
analytics stream ran next door for 33 s. That is the end-to-end lane through this demo code at
reduced scale, not the broker alone; the broker's own tail latency is measured with `iggy-bench` on
dedicated machines (`bench/PLAN.md`). On a laptop or a larger machine, raise the rates.

| File | What it is |
|---|---|
| `payments.py` | Payments: each completed payment's resulting counts for the payer (payments and amount over 60 s and 10 min, distinct payees over 10 min, per-payer sequence) on `payer-velocity`, and a check on `risk-checks` for each repeat payer's next payment. `--burst-share`, `--gap-min/--gap-max` and `--peak` shape the load. |
| `risk.py` | The aggregate store behind the risk check: applies counts by sequence, times payment-to-store, and verifies every next-payment check against the store. |
| `events_flood.py` | A bulk, latency-tolerant analytics stream on its own topic on the same broker. |
| `kafka_bridge.py` | Reads your completed-payment events from Kafka as a read-only consumer group, keeps the same rolling counts and publishes them to the lane, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **The peak:** `python payments.py --payments-per-s 4200 --peak 3 --peak-for 60`.
   Pass: payment-to-store p99 under 50 ms through the peak, and under 0.01% of checks missing the previous payment.
2. **Analytics on the same broker:** `python events_flood.py --mbps 20 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart the store:** stop `risk.py` and start it again. It replays the lane, prints "catching
   up", then times live payments again. Pass: no payment counted twice, back under budget within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <read-cluster-broker>:9092 --kafka-topic <your-completed-payments-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=<user> -X sasl.password=<password>
```

The bridge only reads, from your read cluster; the IngestionService, every producer and every
existing consumer are untouched. Map your field names in `to_payment()`: the kit needs the payer,
the amount in paise, the payee and a transaction ID. In shadow mode the clock starts when the bridge
reads the record. Everything runs inside your data centers; no payment data leaves them.

## Next step

If the numbers are interesting: a 2–4 week design-partner evaluation on one mirrored payments
topic, self-hosted beside your Kafka, in shadow. No advantage, no migration.

kranti@laserdata.com · laserdata.com
