# Real-time merchant usage metering for Cashfree Payments, on LaserData

Every payment's usage reaches the subscription's live day total in milliseconds, counted once even when it is
sent twice, and waits in the log, nothing lost, while Kill Bill is down. It runs beside Kafka, so nothing else
has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_CashfreePayments_UsageLane.pdf`](LaserData_CashfreePayments_UsageLane.pdf)

## Why this use case

From Cashfree's open-source Kill Bill plugins (GitHub, 2024): `killbill-kafka-consumer-plugin` exists "to
provide support to collect usage through Kafka Stream". Services publish usage to a Kafka topic (the README's
example is `stream.usagemetrics`, consumer group `killbillgroup`) instead of calling Kill Bill, so usage keeps
flowing while Kill Bill is briefly unavailable. Each message carries `subscriptionExternalKey`, `trackingId`
(used for deduplication), `tenantId` and `unitUsageRecords`, with unit types such as `txnCount` and `txnVolume`.
`killbill-per-usage-billing-plugin` "Records all raw usage data, ensuring nothing is missed", aggregates it and
backfills charges.

From Cashfree's engineering blog: Kafka is the org-wide event bus beside SQS, on AWS and Kubernetes, Go and Java
(2021); nearly 200 microservices and "transactions worth $80B annually for more than 800,000 businesses" (2025);
and, as next steps for cross-service events, "making it easier for teams to add their own layers of
deduplication and have visibility around idempotency and retries" (2023).

This kit runs that one lane on Apache Iggy: each payment's usage, in the plugin's own message shape, keyed by
subscription, into (1) a live per-subscription day total for usage views and plan-limit checks, counted once
and reconciled to the paisa, and (2) Kill Bill, through a writer that reads at its own pace and waits out a
Kill Bill outage with the usage kept in the log. Kafka, the plugins, Kill Bill and invoicing stay as they are.

## Run it

Needs Docker and Python 3.10+. Defaults: 4,000 payments a second over 200,000 subscriptions, one usage message
per payment, a skewed merchant mix, and 0.5% of messages sent twice as retries.

```
docker compose up -d                                  # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                       # apache-iggy 0.9.0
python meter.py &                                     # the live meter, fed by the usage lane
python killbill.py --outage-at 60 --outage-for 30 &   # the Kill Bill writer; Kill Bill down for 30 s
python payments.py                                    # 4,000 payments/s over 200,000 subscriptions
```

They print a line a second:

```
[meter]      4,000 usage msgs/s  payment-to-usage p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  daily totals sent …
[usage] txnCount …  txnVolume ₹…  subscriptions …  reconciled to the paisa …/…  retries absorbed …
        without dedupe +… txns (+₹…, …%)
[killbill] Kill Bill down for 12.0s: reading paused, usage waits in Apache Iggy  duplicates refused …  subscriptions reconciled …/…
[killbill] outage over: … usage messages (…s of payments) drained in …s, nothing lost, nothing recorded twice
[payments]   4,000 usage msgs/s (steady)  payments volume ₹…/s  subscriptions active today …  resent …
```

**What the numbers mean.** The kit times each usage message from the moment `payments.py` sends it to the moment
`meter.py` has applied it to the subscription's day, through Python clients, on whatever machine you run it on.
On a shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (1,500 payments/s, 2x
for ten seconds, over 20,000 subscriptions, 1% of messages sent twice, Kill Bill down for 12 s), three green runs
held payment-to-usage p50 3.4–6.7 ms, p99 6.5–12.9 ms and p99.9 8.0–15.5 ms (64 of 64 steady-state seconds
inside 100 ms in each), with the meter reading the 16 usage partitions directly on two connections. In every run
all 19,098–19,123 subscriptions with a payment reconciled to the paisa, in the meter and in Kill Bill;
1,187–1,322 retries were absorbed, where a meter that adds every message would have counted that many extra
payments (0.99–1.11% of volume). The Kill Bill backlog, 22,368–22,561 usage messages (12.8–12.9 s of payments),
drained in 2.0–2.1 s after Kill Bill came back, and the meter's p99 while Kill Bill was down was 6.3–12.7 ms
(median per second), no higher than during the 2x peak before it. Kill Bill here is a stand-in that records
usage and refuses repeated trackingIds; its own latency is not measured. That is the end-to-end lane through this
demo code, not the broker alone. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`,
on dedicated machines. CI runs: https://github.com/psinglap/laserdata-kits/actions/runs/39849150074,
https://github.com/psinglap/laserdata-kits/actions/runs/39849449749, https://github.com/psinglap/laserdata-kits/actions/runs/39849721930

| File | What it is |
|---|---|
| `payments.py` | Payment services. One usage message per successful payment to `usagemetrics`, keyed by subscription, in the Kill Bill plugin's shape plus a per-subscription sequence and the day's `txnCount` and `txnVolume` so far. A share of messages is sent twice, as retries. |
| `meter.py` | The live meter. Applies the newest day totals per subscription, ignores retries and replays, checks the records against the totals to the paisa, and publishes changed day totals to `usage-daily` every 250 ms. Prints the lane's p99 and the billing numbers. |
| `killbill.py` | The Kill Bill writer, on its own connection and its own position. Records usage in a stand-in Kill Bill that refuses repeated trackingIds; `--outage-at`/`--outage-for` take Kill Bill down, the writer stops reading, and the backlog drains when it is back. Reconciles Kill Bill against the day totals. |
| `kafka_bridge.py` | Copies `stream.usagemetrics` into Iggy as a read-only consumer group, `iggy-shadow`, adding the lane fields, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A salary-day surge:** `python payments.py --peak 3 --peak-at 60 --peak-for 120`.
   Pass: payment-to-usage p99 under 100 ms, every subscription reconciles to the paisa, no retry counted.
2. **Kill Bill goes away:** `python killbill.py --outage-at 60 --outage-for 300` while payments keep coming.
   Pass: the meter's p99 does not move, the backlog drains after Kill Bill returns, and every subscription's
   Kill Bill usage equals its day totals.
3. **Restart the meter:** stop `meter.py` and start it again. It replays the usage topic and rebuilds every
   subscription's day. Pass: it catches up within seconds and the replay adds nothing twice.

## Shadow your traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers:9093> --topic stream.usagemetrics \
  -X security.protocol=SSL -X ssl.ca.location=<ca.pem>
```

It reads as its own consumer group, `iggy-shadow`, so the services, the Kill Bill plugin and Kill Bill are
untouched. Map your unit names with `--count-unit` and `--volume-unit` if they differ.

## What Apache Iggy 0.9 has, and what it doesn't

- Replication: Viewstamped Replication clusters, shipped September 2026; three nodes tolerate one failure.
- Delivery: at-least-once, not exactly-once. Counting once here comes from the message shape (day totals with a
  sequence, applied idempotently) and Kill Bill's trackingId, and the kit proves it with retries on purpose.
- Kafka API: a Kafka-compatible gateway is in progress and not in 0.9. The Kill Bill plugin speaks Kafka, so on
  the lane it is replaced by a small writer like `killbill.py`; the trial starts with a bridge and leaves the
  plugin as it is.
- SDKs: Rust, Go, Java, Python, TypeScript/Node.js, C#.

Contact: kranti@laserdata.com · laserdata.com
