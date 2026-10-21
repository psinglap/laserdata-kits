# Real-time execution reports for Kraken's gateways, on LaserData

Every order event reaches every FIX and WebSocket gateway in milliseconds, and a session that reconnects gets its
missed fills replayed in order. It runs beside Kafka and Aeron, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs. Apache Iggy is written in Rust, like Kraken's core services.

- **One slide:** [`LaserData_Kraken_ExecutionReports.pdf`](LaserData_Kraken_ExecutionReports.pdf)

## Why this use case

Kraken's FIX documentation describes the job: sessions reach the engine through FIX gateways, HAProxy puts a new
session on the gateway with the fewest connections, and "if a session reconnects with a lower-than-expected
sequence number, the gateway replays `ExecutionReport`s from the requested sequence number", with FIX 4.4 gap-fill
and resend semantics. The WebSocket v2 `executions` channel streams the same order status and fills per account;
on subscribe it sends open orders, and the last 50 fills only if asked. Kraken's April 2023 post counts tens of
thousands of algorithmic connections, over 1mm requests a minute, and jitter held under 30 ms through the
FTX-collapse week; its December 2025 post describes event-driven services on an in-house Kafka stream-processing
framework written in Rust, with C++ kept for the trading engine.

So a client's view of its own orders depends on one feed: the engine's execution reports, in order per account,
on every gateway at once, and readable again by sequence number when a session comes back. A fill that happens
while a session is away has closed the order, so a snapshot of open orders can't show it; only a replay can.
This kit runs that one feed on Apache Iggy, prints its p99 every second, and replays every reconnecting session
from the log, counting gaps.

## Run it

Needs Docker and Python 3.10+. Defaults: 200,000 accounts with orders on the book and 15,000 execution reports a
second (about the "over 1mm requests a minute" Kraken published in 2023; the report rate is our assumption).

```
docker compose up -d                                  # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                       # apache-iggy 0.9.0
python engine.py &                                    # the spot engine's execution reports
python gateway.py --name gw-1 --index 0 --gateways 2 & # a gateway with half the sessions
python gateway.py --name gw-2 --index 1 --gateways 2   # another; each reads every partition
```

At the default rate, run one gateway per machine or lower `--rate`: one Python reader tops out near 4,000
reports/s on a small VM.

They print a line a second:

```
[gw-1] 15,000 reports/s  engine-to-gateway p50 …ms  p99 …ms  p99.9 …ms  PASS (<10ms)  to sessions …/s
       reconnects … recovered …  replayed from the log … (fills …, orders closed while away …)
       resend p99 …ms  gaps 0  dups 0  unrecoverable 0  sessions up … of …
[engine] 15,000 reports/s  new …  trade …  filled …  amended …  canceled …  write ack p50 …ms  p99 …ms
```

**What the numbers mean.** The kit times each report from the moment `engine.py` creates it (including the wait
for its 2 ms write) to the moment a `gateway.py` has read it, through Python clients, on whatever machine you run
it on. On a shared 2-vCPU GitHub runner with the broker and all four scripts on one box (20,000 accounts, 1,500
reports/s with a 2× burst, two gateways with 20 sessions dropped a second on each, 2 MB/s of ledger export next
door), the medians of the steady-state seconds were p50 2.7 ms, p99 4.6 ms and p99.9 6.0–6.5 ms on each gateway,
and every steady-state second was inside the 10 ms budget (worst second p99 9.3–9.7 ms). About 2,500 reconnects per
run were all recovered: 419 missed reports replayed from the log (115 fills; 98 orders that had closed while the
session was away), resend p99 5–6 ms, with zero gaps, duplicates or unrecoverable resends. A gateway started 20 s
into the run rebuilt its index of 17,000 accounts from the log in about 2 s. That is the end-to-end lane through
this demo code, not the broker alone; a Rust gateway would read it with less overhead. The broker's own tail
latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `engine.py` | Order events for every account (new, trade, filled, amended, canceled, as in the WebSocket v2 `executions` channel), published to `execution-reports` keyed by account, with a per-account sequence and the order's resulting state. `--burst` multiplies the rate. |
| `gateway.py` | A gateway: reads every partition directly, delivers each report to the account's session if it is logged on here, keeps only an index of (sequence, partition, offset) per account, and on reconnect replays the missed reports from the log on a separate connection, then goes live. Counts gaps and duplicates as the client sees them. |
| `ledger_export.py` | A heavy, latency-tolerant stream (ledger entries for statements and tax) on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies a Kafka topic of order events into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

Session churn is an assumption of the kit, not a Kraken figure: 20 sessions per gateway drop each second and come
back 0.3–3 s later. Change `--drops` and `--away` to match what you see.

## Three tests

1. **A volatile market:** `python engine.py --burst 3 --burst-at 60 --burst-for 120`.
   Pass: engine-to-gateway p99 under 10 ms on every gateway, and `gaps` stays 0.
2. **Ledger export at full volume:** `python ledger_export.py --mbps 50` while it runs.
   Pass: the lane's p99 moves by less than 10%.
3. **Reconnect storm and a late gateway:** `python gateway.py ... --drops 500`, then start a third gateway
   mid-run; it rebuilds its index from the log before logging sessions on.
   Pass: every reconnect recovered, zero gaps and zero unrecoverable resends, resend p99 under 50 ms.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --topic <order-events> -X security.protocol=SASL_SSL …
```

The bridge reads as its own consumer group, `iggy-shadow`, and never commits offsets; producers and every other
consumer are untouched. Map your record shape in `to_report()`: the kit needs `k` (account), `seq`, `exec_type`
and `order_status`. If the engine's output reaches gateways over Aeron rather than Kafka, a small subscriber that
copies it into Iggy does the same job.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so Kafka clients can't read the lane; gateways use the Apache
Iggy SDK (Rust, among others). Delivery is at-least-once; the per-account sequence makes replays and retries safe,
and the gateway drops repeats. Replicated clusters shipped in September 2026, which is why the evaluation kills a
leader on purpose.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored execution feed, beside the engine and your
gateways. No advantage, no migration.

kranti@laserdata.com · laserdata.com
