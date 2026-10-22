# Real-time payment status for CRED members, on LaserData

Every UPI and bill payment status reaches every Propeller node in milliseconds, and a phone that reconnects gets
the events it missed, in order. It runs beside Propeller's Redis and NATS, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.cred-kit
- **One slide:** [`LaserData_CRED_PaymentStatus.pdf`](LaserData_CRED_PaymentStatus.pdf)

## Why this use case

From CRED's own sources:

- CRED built Propeller "initially to optimize UPI payment flows, where every millisecond matters", and reports up
  to 95% faster transaction status updates for UPI payments, bill payments completed up to 95% faster, instant
  communication of failed transactions with clear reasons, and less backend load than polling. The flows on it:
  bill payments, UPI transfers, reward redemptions, cashback credits, CRED coin balance updates and CRED Win prize
  confirmations (CRED's announcement, CXOToday, December 2024; Swamy Seetharaman is quoted).
- Propeller is open source ([github.com/CRED-CLUB/propeller](https://github.com/CRED-CLUB/propeller)): Go,
  bidirectional streaming gRPC between app and server, a member identified by `x-user-id` and each device by
  `x-device-id`, every event acknowledged by the app (`ChannelEventAck`). Backends publish to a broker channel per
  member, or per member and device, on Redis or NATS. In pub/sub mode (`persistence = false` in the sample config)
  an event reaches only the subscribers connected at that instant; in persistence mode each channel is a Redis
  stream, unbounded, deleted after delivery. The docs say nothing about events published while a device is
  reconnecting.
- CRED runs over 1,000 microservices across regulated business lines, "if we go down, someone's EMI is late"
  (CRED Engineering, DR drills post, June 2026).

Propeller's job is to show a payment's status the moment it exists. This kit runs the lane behind it on Apache
Iggy: each status and balance event keyed by member with a per-member sequence, read by every Propeller node, timed
from the backend to the node every second. What the log adds: a phone that drops (moving from Wi-Fi to 4G, leaving
the app while the bank responds, a node being replaced) gets every event since the sequence it last acknowledged
replayed from the log, in order, so a SUCCESS or a failure reason is never skipped and never needs a poll, and a new
Propeller node rebuilds its state from the log.

## Run it

Needs Docker and Python 3.10+. Defaults: 500,000 members with the app open and 5,000 status events a second, one
member in five on two devices. CRED has not published Propeller's connected devices or event rates, so set them to
yours.

```
docker compose up -d                                              # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                                   # apache-iggy 0.9.0
python payments.py &                                              # payment status and balance events
python propeller_node.py --name node-1 --index 0 --nodes 2 &      # a node with half the device channels
python propeller_node.py --name node-2 --index 1 --nodes 2        # another; each reads every partition
```

At the default rate, run one node per machine or lower `--rate`: one Python reader tops out near 4,000 events/s on a
small VM.

They print a line a second:

```
[node-1] 5,000 events/s  backend-to-node p50 …ms  p99 …ms  p99.9 …ms  PASS (<10ms)  to devices …/s
         (final statuses …)  reconnects … recovered …  replayed from the log … (payments finished while away …,
         failures with reason …, coin balances …)  resend p99 …ms  gaps 0  dups 0  unrecoverable 0
[payments] 5,000 events/s  UPI PENDING …  SUCCESS …  FAILED …  bills …  paid …  refunded …  cashback …  coins …
```

**What the numbers mean.** The kit times each event from the moment `payments.py` creates it (including the wait
for its 2 ms write) to the moment a `propeller_node.py` has read it, through Python clients, on whatever machine you
run it on. On a shared 2-vCPU GitHub runner with the broker and all four scripts on one box (20,000 members on about
24,000 devices, 1,200 events/s with a 2× peak burst to 2,400/s, two nodes with 20 channels dropped a second on each,
half of them mid-payment, 2 MB/s of reconciliation export next door), the medians of the steady-state seconds over
three consecutive green runs were p50 2.4–3.1 ms, p99 4.2–5.2 ms and p99.9 4.5–5.6 ms on each node. Of 330
steady-state node-seconds across the three runs, the 15 burst seconds included, 329 were inside the 10 ms budget;
the other had a p99 of 10.3 ms. All 2,507–2,514 reconnects per run were recovered: 869–974 missed events replayed
from the log (373–466 payments that had finished while the phone was away, 12–31 failures with their reason,
110–123 coin balances), resend p99 1–3 ms, with zero gaps, duplicates or unrecoverable resends. A node started 20 s
into the run rebuilt its index of about 14,000 members (about 26,000 events) from the log in 2.1 s. That is the
end-to-end lane through this demo code, not the broker alone; a Go node would read it with less overhead. The
broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `payments.py` | The payment backends as the lane sees them: UPI payments (PENDING, then SUCCESS or FAILED with a reason, 0.3–4 s later), credit card bill payments (PAYMENT_RECEIVED, PAYOUT_INITIATED, BILL_PAID and the coins it earned, or PAYOUT_FAILED and REFUNDED), cashback, coin balances, reward redemptions, CRED Win prizes; published to `member-events` keyed by member, with a per-member sequence and the resulting state. `--burst` multiplies the rate. |
| `propeller_node.py` | A Propeller node: reads every partition directly, pushes each event to every device of the member connected here, keeps only an index of (sequence, partition, offset) per member, and on reconnect replays the events after the device's last acknowledged sequence from the log on a separate connection, then goes live. Counts gaps and duplicates as each device sees them. |
| `recon_export.py` | A heavy, latency-tolerant stream (settlement, payout, refund and cashback ledger records for reconciliation) on its own topic, for the isolation test. |
| `propeller_bridge.py` | Copies real events from Propeller's Redis or NATS channels into the lane, read-only, for a shadow run. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

Event names, statuses, failure reasons and amounts are illustrative, not CRED's schema. Channel churn is an
assumption of the kit, not a CRED figure: 20 channels per node drop each second, half of them on members with a
live event moments ago, and come back 0.3–3 s later. Change `--drops`, `--mid-payment` and `--away` to match what
you see.

## Three tests

1. **A payment peak:** `python payments.py --burst 5 --burst-at 60 --burst-for 120`.
   Pass: backend-to-node p99 under 10 ms on every node, and `gaps` stays 0.
2. **Reconciliation export at full volume:** `python recon_export.py --mbps 50` while it runs.
   Pass: the lane's p99 moves by less than 10%.
3. **Reconnect storm and a node restart:** run `node-1` with `--drops 500` (a mobile network blip), and stop and
   restart `node-2` mid-run; on restart it rebuilds its index from the log before accepting channels.
   Pass: every reconnect recovered, zero gaps and zero unrecoverable resends, resend p99 under 50 ms.

## Shadow your own traffic

```
pip install redis
python propeller_bridge.py --redis redis://<propeller-redis>:6379
pip install nats-py
python propeller_bridge.py --nats nats://<propeller-nats>:4222     # if Propeller runs on NATS
```

The bridge is one more listener on the channels Propeller already uses (Redis `PSUBSCRIBE`, or a NATS
subscription); it publishes nothing and calls no Propeller API. It writes each event to the lane keyed by member
(the channel name, which is the `x-user-id`), with a per-member sequence and the event name decoded from the
`push.v1.Event` header; map your own payload fields in `to_event()`. Use `--members` to leave custom topics out.
Its timestamps start at the bridge, so it measures bridge-to-node. With Propeller's persistence mode on, events sit
in per-channel Redis streams that Propeller deletes after delivery; shadow the pub/sub path, or the backend that
calls Propeller, instead.

## What Apache Iggy 0.9 does not have yet

Propeller has no Iggy broker: one would be a new implementation in its `pkg/broker` package on Apache Iggy's Go
SDK, a change for CRED's team to make, with our help. A Kafka-compatible API is in progress (not needed here).
Delivery is at-least-once; the per-member sequence makes replays and retries safe, and the node drops repeats.
Replicated clusters shipped in September 2026, which is why the evaluation kills a leader on purpose; replication
across regions is not built in.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored Propeller event feed, beside Propeller and
its brokers. No advantage, no migration.

kranti@laserdata.com · laserdata.com
