# Real-time trade events for Alpaca's Broker API partners, on LaserData

Every trade event the OMS writes reaches each partner's event stream in milliseconds. A partner that reconnects
resumes exactly after its last event_id, and a partner that reads slowly falls behind in its own log instead of
losing events. It runs beside Redpanda, so the OMS and its write-ahead log don't move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Alpaca_PartnerTradeEvents.pdf`](LaserData_Alpaca_PartnerTradeEvents.pdf)

## Why this use case

From Alpaca's own sources:

- OMS v2 keeps account state in memory on sharded nodes, with a distributed write-ahead log on Redpanda; 85% of
  orders process in under 1.5 ms and 95% under 5 ms, down from about 150 ms. "Other services can listen to trade and
  order related events by consuming the WAL" in a decoupled way. (Alpaca's OMS v2 launch post, November 2021; the
  Redpanda case study adds over $1B traded per day and Raja Bhatia, VP Engineering: "The performance, throughput and
  latency we got with almost no configuration was incredible.")
- Partners get those events through the Broker API's "replayable and real-time event streams via Server-Sent Event
  (SSE)", one stream for all accounts under a correspondent, ordered per account only. The docs tell partners to
  reconnect with `since_id` set to the last processed `event_id`, to re-read "a few minutes" back for reconciliation
  and de-duplicate, and warn that "if your consumer cannot keep up with the event rate, the server will drop messages"
  (`: you are reading too slowly, dropped N messages`). Large partners can ask for "sharded streaming". (Alpaca
  Broker API docs: SSE Events, Activity SSE.)
- The partner base is growing: more than 300 organisations in over 40 countries (Series D coverage, January 2026), and
  monthly active API users up nearly 4x in six months (Alpaca, July 2026).

The OMS is already fast; the partner stream is where a fill becomes visible to a partner's app. This kit runs that one
lane on Apache Iggy: trade events into one log per partner, keyed by account ID, with a per-account sequence and a
ULID event_id, read by the event stream gateways and timed from the OMS to the gateway every second. What the log
adds: a reconnect maps `since_id` to an exact offset in every partition (no window to re-read, nothing to
de-duplicate), a slow partner reads its own log at its own pace and catches up with nothing dropped, and a large
partner simply gets more partitions.

## Run it

Needs Docker and Python 3.10+. Defaults: 1,000,000 accounts across six illustrative partners (a robo-advisor with
40% of events, then a neobank, a trading app, a wealth platform, a wallet and an advisor dashboard) at 10,000 trade
events a second. Alpaca does not publish event rates, so set them to yours.

```
docker compose up -d                                         # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                              # apache-iggy 0.9.0
python oms.py &                                              # the OMS's trade events
python event_gateway.py --name gw-1 --partners corr-01 corr-02 corr-03 &
python event_gateway.py --name gw-2 --partners corr-04 corr-05 corr-06 --slow corr-06
```

At the default rate, run one gateway per machine or lower `--rate`: one Python reader tops out near 4,000 events/s
on a small VM.

They print a line a second:

```
[gw-1]  7,200 events/s  OMS-to-gateway p50 …ms  p99 …ms  p99.9 …ms  PASS (<10ms)  to partners …/s
       disconnects … resumes … (lost in flight …, resume-to-live p99 …ms)  held in the log …
       gaps 0  dups 0  unrecoverable 0  loop late max …ms  gc max …ms
[gw-2]  …  slow partner corr-06: slow, lag … events, read from the log …  gaps 0  dups 0 …
[oms] 10,000 trade events/s  new …  partial_fill …  fill …  canceled …  replaced …  trade_correct …
```

**What the numbers mean.** The kit times each event from the moment `oms.py` creates it (including the wait for its
2 ms write) to the moment an `event_gateway.py` has read it, through Python clients, on whatever machine you run it
on. On a shared 2-vCPU GitHub runner with the broker and all three scripts on one box (50,000 accounts, 1,000 trade
events/s with the robo-advisor's rebalance at 3× from second 30 to 45, two gateways, about 32 partner disconnects per
run, one partner limited to 25 events/s for 15 s), the medians of the steady-state seconds over three consecutive
green runs were p50 2.5–2.9 ms, p99 4.3–4.9 ms and p99.9 4.6–5.4 ms on each gateway, and all 126 steady-state
gateway-seconds of every run, the rebalance included, were inside the 10 ms budget (worst single second p99 9.7 ms
over all three runs). All 32 resumes per run started from `since_id`, redelivered the 296 events lost in flight,
with zero gaps, duplicates or unrecoverable resumes; resume-to-live p99 was 4–7 ms in a typical second and 26 ms at worst. The slow partner fell 704–705 events behind in its log, lost none, and was back at the head 0.02–0.03 s after it sped up, while the other partners on its gateway stayed inside the budget. That is the end-to-end lane through this
demo code, not the broker alone; a Go or Rust gateway would read it with less overhead. The broker's own tail
latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `oms.py` | The OMS's trade events for every account (new, partial_fill, fill, canceled, replaced, a rare trade_correct; fractional and whole-share orders, two crypto pairs), written to each partner's log keyed by account ID, with a per-account sequence, a ULID event_id and the order's resulting state. `--rebalance` multiplies the robo-advisor's rate. |
| `event_gateway.py` | An event stream gateway: reads its partners' logs directly, one connection per partition, pushes each event to its partner, keeps the delivery order as (event_id, partition, offset), resumes a reconnecting partner from `since_id`, and lets a slow partner lag in its own log. Counts gaps and duplicates as the partner sees them. |
| `redpanda_bridge.py` | Reads the OMS's trade events from Redpanda over the Kafka API as consumer group `iggy-shadow`, read-only and never committing, into the partners' logs, for a shadow run. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

Partners, rates, disconnects and the slow partner are assumptions of the kit, not Alpaca figures. Change
`--accounts`, `--rate`, `--drops`, `--away`, `--slow-rate` and `PARTNERS` in `common.py` to match what you see.

## Three tests

1. **A big rebalance:** `python oms.py --rebalance 10 --rebalance-at 60 --rebalance-for 300`.
   Pass: OMS-to-gateway p99 under 10 ms on every gateway for live partners, and `gaps` stays 0.
2. **Slow partners:** run `gw-2` with `--slow corr-06 --slow-rate 5 --slow-for 600`, then a second time with the
   robo-advisor as the slow one (`--slow corr-01` on `gw-1`).
   Pass: the other partners' p99 moves by less than 10%, and the slow partner catches up with zero gaps.
3. **Reconnect storm and a gateway restart:** run `gw-1` with `--drops 60 --in-flight 200`, and stop and restart
   `gw-2` mid-run; on restart it reads its partners' logs back and goes live.
   Pass: every resume from `since_id` with zero gaps and zero duplicates, resume-to-live p99 under 50 ms.

## Shadow your own traffic

```
pip install confluent-kafka
python redpanda_bridge.py --bootstrap <redpanda brokers> --topic <a topic of OMS trade events>
```

The bridge is one more decoupled consumer of the WAL, the way the OMS post describes; it never commits offsets and
never writes to Redpanda. Map your fields in `to_event()` (correspondent, account ID, a per-account sequence,
event_id, event) and it writes them with the kit's partition function. Then compare `event_gateway.py` with what your
SSE gateways deliver today, partner by partner.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so franz-go and other Kafka clients can't read the lane; gateways
use the Apache Iggy SDK (Rust, Go, Java, Python and others). Delivery is at-least-once; the per-account sequence makes
retries safe. Replicated clusters shipped in September 2026, which is why the evaluation kills a leader on purpose.
The SSE layer itself (HTTP, auth, the event JSON) stays yours; Iggy is the log behind it.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored partner's trade events, beside the OMS and
Redpanda. No advantage, no migration.

kranti@laserdata.com · laserdata.com
