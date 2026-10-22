# Real-time order updates for Dhan's apps and APIs, on LaserData

Every DEXT order update reaches every order update gateway (the Dhan app, web.dhan.co and DhanHQ API sockets) in
milliseconds, and a socket that reconnects gets the updates it missed, in order. It runs beside DEXT, so nothing in
the order path moves.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Dhan_OrderUpdates.pdf`](LaserData_Dhan_OrderUpdates.pdf)

## Why this use case

From Dhan's own sources:

- DEXT, Dhan's in-house order, risk and execution system on AWS, "processes 40 percent of orders in under 10
  milliseconds, 90 percent in under 20 milliseconds" (95% under 25 ms), with internal and external API responses of
  1–5 ms and more than 150 risk checks per order. "Once trading begins, we can expect a hundredfold jump in the
  number of concurrent customers online." (AWS case study on Dhan; Alok Pandey, Co-founder and CTO, is quoted.)
- DEXT runs active-active across Mumbai, Bangalore and other locations, with servers kept under 30% utilisation
  (Marketcalls' summary of Dhan's "Decoding DEXT" post, March 2025).
- Every order then produces a chain of status changes that must reach the trader on every surface. DhanHQ's Live
  Order Update socket (`wss://api-order-update.dhan.co`) streams them per client (UserType SELF) or for every user of
  a partner platform (UserType PARTNER): TRANSIT, PENDING, REJECTED, CANCELLED, TRADED, EXPIRED, with traded
  quantity, average price, and the Super Order legs (LegNo 1 entry, 2 stop-loss, 3 target). Postbacks send the same
  changes to a webhook. The docs describe no sequence number and say nothing about updates missed while a socket is
  disconnected.

DEXT's speed is the number Dhan publishes; the order update is how the trader sees it. This kit runs that one lane
on Apache Iggy: each update keyed by client ID with a per-client sequence, read by every gateway, timed from DEXT to
the gateway every second. What the log adds: a socket that drops (a phone moving from Wi-Fi to 4G, an algo
restarting, a gateway failover) gets every update it missed replayed from the log, in order, so a fill or a Super
Order's stop-loss going live is never skipped, and a new gateway rebuilds its state from the log.

## Run it

Needs Docker and Python 3.10+. Defaults: 300,000 clients with orders today and 10,000 order updates a second, one
in five orders a Super Order. Dhan publishes over 950,000 active users but not its order or update rates, so set
the rates to yours.

```
docker compose up -d                                         # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                              # apache-iggy 0.9.0
python dext.py &                                             # DEXT's order updates
python order_gateway.py --name ws-1 --index 0 --gateways 2 & # a gateway with half the sockets
python order_gateway.py --name ws-2 --index 1 --gateways 2   # another; each reads every partition
```

At the default rate, run one gateway per machine or lower `--rate`: one Python reader tops out near 4,000
updates/s on a small VM.

They print a line a second:

```
[ws-1] 10,000 updates/s  DEXT-to-gateway p50 …ms  p99 …ms  p99.9 …ms  PASS (<10ms)  to sockets …/s
       reconnects … recovered …  replayed from the log … (fills …, orders closed while away …,
       Super Order exit legs …)  resend p99 …ms  gaps 0  dups 0  unrecoverable 0  sockets up … of …
[dext] 10,000 updates/s  TRANSIT …  PENDING …  TRADED …  CANCELLED …  REJECTED …  Super Orders … (exit legs live …)
```

**What the numbers mean.** The kit times each update from the moment `dext.py` creates it (including the wait for
its 2 ms write) to the moment an `order_gateway.py` has read it, through Python clients, on whatever machine you run
it on. On a shared 2-vCPU GitHub runner with the broker and all four scripts on one box (20,000 clients, 1,200
updates/s with a 2× open burst to 2,400/s, two gateways with 20 sockets dropped a second on each, 2 MB/s of
compliance export next door), the medians of the steady-state seconds over three consecutive green runs were p50
2.6–3.1 ms, p99 4.4–5.0 ms and p99.9 4.8–5.4 ms on each gateway, and all 110 steady-state gateway-seconds of every
run, the 15 burst seconds included, were inside the 10 ms budget (worst single second p99 6.5 ms over all three
runs). All 2,528 reconnects per run were recovered: 309–319 missed updates replayed from the log (48–51 fills,
49–58 orders that had closed while the socket was away, 19 Super Order exit legs), resend p99 1 ms, with zero gaps,
duplicates or unrecoverable resends. A gateway started 20 s into the run rebuilt its index of about 14,600 clients
(about 26,000 updates) from the log in 2.0 s. That is the end-to-end lane through this demo code, not the broker
alone; a Rust gateway would read it with less overhead. The broker's own tail latency is measured with `iggy-bench`
in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `dext.py` | DEXT's order updates for every client (TRANSIT, PENDING, partial fills, TRADED, CANCELLED, REJECTED; Super Orders whose stop-loss and target legs go live when the entry fills, and whose other leg is cancelled when one fills), published to `order-updates` keyed by client ID, with a per-client sequence and the order's resulting state. `--burst` multiplies the rate. |
| `order_gateway.py` | An order update gateway: reads every partition directly, pushes each update to the client's socket if it is connected here, keeps only an index of (sequence, partition, offset) per client, and on reconnect replays the missed updates from the log on a separate connection, then goes live. Counts gaps and duplicates as the client sees them. |
| `compliance_export.py` | A heavy, latency-tolerant stream (trade records for contract notes and exchange and regulatory reporting) on its own topic, for the isolation test. |
| `ws_bridge.py` | Copies real updates from DhanHQ's Live Order Update socket (partner or client login) into the lane, read-only, for a shadow run. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

Socket churn is an assumption of the kit, not a Dhan figure: 20 sockets per gateway drop each second and come back
0.3–3 s later. Change `--drops` and `--away` to match what you see.

## Three tests

1. **The 9:15 open:** `python dext.py --burst 5 --burst-at 60 --burst-for 120`.
   Pass: DEXT-to-gateway p99 under 10 ms on every gateway, and `gaps` stays 0.
2. **Compliance export at full volume:** `python compliance_export.py --mbps 50` while it runs.
   Pass: the lane's p99 moves by less than 10%.
3. **Reconnect storm and a gateway restart:** run `ws-1` with `--drops 500` (a mobile network blip at the open), and
   stop and restart `ws-2` mid-run; on restart it rebuilds its index from the log before accepting sockets.
   Pass: every reconnect recovered, zero gaps and zero unrecoverable resends, resend p99 under 50 ms.

## Shadow your own traffic

```
pip install websockets
python ws_bridge.py --partner-id <partner_id> --secret <partner_secret>    # every user on your partner platform
python ws_bridge.py --client-id <client_id> --token <access_token>        # or one client's own orders
```

The bridge only listens on `wss://api-order-update.dhan.co`; it places, modifies and cancels nothing. It maps each
update onto the lane's record in `to_update()` (client ID, a per-client sequence, order number, leg, status,
quantities, prices) and writes it with the kit's partition function. Its timestamps start at the bridge, so it
measures bridge-to-gateway. Inside Dhan, the same mapping fed from wherever DEXT emits its updates measures the real
path.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so Kafka clients can't read the lane; gateways use the Apache
Iggy SDK (Rust, Go, Java, Python and others). Delivery is at-least-once; the per-client sequence makes replays and
retries safe, and the gateway drops repeats. Replicated clusters shipped in September 2026, which is why the
evaluation kills a leader on purpose.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored order update feed, beside DEXT and your
gateways. No advantage, no migration.

kranti@laserdata.com · laserdata.com
