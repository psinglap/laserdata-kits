# Real-time market ticks for Kite at Zerodha, on LaserData

Every exchange tick reaches every Kite ticker node in milliseconds, in order per instrument, and a
ticker node that restarts rebuilds every quote from the log. It runs beside NATS, so the ticker's
WebSockets, the Kafka bus, Redis and everything else stay where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/JsviYebusJrJFZ8E8Kx3Ez
- **One slide:** [`LaserData_Zerodha_TickLane.pdf`](LaserData_Zerodha_TickLane.pdf)

## Why this use case

From Zerodha's own posts:

- The `ticker` "streams tens of millions of real time market data packets every second to end
  users", to more than a million concurrent users. It has been rewritten at least five times in six
  years, and "its internal streaming technology has changed from a custom TCP protocol to ZeroMQ to
  Nanomsg/Mango to NATS finally" (Kailash Nadh, Zerodha Tech Blog, Jul 2021).
- NATS carries "large volumes of real time-market data across infrastructures across applications
  at high throughputs"; components "depend on the ease of subscriptions and instant and 'automagical'
  failovers NATS offers" (CNCF case study, 2020).
- "Latency is THE metric." "A 10x burst in traffic over a period of 10 seconds is quite common on a
  trading platform." (Zerodha Tech Blog, Jun 2020). Kite's client round trip has a 40 ms baseline
  that triggers a refactor when it drifts (Jul 2021).
- Ticks are relayed as the exchange sends them, not throttled (Kite Connect forum, Zerodha staff),
  and the public ticker sends Kite's binary `quote` packet: 44 bytes, prices in paise.

The ticker hop from the feed handlers to the ticker nodes is the narrowest, most latency-sensitive
stream Zerodha has described. This kit runs that one lane on Apache Iggy and prints its p99 every
second. What the log adds over a pub/sub hop: a ticker node that restarts or is added mid-session
rebuilds the latest quote for every instrument, including ones that have not traded for minutes,
from the last minute of the log, and a session can be replayed afterwards.

## Run it

Needs Docker and Python 3.10+. Defaults: 60,000 instruments across NSE, NFO, BSE and MCX,
20,000 ticks a second, each feed handler writing once a millisecond, and ticker nodes serving
50,000 simulated users each, every user watching 50 instruments.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python feed.py &                     # feed handlers, one per segment
python ticker.py --name ticker-1 &   # a ticker node
python ticker.py --name ticker-2     # and another: each reads every tick
```

Each prints a line a second:

```
[ticker-1] 20,000 ticks/s  feed-to-ticker p50 …ms  p99 …ms  p99.9 …ms  PASS (<10ms)
           packets to users …/s  shown after a newer tick existed …%  gaps 0  quoted 60,000
[feed]     20,000 ticks/s  … writes  write ack p50 …ms  p99 …ms
```

**What the numbers mean.** The kit times each tick from the moment the feed handler received it
(including its wait for the next write) to the moment `ticker.py` has applied it, through Python
clients, on whatever machine you run it on. On a shared 2-vCPU GitHub runner, with the broker, the
feed, two ticker nodes and the postback script all on one box, at 1,000 ticks/s (8,000 instruments),
three green CI runs held a median per-second p50 of 3.3 to 3.5 ms, p99 7.0 to 7.3 ms and p99.9 8.3 to
8.9 ms, with 1 or 2 of 65 seconds over 10 ms; the worst second reached 11 to 20 ms, as the restarted
node replayed the log on the same two CPUs. A 10× postback burst next door moved the median p99 by
0.2 to 0.5 ms. The restarted node rebuilt a quote for all 8,000 instruments from the last 20 s of the
log (about 23,300 ticks) in 0.26 to 0.30 s; 10 s of live ticks alone would have covered 58% of them.
0.3% of the quotes pushed to users had a newer tick already waiting, and there were no sequence gaps.
That is the end-to-end lane through this demo code, not the broker alone. The 10× tick burst is not
part of CI: on two shared vCPUs the Python feed and readers run out of CPU before the broker does, so
run test 1 on your own machines. The broker's own tail latency is measured with `iggy-bench` in
`bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `feed.py` | Feed handlers, one per exchange segment, each writing its segment's ticks to its own partition once per ms. Kite's 44-byte quote packet plus a per-instrument sequence; a rolling snapshot of every quote once a minute. `--burst` for the 10× burst. |
| `ticker.py` | A ticker node: reads every partition with its own offsets, keeps the latest quote per instrument, counts the packets it would push to its users, checks sequence gaps. Rebuilds from the last `--replay-s` seconds of the log on start. |
| `postbacks.py` | Order postbacks on their own topic, with a burst, for the isolation test. |
| `nats_bridge.py` | Subscribes to your existing tick subject on NATS and writes it into Iggy, for a shadow run on real ticks. |
| `ci.sh` | The CI run: 1,000 ticks/s, two ticker nodes, a 10× postback burst next door, a node restart; pass only on 90% PASS seconds per node (the 10 ms budget is tight for a shared runner; p50 stays near 4 ms), zero gaps and a full rebuild. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests

1. **The 10× burst:** `python feed.py --burst 10 --burst-at 60 --burst-for 10`.
   Pass: feed-to-ticker p99 stays under 10 ms, and `gaps` stays 0.
2. **More ticker nodes:** start a third and a fourth `ticker.py`.
   Pass: the first node's p99 moves by less than 10%; every node reports the same tick count.
3. **Restart a node mid-session:** stop `ticker.py` and start it again.
   Pass: `rebuilt from the last 75 s of the log … quotes for 60,000 of 60,000 instruments` within
   seconds, and the line after it is PASS. Compare with the `without the log` line.
4. **A busy neighbour:** `python postbacks.py --rate 2000 --x 10` while the rest runs.
   Pass: the tick lane's p99 moves by less than 10%.

## Shadow your own ticks

```
pip install nats-py
python nats_bridge.py --servers nats://<your-nats>:4222 --subject '<your tick subject>'
python ticker.py --name shadow --replay-s 0
```

The bridge is one more NATS subscriber, so the feed handlers and the ticker nodes on NATS are
untouched. Map your packet in `to_tick()`: the kit expects Kite's quote layout (token first, prices
in paise). The latency it prints then starts at the bridge, not at the exchange.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored tick subject, alongside NATS.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
