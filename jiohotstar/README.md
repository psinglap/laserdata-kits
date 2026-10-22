# Real-time playback health signals for JioHotstar, on LaserData

Every playback heartbeat reaches your health check, and every CDN health signal reaches routing and
alerting, in milliseconds, even in the death overs. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/NLs7irAzWx8wD9ARog2hx6
- **One slide:** [`LaserData_JioHotstar_PlaybackHealth.pdf`](LaserData_JioHotstar_PlaybackHealth.pdf)

## Why this use case

From JioHotstar's engineering blog ("The Kafka Optimization That Saved Us $350K a Year", Oct 2026): the
platform "receives in excess of 40TB data every day. At 2.1 million messages per second." Every active
playback session emits a 7.5 KB protobuf heartbeat with 300+ fields; "Six independent downstream
consumers subscribe to it. Most of them need about 1.1 KB." The heartbeat tells your systems whether a
session "started, stalled, buffered, dropped quality, or failed", and "Our real-time alerting system, QoS
dashboards, churn models, and several other systems all depend on it." The `kafka-message-splitter` now
publishes a minimized ~1.1 KB HeartbeatEvent for the subset consumers.

From "Orchestrating JioHotstar Traffic" (Mar 2026): the QoS Routing Manager scores playback failure rate,
rebuffering and RTT per cohort (ASN-Country-State-City-UserType), smooths them with an EWMA (alpha about
0.6), puts them in severity buckets (Ideal, Baseline, Sev3, Sev2, Sev1) and moves CDN weights per cohort.
With ASN-State cohorts in a T20 match, playback failure rate improved 11%, "In our world of 50M+
concurrent users".

So the time that decides how many plays land on a failing CDN is the heartbeat-to-health-check and
signal-to-routing path, at its busiest moment. This kit runs that one lane on Apache Iggy, beside Kafka:
minimized heartbeats in, keyed by cohort and CDN; health signals out to routing and alerting. It times
both hops every second and counts the plays a late signal costs.

## Run it

Needs Docker and Python 3.10+. Defaults: 48 network-state cohorts on 3 CDNs (144 pairs), 10,000
heartbeats a second of ~1.1 KB with a 3× death-overs surge for the first minute, 2,000 new plays a
second, and a CDN incident every 30 s (one busy pair fails 25% of heartbeats for 12 s). The kit's
2.1M figure is your total; set the rate to the slice you want to test.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python qos.py &                        # the health check: score per cohort and CDN, signals out
python routing.py &                    # routing: skips flagged CDNs, counts late signals
python heartbeats.py &                 # minimized heartbeats, death overs and incidents
python full_flood.py --at 20           # full 7.5 KB heartbeats next door
```

Each prints a line a second, and a line per incident:

```
[heartbeats] 30,000 heartbeats/s (death overs)  failed …%
[qos]  30,000 heartbeats/s  heartbeat-to-check p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  pairs 144  degraded 1
[routing] degraded jio:MH|cdn-a  Sev2  degraded … s before the flag (detection window)  flag to routing … ms (lane)  plays sent after the flag 0
[routing]   144 signals/s  check-to-routing p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  pairs degraded 1
[routing] plays 2,000/s  steered to a healthy CDN …  sent after the flag 0
```

`plays sent after the flag` is the business number: new plays still sent to a CDN after the health check
had flagged it for that cohort, because the signal had not reached routing yet. That count belongs to the
lane alone. Each incident also prints its **detection window** (how long the pair was degraded before the
score crossed into Sev2, set by the scoring and how many heartbeats the pair sees) and the **lane** time
(health check to routing). The lane cannot shorten the detection window; it keeps the pipe from adding to it.

**What the numbers mean.** The kit times each heartbeat from the moment `heartbeats.py` creates it to the
moment `qos.py` has applied it, and each signal from the moment the health check sends it to the moment
`routing.py` has applied it, through Python clients, on whatever machine you run it on. On a shared GitHub
runner, with the broker and four Python processes on one box, two green CI runs at 800 heartbeats a
second of ~1.1 KB (2.5× for 15 s as the death overs) held heartbeat-to-check p50 4.3 to 6.8 ms, p99
8.0 to 13.4 ms and p99.9 9.3 to 15.2 ms (median per second over 62 steady-state seconds each, worst
second's p99 9.8 to 16.9 ms, none over the 100 ms budget). Through the death overs (2,000 heartbeats a
second) the median per-second p99 was 9.3 to 16.2 ms, worst 16.9 ms. Check-to-routing ran at about 120
signals a second (every pair's state every second, plus every change): p50 5.1 to 7.3 ms, p99 6.1 to
9.0 ms, worst second 11.3 ms. Both readers poll every partition directly rather than through a consumer
group. In each run four injected incidents reached routing 2.8 to 4.6 ms after the flag; of about 36,000
plays per run, 810 to 840 were steered to a healthy CDN, and 0 and 2 were sent to a flagged CDN before its
signal arrived. Full 7.5 KB heartbeats ran next door at 2 MB/s for 34 s. That is the end-to-end lane through
this demo code, not the broker alone; the broker's own tail latency is measured with `iggy-bench` on
dedicated machines (`bench/PLAN.md`). The kit's scoring (weights, thresholds, a 40-heartbeat window) is
illustrative, so its detection windows of 0.3 to 0.8 s say nothing about yours.

| File | What it is |
|---|---|
| `heartbeats.py` | Minimized heartbeats on `heartbeats-lite`, keyed by `cohort|cdn`, with a per-pair sequence. `--surge` plays the death overs; `--incident-*` injects CDN incidents. |
| `qos.py` | The health check: PFR, rebuffering and RTT of the last N heartbeats per pair, piecewise scores, EWMA, severity buckets, every 100 ms; each pair's resulting state on `cdn-health`, on change and every second. Times heartbeat-to-check. |
| `routing.py` | Routing: applies signals by sequence, sends new plays away from flagged CDNs, counts plays sent after a flag. Run a second copy with `--name alerting` for the alerting path. |
| `full_flood.py` | Full ~7.5 KB heartbeats on their own topic on the same broker, for the full-data consumers. |
| `kafka_bridge.py` | Copies your minimized heartbeat topic from Kafka into Iggy, as a read-only consumer group, for a shadow run on real traffic. Only cohort, CDN, session ID, playback state and RTT cross over. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Death overs:** `python heartbeats.py --heartbeats-per-s 10000 --surge 3 --surge-for 60`.
   Pass: both hops' p99 under 100 ms through the surge, and `plays sent after the flag` at 0.
2. **Full heartbeats on the same broker:** `python full_flood.py --mbps 50 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart a reader:** stop `routing.py` and start it again. It replays the signals, prints
   "catching up", and rebuilds every pair's state before timing live signals. Pass: no flagged CDN gets
   new plays again by mistake, back under budget within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <kafka-brokers> --kafka-topic <your-minimized-heartbeat-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=<user> -X sasl.password=<password>
```

The bridge only reads, as its own consumer group `iggy-shadow`; the clients, the splitter, ARGUS and every
existing consumer are untouched. Decode your HeartbeatEvent in `decode()` and map field names in
`to_msg()`: the kit needs `k` (cohort and CDN), `seq`, `sess`, `st` and `rtt`. No user, device or content
traits are copied into the lane. In shadow mode the clock starts when the bridge reads the record.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored heartbeat topic and its health
signals, in your cloud account, beside Kafka. No advantage, no migration.

kranti@laserdata.com · laserdata.com
