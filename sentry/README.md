# Real-time cron check-ins for Sentry Crons, on LaserData

Every check-in from Relay, and every clock pulse, reaches the monitor clock in milliseconds, keyed by monitor,
so a job that never checked in is marked missed on the minute, a hung job times out on the minute, and a late
partition holds the tick instead of marking jobs missed. It runs beside Kafka, so Relay, the SDKs, the
monitors' Postgres state and alert rules stay where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Sentry_CronsCheckins.pdf`](LaserData_Sentry_CronsCheckins.pdf)

## Why this use case

From Sentry's own engineering blog, issues and pull requests:

- Check-ins go from SDKs through Relay into a Kafka topic; consumers drive a clock from the messages
  themselves, so "backlogs no longer have a ripple effect that creates false positive missed check-ins"
  (Evan Purkhiser, "Scaling Cron Monitoring", Nov 2023, at "over 7 million check-ins per day").
- The check-in topic has 128 partitions; the global clock is the slowest partition's, and the accepted
  trade-off is that a backlogged partition halts missed and timeout processing for all partitions. A
  scheduled task sends a clock pulse into every partition so quiet ones still move (getsentry/sentry #55821).
- Dropped check-ins during an ingestion outage would mark customers' monitors missed; ticks during an
  incident are now marked unknown (#79328, 2024).
- September 2026: "When redis loses the monitor partition clock set ... The clock jumps straight to that one
  partition's timestamp, and every check-in behind it gets marked missed or timed out. This makes the clock
  wait instead." (Scott Strong, #124511, with stall-gap metrics in #124424).

The clock moves at the pace of the slowest partition, so every partition's tail latency is how late every
missed-job and timeout alert is. This kit puts check-ins and clock pulses on one small lane on its own
broker, runs the clock the way Sentry describes it, takes each partition's clock from the broker's append
time in the log (so a restarted consumer rebuilds it from the retained lane), and checks every missed and
timeout mark against the truth.

## Run it

Needs Docker and Python 3.10+. Defaults: 50,000 monitors (10% every minute, 30% every 5, 30% every 15, 30%
hourly), about 26 million check-ins a day with the burst at the top of each minute, a one-minute check-in
margin, a 3-minute max runtime, 2% of runs that never check in and 1% that hang.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python monitors.py &                 # the monitor consumer and its clock
python crons.py                      # 50,000 monitors checking in, every mark checked
```

They print a line a second, and the clock prints each tick:

```
[monitors]  … check-ins/s  check-in-to-monitor p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
            monitors 50,000  jobs running …  clock minute …  ticks 1  missed …  timed out …
[clock]     tick minute …  lag …ms  missed so far …  timed out so far …
[crons]     … check-ins/s  monitors 50,000 (minute = 60 s)  runs due …  missed: right … (by next check-in …)
            false 0  undetected 0  timeouts: right …  false 0  undetected 0  mark after deadline p50 …ms
```

Tick lag is how long after the minute boundary the clock ticked: the delay of the slowest partition's
clock pulse. `missed: right` counts runs that really never checked in; `false` counts a run that did check
in (or a job that finished) marked anyway, which is a false alert to a customer.

**What the numbers mean.** The kit times each check-in from the moment `crons.py` sends it to the moment
`monitors.py` has applied it, through Python clients, on whatever machine you run it on. Tick lag is how long
after the minute boundary the clock ticked. On a shared 2-vCPU CI runner with the broker and all scripts on one
box, with a minute compressed to 5 s, 6,000 monitors running every 1, 2 or 5 minutes (about 900 to 950
check-ins/s on average, near 4,000/s in the burst at the start of each minute), 3% of runs never checking in
and 2% hanging, two green runs held check-in-to-monitor p50 2.9 to 3.4 ms, p99 5.7 to 6.3 ms and p99.9 6.7 to
7.1 ms (median of 63 to 64 steady-state seconds each), with no second over the 50 ms budget, while 3 MB/s of
error events ran on the topic next door. Of 14 ticks per run, those not held by the stuck partition fired a
median of 20 ms after the minute boundary (at most 26 to 32 ms). With partition 3 unread for 8 s, the clock
held one or two ticks (up to 4.1 to 7.5 s late), then ticked them in order. Each run marked 1,371 to 1,373 runs
missed and 689 to 733 jobs timed out, every one a run that really never checked in or a job that really hung;
no run that checked in was marked missed, no finished job timed out, and none was left unmarked. That is the
end-to-end lane through this demo code, not the broker alone. The broker's own tail latency is measured with
`iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `crons.py` | Monitors on their schedules; `in_progress` then `ok` or `error`, keyed by monitor, and a clock pulse into every partition at each minute, on `ingest-monitors`; jobs that never check in and jobs that hang on purpose. Checks every mark against the truth. |
| `monitors.py` | The monitor consumer: per-partition clocks from the log's append time, a tick when every partition has passed the minute, backfill of skipped minutes, missed and timeout marks to `monitor-verdicts`. `--pause-partition` simulates a stuck partition. |
| `event_storm.py` | A neighbour on the same broker: one customer's error storm (4 KB events) on `ingest-events`. |
| `kafka_bridge.py` | Copies your `ingest-monitors` topic (Relay's msgpack check-ins and clock pulses) into the lane as a read-only consumer group. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

Time runs on a real minute by default. `--minute-s 5` on both scripts compresses a minute to five seconds, as
CI does, so a one-minute run sees a dozen ticks.

## Three tests

1. **A stuck partition:** `python monitors.py --pause-partition 3 --pause-at 120 --pause-for 150` (the
   consumer stops reading one partition for two and a half minutes). Pass: the clock holds, then backfills
   every skipped minute in order; no run that checked in is marked missed, and no finished job times out.
2. **An error storm next door:** `python event_storm.py --mbps 100` while both run. Pass: check-in p99 and
   tick lag move by less than 10%. The script prints the rate it actually reached; one Python process may not
   reach 100 MB/s on a laptop.
3. **Restart the monitor consumer:** stop `monitors.py` and start it again; it replays the retained lane (one
   hour) and rebuilds every monitor and every partition clock from the log, with no separate clock store.
   Pass: it catches up within seconds, the clock does not jump, and no later mark is false. Marks for minutes
   it replays are sent again, so the downstream must be idempotent, as Sentry's missed and timeout tasks are.

## Shadow your own traffic

```
pip install confluent-kafka msgpack
python kafka_bridge.py --bootstrap <brokers> --kafka-topic ingest-monitors -X security.protocol=SSL
```

The bridge only reads, as its own consumer group `iggy-shadow`; Relay and the monitors consumer are
untouched. It forwards each check-in (slug, status, check_in_id, monitor_config) keyed by monitor, and each
clock pulse into every lane partition. Monitors created in the UI send no `monitor_config`, so the kit's
consumer would treat them as every-minute monitors; to shadow those, load schedules from your Postgres into
`mons` at start. Run it next to your brokers.

## Next step

If the numbers are interesting: a 2–4 week evaluation on a mirror of `ingest-monitors`, beside your current
consumer. No advantage, no migration.

kranti@laserdata.com · laserdata.com
