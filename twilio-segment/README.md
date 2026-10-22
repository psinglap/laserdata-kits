# Real-time destination delivery alerts for Twilio Segment, on LaserData

Every delivery attempt reaches the alert check in milliseconds, so a failing destination alerts the customer
within a second, not over 24 hours. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.twilio-segment-kit
- **One slide:** [`LaserData_TwilioSegment_DeliveryAlerts.pdf`](LaserData_TwilioSegment_DeliveryAlerts.pdf)

## Why this use case

From Twilio Segment's docs: Segment's systems "retry failed destination API calls for four hours with a randomized
exponential backoff" (Destinations); the successful delivery rate alert looks at "the volume of events
successfully received by your destination in the last 24 hours" and fires when the rate "falls below a percentage
you set", and the source volume alert notifies "the following day" (Connections Alerting); Delivery Overview data
has "an expected latency of approximately 30 seconds after event ingestion" (Delivery Overview). From Segment's
Centrifuge post (2018): 400,000 outbound HTTP requests a second to hundreds of public APIs over ~88,000
source-destination pairs, with power-law traffic per customer, and "In the best case, these API failures cause
delays. In the worst case, data loss." A June 2026 posting for the Twilio Segment Data platform/pipelines team asks
for "high-scale routing" and Kafka or Kinesis experience.

So a workspace whose CRM credentials are revoked has every event go into four hours of retries, while the alert it
set judges the whole last day: ten minutes of every event failing, at a steady rate, is 0.7% of a day, and a 99%
alert stays quiet. This kit carries every delivery attempt's outcome, keyed by workspace:destination, to one alert
check that judges the last few seconds against that workspace's own threshold and publishes ALERT and RESOLVED
to the alert service (email, Slack, in-app). It prints the lane's latency every second and, per incident, the time
from the first failed attempt to the alert service.

Segment has not published how its delivery results flow internally today, or how often the 24-hour alerts are
evaluated; the shadow run on your delivery results is how to compare.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 delivery workers, 20,000 workspace destinations (each workspace
connects 1–4 destinations) and 20,000 attempts a second, with power-law volume per key (`--skew 1.1`), 0.2% of
attempts failing at random, alert thresholds of 99%, 98% or 95% per workspace destination (`common.threshold`),
and a planted incident every 10 s for 6 s on one busy workspace destination: 401 (credentials revoked, every
attempt fails), 429 (rate limited, half fail) or 503 (partner down, 80% fail). All illustrative; set them in
`common.py` and the flags.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python alerts.py &                   # the alert check, fed by the outcomes lane
python delivery.py                   # 8 delivery workers: outcomes out; the alert service: alerts in
```

They print a line a second, and one per incident:

```
[alerts]   20,000 outcomes/s  outcome-to-check p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  alerts sent … (alert …, resolved …)  open …
[delivery] 20,000 attempts/s (steady)  failed …  failed on an incident before its alert …  alerts received … (resolved …, no incident 0)
           alert-to-service n … p50 …ms p99 …ms
[delivery] incident ws-00532:crm 401 (credentials revoked, ~… attempts/s): first failure to alert … ms,
           alert to alert service … ms, failed attempts before the alert …, delivered …% (threshold 99%)
```

"First failure to alert" is mostly the alert rule (`decide()` in `alerts.py`: at least 20 failed attempts in a 5 s
window and the delivered share under the workspace's threshold; RESOLVED once 50 attempts are in the window with
under half the allowance failing), not the broker. "No incident" counts alerts on a workspace destination that
had no planted incident; it should stay 0.

**What the numbers mean.** The kit times each outcome from the moment `delivery.py` creates it to the moment
`alerts.py` has counted it, and each alert from `alerts.py` to the alert service, through Python clients, on
whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at
reduced scale (3,000 attempts a second over 3,000 workspace destinations and 4 delivery workers), five green runs
held outcome-to-check p50 3.7–6.4 ms, p99 6.6–11.9 ms and p99.9 7.2–13.0 ms (every one of 62 steady-state seconds per
run inside 100 ms), with the check reading the 16 outcome partitions directly. The six planted incidents per run
(on workspace destinations carrying 17–107 attempts a second) reached the alert service a median 597–684 ms after
their first failed attempt (slowest 1.1–1.8 s), after 19–20 failed attempts, and the alert service had each alert
a median 2.0–2.6 ms after the check published it. No workspace destination without an incident alerted; five of
six incidents per run were RESOLVED before the run ended. With 2 MB/s of 5 KB event payloads on the same broker,
the lane's p99 moved by 0.3 ms or less. That is the end-to-end loop through this demo code, not the broker alone.
The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. The
destinations in the kit are simulated; real partner APIs add their own time. CI runs:
https://github.com/psinglap/laserdata-kits/actions/runs/39870235847,
https://github.com/psinglap/laserdata-kits/actions/runs/39870455585,
https://github.com/psinglap/laserdata-kits/actions/runs/39870681188,
https://github.com/psinglap/laserdata-kits/actions/runs/39871096119,
https://github.com/psinglap/laserdata-kits/actions/runs/39871318818 (the last two on the final code)

| File | What it is |
|---|---|
| `delivery.py` | The delivery fleet and the alert service. Delivery workers publish every attempt's outcome to `delivery-outcomes`, keyed by workspace:destination, with power-law volume per key; plants the 401, 429 and 503 incidents. The alert service reads `delivery-alerts` on its own connection and prints each incident's first failure to alert. |
| `alerts.py` | The alert check. Keeps a 5 s window per workspace destination, judges it against that workspace's threshold (`decide()` is where your rule goes), publishes ALERT and RESOLVED with the resulting state, and rebuilds every key's state from the log after a restart. Prints the lane's p99. |
| `archive_flood.py` | Raw event payloads (5 KB, the median job payload in the Centrifuge post) on their own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your delivery attempt results from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A traffic peak:** `python delivery.py --peak 5 --peak-at 60 --peak-for 120`.
   Pass: outcome-to-check p99 under 100 ms, every planted incident alerted within 2 s of its first failure, and no
   alert on a healthy workspace destination.
2. **The event firehose next door:** `python archive_flood.py --mbps 50` while both run.
   Pass: the outcomes lane's p99 moves by less than 10%.
3. **Restart the alert check mid-incident:** stop `alerts.py` while a destination is alerting and start it again;
   it replays the outcomes topic, rebuilds every key's state, and only publishes alerts for outcomes from the last
   10 seconds. Pass: it catches up within seconds, no outcome is counted twice, an incident still open is announced
   at most once more (the newest alert per key wins, so the alert service can drop the repeat), and RESOLVED
   follows when delivery recovers.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <bootstrap-brokers> --kafka-topic <delivery-results-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads; delivery, producers and every existing consumer are untouched. Map your record in
`to_outcome()`: the kit needs the workspace, the destination, the status (2xx counts as delivered, set
`--ok-statuses`) and optionally the attempt number. The topic name and fields are placeholders.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and every existing
producer and consumer stays on Kafka. Delivery is at-least-once (alerts carry the state and outcomes carry a
sequence, so repeats are harmless). Replicated clusters (Viewstamped Replication) shipped in September 2026. There
is no cross-region replication in 0.9: run one small lane per region where delivery runs.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored delivery-results topic, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
