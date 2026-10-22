# Real-time device throttling for Amplitude, on LaserData

Every ingestion node's event counts reach the throttle service, and every throttle reaches every ingestion
node, in milliseconds, even through a spam burst. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_Amplitude_DeviceThrottle.pdf`](LaserData_Amplitude_DeviceThrottle.pdf)

## Why this use case

From Amplitude's API docs: "Amplitude throttles requests for users and devices that exceed 30 events per
second", "measured as an average over a recent time window", and the client must "pause sending events for
that user or device for 30 seconds" ([HTTP V2 API](https://amplitude.com/docs/apis/analytics/http-v2)). The
limit is "30 events per user or device per second across your entire organization" and "applies to all
endpoints, including the SDK endpoint" ([SDK troubleshooting](https://amplitude.com/docs/sdks/sdk-debugging)).
The 429 carries `eps_threshold` and `throttled_devices`, "a map from device_id to its current events per
second rate". Rates like these "often signal broken instrumentation or spam data", and "because of how the
data processing pipeline works, processing a high volume of events for a single device or user can be
challenging" ([Which API should I use?](https://amplitude.com/docs/api/content/faq/api-choice)).

From the Data Warehouse and Integrations job post (2026): "batch and streaming pipelines moving billions of
events a day" on "Java, Temporal, Kafka, Kubernetes, Terraform, DynamoDB, S3, and Snowflake".

The throttle is a loop with two hops. Behind a load balancer each ingestion node sees only its share of a
device's requests, so the device's rate exists only once every node's counts meet: that is the first hop,
counts keyed by device into the service that keeps the 30-second average. The throttle then has to reach
every node before the device's next request. Whatever the loop takes, the nodes keep accepting the device's
events into Kafka, the pipeline and Nova. This kit runs that one loop on Apache Iggy, prints both hops'
latency every second, and prints the events let in past the throttle. Amplitude has not published how its
throttle is built today; the kit is a way to measure this design against it.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 ingestion nodes, 100,000 devices sending 10,000 events a second
at human rates, and 60 devices with broken instrumentation that start one after another over two minutes at
40–300 events a second. The threshold is 30 events a second averaged over 30 seconds (HTTP V2); use
`--threshold 1000` on both scripts for the Batch API.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python throttle.py &                 # the throttle service, fed by the counts lane
python ingest_nodes.py               # 8 ingestion nodes: counts out, throttles in
```

They print a line a second:

```
[throttle]  10,000 count records/s  count-to-throttle p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  decisions sent …
[spam] throttled now …  throttles …  released …  events let in past the throttle … (… per throttle)
       a throttle 1 s later: about … more
[ingest]  10,000 events/s accepted  … count records/s  refused with 429 … events  held by SDK backoff …
          throttled in ingest-00's view …  throttle-to-ingestion-nodes n … p50 …ms p99 …ms
[compare] throttles this second …  human devices throttled 0  a node counting alone would have caught … of … by then
```

**What the numbers mean.** The kit times each count record from the moment `ingest_nodes.py` creates it to
the moment `throttle.py` has applied it, and each throttle from `throttle.py` to every ingestion node,
through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and
all scripts on one box, at reduced scale (2,000 events a second from 20,000 devices over 4 ingestion nodes,
plus 30 spam devices starting over 40 seconds, about 2,500 count records a second), three green runs held
count-to-throttle p50 3.0–7.3 ms, p99 5.8–13.5 ms and p99.9 6.4–15.1 ms (every steady-state second inside
50 ms, 62 per run), and throttle-to-ingestion-nodes p50 2.4–3.4 ms and p99 2.7–4.2 ms. With 2 MB/s of
Session Replay segments on the same broker, the lane's p99 moved by less than 0.5 ms. Each run made 48–49
throttles (a spam device is throttled again after its SDK's 30-second pause), let in 2–46 events in total
past the throttle, against about 7,840 more had each throttle come one second later; no device at human
rates was throttled, and at the moment of each throttle no single node, counting only its own share, had
reached 30 events a second (0 of 48). That is the end-to-end loop through this demo code, not the broker
alone. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated
machines. CI runs: https://github.com/psinglap/laserdata-kits/actions/runs/39869766256,
https://github.com/psinglap/laserdata-kits/actions/runs/39869997516,
https://github.com/psinglap/laserdata-kits/actions/runs/39870215532

| File | What it is |
|---|---|
| `ingest_nodes.py` | Ingestion nodes behind a load balancer, and the SDKs sending to them. Every accepted request adds to the node's running count for that device in that second, published to `device-counts`, keyed by device, with a sequence. Each node reads `throttle-state` directly on its own connection and answers a throttled device with a 429; the SDK pauses that device 30 s. |
| `throttle.py` | The throttle service. Sums the latest count from every node per device per second, keeps the 30-second average, throttles at the threshold, lifts the throttle when the average falls, and publishes each decision. Prints the lane's p99 and events let in past the throttle. |
| `replay_flood.py` | A heavy Session Replay stream on its own topic, for the isolation test. |
| `kafka_bridge.py` | Reads your raw event topic from Kafka as a read-only consumer group and writes running counts per device into Iggy, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A launch-day spike:** `python ingest_nodes.py --events-per-s 30000 --spam-devices 120`.
   Pass: count-to-throttle p99 under 50 ms, and no device at human rates throttled.
2. **Session Replay next door:** `python replay_flood.py --mbps 20` while both run.
   Pass: the counts lane's p99 moves by less than 10%.
3. **Restart the throttle service:** stop `throttle.py` and start it again; it replays `throttle-state` and
   the counts, rebuilds every device's average from the running counts without double counting, and lifts
   throttles that ran out while it was down.
   Pass: it catches up within seconds and throttled devices stay throttled until their average falls.

## Design notes

- Running counts with a sequence make at-least-once delivery idempotent: a repeated or replayed record
  changes nothing. Apache Iggy 0.9 does not offer exactly-once, and this lane doesn't need it: it only
  decides 429s. Billing on event volume stays where it is.
- The same lane keyed by `user_id` gives the per-user limit (EPUS). The daily quota (500,000 events per
  rolling 24 hours once a device is judged spammy) is one more counter on the same records.
- Throttled devices are not counted while their SDK pauses, so the average falls and the throttle lifts,
  and a device still in a loop is throttled again: the kit's throttle count is higher than its spam devices.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <broker:9092> --topic <raw-event-topic> --device-field device_id
python throttle.py
```

The bridge only reads, as its own consumer group `iggy-shadow`; the SDK endpoints, the pipeline and Nova are
untouched. Map your record in `to_device()`. Compare the kit's throttles with the `throttled_devices` your
endpoints return today.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored event topic, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
