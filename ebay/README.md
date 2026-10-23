# Real-time broadcast notifications for eBay, on LaserData

Each broadcast is split into windows of 200 and pushed to all 20,000 subscriptions in tens of milliseconds, with
unicast notifications never queued behind it. It runs beside the Event Subscription Platform's Kafka pipelines,
so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.ebay-kit
- **One slide:** [`LaserData_eBay_BroadcastPush.pdf`](LaserData_eBay_BroadcastPush.pdf)

## Why this use case

From eBay's tech blog ("eBay's Notification Streaming Platform: How eBay Handles Real-Time Push Notifications at
Scale", Xiaoye Wang, Jul 2022): partners subscribe to "eBay's business events such as product feeds updates, bid
status changes and item shipment information, and receive push notifications in real time". The domain team
needed "broadcast notifications to 20,000 subscribers in real time", and loading all of them on the event thread
"blocks the event processing thread until it finishes, thus increasing the overall latency". The fix: windows of
subscriptions ordered by creation time ("a window size of 200"), one subtask per window distributed through a
message queue, and each window bound to a partition so the instance that owns it can cache its subscriptions.
eBay's 2026 postings call the Event Subscription Platform (ESP) "the backbone for push notifications at eBay",
built with Java, Spring Boot and "event pipelines with Apache Kafka". eBay's public docs say an unacknowledged
account-deletion notification is resent "until it is acknowledged", and the endpoint is marked down after 24 hours.

That design is a lane in its own right: window tasks with partition affinity, unicast notifications that must not
wait behind a broadcast, and retries that must not hold anyone else. This kit runs that one lane on Apache Iggy,
prints the push workers' p99 every second, and each broadcast's time from the event to its last window.

eBay has not published which topics are broadcasts today, how many subscriptions they have now, or ESP's
volumes and latency targets. The kit uses MARKETPLACE_ACCOUNT_DELETION, a topic every app must handle, as the
illustrative broadcast, and the post's 20,000 subscriptions and windows of 200.

## Run it

Needs Docker and Python 3.10+. Defaults: 4 push workers; a broadcast to 20,000 subscriptions every 5 s, split into
100 windows of 200; 3,000 unicast notifications a second over 200,000 subscriptions (topic names LISTING,
ITEM_AVAILABILITY, ITEM_PRICE_REVISION, SELLER_STANDARDS_PROFILE_METRICS); 1% of endpoints in planted outages
(6 s not acknowledging, 14 s acknowledging). Rates and outages are illustrative; set them in the flags.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python push_workers.py &             # 4 push workers: unicast and window tasks in, pushes out
python retry_worker.py &             # pushes an endpoint did not acknowledge, retried on their own topic
python event_processor.py            # unicast notifications out, broadcasts split into window tasks
```

They print a line a second, and one per broadcast:

```
[push]   3,000 unicast/s  100 windows/s (19,9xx pushed)  event-to-push p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
         unicast p99 …ms  held for retry …
[esp]    broadcast evt-… MARKETPLACE_ACCOUNT_DELETION: 20,000 subscriptions in 100 windows, all pushed +… ms
         after the event (acknowledged first time 19,9xx, held for retry …)
[retry]  retried …  acknowledged after retry … (held p50 …s)  waiting …
```

"Event-to-push" is the time from the event (for a window, the broadcast event; for unicast, the notification)
until the push worker has handed that notification, or that window's 200 notifications, to the HTTP sender.
"All pushed +… ms" is the broadcast's completion: from the event until the last of its 100 windows was pushed.

**What the numbers mean.** The kit times each notification from the moment `event_processor.py` creates it to the
moment a push worker has pushed it, through Python clients, on whatever machine you run it on. The HTTP call to the
endpoint is simulated (an endpoint acknowledges unless it is in a planted outage); your real pushes add their own
time. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (1,500 unicast
notifications a second and a 20,000-subscription broadcast every 2 s, 4 push workers in one Python process), three
green runs on the final code held event-to-push p50 5.6–6.8 ms, p99 19.8–31.2 ms and p99.9 24.0–41.2 ms (63–64 of
64 steady-state seconds inside 50 ms). Every broadcast (36 per run) reached all 100 windows, completion p50 38–46 ms
and max 47–59 ms. Unicast p99 was 9.5–12.0 ms in seconds without broadcast windows and 24.1–31.4 ms in seconds
with them: unicast never waits in a queue behind a broadcast, but the four workers share one Python process, so
pushing 20,000 notifications takes CPU time from the unicast reads; separate worker processes should avoid that (not measured here). 0.29–0.30% of
broadcast pushes were held for retry, and 2,301–2,310 per run were acknowledged after retry, held a median 3.1 s,
without moving the lane. That is the end-to-end lane through this demo code, not the broker alone. The broker's own
tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.
CI runs: https://github.com/psinglap/laserdata-kits/actions/runs/39880188455, https://github.com/psinglap/laserdata-kits/actions/runs/39880201782, https://github.com/psinglap/laserdata-kits/actions/runs/39880453441

| File | What it is |
|---|---|
| `event_processor.py` | The event processor. Publishes unicast notifications keyed by subscription; splits each broadcast into window tasks on `broadcast-windows`, window w on partition w % 16, without loading a subscription. Reads the window receipts and prints each broadcast's completion time. |
| `push_workers.py` | The push workers. Each owns 16/N partitions of both topics, reads them directly, serves its windows from its own cache, pushes, and sends a receipt per window. Pushes that are not acknowledged go to `push-retry`; the window carries on. The kit reads each partition from the start of the log on start-up; a production worker would store its position about once a second and resume from it. |
| `retry_worker.py` | Holds each unacknowledged push until it is due, pushes it again, and puts it back on `push-retry` with a doubled delay if the endpoint is still down. `--mark-down-s` stands in for the 24-hour rule. |
| `kafka_bridge.py` | Copies your raw business events from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Broadcast every second:** `python event_processor.py --broadcast-every 1`.
   Pass: event-to-push p99 under 50 ms, and every broadcast reaches all 100 windows within 250 ms.
2. **A bigger topic:** `python event_processor.py --subs 100000` with `python push_workers.py --subs 100000`
   (500 windows of 200). Pass: completion grows roughly in line with the windows, unicast notifications keep
   flowing every second, and no broadcast stays open.
3. **Endpoints that stay down:** `--down-share 0.05 --down-s 30` on both `push_workers.py` and `retry_worker.py`.
   Pass: event-to-push p99 and broadcast completion stay where they were, the waiting count grows on `push-retry`
   alone, and the held pushes are acknowledged once their endpoints come back.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <kafka-bootstrap-brokers> --kafka-topic <raw-business-events-topic> \
  --broadcast-subs 20000 -X security.protocol=SSL ...
```

The bridge only reads, in its own consumer group, `iggy-shadow`; the event producers, ESP and every existing
consumer are untouched. Map your record in `to_event()`: the kit needs the notification topic and either the
subscription ID or a broadcast flag. Leave the push simulated, or aim it at a test endpoint.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and every existing
producer and consumer stays on Kafka. Delivery is at-least-once: window tasks and unicast notifications carry IDs
and sequences, so a push worker skips repeats, and eBay's own guidance already asks endpoints to handle retries
idempotently. Replicated clusters (Viewstamped Replication) shipped in September 2026. There is no cross-region
replication in 0.9: run one lane per region or data center.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored business-event topic, alongside ESP's Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
