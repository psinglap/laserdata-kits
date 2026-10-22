# Real-time Soundbox payment alerts for Paytm, on LaserData

Every confirmed payment for a device merchant reaches the announcement service in milliseconds, in order per
Soundbox, and is announced once with the day's total, even when a bank recovers from an outage and a burst of
late confirmations lands at once. It runs beside your Kafka and AWS IoT, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Paytm_SoundboxAlerts.pdf`](LaserData_Paytm_SoundboxAlerts.pdf)

## Why this use case

From Paytm, its cloud provider and its own job posts:

- Soundbox devices "provide audio confirmations for merchants accepting payments from Paytm QR codes", and "the
  broadcast on Soundbox happens almost instantaneously after the customer completes a transaction on the Paytm
  app". Paytm moved device management to AWS IoT services and reports "availability of up to 99.99 percent",
  "improved success rate of message delivery for broadcast" and nearly 7 million devices, adding close to one
  million a quarter (AWS case study, 2023).
- "The number of merchants paying for our payment devices increasing to 1.57 crore, an addition of 27 lakh
  merchants over the past year" (Paytm Q1 FY27 results, quarter to June 2026).
- Newer Soundboxes show "real-time transaction updates, total collections, and device status" on a screen, in
  11 languages (Paytm press release, Apr 2025).
- Backend roles list "Messaging: Kafka/RabbitMQ, Caching: Redis/Aerospike, Micro services, AWS" (Paytm job
  post, 2025).

The merchant hands over the goods when the box says the amount. So the announcement has to be fast, said once,
and about the customer at the counter now. Two everyday events stress that: status callbacks are delivered at
least once, so some arrive twice; and when a bank recovers after an outage, payments its customers made over the
last minutes are confirmed in a burst. This kit runs the stretch from payment status to the device push as its
own lane on Apache Iggy:

- **Route by when the customer paid.** Confirmations from the last `FRESH_S` (60 s, illustrative) go on
  `payment-confirmations`; older ones go on `late-confirmations`, which the passbook files at its own pace. A
  recovery burst never queues ahead of the payment at the counter now.
- **Say it once.** Each status carries the device's sequence and the resulting day total and count, never a
  delta. A callback delivered twice changes nothing, and the speaker says the amount once.
- **Late is credited, not announced.** A payment from minutes ago is never said aloud as if the person at the
  counter just paid; the next announcement's total already includes it.

Paytm has not published how a confirmation travels from the payment switch to the AWS IoT publish, or a latency
target for it. The kit's 50 ms budget is for this lane only, and the bridge assumes the payment status is on
Kafka.

## Run it

Needs Docker and Python 3.10+. Defaults: 1,000,000 Soundbox merchants with a payment every 200 s each (about
5,000 confirmations/s; illustrative, as Paytm has not published per-device payment rates), 1% delivered twice,
and a bank recovery at 60 s: 5% of the merchants (50,000) have 15 minutes of pending payments settle within 10 s.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python announcer.py &                # the announcement service, fed by the lane
python passbook.py &                 # files late confirmations
python payments.py                   # 1M Soundbox merchants: payments out, announcements in, checked
```

They print a line a second:

```
[announcer]  5,000 confirmations/s  confirmation-to-announcer p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
             announced … (UPI …, card …, Rs …)  late, passbook only …  repeats dropped …
[payments]   5,000 confirmations/s (steady)  announcements expected …  at the device …, on time (<1000 ms) …
             missing …  announced twice …  stale announced …  total backwards …  wrong total …  to passbook …
             confirmation-to-device p50 …ms p99 …ms
[passbook]   filed …/s  repeats dropped …  behind the newest by … s  oldest payment … min old
```

`payments.py` knows every payment it made, so its device push reader checks each announcement against the
truth: was every fresh payment announced once, within a second, with the right day total; was any payment
announced twice; was any payment older than the fresh window announced; did any device's total go backwards.
That is the merchant's side of a slow or noisy lane.

**What the numbers mean.** The kit times each confirmation from the moment `payments.py` creates it to the moment `announcer.py` has applied it, and each announcement from that confirmation to the device push reader, through Python clients, on whatever machine you run it on. On a shared GitHub runner with the broker and all scripts on one box, at reduced scale (60,000 merchants, a payment every 30 s each, about 2,000 confirmations/s, 1% delivered twice; a bank recovering 30 s in, settling 6 minutes of pending payments for 3,000 merchants over 8 s; 2 MB/s of app events next door, 4 MB/s during a 10 s burst), three green runs on the final code held confirmation-to-announcer p50 7.5–7.7 ms, p99 14.4–14.7 ms and p99.9 16.1–16.7 ms (medians of 61 to 62 steady-state seconds each, none over the 50 ms budget). Through the bank recovery the lane's p99 was 15.7–15.9 ms against 14.0–14.3 ms before it, with the worst second at 16.8–19.4 ms. Confirmation to the device push reader (both hops): p50 11.5–11.8 ms, p99 18.4–18.8 ms. Of about 115,000 fresh payments per run, every one was announced within a second; none was missing, none was announced twice, no payment older than the fresh window was announced, and no total was wrong or went backwards; about 4,200 repeated confirmations per run were dropped by sequence. The passbook filed every late confirmation, about 32,150 per run, never more than 0.04 s behind. That is the end-to-end lane through this demo code, not the broker alone, and it does not include AWS IoT or the mobile network. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. The announcement rules in the kit are stand-ins that take no time; yours add their own.

| File | What it is |
|---|---|
| `payments.py` | Soundbox merchants taking UPI and card payments, a status every `--pay-s` s each with the payment time, sequence and resulting day total, routed by payment time (`common.route()`). Plays repeated callbacks, a bank recovery, and the readers (device push, business app, merchant support); checks every announcement against the truth. |
| `announcer.py` | The announcement service: drops repeats by sequence, keeps late payments quiet, sends announcements to `soundbox-announcements`. `rules()` is where yours go. Prints the lane's p99. |
| `passbook.py` | Files late confirmations into each merchant's passbook at its own pace, dropping repeats. |
| `analytics_flood.py` | A neighbour on the same broker: app events on `app-events`, with a burst. |
| `kafka_bridge.py` | Copies your payment status topic from Kafka into the lane as a read-only consumer group, routing by payment time. |
| `common.py` | Topics, partitioning by Soundbox, status kinds, the fresh window and the routing rule. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A bank recovers:** `python payments.py --held-share 0.2 --held-min 30 --flush-s 10` (a fifth of the
   merchants' pending payments from a 30-minute outage settle within 10 s). Pass: confirmation-to-announcer p99
   under 50 ms through the burst, no payment announced twice, no stale payment announced, no total going
   backwards, and the passbook files every late confirmation.
2. **Festival evening, with app events next door:** `python payments.py --peak 3 --peak-at 30 --peak-for 60`
   and `python analytics_flood.py --mbps 100 --burst-x 3 --burst-at 60 --burst-for 10`. Pass: the lane's p99
   stays under 50 ms and moves by less than 10% when the neighbour starts. One Python process may not reach
   100 MB/s on a laptop; the script prints the rate it reached.
3. **Restart the announcer:** stop `announcer.py` and start it again; it resumes from its stored offset (stored
   once a second) and still announces confirmations from the last 10 seconds. Pass: it catches up within
   seconds; up to a second of announcements is sent again with the same transaction ID and sequence, so the
   device push drops them and the merchant hears each payment once.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --kafka-topic <payment-status-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads, as its own consumer group `iggy-shadow`; the payment switch, every producer, your
notification services and AWS IoT are untouched. Map your record in `to_event()`: the kit needs the device ID,
the status, the amount, the payment time and the transaction ID, and the device's sequence and day total if the
record carries them. Run it next to your brokers.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and Kafka, AWS IoT and
everything else stay as they are. Delivery is at-least-once (statuses carry their sequence and resulting total,
announcements carry their transaction ID, so repeats are harmless). Replicated clusters (Viewstamped Replication)
shipped in September 2026. Storage is local disk per node, not object storage: that keeps the tail in
milliseconds, and it is why this is one lane, not your payment history. Apache Iggy does not speak MQTT or manage
devices; AWS IoT keeps the last hop. No SOC 2 report is cited here; ask us for the current status before a
production review.

Contact: kranti@laserdata.com · laserdata.com
