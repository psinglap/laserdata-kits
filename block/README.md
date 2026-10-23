# Real-time payment token changes for Afterpay, on LaserData

Every token change from the payments platform's scheduled jobs reaches every subscribing service in milliseconds,
signed and free of card numbers. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **One slide:** [`LaserData_Block_PaymentTokens.pdf`](LaserData_Block_PaymentTokens.pdf)

## Why this use case

From the Cash App Code Blog ("Implementing Kafka in the Payments PCI World", Jing Li, September 2022), on
Afterpay's payments platform: "we have scheduled Quartz jobs that make changes to our data, such as payment
tokens", and other Afterpay systems hold that data too, so without a signal it can become inconsistent. The team
chose a message queue with opt-in subscriptions over synchronous API calls, and reached the Platform team's Kafka
from the PCI zone through two AWS PrivateLink hops, with custom producer and consumer libraries that detect and
obfuscate card numbers during serialization, publish without blocking the payment flow, and sign every message
with HMAC so consumers can check its integrity.

From "Project Teleport" (Unni Krishnan, March 2025): Afterpay's Kafka pipelines process more than 9 TB a day, on
about 120 topics, landed in S3 by Confluent sink connectors and merged in Databricks.

A subscriber that charges an instalment on a token the vault has already replaced sends it to a card that was
reissued, replaced or removed, and Afterpay's help pages say a failed scheduled payment may be retried, can pause
the account and can lead to late fees. The window in which that can happen is the lane's tail latency. This kit
runs that one lane on Apache Iggy, with the same signing and card number guard, and prints change-to-cache p99
every second next to the instalments charged on a superseded token, and how many a subscriber fed 5 s behind
would have charged.

## Run it

Needs Docker and Python 3.10+. Defaults: 500,000 customers with a payment token, 500 token changes a second
(card updater results, cards replaced in the app, cards removed), and 2,000 instalment charges a second per
subscriber. All rates and shares are illustrative; set yours.

```
docker compose up -d                       # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt            # apache-iggy 0.9.0
python token_jobs.py &                     # the token vault and its scheduled jobs
python instalments.py --name collections-1 &   # a subscriber: instalment collections
python instalments.py --name collections-2     # another; each holds every customer's token
```

Set `TOKEN_LANE_HMAC_KEY` to the same value for every script to sign with your own key.

They print a line a second:

```
[collections-1]    500 changes/s  change-to-cache p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  instalments charged …
                   no usable card …  bad signatures 0  card numbers seen 0
[tokens]    500 changes/s (steady)  (card_updater … card_replaced … token_revoked …)  card numbers masked …
            charges checked …  on a superseded token …  a subscriber 5s behind would send …
```

**What the numbers mean.** The kit times each token change from the moment `token_jobs.py` creates it to the
moment an `instalments.py` subscriber has verified it and applied it to its cache, through Python clients, on
whatever machine you run it on. On a shared 2-vCPU GitHub runner with the broker and all four scripts on one box,
at reduced scale (100,000 customers, 600 token changes/s with a 4× card updater run to 2,400/s, two subscribers
charging 400 instalments/s each, 2 MB/s of warehouse events next door), the medians of the steady-state seconds
over two green runs on the final code were p50 4.7–7.0 ms, p99 8.8–13.2 ms and p99.9 10.3–14.2 ms on each
subscriber. 119 of 120 steady-state seconds per subscriber were inside the 50 ms budget; the one that wasn't
(p99 117 ms, both subscribers in the same second) was a second in which `token_jobs.py`'s own send to the broker
took 112 ms on the shared runner. Of about 49,900 instalment charges per run checked against the vault, 1–4 went
out on a superseded token; a subscriber fed 5 s behind would have sent 2,345–2,473 (4.7–5.0%). The guard masked
112–113 card numbers in updater notes per run; subscribers saw none, and no record failed the signature check.
That is the end-to-end lane through this demo code, not the broker alone. The broker's own tail latency is
measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. CI runs:
https://github.com/psinglap/laserdata-kits/actions/runs/39886853345,
https://github.com/psinglap/laserdata-kits/actions/runs/39887034683

| File | What it is |
|---|---|
| `token_jobs.py` | The token vault and its scheduled jobs. Every change goes to `payment-token-changes`, keyed by customer, with a per-customer sequence and the resulting token (ID, last four, expiry, status), masked for card numbers and then signed with HMAC-SHA256. Checks every instalment charge against the token that was current when it was made. `--run` multiplies the rate during a card updater run. |
| `instalments.py` | A subscriber: reads every partition of the lane directly into its token cache, verifies every signature, checks for card numbers, rebuilds from the log on start, charges instalments from the cache, and keeps the same cache 5 s behind for comparison. |
| `warehouse_feed.py` | A heavy, latency-tolerant stream (payment events for the warehouse) on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies the token notification topic your subscribers read today into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

The instalment rate, the change mix and the 5 s comparison are assumptions of the kit, not Afterpay figures.
Afterpay has not published how fast its token notifications reach subscribers today; the 5 s subscriber is a
yardstick, not a claim about your system. Set `--lag` to what you see.

## Three tests

1. **A card updater run:** `python token_jobs.py --run 10 --run-at 60 --run-for 120`.
   Pass: change-to-cache p99 under 50 ms on every subscriber, and instalments on a superseded token near zero
   through the run.
2. **The warehouse feed at full volume:** `python warehouse_feed.py --mbps 100` while it runs.
   Pass: the lane's p99 moves by less than 10%.
3. **Restart a subscriber:** stop one `instalments.py` and start it again; it rebuilds from the log, verifying
   every record, before it charges anything.
   Pass: every customer's token rebuilt within seconds, and no charge on a superseded token from the first one.

## Design notes

- A change carries the customer's resulting token and a sequence, so at-least-once delivery is safe: a repeated or
  replayed change changes nothing. Apache Iggy 0.9 does not offer exactly-once, and this lane doesn't need it.
- The lane carries token IDs and last fours, never card numbers. The guard masks any 13–19 digit run that passes
  the Luhn check in a text field before the record is signed, and every subscriber checks again.
- Signing is in the client, as in your libraries today: HMAC-SHA256 over the record body. In production the key is a
  KMS key; the kit reads `TOKEN_LANE_HMAC_KEY`. Apache Iggy adds TLS, users and per-stream permissions on the broker.
- Apache Iggy is a single Rust binary, so the lane can run in the same VPC as the payments platform.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --topic <token-notifications> -X security.protocol=SSL …
```

The bridge reads as its own consumer group, `iggy-shadow`, and never commits offsets; the payments platform's
producers and every other consumer are untouched. Map your record shape in `to_change()` (decode Avro there with
your schema registry client, and check your own HMAC): the kit needs `k` (customer), `seq`, `token` and `status`.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so Kafka clients can't read the lane; subscribers use an Apache
Iggy SDK (Java, Rust, Go, Python, C#, Node.js; no official Kotlin SDK, Kotlin services use the Java one). Delivery
is at-least-once; the per-customer sequence makes replays and retries safe. Replicated clusters shipped in
September 2026, which is why the evaluation kills a broker on purpose.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored token notification topic, beside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
