# Real-time payment downtime signals for Razorpay Checkout, on LaserData

Payment outcomes reach the downtime detector, and its signals reach Checkout and merchant webhooks, in
milliseconds, even through a match-day surge. It runs beside MSK, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/AQdS9hF3txezbZAJxzjjMS
- **One slide:** [`LaserData_Razorpay_DowntimeSignals.pdf`](LaserData_Razorpay_DowntimeSignals.pdf)

## Why this use case

From Razorpay Engineering ("Detecting downtimes to improve payment experiences", Nov 2021): "over 20,000
payments fail per month purely because customers select a payment method that is unavailable". The
first fix polled the database every 2 minutes; the rebuild on Flink turns about 15 workflow events per
payment into one payment status event on Kafka, and a detection job computes the success rate of each
payment cohort (method, bank, issuer, PSP) against thresholds, on 50+ cohorts at the Razorpay level and
200+ merchant cohorts. The post's own next step: a "low latency downtime notification broadcast system".

The signals matter. Razorpay's blog (Feb 2024) says Standard Checkout consumes them to warn customers and
steer them to another method, worth "a 0.5-0.6% bump in success rate"; merchants get them through the
Downtime API and the `payment.downtime.started / updated / resolved` webhooks.

From the 2026 post on the UPI Switch ("Five Years of Kafka at Razorpay's UPI Switch"): MSK carries the
Switch, partition count per broker became the limit before throughput did, and the fix for heavy flows
starving latency-sensitive ones was to isolate them at the infrastructure level. This kit is that kind of
isolated lane: one topic for every cohort's payment status, one for the downtime signals, keyed by
cohort, timed hop by hop, and a heavy workflow-event stream next door.

## Run it

Needs Docker and Python 3.10+. Defaults: 50 payment cohorts named like the Downtime API's instruments,
2,000 payments a second with a 5× surge for the first minute, 2,000 customers opening Checkout a second,
and a downtime every 30 s (one busy cohort's success rate falls to 10% for 12 s). Razorpay does not
publish payments per second for this path, so set the rates to yours.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python downtime.py &                   # the detector: success rate per cohort, signals out
python checkout.py &                   # Checkout: hides flagged methods, counts late signals
python payments.py &                   # payment status events, with the surge and the downtimes
python workflow_flood.py --at 20       # a heavy workflow-event stream next door
```

Each prints a line a second, and a line per downtime:

```
[payments] 10,000 payments/s (surge)  success rate …%
[downtime] 10,000 payment events/s  status-to-detector p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  cohorts 50  down 1  signals sent …
[checkout] downtime started upi:psp:okhdfcbank  degraded … s before the flag (detection window)  flag to checkout … ms (lane)  offered after the flag 0
[checkout]    50 signals/s  detector-to-checkout p50 …ms  p99 …ms  p99.9 …ms  PASS (<100ms)  cohorts down 1
[checkout] checkouts 2,000/s  method hidden …  offered after the flag 0  (so far: …)
```

`offered after the flag` is the business number: checkouts that still showed a method after the
detector had flagged it, because the signal had not reached Checkout yet. That count belongs to the lane
alone. Each downtime also prints its **detection window** (how long the cohort was degraded before the
rule fired, set by the rule and how many payments the cohort sees) and the **lane** time (detector to
Checkout). The lane cannot shorten the detection window; it keeps the pipe from adding to it.

**What the numbers mean.** The kit times each payment from the moment `payments.py` creates its status
event to the moment `downtime.py` has applied it, and each signal from the moment the detector sends it to
the moment `checkout.py` has applied it, through Python clients, on whatever machine you run it on. On a
shared GitHub runner, with the broker and four Python processes on one box, two green CI runs at 500
payments a second (2.5× for 15 s as the surge), both readers reading every partition directly, held
status-to-detector p50 4.8 to 6.1 ms, p99 8.8 to 11.7 ms and p99.9 9.4 to 12.3 ms (median per second over
62 steady-state seconds each, worst second's p99 17.1 ms, none over the 100 ms budget). Through the surge
(1,250 payments a second) the median per-second p99 was 9.9 to 13.3 ms, worst 17.1 ms. Detector-to-Checkout
ran at about 50 signals a second (each cohort's state every second, plus every change): p50 3.5 to 4.0 ms,
p99 4.3 to 5.5 ms, worst second 6.7 ms. In each run four injected downtimes reached Checkout 0.8 to 2.9 ms
after the flag; of about 36,000 checkouts per run, 1,169 to 1,181 had the degraded method hidden, and 0 to 2
were offered it in the milliseconds between the flag and the signal arriving. A 1 MB/s workflow-event stream ran next door for 34 s. That is the end-to-end lane through
this demo code, not the broker alone; the broker's own tail latency is measured with `iggy-bench` on
dedicated machines (`bench/PLAN.md`). The kit's detection rule (success rate of the last 40 payments
below 50%, checked every 100 ms) is deliberately simple, so its detection windows of 0.3 to 1.2 s say
nothing about your detector's.

| File | What it is |
|---|---|
| `payments.py` | Payment status events on `payment-status`, keyed by cohort, with a per-cohort sequence. `--surge` plays a match day; `--incident-*` injects downtimes. |
| `downtime.py` | The detector: success rate of the last N payments per cohort, every 100 ms; each cohort's resulting state on `downtime-signals`, on change and every second. Times status-to-detector. |
| `checkout.py` | Checkout: applies signals by sequence, hides flagged methods, counts checkouts offered a method after its flag. Run a second copy with `--name webhooks` for the merchant webhook path. |
| `workflow_flood.py` | A bulk, latency-tolerant stream of workflow events on its own topic on the same broker. |
| `msk_bridge.py` | Copies your payment-status topic from MSK into Iggy, as a read-only consumer group, for a shadow run on real traffic. Only cohort, payment ID and status cross over. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Match-day surge:** `python payments.py --payments-per-s 2000 --surge 5 --surge-for 60`.
   Pass: both hops' p99 under 100 ms through the surge, and `offered after the flag` at 0.
2. **Workflow events on the same broker:** `python workflow_flood.py --mbps 20 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart Checkout:** stop `checkout.py` and start it again. It replays the signals, prints
   "catching up", and rebuilds every cohort's state before timing live signals. Pass: no flagged method
   shown again by mistake, back under budget within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python msk_bridge.py --bootstrap <msk-bootstrap-brokers>:9096 --kafka-topic <your-payment-status-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=<user> -X sasl.password=<password>
```

The bridge only reads; the Payment Status Job, the detector and every existing consumer are untouched.
Map your field names in `to_msg()`: the kit needs `k` (the cohort), `seq`, `pay` and `status`. No VPA,
card or customer detail is copied into the lane. In shadow mode the clock starts when the bridge reads
the record.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored payment-status topic and its
downtime signals, self-hosted in your AWS account, beside MSK. No advantage, no migration.

kranti@laserdata.com · laserdata.com
