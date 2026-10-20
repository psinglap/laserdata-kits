# Real-time release success rates for Juspay deployments, on LaserData

Every payment's outcome, tagged with the release that served it, reaches the A/B comparison in
milliseconds, so a bad release is aborted in seconds while it still serves a small share of traffic.
It runs beside Kafka and ClickHouse, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs. Apache Iggy is written in Rust, like Hyperswitch.

- **Write-up:** https://claude.ai/artifact/6nnfNd3urErwZAooDoVm2R
- **One slide:** [`LaserData_Juspay_ReleaseGuard.pdf`](LaserData_Juspay_ReleaseGuard.pdf)

## Why this use case

From Juspay's own posts:

- Autopilot is "A tool that creates a new deployment and performs traffic staggering from 1% onwards."
  The A/B testing framework "monitors and benchmarks the new deployment's performance against the
  previous stable version", and "Based on this benchmark, the system automatically decides to scale up
  the traffic or abort the deployment." (Hyperswitch on DEV, Jun 2024, "Scaling to 125 Million
  Transactions per Day: Juspay's Engineering Principles")
- Releases are frequent, "sometimes even multiple releases per day, whether they are staggered or
  full-scale", so Juspay "needed a monitoring solution that would allow us to maintain fast-paced
  release cycles while ensuring that the newest release did not affect the functioning of the payment
  systems that are currently running." (Juspay blog, Mar 2023, "Mastering A/B Testing with Clickhouse")
- The comparison runs on ClickHouse: "ClickHouse enables Juspay to compare the success rate percentage
  between version A and version B", with data that goes through "Kafka queues to transfer data to
  ClickHouse via the Kafka engine." (ClickHouse blog, Mar 2023, on Juspay)

While a bad release serves traffic, it fails payments every second. At 25% of 3,500 payments a
second, a defect that breaks most card payments fails about 150 more payments a second than the
stable release would. The statistics need a second or two of outcomes to be sure; the pipeline adds
its own delay on top. ClickHouse's Kafka engine writes a block every 7.5 s by default when the block
is not full, and the comparison query runs on its own schedule. We do not know your settings, or
whether the comparison still runs that way, so this kit measures the lane on its own: outcome to
comparison, and defect to ABORT.

It is a shadow lane. Kafka, ClickHouse, Grafana and the merchant dashboards stay as they are, and
the guard's verdicts are advisory until you choose to wire them to Autopilot.

## Run it

Needs Docker and Python 3.10+. Defaults: 3,500 payments a second (about 300 million a day), a new
release staggered 1% → 5% → 10% → 25% → 50% with stages of at least 60 s, and a defect injected into
the new release at second 200 (illustrative: its card 3DS redirect breaks, so most card payments on
it fail authentication). Juspay does not publish Autopilot's ladder, stage lengths or thresholds,
so set them to yours.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python guard.py &                      # the A/B comparison, fed by the lane
python router.py &                     # the router fleet: stable and new release, then the defect
python api_log_flood.py --at 20        # a heavy API-log stream next door
```

Each prints a line a second, and the guard prints every decision:

```
[router]   3,500 payments/s (steady)  canary 2026.10.20.0 at 10% (350/s)  DEFECT LIVE 1.0 s: payments on it since the defect …
[guard]    3,500 outcomes/s  outcome-to-guard p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  payments tracked …
[ab] canary 2026.10.20.0 at 10%  success …% (n …) vs stable 2026.10.14.0 …% (n …) over 5 s  z …  HOLD
[verdict] ABORT 2026.10.20.0 -> 0%  success canary …% vs stable …%  z …  top error growth on canary: 3DS_REDIRECT_ERROR …
[router] ABORT applied … s after the defect (verdict reached the rollout in … ms): … failed by the defect …
```

The business numbers are on the `[verdict]` and `[router]` lines: how long after the defect the
guard decided, how long until the rollout applied it, and how many payments the defective release
served and failed in between. `top error growth` is the error code whose share grew most on the new
release, as Juspay's framework benchmarks error codes against the stable release.

How the guard decides (`guard.py`): every 200 ms it compares the new release with the stable one
over the last 5 s of outcomes. **ABORT** when the new release's success rate is at least 3 points
lower with a z-score at or below −4.5 (two-proportion test) on at least 100 of its payments.
**SCALE** to the next share when the current stage has enough payments and time and the new release
is not worse. Each attempt carries a per-payment sequence, so a replayed or repeated attempt is not
counted twice. These rules are a starting point; replace them with your framework's.

**What the numbers mean.** The kit times each outcome from the moment `router.py` publishes the
final status to the moment `guard.py` has counted it, through Python clients, on whatever machine
you run it on. On a shared GitHub runner (2 vCPU), with the broker and three Python processes on one
box, two green CI runs at 500 payments a second held p50 12.6 to 12.8 ms, p99 25.4 to 25.7 ms and
p99.9 26.8 to 27.2 ms (median per second over 63 steady-state seconds each, no second over the 50 ms
budget), with 1 MB/s of API logs next door for 33 to 34 s. In both runs the guard scaled the healthy
release 1% → 5% → 10% → 25% (short CI stages) and, after the defect started at 25%, decided ABORT
1.77 to 1.88 s after the defect's first failed payment; the rollout applied it 1.83 to 1.90 s after
the defect, with the verdict delivered in 1.9 to 2.0 ms. In that time the defective release served
242 to 247 payments and failed 46 to 53 because of the defect. That is
the end-to-end lane through this demo code at reduced scale, not the broker alone; the broker's own
tail latency is measured with `iggy-bench` on dedicated machines (`bench/PLAN.md`). On a laptop or a
larger machine, raise the rates.

| File | What it is |
|---|---|
| `router.py` | The router fleet: each attempt's final status, release, payment method, merchant and error code on `payment-outcomes`, keyed by payment ID, with retries as attempt 2. Applies SCALE and ABORT from `release-verdicts`. `--bad-at`, `--bad-fail` and `--peak` shape the run. |
| `guard.py` | The A/B comparison: success counts per release in 100 ms buckets, the ABORT and SCALE rules, verdicts out, outcome-to-guard latency and the business numbers every second. |
| `api_log_flood.py` | A bulk, latency-tolerant API-log stream on its own topic on the same broker. |
| `kafka_bridge.py` | Reads your payment-attempt events from Kafka as a read-only consumer group and publishes final outcomes to the lane, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A surge during a release:** `python router.py --payments-per-s 1000 --peak 10 --peak-for 60`.
   Juspay's blog says traffic "could surge as much as 10x in a matter of minutes". Pass: outcome-to-guard
   p99 under 50 ms through the surge, and no ABORT before the defect.
2. **API logs on the same broker:** `python api_log_flood.py --mbps 20 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart the guard:** stop `guard.py` and start it again. It replays the lane, prints "catching
   up", then times live outcomes again. Pass: no attempt counted twice, back under budget within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <broker>:9092 --kafka-topic hyperswitch-payment-attempt-events \
  --release-header x-release --canary <release-under-test> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=<user> -X sasl.password=<password>
```

The bridge only reads, as its own consumer group (`iggy-shadow`); the router, every producer and every
existing consumer, the ClickHouse Kafka engine tables included, are untouched. It reads Hyperswitch's
attempt rows by default (`payment_id`, `attempt_id`, `status`, `payment_method`, `error_code`,
`sign_flag`). Those rows do not name the server release, so pass the header or field that does
(`--release-header` or `--release-field`); map other topics in `to_outcome()`. Then compare the
guard's verdicts and timing with your A/B framework's on the same releases. In shadow mode the clock
starts when the bridge reads the record. Everything runs on your infrastructure.

## Next step

If the numbers are interesting: a 2–4 week design-partner evaluation on one mirrored payment-attempt
topic, beside Kafka and ClickHouse, in shadow, across real releases. A Rust consumer could later run
inside your rollout tooling, and an Iggy option for Hyperswitch's `events.source` is a natural
follow-on for self-hosters. No advantage, no migration.

kranti@laserdata.com · laserdata.com
