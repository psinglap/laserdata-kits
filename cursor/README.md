# Real-time spend limits for Cursor, on LaserData

Every request's paid usage reaches the spend-limit check, and every stop reaches every API server, in
milliseconds, even at peak. It runs beside WarpStream, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/JjZfCPgyyWeKUyW2r4AMfo
- **One slide:** [`LaserData_Cursor_SpendLimits.pdf`](LaserData_Cursor_SpendLimits.pdf)

## Why this use case

From Cursor's own docs on spend limits: "Enforcement is not instant, so usage can briefly exceed your
spend limit." When the limit is recognized, "Paid additional usage stops for that specific user", and on
legacy plans the overage before enforcement is credited back. Limits come from member overrides, group
overrides and the team limit, and "Cursor applies the highest applicable limit."

From the billing job post (Software Engineer, Billing, Mar 2026): the role runs "from updates in the UI and
raw usage events at the edge to invoiced amounts in Stripe, including the metering pipeline, aggregation
logic, entitlement enforcement, and ledger", across "millions of developers and enterprise teams", and
watches "metering lag".

And from WarpStream's case study (Oct 2025), Alex Haugland at Anysphere: Cursor's Kafka-compatible
streaming runs on WarpStream, "mostly" for "slower stuff", where latency "like 500 milliseconds" doesn't
matter. That is the right tool for training data and telemetry, and this kit leaves it alone.

Enforcement is a loop with two hops: a request's usage has to reach the check that owns the user's limit,
and the check's stop has to reach every API server before the user's next request. Whatever the loop
takes, the fleet keeps serving paid usage past the limit. This kit runs that one loop on Apache Iggy,
prints both hops' latency every second, and prints the usage served past the limit in USD.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 API servers, 20,000 users, 10,000 billable requests a second
at $0.01–$0.20 each, and 60-second billing periods (a compressed period, so limits are reached while you
watch).

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python limits.py &                   # the spend-limit service, fed by the usage lane
python api_servers.py                # 8 API servers: usage out, stops in
```

They print a line a second:

```
[limits]  10,000 usage events/s  usage-to-limit-check p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  decisions sent …
[spend] period …: paid usage $…  users stopped …/20,000 (team … group … member …)  alerted …
        past the limit $… (…% of usage)
[api]  10,000 billable requests/s (steady)  paid usage $…/s  refused at the limit …  stopped in api-00's view …
       decision-to-api-servers n … p50 …ms p99 …ms
```

**What the numbers mean.** The kit times each usage record from the moment `api_servers.py` creates it to
the moment `limits.py` has applied it, and each decision from `limits.py` to every API server, through
Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all
scripts on one box, at reduced scale (3,000 billable requests/s, 2,000 users, 4 API servers, 20 s
periods), it held usage-to-limit-check p50 11 ms and p99 22 ms (62 of 62 steady-state seconds inside
50 ms), and decision-to-API-servers p50 3 ms and p99 6 ms. With 2 MB/s of Tab events on the same broker,
the lane's p99 stayed at 22 ms. Up to 1,251 of 2,000 users reached their limit and were stopped in a
period, and usage served past the limit was 0.04–0.06% of paid usage. That is the end-to-end loop through
this demo code, not the broker alone. The broker's own tail latency is measured with `iggy-bench` in
`bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `api_servers.py` | A fleet of API servers. Every billable request publishes the server's running paid usage for that user (the resulting total, with a sequence) to `usage`, keyed by user. Each server reads `limit-decisions` on its own connection and stops that user's paid usage when told. |
| `limits.py` | The spend-limit service. Sums the latest total from every API server per user, applies the highest of team, group and member limits, alerts at 80%, stops at 100%, and publishes each decision. Prints the lane's p99 and usage past the limit. |
| `tab_flood.py` | A heavy Tab accept/reject stream on its own topic, for the isolation test. |
| `warpstream_bridge.py` | Copies your raw usage topic from WarpStream (Kafka protocol) into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A traffic spike:** `python api_servers.py --peak 5 --peak-at 60 --peak-for 120`.
   Pass: usage-to-limit-check p99 under 50 ms, and usage past the limit under 0.1% of paid usage.
2. **Tab events next door:** `python tab_flood.py --mbps 20` while both run.
   Pass: the usage lane's p99 moves by less than 10%.
3. **Restart the check:** stop `limits.py` and start it again; it replays the usage topic and rebuilds
   every user's total from the running totals, without double counting.
   Pass: it catches up within seconds and stopped users stay stopped.

## Design notes

- Running totals with a sequence make at-least-once delivery idempotent: a repeated or replayed record
  changes nothing. Apache Iggy 0.9 does not offer exactly-once, and this lane doesn't need it: it only
  decides when to stop serving paid usage. Invoices stay on your ledger and Stripe.
- Periods here are compressed and aligned to the clock. In production, key the period by each
  subscription's billing cycle.

## Shadow your own traffic

```
pip install confluent-kafka
python warpstream_bridge.py --bootstrap <warpstream-agent:9092> --topic <usage-topic> \
  --user-field <user-id-field> --server-field <host-field> --cost-field <cost-usd-field>
```

The bridge only reads, as its own consumer group `iggy-shadow`; the services that emit usage, the
metering pipeline, the ledger and Stripe are untouched. Map your record in `to_usage()`: the kit needs the
user, the server and the charged cost of the request.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored usage topic, alongside WarpStream.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
