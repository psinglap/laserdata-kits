# Real-time API key shutoff for OpenAI, on LaserData

Every request reaches the enforcement service, and every key disable reaches every request router, in
milliseconds, before that key's next request. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.openai-kit
- **One slide:** [`LaserData_OpenAI_KeyShutoff.pdf`](LaserData_OpenAI_KeyShutoff.pdf)

## Why this use case

From the Scaled Abuse job post (Jun 2026): the team "build[s] and operate[s] the backend and data systems
that power real-time detection, investigation workflows, and enforcement", and designs fraud systems
"balancing fraud loss, cost of implementation, and customer experience", on "Terraform, Kubernetes,
Azure, Python, Postgres, and Kafka".

From the Help Center ("Keeping your OpenAI account secure"): when OpenAI finds an API key on the public
internet or inside an app, "the API key is disabled immediately", because "an exposed API key can allow
unauthorized API usage through your account, which can result in charges".

From the API Enterprise Controls job post (Aug 2026): one team owns "service accounts and key management"
and works "across databases, streaming systems, request routing, authentication".

Enforcement is a loop with two hops: a request on a stolen key has to reach the service that judges the
key, and its disable has to reach every request router before the key's next request. Whatever the loop
takes, the fleet keeps serving the attacker on the owner's account. This kit runs that one loop on
Apache Iggy, prints both hops' latency every second, and prints the requests served on keys after their
disable, in USD, next to what a lane one second behind would have served.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 request routers, 50,000 API keys, 10,000 requests a second
at $0.0005–$0.02 each, a stolen key put to use every 2 s (200 requests a second from 40 networks, at
$0.01–$0.06 each) and a leaked key reported every 3 s. All rates and prices are illustrative; set yours.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python enforcement.py &              # the enforcement service, fed by the request lane and leak reports
python routers.py                    # 8 request routers: requests out, disables in
```

They print a line a second:

```
[enforcement]  10,000 requests/s  request-to-enforcement p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
               disables sent … (hijack …, leak …)  report-to-disable max …ms  keys disabled …
[routers]  10,000 requests/s (steady)  $…/s  abusive attempts …  refused …  disables … (hijack detected in …ms median)
           served after the disable … ($…; at 1000 ms it would be …, $…)  false disables 0
           disable-to-routers n … p50 …ms p99 …ms
```

**What the numbers mean.** The kit times each request record from the moment `routers.py` serves it to
the moment `enforcement.py` has applied it, and each disable from `enforcement.py` to every router,
through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker
and all scripts on one box, at reduced scale (3,000 requests/s, 20,000 keys, 4 routers), three green runs
held request-to-enforcement p50 7.8–8.0 ms, p99 14.4–14.7 ms and p99.9 15.9–16.4 ms (every steady-state
second inside 50 ms, 60–61 per run), and disable-to-routers p50 3.3–3.9 ms and p99 4.3–4.8 ms. A leak
report became a published disable in about 3 ms (3.0–3.2 ms, median of each second's slowest). With 2 MB/s
of request logs on the same broker, the lane's p99 moved by less than 0.5 ms. Each run disabled 30
hijacked and 20 leaked keys, with no false disables. Routers served 25–34 requests ($1.02–$1.30) on keys
after their disable was published; a lane one second behind would have served 9,341–9,349 ($326–$329),
so the lane kept 99.6–99.7% of it. The kit's stand-in detector needs about 100 ms (102–105 ms) of abuse to
call a hijack; that part is your models' and is not what the lane measures. All of this is the end-to-end
loop through this demo code, not the broker alone. The broker's own tail latency is measured with
`iggy-bench` in `bench/PLAN.md`, on dedicated machines. CI runs:
https://github.com/psinglap/laserdata-kits/actions/runs/39872473462,
https://github.com/psinglap/laserdata-kits/actions/runs/39872702841,
https://github.com/psinglap/laserdata-kits/actions/runs/39872925713

| File | What it is |
|---|---|
| `routers.py` | A fleet of request routers. Every served request publishes one record (router, key, project, network, sequence, cost) to `api-requests`, keyed by key. Each router reads `key-status` directly on its own connection and refuses a key the moment its disable arrives. It also plays the outside world: the keys' own traffic, stolen keys put to use, and leaks reported by secret scanning. |
| `enforcement.py` | The enforcement service. Reads the 16 request partitions directly, judges each key over a 500 ms window (`hijacked()` is the stand-in for your abuse models), disables leaked keys on report, and publishes each disable with the key's sequence. |
| `log_flood.py` | A heavy request and response log stream on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your request records from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A model launch:** `python routers.py --peak 3 --peak-at 60 --peak-for 120`.
   Pass: request-to-enforcement p99 under 50 ms, and requests served after the disable under 1% of what a
   lane one second behind would serve.
2. **Request logs next door:** `python log_flood.py --mbps 20` while both run.
   Pass: the request lane's p99 moves by less than 10%.
3. **Restart the enforcement service:** stop `enforcement.py` and start it again; it replays the request
   topic and leak reports and rebuilds its windows and its list of disabled keys, publishing again only
   disables younger than `--replay-disable-s`. Restart `routers.py` and every router rebuilds its view
   from `key-status`.
   Pass: it catches up within seconds and disabled keys stay disabled.

## Design notes

- A disable carries the key's resulting state with a sequence, so at-least-once delivery is safe: a
  repeated or replayed disable changes nothing. Apache Iggy 0.9 does not offer exactly-once, and this
  lane doesn't need it.
- The lane carries key IDs, never secrets. The key store stays the source of truth for keys; the log is
  how every router hears about a change, and a durable, replayable record of each disable and its evidence.
- Owner revocations and re-enables from the dashboard or Admin API fit the same topic: publish them on
  `key-status` with the next sequence.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <kafka:9092> --kafka-topic <request-records-topic> \
  --key-field <api-key-id-field> --network-field <asn-field> --host-field <router-field>
```

The bridge only reads, as its own consumer group `iggy-shadow`; the routers, your abuse pipelines, Flink
jobs and every other consumer are untouched. Map your record in `to_request()`: the kit needs the API key
ID (never the secret), the source network and, if you have them, the router, project and cost.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored request-records topic, alongside
Kafka. No advantage, no migration.

kranti@laserdata.com · laserdata.com
