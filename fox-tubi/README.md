# Creative readiness for the Tubi ad server, on LaserData

Every new programmatic creative reaches the creative registry once, and its verdict reaches every ad server
node in milliseconds, even at kickoff. It runs beside your ad server and creative pipeline, so nothing else has
to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_FoxTubi_CreativeReady.pdf`](LaserData_FoxTubi_CreativeReady.pdf)

## Why this use case

From Tubi Engineering's own posts: Tubi runs "our own in-house ad server, one of the first products the company
built", and when a request comes in "our ad server then has less than one second to evaluate the request".
Inventory is sold directly and "programmatically through direct integrations with top-tier DSPs". After the bids
are in, "we check if we have our own transcodes for each included video creative, and if not, take it through
the transcoding process", and for programmatic ads Tubi wants to "identify the brand they are advertising, both
for reporting as well as frequency capping purposes"; the auction fills "our ad pod, which ranges from one to
five slots", with constraints such as "competitive separation" (ad serving posts, 2019–20). Since 2026 the brand
step is an LLM pipeline against a brand knowledge graph, because a wrong brand means "incorrect campaign
attribution and substantial cumulative loss in revenue and targeting effectiveness" (AdRise Brand Intelligence,
Feb 2026).

So a new creative cannot win a bid until two things have happened: its job (transcode, checks, brand) has run,
and the ad server node deciding the next pod knows the result. The first is work; the second is distribution,
and with many nodes it has two failure modes: every node that meets the creative first starts its own job (each
brand job is an LLM call), and a finished verdict waits for each node's next cache refresh while that creative's
bids are held. This kit runs that one loop on Apache Iggy: sightings keyed by creative into one registry, which
starts one job per creative, and verdicts out to every node through the log. It prints both hops' latency every
second, the jobs started against sightings absorbed, and bids held, pod fill and revenue per thousand requests
for live verdicts next to the same verdicts on a slower refresh, judged on the same bids.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 ad server nodes, 5,000 ad requests a second with 1–9 programmatic
bids each and pods of 1–5 slots at $10–$35 CPM, 6,000 creatives in rotation, 30 new creatives a second plus a
wave of 400 every 120 s (new flights at the top of a live event), an 8 s job per creative, one ad per category
per pod, and a 60 s verdict refresh to compare against. All illustrative; set them to yours.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python creative_registry.py &        # the registry: one job per creative, verdicts out
python ad_servers.py --join-at 60    # 8 ad server nodes: sightings out, verdicts in; a 9th joins at 60 s
```

They print a line a second:

```
[registry]   … sightings/s  seen-to-registry p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  jobs started …  absorbed …  in progress …  verdicts ready … held …
[ad-servers] 5,000 requests/s (flight wave)  nodes 8  verdicts in as-00's table …  sightings sent …  verdict-to-ad-servers n … p50 …ms p99 …ms
[yield] live verdicts: bids held …%  pod fill …%  RPM $…  | verdicts refreshed every 60 s: bids held …%  pod fill …%  RPM $…  | live vs refreshed +…%
[new] … new creatives servable on all 8 nodes this second  first bid to servable everywhere p50 …s (job ~8s + lane)  | with a 60 s refresh p50 …s
[join] late node rebuilt … creative verdicts from the log in … ms, now taking requests
```

**What the numbers mean.** The kit times each sighting from the moment `ad_servers.py` creates it to the moment
`creative_registry.py` has applied it, and each verdict from the registry to every ad server node, through Python
clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one
box, at reduced scale (2,000 requests/s, 4 nodes, 3,000 creatives in rotation, 20 new a second and a wave of 200
every 40 s, a 3 s job, a 20 s refresh to compare against), over three green runs it held seen-to-registry
p50 2.7–4.7 ms and p99 5.0–8.5 ms (183 of 184 steady-state seconds inside 50 ms; the one miss was a single 64 ms sighting), and verdict-to-ad-servers p50 2.6–2.9 ms and p99 4.4–5.2 ms to all four nodes. With 2 MB/s of ad event log on the same broker, the lane's
p99 moved by 5% or less. That is the end-to-end loop through this demo code, not the broker alone; the
broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

**The business numbers.** Over the same runs the registry started 1,414–1,431 jobs and absorbed 75% of all
sightings (repeat sightings of a creative another node had already reported). Live verdicts held 2.3%
of bids against 7.7–8.2% with a 20 s refresh, filled 81.9% of pod slots against 79.7–80.0%,
and earned 3.8–3.9% more per thousand requests; a new creative was servable on every node 3.1 s after its
first bid, against 11.1–12.9 s with the refresh. A fifth node started 45 s in rebuilt its table of ~4,000
verdicts from the log in 46–64 ms. Those gaps belong to the demo's demand model and its 3 s job, not to Tubi;
your bids, job times and refresh cadence set the real number.

## Three tests that match your peaks

1. **New flights at kickoff.** `python ad_servers.py --flight-every 60 --flight-n 2000`: 2,000 new creatives
   at once. Pass: exactly one job per creative, both hops' p99 under 50 ms through the wave.
2. **A heavy ad event log next door.** `python ad_event_log.py --mbps 50`: impression, quartile and error
   beacons on their own topic. Pass: the sighting lane's p99 moves by less than 10%.
3. **Add a node before the event.** `--join-at 60` starts one more node that reads the verdict log from the
   start and only then takes requests. Pass: its table is complete within a second.

## Try it on your traffic

`kafka_bridge.py` joins your bid or ad-request log as its own consumer group, `iggy-shadow`, and writes the
first sighting of each creative per node into Apache Iggy, keyed by creative. It only reads, so the ad servers,
producers and every existing consumer are untouched. Map your record in `to_sighting()` (node, creative ID such
as OpenRTB `crid`, DSP), pass `--known` with the creatives you have already approved, and compare the jobs
`creative_registry.py` starts with the jobs your pipeline starts today.

```
python kafka_bridge.py --bootstrap <broker> --kafka-topic <bid-log-topic> \
  --node-field <host-field> --creative-field <crid-field> --dsp-field <dsp-field> --known approved.txt
```

When the ad server writes to the lane directly: Apache Iggy has an official Rust SDK (Tokio-based). There is no
Elixir SDK; Elixir services can use Iggy's HTTP API or a Rust NIF.

## What Apache Iggy 0.9 has today, and what it doesn't

- **Replication:** Viewstamped Replication clusters, shipped September 2026; three nodes tolerate one failure.
- **Delivery:** at-least-once. The per-(node, partition) sequence and the registry's one-job-per-creative rule
  make repeats harmless.
- **Kafka API:** a Kafka-compatible gateway is in progress and not in 0.9, which is why the trial uses
  `kafka_bridge.py` and your Kafka consumers stay on Kafka.
- **No log compaction:** a node rebuilds its verdict table by replaying the verdict topic; for millions of
  creatives, add a periodic snapshot and replay from its offset.
- **Clients:** official SDKs for Rust, Java, Go, C#, Python and Node.js.

LaserData Cloud runs as a managed service on AWS or GCP, or inside your own AWS or GCP account (BYOC). On Azure,
run Apache Iggy self-hosted on your own VMs; LaserData supports it.

## Files

| File | What it does |
|---|---|
| `ad_servers.py` | Ad server nodes: sightings out, verdicts in, pod decisions live and on a slower refresh |
| `creative_registry.py` | The registry: one job per creative, verdicts out, lane p99 |
| `ad_event_log.py` | A heavy ad event log on its own topic |
| `kafka_bridge.py` | Shadow a real Kafka bid log, read-only |
| `common.py` | Topics, keys, partitioning, illustrative creative attributes |
| `bench/PLAN.md` | The benchmark, rules first |
| `ci.sh` | The CI test on a real Apache Iggy 0.9.0 broker |
| `docker-compose.yml` | Apache Iggy 0.9.0 |

Contact: kranti@laserdata.com · laserdata.com
