# Every publish on every serving node for The New York Times, on LaserData

Every new version of every asset reaches every serving node in milliseconds, in order per asset, well inside
the one-second publish SLA, even while a tag change republishes thousands of articles on the same lane. A
serving node added or restarted rebuilds the latest version of every asset from the log before it serves. It
runs beside Kafka, so the Gateway, the Monolog, the denormalizer and every other consumer stay where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** (link added when published)
- **One slide:** [`LaserData_NewYorkTimes_PublishToSite.pdf`](LaserData_NewYorkTimes_PublishToSite.pdf)

## Why this use case

From The New York Times' own engineering sources:

- "For all the really important things like when an article is updated, our SLA is one second. Basically one
  second after the editor clicks publish, the latest version has to be available on the site" (Boerge
  Svingen, Director of Engineering, Software Engineering Daily, October 2017).
- The Publishing Pipeline keeps every asset since 1851 in the Monolog, a single-partition Kafka topic. A Kafka
  Streams denormalizer writes each top-level asset, bundled with what it references, to a log partitioned by
  top-level asset URI, and "when a dependency changes, it republishes every top-level asset that references
  it". A live content service needs only the latest version; a latest-assets service starts "a few hours back"
  on the log (Confluent blog, September 2017).
- Kafka runs on Compute Engine VMs in Google Cloud: "we want to use managed services whenever we can … Kafka is
  an exception", and "I don't particularly want to manage Kafka if we don't have to" (Software Engineering
  Daily, 2017).
- In October 2026 the Data Platform team still lists Spark, Trino, Flink, Kafka and Airflow, and owns "vendor
  evaluations" (Engineering Manager, Compute and Storage, NYT careers).

This kit runs one narrow lane: every published version of every asset, keyed by top-level asset URI, carrying
its version and the whole bundled asset (never a diff), to every serving node, the service instances that
serve nytimes.com and the apps. Each node reads the lane with its own offsets, so adding nodes rebalances
nothing, and a node that joins replays the news day and serves the latest version of everything. The Monolog
stays the source of truth. The public sources are from 2017; the topic layout is the first thing to confirm
with your team.

## Run it

Needs Docker and Python 3.10+. Defaults: 400 assets on the live desk (40 live coverage, 60 breaking stories,
300 articles), about 4 KB per version, newsroom time 20x real time so every second has versions to time
(`--speed 1` for real time), and 20,000 page views a second spread over the assets by readership (an
illustrative figure; pass yours with `--page-views`).

```
docker compose up -d                          # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt               # apache-iggy 0.9.0
python newsroom.py &                          # the live desk, every version of every asset
python serving_node.py --name serving-1 &     # a serving node
python serving_node.py --name serving-2       # and another: each reads every version
```

Each prints a line a second:

```
[serving-1]   35 editor versions/s  publish-to-node p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
              inside the 1 s SLA 35 of 35  page views served the previous version … of 20,000/s (…%)
              republishes 0/s  gaps 0  top asset v12
[serving-2] rebuilt from the log in …s: latest version of 400 of 400 live assets and … other assets, … versions replayed
[newsroom]    35 versions/s  … writes  write ack p50 …ms  p99 …ms
```

`page views served the previous version` counts the views that arrive after a version exists but before this
node holds it, so they get the older one: each asset's page views a second times how late its version was.
With newsroom time compressed 20x (60x in CI) there are that many more versions, so the figure is that many
times higher than at real time. The 50 ms budget is 5% of the one-second SLA, for this hop alone.

**What the numbers mean.** The kit times each version from the moment the live desk created it (including its
wait for the next write) to the moment `serving_node.py` can serve it, through Python clients, on whatever
machine you run it on. On a shared 2-vCPU GitHub runner, with the broker, the live desk, the republish script
and two serving nodes all on one box, at about 100 editor versions a second of about 4 KB across 400 assets,
three green CI runs held a median per-second p50 of 2.7 to 3.5 ms, p99 4.6 to 5.9 ms and p99.9 4.7 to 6.2 ms
against the 50 ms budget. Every editor version on every node landed inside the one-second SLA, and no node saw a
version gap. The worst second reached 35 to 67 ms; in one run one second of 63 went over 50 ms, while the
restarted node was replaying the log on the same two vCPUs. A tag change republishing 6,000 archive articles at
400 a second on the same lane moved the editors' median p99 from 4.6 to 5.8 ms to 4.7 to 6.0 ms. The node
restarted during the burst rebuilt all 400 live assets and the 6,000 archive articles from the log (about 5,130
versions) in 0.18 to 0.32 s. At CI's 60x newsroom time, 0.17 to 0.22% of the illustrative 20,000 page views a
second were served the previous version. That is the end-to-end lane through this demo code at reduced scale, not the broker alone, and it
excludes the Gateway, the Monolog, the denormalizer, the CDN and the reader's network. The broker's own tail
latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `newsroom.py` | The live desk: one writer per partition, every version of every live asset, keyed by asset URI. Each version carries its number, why it was written (publish, update, correction, live post) and the bundled asset. Headlines and ids are illustrative. |
| `serving_node.py` | A serving node: reads every partition with its own offsets, keeps the newest version of every asset, counts editor versions inside the one-second SLA, page views served the previous version and version gaps; times republishes separately. Rebuilds from the log on start. |
| `tag_republish.py` | A tag change: version 1 of 20,000 archive articles that reference one tag, then every one of them republished on the same lane over 20 s, as the denormalizer does when a dependency changes. |
| `kafka_bridge.py` | Reads the denormalized log as consumer group `iggy-shadow` and writes it into Iggy keyed by asset URI, for a shadow run on real publishes. |
| `ci.sh` | The CI run: 400 assets at 60x, two nodes, a 6,000-article republish burst, a node restart; pass only on 95% PASS seconds per node, every editor version inside one second, zero gaps and a full rebuild. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests

1. **A tag change on election night:** `python tag_republish.py` while the rest runs. Pass: editor versions'
   p99 moves by less than 10%, every editor version stays inside the one-second SLA, `gaps` stays 0.
2. **More serving nodes:** start a third and a fourth `serving_node.py`. Pass: the first node's p99 moves by
   less than 10%; every node reports the same version for the top asset.
3. **A node added mid-run:** stop a `serving_node.py` and start it again during a burst. Pass:
   `rebuilt from the log … latest version of 400 of 400 live assets` within a second, and PASS from the next line.
4. **Real time, bigger bundles:** `python newsroom.py --speed 1 --assets 2000 --body-bytes 32000` and nodes
   with `--assets 2000`. Pass: p99 holds and `gaps` stays 0.

## Shadow your own publishes

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <your-brokers> --topic '<denormalized log topic>' \
  -X security.protocol=SSL -X ssl.ca.location=... -X ssl.certificate.location=... -X ssl.key.location=...
python serving_node.py --name shadow
```

The bridge is one more consumer group, `iggy-shadow`, so the Gateway, the denormalizer and existing consumers
are untouched. Decode your protobuf records in `to_version()`: the kit needs the top-level asset URI (the
record key, when the topic is partitioned by it), and uses your version and republish cause if the records
carry them. Compare the shadow node's version for an asset with what nytimes.com serves. The latency it prints
then starts at the bridge, not at the editor's click.

## What Apache Iggy 0.9 does not do yet

No Kafka wire protocol (a gateway is in progress), so the bridge uses the Iggy SDK; no Kafka Streams
equivalent, so the denormalizer stays where it is; no log compaction or infinite-retention archive tier, so the
Monolog stays on Kafka as the source of truth; delivery is at-least-once, which is why every version carries its
number and the whole asset; clustering with replication is new (September 2026), so the evaluation includes a
failover step.

LaserData Cloud runs as a managed service on AWS or GCP, or inside your own AWS or GCP account (BYOC). On
Azure, run Apache Iggy self-hosted on your own VMs; LaserData supports it.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored denormalized log, beside Kafka. No
advantage, no migration.

kranti@laserdata.com · laserdata.com
