# Live PR-review context for CocoIndex agents, on LaserData

Review-thread changes on Apache Iggy reach a Postgres table through the Iggy connector CocoIndex already ships
(`cocoindex.connectors.iggy.topic_as_map`), readable in about 45 ms, and a crashed indexer replays from Iggy without
losing a change. Resolved threads are deleted from the table. Your Kafka path stays as it is.

This kit lets your team check that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs, CocoIndex 1.0.26 and Postgres 17 with pgvector.

- **One slide:** [`LaserData_CocoIndex_ReviewContext.pdf`](LaserData_CocoIndex_ReviewContext.pdf)

## Why this use case

- **CocoIndex's own argument.** "For long-running agents that may execute for hours at a time, a snapshot captured
  at the start of the run quickly drifts out of sync with the underlying data", so production agents do better on
  streams of change events (George He, "Live CSV to Kafka with CocoIndex", April 2026).
- **CocoIndex already speaks Iggy.** Since May 2026 it ships an Apache Iggy source and target (PR #1969), with
  offsets stored only after the downstream write, and a Rust `csv_to_iggy` example (October 2026). There is no
  Python Iggy consumer example yet; `kafka_to_lancedb` is the Kafka twin of this kit.
- **PR reviews are a source CocoIndex names.** Its home page turns "codebases, meeting notes, and PR reviews into
  live context" for agents.

So the kit is narrow: one topic of review-thread changes (comment, edit, resolve), one `topic_as_map` source, one
Postgres table an agent reads, and the two numbers that matter to an agent: how old its context is, and how often a
lookup returns an outdated or resolved thread.

## What we found on a real broker, and what the kit does about it

Running CocoIndex's Iggy source against a real Apache Iggy 0.9.0 broker in CI surfaced four issues. The kit works
around each one in a few lines; all four are fixes we can send upstream.

1. **Rows declared before readiness are swept** (cocoindex 1.0.25 and 1.0.26). A `LiveMapFeed` that delivers its
   backlog and then calls `mark_ready()`, as the Kafka and Iggy sources do, lost those rows when the engine finished
   its readiness pass; builds in flight failed with "tracking info not found for commit". A fresh index of 5,000
   threads came out 58 to 75 threads short on CI, and 2,231 short in an in-process run of the same connector.
   `index_app.py` marks ready first and waits for the engine's READY.
2. **One message dropped per start.** When the source is caught up it switches loops and calls `anext()` again while
   the previous `anext()` is still pending; the message that pending call receives is never processed. Seen on CI as
   one thread left one version behind after a restart. The kit passes an `initial_high_watermark` the source never
   reaches, so it stays in the loop that awaits its pending read, and logs "caught up" itself.
3. **The source stalls at `batch_length` in flight.** It reads with `PollingStrategy.Next()` and stores offsets
   after the write; Iggy's consumer re-serves from the stored offset and "goes quiet once the reading position is
   `batch_length()` or more ahead of the stored offset" (apache-iggy 0.9 consumer docs). The default of 100 caps
   what can be in flight. The kit uses `batch_length=2000` and a 10 ms `poll_interval`.
4. **The LanceDB target deletes one row per commit.** With resolves as 8% of changes, each apply took about 1.2 s on
   CI. The kit writes to Postgres, whose target batches deletes.

Not run, read from code: with apache-iggy 0.9 a consumer-group member is assigned every partition and `partition_id`
is ignored, so the source rejects multi-partition topics ("untracked partition"). The kit uses one partition; the
per-partition high watermark the connector asks for is in 0.9 (`TopicDetails.partitions[i].current_offset`).

## Run it

Needs Docker and Python 3.11+. Defaults: 20,000 open review threads across 12 repositories, 300 changes a second,
8% of them resolving a thread (a new one opens in its place), and a 1 s budget for change-to-index freshness, from
CocoIndex's own "within a second". The shape is illustrative.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090, Postgres 17 + pgvector on :5432
pip install -r requirements.txt      # apache-iggy 0.9.0, cocoindex[iggy,postgres] 1.0.26
python review_events.py &            # the review service: thread changes into Iggy
python index_app.py &                # CocoIndex: Iggy topic -> Postgres review_context
python context_check.py              # what an agent sees, every second
```

They print a line a second:

```
[review]      … changes/s (… resolved)  open threads 20,000  send p50 …ms p99 …ms  total …
[index]       … read from Iggy       … declared through CocoIndex  at offset …
[check]       … changes/s indexed  change-to-index p50 …ms p99 …ms p99.9 …ms  PASS (<1000ms)  overdue 0
            stale answers 0.0% (snapshot from 240s ago: …%)
[verify] threads on the topic …  rows in Postgres …  missing 0  stale 0  resolved but still indexed 0  extra 0
```

Freshness runs from the review service writing the change to Iggy to the row being readable in Postgres (for a
resolved thread, to the row being gone). `context_check.py` reads the topic itself as ground truth and polls the
table every 50 ms, so up to 50 ms of each number is the poll. **Stale answers** are 400 random thread lookups a
second, as an agent makes them, that returned an older version of the thread (or a resolved one) than the topic held
at that moment; next to it, the same lookups answered from a snapshot taken when the agent's session started
(`--session-s`, 300 s by default). While the indexer is down and until it has caught up again, seconds print as
`catching up` with the changes held in Iggy, and are not counted as live latency.

**What the numbers mean.** CI runs the kit on a shared 2-vCPU GitHub runner with Iggy, Postgres and every script on
one box, at reduced scale: 5,000 open threads, 250 changes a second for 85 s with a 2× spike for 8 s, the CocoIndex
app killed with SIGKILL 45 s in and restarted 5 s later, and 30 s agent sessions. In three green runs on the final
code, change-to-index freshness was p50 42 ms, p99 75 ms and p99.9 76 to 77 ms (medians per second), with 48 to 53
steady-state seconds all inside the 1 s budget and no second's p99 above 820 ms. While the app was down, 1,276 to
1,303 changes waited in Iggy; the restarted app resumed at the last offset stored after a write, replayed about
1,420 changes and was live again 2.1 to 2.7 s after it started. Agent lookups returned an outdated thread 0.0% of
the time (median; worst second 1.9%), against 44.8% to 46.5% from a snapshot up to 30 s old. At the end every one
of about 6,800 threads matched the topic: missing 0, stale 0, resolved but still indexed 0, extra 0. That is the
end-to-end path through CocoIndex and this demo's Python code, not the broker alone; the Iggy hop is a few
milliseconds. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`.

| File | What it is |
|---|---|
| `index_app.py` | The CocoIndex app: `iggy.topic_as_map` → `process_thread` → `postgres` table target, live mode, with the workarounds above. |
| `review_events.py` | The review service: opens the threads, then comments, edits and resolves at `--rate`, each message carrying the thread's whole state and a per-thread `seq`. |
| `context_check.py` | Ground truth from the topic, freshness, stale agent answers, overdue threads, and the final thread-by-thread check. |
| `kafka_shadow.py` | Mirrors a keyed Kafka topic your CocoIndex app already reads into Iggy, read-only, as consumer group `iggy-shadow`. |
| `common.py` | Topic, payload and key rules shared by every script. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **The indexer dies:** `kill -9` the `index_app.py` process for a minute, then start it again.
   Pass: changes wait in Iggy, the app resumes from the last offset stored after a write, `context_check.py` is live
   again within seconds, and the final check reports missing 0 and stale 0.
2. **A bot comments on every open pull request:** `python review_events.py --spike-x 10 --spike-at 60 --spike-for 30`.
   Pass: freshness p99 stays under 1 s, or the index is live again within a minute after the spike.
3. **A long agent session:** `python context_check.py --session-s 3600`.
   Pass: stale answers from the live index stay near 0% while the snapshot's climb with every minute of the session.

## Shadow a topic your CocoIndex app already reads

```
pip install confluent-kafka
python kafka_shadow.py --bootstrap <brokers> --kafka-topic <topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN -X sasl.username=… -X sasl.password=…
python index_app.py --topic shadow-events --table shadow_context
python context_check.py --topic shadow-events --table shadow_context
```

The shadow reads as its own consumer group (`iggy-shadow`); your app's consumer group, its offsets and your
producers are untouched. Each keyed record becomes one Iggy message (the key as `thread_id`, the value as `body`),
and a tombstone becomes `{"_deleted": key}`. The Iggy path writes its own table, so it never touches yours, and
freshness is measured from the Kafka record timestamp.

## What Apache Iggy 0.9 doesn't have yet

Iggy does not speak the Kafka protocol in 0.9; a Kafka-compatible gateway is in progress, and until it ships an Iggy
topic needs CocoIndex's Iggy connector, not the Kafka one. Python messages carry no Kafka-style key or tombstone,
which is why the key and the deletion live in the payload. Delivery into Iggy is at-least-once; the index stays
exact because every message carries the thread's whole state and a replay can only rewrite the same row. Replicated
clusters (Viewstamped Replication) shipped in September 2026. LaserData Cloud runs as a managed service on AWS or GCP,
or inside your own AWS or GCP account (BYOC). On Azure, run Apache Iggy self-hosted on your own VMs; LaserData
supports it.

## Next step

If the numbers are interesting: a 2–4 week joint evaluation. We mirror one topic you index today into Iggy, run the
same CocoIndex flow on both paths, and send the four fixes above as pull requests to `cocoindex-io/cocoindex`. No
advantage, no migration.

kranti@laserdata.com · laserdata.com
