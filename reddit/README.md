# Real-time ML safety signals for Reddit's REV2, on LaserData

Every ML signal reaches the post or comment it scores in milliseconds, so REV2 gets the fully enriched
content as soon as the last model has scored it, instead of when a join window closes. It runs beside
Kafka and Flink, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_Reddit_SafetySignals.pdf`](LaserData_Reddit_SafetySignals.pdf)

## Why this use case

From Reddit's own engineering posts and talks:

- REV2, the real-time rules engine, runs Lua rules "triggered on certain configured events (via
  Kafka), such as a user posting or commenting", on Flink Stateful Functions and Baseplate, and emits
  Protobuf actions to one Kafka topic per action type (Vignesh Raja and Jerry Chu, "Protecting Reddit
  Users in Real Time at Scale", r/RedditEng, Sep 2023).
- The enrichment service that feeds REV2 its context "processes around 600 messages per second under
  normal traffic", with P90 enrichment latency under a second (Stephan Weinwurm, Bhavani
  Balasubramanyam and Jerry Chu, "Enriching data for Reddit Safety's rules engine", r/RedditEng, Jun 2024).
- ML services score content independently of REV2, and ML inferencing "typically carries a much higher
  latency" than a rule, so REV2 would query a store for a signal and find it not there yet. Signals-Joiner,
  a Flink job, joins the signal topics to the preliminary content stream by content ID in event-time
  tumbling windows and writes a fully enriched stream; because waiting for the signals delays that
  stream, REV2 also keeps reading the preliminary stream. Reddit tracks join-rate ("# events containing a
  signal / # events that should have a signal") and Kafka consumer lag (Vignesh Raja and Jerry Chu, "How
  Reddit uses Signals-Joiner in its real-time safety systems", r/RedditEng, Aug 2024; Current 2025 talk).
- The Kafka fleet underneath is 500+ brokers holding petabytes, moved from EC2 to Strimzi on Kubernetes
  with zero downtime (Sky Kistler, StrimziCon 2026).

A window has to choose: wait longer for slow signals (REV2 gets the content later) or close sooner (some
signals miss their content). A lane keyed by content ID, where one joiner owns a partition number of every
input topic, can write the content out the moment its last expected signal lands, and still send it on
without a signal that never comes after a wait limit. This kit runs that lane and prints the two numbers
Reddit already watches: join-rate and how long REV2 waits.

We do not know Reddit's signal topics, window sizes, peak rates or latency targets. Reddit's Current 2025
talk mentions a custom windowing strategy we have not seen; the kit's 5 s tumbling window is a plain
comparison, not your join. The signal names, inference delays and the 50 ms budget are ours. Set them to
yours.

## Run it

Needs Docker and Python 3.10+. Defaults: 2,000 posts and comments a second (about 3× the published normal
rate) at 3× for the first minute, 30% posts of which 45% have media, a text and a spam signal for
everything plus a media signal for media posts (inference medians 250, 120 and 900 ms), 1% of signals
6 to 15 s late and 0.2% never sent, 32 partitions per topic, four joiners, a 5 s wait limit, and a
Time-Travel replay of the content stream at 100,000 messages a second next door.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
for j in 0 1 2 3; do python signals_joiner.py --joiner $j --joiners 4 & done
python rev2_reader.py &                # REV2's side: fully enriched content, a toy rule
python content_events.py &             # content and its ML signals, keyed by content ID
python time_travel.py --at 30          # a Time-Travel replay next door
```

Each prints a line a second:

```
CI_SAMPLE_PENDING
```

**What the numbers mean.** CI_NUMBERS_PENDING

| File | What it is |
|---|---|
| `content_events.py` | Plays the enrichment service (preliminary content keyed by content ID) and the ML services (one signal topic each, written after each model's inference delay, some late, some never). |
| `signals_joiner.py` | A joiner: owns partition numbers `p % joiners == joiner` of every input topic, reads them directly, writes fully enriched content when the last expected signal lands, or without it past the wait limit. Prints join-rate and the same events under a tumbling window. |
| `rev2_reader.py` | REV2's side: reads the fully enriched topic, drops repeats, acts on any ML score at or above 0.97, prints last-signal-to-REV2 latency and time to action. |
| `time_travel.py` | A Time-Travel replay of the content stream from its first offset, for the isolation test. |
| `kafka_bridge.py` | Copies your preliminary content and ML signal topics from Kafka into the lane, read-only, in its own `iggy-shadow` consumer group. |
| `common.py` | Names, signals, the partition and ownership functions, shared by every script. |
| `bench/PLAN.md` | The benchmark we run with you, Kafka included; rules fixed before results. |

## Three tests

1. **A busy hour with a Time-Travel next door:** `content_events.py --rate 6000 --peak 1` and
   `time_travel.py --rate 300000`. Pass: every joiner keeps last-signal-to-joined p99 under 50 ms and
   REV2 stays under 100 ms; join-rate stays at the share of signals that arrived inside the wait limit.
2. **A model backs up:** `content_events.py --late-share 0.2`. One in five signals comes 6 to 15 s late.
   Expect join-rate to fall to about 80% at a 5 s wait limit, content to keep reaching REV2 on time
   without the late signal (`without a signal`), and `late` to count what came after. Try
   `signals_joiner.py --max-wait-s 15` to trade time for join-rate.
3. **Move a joiner:** stop the four joiners and start eight with `--joiners 8`. Offsets are stored per
   partition and topic, once a second, so each partition number's new owner resumes from there. Content
   that was waiting for a signal at the stop is not rebuilt in this kit: its late signal is counted as an
   orphan, and REV2 still saw it on the preliminary stream. Move ownership stop-then-start: there is no
   fencing, so never run two joiners that claim one partition number.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <a broker> --prelim-topic <preliminary content topic> \
  --signal-topic text_harm=<signal topic> --signal-topic spam=<signal topic> --signal-topic media_safety=<signal topic>
for j in 0 1 2 3; do python signals_joiner.py --joiner $j --joiners 4 & done
python rev2_reader.py
```

The bridge reads in its own consumer group, `iggy-shadow`, and only reads: the enrichment service, the
ML services, Signals-Joiner, REV2 and everything else are untouched. Map your fields in `to_content()`
and `to_signal()` (decode Protobuf signals there), and the signal names in `common.SIGNALS`. Compare the
kit's join-rate and timing with Signals-Joiner's on the same traffic. Delete the consumer group when the
run ends.

## What Apache Iggy 0.9 has today, and what it doesn't

Today: replicated clusters (Viewstamped Replication, new in September 2026), direct partition reads,
consumer offsets stored on the broker, message expiry per topic, at-least-once delivery (the joiner and
REV2 drop repeats), official Rust, Python, Go, Java, C# and Node.js SDKs. Ownership is by partition
number, and moving it is stop-then-start, with no fencing tokens. Not yet: a Kafka API gateway (in
progress), so the trial uses `kafka_bridge.py`, and a joiner written in Flink or REV2's Flink Stateful
Functions would read the lane through an Iggy client, not a Kafka source. There is no stream join
operator in Iggy: the join is the kit's code, about two hundred lines you can read.

## Next step

If the numbers are interesting: a 2–4 week evaluation on mirrored content and signal topics, beside
Kafka and Signals-Joiner, priced per billion events. No advantage, no migration.

kranti@laserdata.com · laserdata.com
