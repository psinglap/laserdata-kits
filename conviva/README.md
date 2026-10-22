# Real-time ad break alerts for Conviva, on LaserData

Every ad event from every viewer session reaches the ad break checker, and every failing ad server and market
reaches ad ops, in milliseconds and in order per session, while the break is still running. It runs beside Kafka,
Time-State and Druid, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.conviva-kit
- **One slide:** [`LaserData_Conviva_AdBreaks.pdf`](LaserData_Conviva_AdBreaks.pdf)

## Why this use case

From Conviva's Confluent Data Streaming Award entry (Case Study of the Year, 2024): Time-State, Apache Kafka and
Apache Druid processed "3.6 billion events per minute and 7 billion stateful metrics per minute" for "more than 16
million concurrent users" during streaming-exclusive football games. From the Streaming Experience Insights
announcement (October 21, 2026): SEI puts "video, ad, and app experience on one real-time timeline" per session;
Conviva measured 56 million peak concurrent plays at the 2026 FIFA World Cup, and one Nexa prompt found more than
528,000 unfilled ad breaks on opening day, about 59,000 of them fixable "before the next day's kickoff", with fill
rates by market, device and ad server and a mid-roll left unfilled by an ad server timeout. From the Distributed
Systems job post (2026): "Rust-based microservices for high-throughput, low-latency data processing", with
"effective backpressure and failure handling", "sessionization, aggregations, and stateful computations", and
Kafka, Flink, ClickHouse and Druid. From the docs: Ad Start Failures carry "a response failure or creative media
failure"; % Slate Plays marks live server-side breaks where slates played; real-time views refresh about every
20 seconds with under 30 seconds of latency.

A live break is the same moment for everyone watching a channel, and an unfilled slot is revenue that is gone when
the break ends. This kit runs that one lane on Apache Iggy: ad events keyed by session into a checker that watches
the unfilled share per ad server × market, and alerts out to the ad ops console, webhooks and agents. It prints both
hops' latency every second, and counts the failing cohort's breaks that were flagged while they were still running
and the failing cohort's ad requests that came after the console had the alert, the ones that could have gone to a
backup ad server.

Conviva has not published how ad events flow through its pipeline or how fast its ad alerts fire today; this kit is
one lane beside Kafka, Time-State, Druid and ClickHouse, not a replacement for them.

## Run it

Needs Docker and Python 3.10+. Defaults: 40,000 sessions on 8 live channels, a break of four 30-second slots every
10 minutes per channel (staggered across channels), every session on a channel entering the break together, an
illustrative outcome mix (`live_match.outcome`; set it to yours) and `ad-server-b` failing in `dallas` (70% of its
requests time out after 2 s) from second 60 for five minutes.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python ad_break_checker.py &         # the checker, fed by the ad break lane
python live_match.py                 # 40,000 sessions: ad events out, alerts in
```

They print a line a second, and a line when the failing cohort reaches the console:

```
[ad-breaks]  … events/s  event-to-checker p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  ad attempts …
             impressions …  unfilled … (response failures …, creative failures …, slates …)  sessions in a break …
             alerts sent … (fire …, clear …)  cohorts in alert: …  stall gc …ms loop …ms
             slowest event->broker … ms, broker->checker … ms
[live-match] ALERT ad-server-b x dallas at the ad ops console … ms after the fault began: unfilled share …% of …
             slots, causes {…}, devices …; checker-to-console … ms after the event that tipped it
[live-match]  … events/s (fault)  ad attempts …  unfilled … (response failures …, of them the fault …)
             failing-cohort breaks that lost a slot, ended …: flagged while the break was still running …
             failing-cohort ad requests …, after the console had the alert … (at 30 s: …)
             alerts at the console: fire …, clear …, other cohorts …  event-to-consumers n … p50 …ms p99 …ms
```

"At 30 s" counts the same ad requests for a path that delivered the alert 30 seconds after the event that raised
it: about the latency Conviva's docs give for its real-time views. Set `--compare-s`. "Stall" is the longest
garbage-collection pause and event-loop stall in that process that second, and "slowest event->broker" and
"broker->checker" split that second's slowest event between the send and the read, so a slow second can be traced
rather than guessed at.

**What the numbers mean.** The kit times each ad event from the moment `live_match.py` creates it to the moment `ad_break_checker.py` has applied it, and each alert from the ad event that tipped it to all three consumers, through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (12,000 sessions on 12 channels, a break of three 15-second slots every 55 s per channel, about 1,600 ad events/s in bursts as breaks and slots begin, and 3 consumers), two green runs held event-to-checker p50 6.6–6.8 ms, p99 12.8–12.9 ms and p99.9 14.4 ms (120 of 120 steady-state seconds inside 50 ms), and event-to-consumers (both hops, to all three consumers) p99 7.2–8.8 ms. With 2 MB/s of heartbeats on the same broker, the lane's p99 went from 10.4–10.6 ms to 12.8–13.0 ms (medians per second). The failing cohort (ad-server-b x dallas, 70% of requests timing out after 2 s) was flagged at the ad ops console 2.9–3.5 s after the fault began, 9.7–10.4 ms after the event that tipped it; every one of its breaks that lost a slot to the fault (291 and 301 per run) was flagged while still running, about 90% of its ad requests during the fault (898 of 1,000 and 905 of 1,004) came after the console had the alert, and no healthy cohort was alerted. In this 30-second fault, an alert 30 s behind would have come too late for every one of them. That is the end-to-end loop through this demo code, not the broker alone. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. The checker in the kit is a stand-in that takes no time; yours adds its own, and the player's ad request timeout comes before any of this.

| File | What it is |
|---|---|
| `live_match.py` | Live channels and the sessions watching them. Each session publishes its ad events to `ad-events`, keyed by session, with its ad server, market, device, break, slot and event sequence; one ad server fails in one market. Three consumers (ad ops console, webhooks, agents) read `ad-alerts`; the console checks whether each failing-cohort break was flagged while it was still running. |
| `ad_break_checker.py` | The checker: unfilled share per ad server × market over a 10 s window, fire at 25%, clear below 10%, causes and devices named. `Checker.decide()` is where yours goes. Prints the lane's p99. |
| `heartbeat_flood.py` | Full-census playback heartbeats on their own topic, for the isolation test. |
| `kafka_bridge.py` | Copies the ad events from your session events topic in Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `common.py` | Topics, partitioning by session, cohort dimensions, outcome names. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **The big game's break:** `python ad_break_checker.py --readers 4 &` and `python live_match.py --channels 4` (twice as many players per break).
   Pass: event-to-checker p99 under 50 ms through the break start, and the failing cohort is flagged while its
   breaks are still running.
2. **Heartbeats next door:** `python heartbeat_flood.py --mbps 50` while both run.
   Pass: the ad break lane's p99 moves by less than 10%.
3. **Restart the checker:** stop `ad_break_checker.py` and start it again; it resumes from its stored offset (stored
   once a second), rebuilds each cohort's window from the events' own timestamps, and still sends alerts for events
   from the last 30 seconds.
   Pass: it catches up within seconds, and the console does not count an alert twice.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --kafka-topic <session-events-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads, and keeps only the ad events; the sensors, ingestion, every producer and Time-State are
untouched. Map your record in `to_event()`: the kit needs the session ID, the ad event kind, the ad server, market
and device. Run it in your own cloud account or data centre, next to your brokers.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and Time-State and
everything else stay on Kafka. Delivery is at-least-once (events carry their sequence and alerts their number, so
repeats are harmless). Replicated clusters (Viewstamped Replication) shipped in September 2026. Storage is local
disk per node, not object storage: that keeps the tail in milliseconds, and it is why this is one lane, not the
whole session history. There is no Scala SDK; Scala services can use the Java SDK, and Rust services the Rust SDK.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored ad events topic, alongside Kafka and
Time-State. No advantage, no migration.

kranti@laserdata.com · laserdata.com
