# Real-time livestream SLOs for Whatnot, on LaserData

Every story event (trigger, steps, outcomes, expected failures) reaches the story judge in milliseconds, so each
livestream join is judged the moment its last outcome lands or its deadline passes, and each SLO alert reaches
on-call milliseconds later. It runs beside Kafka and ClickHouse, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Whatnot_StorySLO.pdf`](LaserData_Whatnot_StorySLO.pdf)

## Why this use case

From Whatnot Engineering's "All Dashboards Green, All Screens Black" (Karol Gil, Reliability Platform, September
2026): on June 8, 2026 newly loaded web clients showed "black screens instead of livestream video" while "most
internal dashboards" stayed green, and "3,000 users were impacted in the first 30 minutes". End-to-end SLO stories
caught it: a livestream join succeeds only if video plays, auction details show and chat is visible within a fixed
time. Story events go "through our events ingestion service and forwarded to Kafka, from where they go to
ClickHouse"; the pipeline "delivers events at least once"; "every minute, a background worker then finds triggers
that have reached a 60-second deadline". The rules: count users, not attempts (one result per client, its worst);
expected failures such as a seller blocking a user do not count. And the next step, in their words: "hundreds of
thousands of story events each minute. This volume is also pushing our minute-based ClickHouse queries toward
their limit, so stream processing is our next step."

This kit runs that one lane on Apache Iggy: story events keyed by story key into a judge that holds every open
story, judges it at its last outcome or its deadline, drops repeats, computes the SLI per story type × surface with
each client counted once, and sends SLO alerts and one SLI snapshot a second to the pager, the Datadog exporter and
the incident bot. It prints the lane's latency every second, when the pager had the alert, how many broken joins
came before it, and when a once-a-minute judge over the same stories would have paged.

Whatnot has not published its story schema, topic layout or alert configuration; this kit is one lane beside the
events ingestion service, Kafka, ClickHouse and the minute worker, not a replacement for them. The 60-second
deadline is a product rule and stays; what the lane removes is the wait after it.

## Run it

Needs Docker and Python 3.10+. Defaults: 200,000 clients (45% iOS, 30% Android, 25% web), 300 livestream joins
and 600 bids a second (about 3,300 story events a second, 200,000 a minute), a 60-second deadline, 1% of events delivered twice, an illustrative outcome mix
(`live_shows.join()` and `bid()`; set them to yours), and from second 120 for five minutes a black screen on 80% of
web joins: auctions and chat still arrive, video never does.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python story_judge.py &              # the judge, fed by the story lane
python live_shows.py                 # 200,000 clients: story events out, alerts in
```

They print a line a second, and a line when the page reaches the pager:

```
[judge] … events/s  event-to-judge p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  stories judged ok … failed …
        excluded …  repeats dropped …  late outcomes …  open …  deadline-to-verdict p99 …ms
        SLI livestream_join/android …%  livestream_join/ios …%  livestream_join/web …%  place_bid/…  alerts sent …
        alerting: …  stall gc …ms loop …ms  slowest event->broker … ms, broker->judge … ms
[live-shows] ALERT livestream_join x web at the pager … ms after the fault began: SLI …% of … clients,
        missing {'video_playing': …}; deadline-to-pager … ms
[live-shows] … events/s (fault)  joins …  bids …  repeats sent …  web joins during the fault …: no video …, of them
        before the pager had the alert …  pager had it after the fault began: … s; a once-a-minute judge: … s
        alerts at the pager: fire …, clear …, other cohorts …  knowable-to-consumers n … p50 …ms p99 …ms
```

"A once-a-minute judge" applies the rule from Whatnot's post to the same stories: the run at the top of minute t
judges the stories whose deadline fell in minute t-2, and pages if that minute's web joins are below the SLO. It
leaves out burn-rate windows, which both paths would add. "Deadline-to-verdict" includes the judge's 200 ms wait
for events in flight (`--grace-ms`). "Stall" is the longest garbage-collection pause and event-loop stall in that
process that second, and "slowest event->broker" and "broker->judge" split that second's slowest event between the
send and the read, so a slow second can be traced rather than guessed at.

**What the numbers mean.** The kit times each story event from the moment `live_shows.py` creates it to the moment `story_judge.py` has applied it, and each alert from the moment its verdict became knowable (the deadline, or the last outcome) to the consumers, through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (20,000 clients, 150 livestream joins and 250 bids a second, about 1,500 story events/s with 1% sent twice, the deadline and SLI window cut to 10 s so the run fits in a minute, and 3 consumers), three green runs held event-to-judge p50 3.3–6.5 ms, p99 6.1–12.3 ms and p99.9 6.9–14.0 ms (186 of 186 steady-state seconds inside 50 ms). With 2 MB/s of client telemetry on the same broker, the lane's p99 went from 5.9–12.0 ms to 6.2–12.5 ms (medians per second). With a black screen on 80% of web joins from second 20, the judge paged for livestream_join × web 10.3–10.4 s after the black screen began, 218–232 ms after the deadline of the story that tipped it (200 ms of that is the judge's wait for events in flight), naming video_playing as the missing outcome; no other story type or surface paged. A once-a-minute judge over the same stories would have paged 86–105 s after the black screen began. Of 865–911 web joins per run with a black screen, 289–325 came before the page: the joins of the first deadline, which no judge can call broken before it passes. Every story was judged once, with 2,139–2,189 repeated events dropped per run. That is the end-to-end loop through this demo code, not the broker alone. The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. The judge in the kit is a stand-in; your events ingestion service and Kafka add their own time before any of this.

| File | What it is |
|---|---|
| `live_shows.py` | Clients joining livestreams and bidding. Each action is a story with a random story key; its events go to `story-events`, keyed by story key, 1% of them twice; a black screen breaks most web joins. Three consumers (pager, Datadog exporter, incident bot) read `slo-alerts`; the pager counts the broken joins that came before it had the alert. |
| `story_judge.py` | The judge: success at the last required outcome, failure at the deadline naming what is missing, expected failures excluded, repeats dropped; SLI per story type × surface, each client once with its worst result, page below 95%, clear at 98.5%. `Judge.sli()` and `Judge.decide()` are where yours go. Prints the lane's p99. |
| `client_events_flood.py` | The rest of client telemetry on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies the story events from your Kafka topic into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `common.py` | Topics, partitioning by story key, story schemas (trigger, steps, outcomes, expected failures). |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Super Bowl weekend:** `python story_judge.py --readers 4 &` and `python live_shows.py --joins 900 --bids 1800`.
   Pass: event-to-judge p99 under 50 ms, no healthy cohort pages, and the black screen pages within the deadline
   plus a second.
2. **Telemetry next door:** `python client_events_flood.py --mbps 50` while both run.
   Pass: the story lane's p99 moves by less than 10%.
3. **Restart the judge:** stop `story_judge.py` and start it again; it reads back the last 75 seconds of the log
   (the deadline plus 15 s), rebuilds every open story before it judges any deadline, and still sends alerts for
   verdicts from the last 30 seconds.
   Pass: it catches up within seconds, and no story is failed only because the judge was down.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --kafka-topic <story-events-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=<api key> -X sasl.password=<secret>
```

The bridge only reads, and keeps only the story events; clients, ingestion, every producer, ClickHouse and the
minute worker are untouched. Map your record in `to_event()`: the kit needs the story key, story type, role, client
and surface, and the client's time. Add your 30 story types to `common.STORIES`. Run it in your own cloud account,
next to your brokers, and compare the judge's verdicts and page times with your E2E SLOs story by story.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and ingestion, ClickHouse
and everything else stay on Kafka. Delivery is at-least-once, as your pipeline is today (the judge applies each role
of a story once). Replicated clusters (Viewstamped Replication) shipped in September 2026. Storage is local disk per
node, not object storage: that keeps the tail in milliseconds, and it is why this is one lane, not the story
history, which ClickHouse keeps. There is no Elixir SDK; Python, Go and the other official SDKs cover your other
services, and Live Service events reach the lane through ingestion and Kafka.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored story events topic, alongside Kafka and
ClickHouse. No advantage, no migration.

kranti@laserdata.com · laserdata.com
