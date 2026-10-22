# Real-time click-to-install fraud checks for Affle, on LaserData

Every click and install postback reaches the mFaaS click-to-install scorer, and every publisher block reaches
every attribution node, in milliseconds, before that publisher's next conversion is billed. It runs beside
Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Affle_ClickToInstall.pdf`](LaserData_Affle_ClickToInstall.pdf)

## Why this use case

From Affle 3i's stock exchange filing of June 24, 2026, on its Indian patent "Method and system for
click-to-install behavior based detection of fraud": the method analyses "click-to-install behavioural patterns
in real time", using "click time, redirection time, time to install and network signals", and blocks "offending
publishers in real time so that advertisers pay only for authentic consumer conversions", protecting "the
integrity of our CPCU business model". From Affle's corporate presentation (Dec 2025): 99.6% of revenue from
contracts with customers comes from the CPCU model; 119.7 million conversions in Q3 FY2026 at an average CPCU of
₹58.5 (9M FY2026); "Real-time Fraud checks"; mFaaS, "Real-time solution for addressing digital ad fraud". mFaaS
was launched to catch fraud "not at a post mortem level but during run time" (Anuj Khanna Sohum, 2017), with
reason codes including install hijacking and click spamming. Affle's job postings describe Kafka in the RTB
exchange's event and reporting pipelines, and testing of "attribution accuracy, and postback reliability".

Click injection is where the clock matters most: the injected click lands seconds before the install postback,
and takes the credit for it. Two things have to happen in milliseconds. The scorer must already have the click
when the install arrives, or it cannot compute click-to-install time (CTIT). And once it blocks the publisher,
every node that bills conversions must have the block before that publisher's next install lands, or the
conversion is billed anyway. This kit runs that one loop on Apache Iggy, keyed by device so a device's click is
always ahead of its install, prints both hops' latency every second, and counts the conversions billed to a
publisher after it was already blocked, in conversions and in rupees.

Affle has not published how mFaaS receives clicks and postbacks today, or how fast its blocks propagate; the
shadow run on your click and install topics is how to compare.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 attribution nodes, 10,000 clicks a second from 500 publishers over a
million devices, 1% followed by an install 11–30 s later (about 100 installs a second), and from second 30 a new
publisher every 10 s injecting a click on 60% of beginning installs, 0.5–4 s before the postback, for 8 s. The
click-to-install times are compressed so a one-minute run shows the loop; real ones run from minutes to days.
All illustrative; set them in `common.py` and the flags.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python ctit_scorer.py &              # the scorer, fed by the click-install lane
python attribution_nodes.py          # 8 attribution nodes: clicks and installs out, blocks in
```

They print a line a second, and one per injecting publisher:

```
[ctit]  10,100 events/s  event-to-scorer p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  clicks …  installs … (with their click …, short CTIT …)  blocks sent …
[nodes] 10,000 clicks/s (steady)  installs …  billed …  held …  hijacked billed …, after its block was out … (at 1000 ms it would be …, Rs …)
        false blocks 0  block-to-nodes n … p50 …ms p99 …ms
[nodes] incident pub-0500: onset to block … ms, block to all 8 nodes … ms, hijacked conversions billed before the block … (Rs …)
```

"At 1000 ms it would be …" counts the conversions credited to a publisher in the second after its block was
published: what a lane with a 1 s end-to-end delay would still have billed. Set `--compare-ms`. Rupees are at
the ₹58.5 average CPCU, per conversion. "Onset to block" and the conversions billed before the block are
detection, set by the scorer's rule (`decide()` in `ctit_scorer.py`: 15 attributed installs in 30 s, half of
them under 10 s CTIT), not by the broker. "With their click" counts installs scored with their click already in
the scorer's state; on this lane it is every install.

**What the numbers mean.** The kit times each click and install from the moment `attribution_nodes.py` creates it to the moment `ctit_scorer.py` has applied it, and each block from `ctit_scorer.py` to every attribution node, through Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at reduced scale (2,500 clicks and about 100 installs a second over 4 attribution nodes), four green runs held event-to-scorer p50 4.9–7.8 ms, p99 8.4–14.2 ms and p99.9 9.2–15.9 ms (every steady-state second inside 50 ms, 61–62 per run), and block-to-nodes p50 2.8–4.0 ms and p99 3.4–4.1 ms. All 4,900–5,069 installs per run were scored with their click already on the lane. With 2 MB/s of bid logs on the same broker, the lane's p99 moved by at most 0.4 ms. The 5 injecting publishers per run were each blocked a median 1.6–1.8 s after they started (the rule: 15 installs, after a 0.5–4 s injection delay) and the block reached all four nodes a median 3.4–4.1 ms later (at most 5.2 ms); no genuine publisher was blocked. Of 169–210 conversions per run that a lane 1 s behind would still have billed to a blocked publisher, 0–1 (at most 0.5%) were billed on this lane. That is the end-to-end loop through this demo code, not the broker alone.
The broker's own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. Clicks, installs and fraud in the kit are simulated. CI runs: https://github.com/psinglap/laserdata-kits/actions/runs/39859035881, https://github.com/psinglap/laserdata-kits/actions/runs/39859305594, https://github.com/psinglap/laserdata-kits/actions/runs/39859315876, https://github.com/psinglap/laserdata-kits/actions/runs/39859325980

| File | What it is |
|---|---|
| `attribution_nodes.py` | Click trackers and attribution nodes. Every click (with redirect time) and install postback goes to `click-install`, keyed by device; each install is credited to the device's last click. Each node reads `publisher-blocks` on its own connection and holds a blocked publisher's conversions instead of billing them. Plants the click injection. |
| `ctit_scorer.py` | The scorer. Keeps each device's last click and each publisher's 30 s window of CTIT (`decide()` is where your rule goes), publishes BLOCKED, and rebuilds every device and publisher from the log after a restart. Prints the lane's p99. |
| `bid_log_flood.py` | Bid request, response and win logs on their own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your clicks and install postbacks from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Peak traffic:** `python attribution_nodes.py --peak 5 --peak-at 60 --peak-for 120`.
   Pass: event-to-scorer p99 under 50 ms, every install scored with its click, and under 1% of the conversions a
   1 s lane would bill to a blocked publisher billed on this lane.
2. **The bid-log firehose next door:** `python bid_log_flood.py --mbps 50` while both run.
   Pass: the click-install lane's p99 moves by less than 10%.
3. **Restart the scorer mid-injection:** stop `ctit_scorer.py` while a publisher is injecting and start it again;
   it replays the click-install topic, rebuilds every device's last click and every publisher's window, and only
   publishes blocks decided on events from the last 10 seconds. Pass: it catches up within seconds, no event is
   applied twice, and the injecting publisher is still blocked.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <kafka-bootstrap-brokers> --kafka-topic <clicks-topic> --kafka-topic <installs-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads; the click trackers, postback receivers, producers and every existing consumer are
untouched. Map your records in `to_event()`: the kit needs the event type, the device ID, the publisher for
clicks, and the event time (CTIT is taken from your event times). Run one bridge per Kafka cluster, next to it.

## What Apache Iggy 0.9 doesn't have yet

A Kafka-compatible gateway is in progress and not in 0.9, so the trial uses the bridge, and every existing
producer and consumer stays on Kafka. Delivery is at-least-once (blocks carry the state and events carry a
sequence, so repeats are harmless). Replicated clusters (Viewstamped Replication) shipped in September 2026.
There is no cross-region replication in 0.9: run one small lane per region your trackers run in.

## Next step

If the numbers are interesting: a 2–4 week evaluation on mirrored click and postback topics, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
