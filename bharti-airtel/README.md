# Real-time fraud domain verdicts for Airtel, on LaserData

When the SMS link scan, a partner score, a threat feed or a customer report turns a domain's verdict to
block, every DNS resolver and the SMS link scanner has the block in milliseconds, in order per domain,
before the next click on that link; a lifted block reaches them just as fast. A resolver that restarts or is
added mid-campaign rebuilds every verdict from the log. It runs beside the resolvers, the partner scoring and
the daily database build, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_BhartiAirtel_DomainVerdicts.pdf`](LaserData_BhartiAirtel_DomainVerdicts.pdf)

## Why this use case

From Airtel's own sources:

- Airtel's Fraud Detection Solution (May 2025) protects users "from fraudulent domains and phishing URLs
  encountered while browsing the internet or across platforms like SMS, emails and OTT apps … in real-time".
  Its database of fraudulent domains is built from global threat intelligence feeds "and is updated every 24
  hours"; a domain not in it gets "a composite fraud score … calculated in milliseconds and the domain is
  blocked at the DNS level if malicious". Airtel describes the whole as "one seamless loop" from user
  interaction signals to network-level enforcement
  ([GSMA case study](https://www.gsma.com/solutions-and-impact/technologies/security/scams/wp-content/uploads/2025/08/Airtel-Fraud-detection-Case-Study-0925-3.pdf), Oct 2025).
- 187,154 fraudulent domains blocked and about 30 lakh fraud attempts prevented a day (GSMA, Oct 2025); more
  than 800,000 fraudulent links blocked by January 2026
  ([tele.net](https://tele.net.in/airtel-flags-71-billion-spam-calls-since-ai-filter-rollout/)).
- Every SMS is scanned against a central database of malicious links, and 1.5 billion messages and 2.5
  billion calls a day pass Airtel's spam filters
  ([Airtel, Sep 2024](https://www.bharti.com/press-release-2023-2024-airtel-cracks-down-on-spam-launches-indias-first-ai-powered-network-solution-for-spam-detection.html)).
  The link blocking works "across all platforms and block the link across devices"
  ([Airtel, May 2025](https://www.bharti.com/press-release-2024-2025-airtel-launches-fraud-detection-solution-a-first-in-the-world.html)).
- "We are on a mission to make Airtel the safe network" (Shashwat Sharma, MD & CEO,
  [Feb 2026](https://nsearchives.nseindia.com/corporate/BHARTIARTL_11022026110814_BhartiAirtel-Reg30PRfeb11.pdf)).

A smishing link is clicked within seconds of the SMS landing. Its first lookups may score clean; minutes
later the evidence arrives elsewhere (the SMS link scan sees it in thousands of messages from flagged
senders, a customer reports it, a feed lists it). A resolver that already answered the domain as clean keeps
resolving it until it hears otherwise. This kit carries one signal, the verdict per domain, from wherever it
is made to every enforcement point on Apache Iggy, and prints its p99 every second next to the cost of being
late: clicks let through on a link that was already blocked, and legitimate lookups still blocked after a
block was lifted. The log adds rebuilds and history: a resolver that restarts reads the last minute of the
log and has every domain's verdict, and the log shows which verdict was in force when.

We don't know how verdicts reach Airtel's resolvers today between database builds. The kit's "at the daily
refresh" figure is the case where a resolver keeps a cached clean verdict until the next build, for the
link's assumed click life; it is a comparison, not a claim about your system.

## Run it

Needs Docker and Python 3.10+. Defaults: 500,000 domains (40% blocked at start), 3,000 verdicts a second
(re-scores of domains being looked up), 5 flips a second (90% clean to block, 10% a block lifted), a
smishing campaign from second 60 for 30 s at 10× the usual blocks, all from the SMS link scan, and resolvers
that each answer 200,000 lookups a second, with 2 clicks a second on a just-blocked link and a one-hour click
life per link. All illustrative: set them to yours.

```
docker compose up -d                           # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                # apache-iggy 0.9.0
python verdicts.py &                           # the verdict writer: one writer per partition
python resolver.py --name resolver-haryana &   # a DNS resolver node
python resolver.py --name resolver-delhi       # and another: each reads every verdict
```

Each prints a line a second:

```
[resolver-haryana]  3,000 verdicts/s  verdict-to-resolver p50 …ms  p99 …ms  p99.9 …ms  PASS (<25ms)  to block 5
                    clicks let through …/s (… per 1M lookups)  at the daily refresh 36,000  lifted 1
                    legit lookups still blocked …/s  gaps 0  domains 500,000
[verdicts]  3,000 verdicts/s  to block 5  lifted 1  … writes  write ack p50 …ms  p99 …ms  smishing campaign x10
```

- **to block / lifted:** verdicts that flipped this node's own verdict, clean to block or block to clean,
  judged against the verdict the node held just before the flip.
- **clicks let through:** lookups this node resolved on a domain after it was flipped to block, before the
  node had the block (latency × `--clicks-per-s`), per second and per million lookups. **at the daily
  refresh:** the clicks the same blocks would let through if the node kept its cached clean verdict for the
  link's click life (`--campaign-life-s`), judged against the same pre-flip verdict.
- **legit lookups still blocked:** lookups on a domain after its block was lifted, before the node had it
  (latency × `--legit-per-s`).
- **gaps:** a missing sequence number for a domain; must stay 0.

**What the numbers mean.** The kit times each verdict from the moment it was set (including its wait for
the next write) to the moment `resolver.py` has applied it, through Python clients, on whatever machine you
run it on. In CI, on a shared 2-vCPU GitHub runner with the broker, the verdict writers, three resolvers and
the call and SMS records all on one box, at about 1,020 verdicts/s over 20,000 domains, two green CI runs
held a median per-second p50 of 3.5 to 3.9 ms, p99 6.0 to 7.2 ms and p99.9 6.6 to 8.0 ms on each resolver;
244 of 246 steady-state resolver-seconds passed the 25 ms budget, and the one slow second in each run (p99
40 to 47 ms) came as the added resolver replayed about 37,000 verdicts on the same two CPUs. The smishing
campaign (blocks up from about 18 to 98 a second) with a 4× call and SMS record burst next door (to 4 MB/s)
moved the median p99 by less than 0.1 ms. At 2 clicks a second on a just-blocked link, the lane let through
about 0.008 clicks per block (17 to 20 clicks over 2,400+ blocks in 69 s); a resolver holding a cached clean
verdict for a one-hour click life would have let through 7,200 per block. The resolver added as the campaign
started rebuilt all 20,000 domains from the last 20 s of the log (about 36,800 verdicts) in 0.34 to 0.50 s.
No sequence gaps. CI uses far more flips than a real day so every second has some to count. That is the
end-to-end lane through this demo code, not the broker alone; the broker's own tail latency is measured with
`iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `verdicts.py` | The verdict writer: per domain, block or clean with a composite score and the signal that produced it, written on every score with a per-domain sequence; flips to block and lifted blocks; a smishing campaign; a rolling snapshot of every verdict once a minute. One writer per partition, each writing every 2 ms. |
| `resolver.py` | A DNS resolver node (or the SMS link scanner): reads every partition with its own offsets, keeps the live verdict table, counts clicks let through and legit lookups still blocked, checks sequence gaps. Rebuilds from the last `--replay-s` seconds of the log on start. |
| `records.py` | Call and SMS records (caller, callee, circle, frequency, duration, links; ~220 B) on their own topic, with a burst, for the isolation test. |
| `verdict_bridge.py` | Copies your verdicts, read-only, from a JSON-lines export or from Kafka through its own consumer group (`iggy-shadow`), for a shadow run on real verdicts. |
| `ci.sh` | The CI run: two resolvers, a smishing campaign with a 4× record burst next door, a third resolver added as it starts; pass only on 95% PASS seconds per resolver, zero gaps, blocks and lifts counted, and a full rebuild. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests

1. **A nationwide blast:** `python verdicts.py --rate 30000 --flips 50 --campaign-x 50`.
   Pass: verdict-to-resolver p99 stays under 25 ms through the campaign and `gaps` stays 0.
2. **More resolvers:** start a third and a fourth `resolver.py`, one per circle.
   Pass: the first resolver's p99 moves by less than 10%; every resolver reports the same verdict count.
3. **A resolver restarts mid-campaign:** start `resolver.py --name resolver-mumbai` while the
   `smishing campaign` line shows. Pass: `rebuilt from the last 75 s of the log … verdicts for 500,000 of
   500,000 domains` within seconds, and the line after it is PASS.
4. **Call and SMS records next door:** `python records.py --mbps 20 --x 4` while the rest runs.
   Pass: the lane's p99 moves by less than 10%.

## Shadow your own verdicts

```
python verdict_bridge.py --source file --path /var/log/fraud/verdicts.jsonl \
  --domain-field domain --verdict-field verdict --score-field score
python resolver.py --name shadow --replay-s 0
```

The bridge only reads: from a JSON-lines file your scorer or policy engine appends to, or with
`--source kafka --bootstrap <brokers> --topic <topic>` through its own consumer group `iggy-shadow`
(`pip install confluent-kafka`), so your producers and consumers are untouched. Map your fields with the
`--*-field` options or in `to_record()`. In a shadow run the latency `resolver.py` prints starts at the
bridge. Compare the shadow resolver's counts with your resolvers' block logs.

## What Apache Iggy 0.9 has today, and what it doesn't

- **Replication:** Viewstamped Replication clusters, new in September 2026. Three nodes tolerate one
  failure. It is young; the evaluation's failover row is there to test it.
- **Delivery:** at-least-once. The per-domain sequence makes repeats harmless and gaps visible.
- **Replay:** polling from an offset or a timestamp, which is how a resolver rebuilds; per-topic expiry.
  There is no log compaction, which is why the verdict writer writes a rolling snapshot.
- **Kafka API:** a Kafka-compatible gateway is in progress and not in 0.9. The lane does not need it; the
  bridge reads an export, or Kafka as one more consumer group.
- **Where it runs:** self-hosted, one Rust binary or a container, in your data centres or on Airtel Cloud,
  with LaserData's support; the lane sits beside the resolvers, outside the lookup path.
- **Clients:** official SDKs for Rust, Go, Java, Python, C# and Node.js.
- **Footprint:** a single Rust binary, Apache-licensed: thread-per-core, no garbage collector.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored verdict feed (one circle's resolvers is
enough), alongside your resolvers. No advantage, no migration.

kranti@laserdata.com · laserdata.com
