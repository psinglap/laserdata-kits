# Real-time ad floors for Dailyhunt and NexVerse.ai, on LaserData

When breaking news moves what advertisers pay on one language, every NexVerse.ai bid node has the new
floor for each Dailyhunt placement in milliseconds, in order, and a bid node the autoscaler adds mid-spike
rebuilds every floor from the log. It runs beside BigQuery and whatever sets floors today, so nothing else
has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.verse-dailyhunt-kit
- **One slide:** [`LaserData_VerSeDailyhunt_AdFloors.pdf`](LaserData_VerSeDailyhunt_AdFloors.pdf)

## Why this use case

From VerSe's and Dailyhunt's own sources:

- Dailyhunt "derives 100% revenue from advertising", and VerSe aims to build "Bharat's biggest ad exchange"
  through NexVerse.ai (CEO Umang Bedi, Inc42, Aug 2024).
- NexVerse.ai takes OpenRTB 2.5 bid requests with a `tmax` of 120 ms in its own field table, fires the win
  notice when the ad renders, and its sample bid request is for the Dailyhunt app (`com.eterno`)
  ([OpenRTB integration guide](https://nexverse.ai/developer/docs/ortb-integration-guide/)). Its SSP offers
  "real-time yield optimization, dynamic floor management, and transparency" (NexVerse.ai, Oct 2025).
- Dailyhunt serves 350 million+ users in 16+ languages, and "we have huge and instantaneous spikes in demand
  that are often unpredictable, which are caused by breaking news and viral content", around elections and
  sports. CDN logs are streamed into BigQuery and analysed in seconds
  ([Google Cloud customer story](https://cloud.google.com/customers/dailyhunt)).
- VerSe's CPTO, Prasanna Prasad (since April 2026, previously CTO at Verve Group), names "monetization
  efficiency" as one of the outcomes technology must differentiate on.

When readers rush to one language's stories, what advertisers will pay for that language's placements moves
within seconds. A bid node that still holds the old floor sells below the new one, or, when a floor drops,
lets impressions go unfilled under a floor that is now too high. This kit carries one signal, the floor per
placement, from the floor service to every bid node on Apache Iggy, and prints its p99 every second next to
the cost of being late: stale-floor auctions, and the rupees given away below a raised floor. The log adds
rebuilds: a bid node added during the spike reads the last minute of the log and has every placement's
floor, including ones that have not changed for minutes.

We don't know how NexVerse.ai sets or distributes floors today; the kit's "at a 1 s refresh" figure is a
comparison, not a claim about your system.

## Run it

Needs Docker and Python 3.10+. Defaults: 15 languages × 2,000 ad slots (30,000 placements), floors from
₹20 to ₹180 CPM, 5,000 floor changes a second (100 of them steps, where a floor jumps up or down), breaking
news on Hindi from second 60 for 30 s (4× its floor changes, steps mostly up), and bid nodes that each run
100,000 auctions a second at 60% fill. All illustrative: set them to yours.

```
docker compose up -d                    # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt         # apache-iggy 0.9.0
python floors.py &                      # the floor service: one writer per partition
python bidnode.py --name bidnode-1 &    # a NexVerse.ai bid node
python bidnode.py --name bidnode-2      # and another: each reads every floor
```

Each prints a line a second:

```
[bidnode-1]  5,000 floor changes/s  floor-to-node p50 …ms  p99 …ms  p99.9 …ms  PASS (<20ms)  steps 100
             stale-floor auctions …/s (… per 1M): below floor …, unfilled …, Rs … given away
             at a 1 s refresh …/s, Rs …  gaps 0  placements 30,000
[floors]  5,000 floor changes/s  steps 100  … writes  write ack p50 …ms  p99 …ms  breaking news: hi x4
```

- **stale-floor auctions:** auctions this node ran on a placement after its floor stepped, before the node
  had the new floor (latency × the node's auction rate on that placement), per second and per million
  auctions. **below floor:** after a raise, sold below the new floor; **Rs given away:** those sales times
  the floor difference, at `--fill`. **unfilled:** after a drop, auctions run under a floor that is now too
  high. **at a 1 s refresh:** the same steps if the node picked them up a second later.
- **gaps:** a missing sequence number for a placement; must stay 0.

**What the numbers mean.** The kit times each floor from the moment the floor service set it (including its
wait for the next write) to the moment `bidnode.py` has applied it, through Python clients, on whatever
machine you run it on. In CI, on a shared 2-vCPU GitHub runner with the broker, the floor writers, three bid
nodes and the ad-event log all on one box, at about 2,050 floor changes/s over 12,000 placements
(15 languages × 800 ad slots), three green CI runs held a median per-second p50 of 4.0 to 4.3 ms, p99 7.9 to 8.5 ms and p99.9 9.2 to
9.9 ms on each node; 368 of 369 steady-state node-seconds passed the 20 ms budget, and the one slow second
(p99 25.8 ms) came as the added node replayed 50,000 changes on the same two CPUs. Breaking news on Hindi
(floor changes up from about 2,050 to 2,460 a second) with a 4× ad-event burst next door (to 4 MB/s) moved
the median p99 by 0.3 to 0.6 ms. About 48 steps a second put a median of 0.42 to 0.43 auctions a second on
a stale floor (about 21 per million) at 20,000 auctions/s per node, giving away about Rs 0.007 a second;
had the same steps arrived a second later, about 80 auctions and Rs 1.3 a second. The node added as the
spike started rebuilt all 12,000 placements from the last 20 s of the log (about 50,500 changes) in 0.62 to
0.68 s; 10 s of live changes alone would have covered 74% of them. No sequence gaps.
That is the end-to-end lane through this demo code, not the broker alone; the broker's own tail latency is
measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `floors.py` | The floor service: per placement (language × ad slot), the floor in rupees CPM, written on every change with a per-placement sequence; steps when a floor jumps; breaking news on one language; a rolling snapshot of every floor once a minute. One writer per partition, each writing every 2 ms. |
| `bidnode.py` | A NexVerse.ai bid node: reads every partition with its own offsets, keeps the live floor table, counts stale-floor auctions and rupees given away at its auction rate, checks sequence gaps. Rebuilds from the last `--replay-s` seconds of the log on start. |
| `ad_events.py` | The ad event log (request, bids or 204, win notice, clearing price; ~330 B records) on its own topic, with a burst, for the isolation test. |
| `pubsub_bridge.py` | Reads your auction outcomes (or your floor changes) from Cloud Pub/Sub through its own subscription (`iggy-shadow`) and writes floor changes into Iggy, for a shadow run on real traffic. |
| `ci.sh` | The CI run: two bid nodes, breaking news with a 4× ad-event burst next door, a third node added as it starts; pass only on 95% PASS seconds per node, zero gaps, steps counted and a full rebuild. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests

1. **Results night:** `python floors.py --rate 20000 --steps 400 --spike-x 8`.
   Pass: floor-to-node p99 stays under 20 ms through the spike and `gaps` stays 0.
2. **More bid nodes:** start a third and a fourth `bidnode.py`.
   Pass: the first node's p99 moves by less than 10%; every node reports the same change count.
3. **The autoscaler adds a node mid-spike:** start `bidnode.py --name bidnode-9` while the `breaking news`
   line shows. Pass: `rebuilt from the last 75 s of the log … floors for 30,000 of 30,000 placements` within
   seconds, and the line after it is PASS. Compare with the `without the log` line.
4. **The ad-event log next door:** `python ad_events.py --mbps 20 --x 4` while the rest runs.
   Pass: the lane's p99 moves by less than 10%.

## Shadow your own auctions

```
pip install google-cloud-pubsub
gcloud pubsub subscriptions create iggy-shadow --topic=<your auction-outcome topic> --message-retention-duration=1h
python pubsub_bridge.py --project <gcp project> --placement-field placement --price-field clear_cpm_rs
python bidnode.py --name shadow --replay-s 0
```

The bridge reads its own subscription, so the bid nodes, publishers and every existing subscriber (the
BigQuery feed included) are untouched. With `--source outcomes` it derives a floor per placement from a
moving clearing price (a stand-in for your yield logic); with `--source floors` it copies the floors your
yield engine already sets. Map your fields in `to_record()`. In a shadow run the latency `bidnode.py` prints
starts at the bridge. If your auction outcomes ride Kafka instead, swap the subscriber for a consumer group
named `iggy-shadow`.

## What Apache Iggy 0.9 has today, and what it doesn't

- **Replication:** Viewstamped Replication clusters, new in September 2026. Three nodes tolerate one
  failure. It is young; the evaluation's failover row is there to test it.
- **Delivery:** at-least-once. The per-placement sequence makes repeats harmless and gaps visible.
- **Replay:** polling from an offset or a timestamp, which is how a node rebuilds; per-topic expiry. There
  is no log compaction, which is why the floor service writes a rolling snapshot.
- **Kafka API:** a Kafka-compatible gateway is in progress and not in 0.9. The lane does not need it; the
  bridge reads Pub/Sub (or Kafka) as one more subscriber.
- **Managed service on Google Cloud:** none from Google; you run the broker (one Rust binary or a container),
  with LaserData's support.
- **Clients:** official SDKs for Rust, Go, Java, Python, C# and Node.js.
- **Footprint:** a single Rust binary, Apache-licensed: thread-per-core, no garbage collector.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored auction-outcome feed (one language's
placements is enough), alongside your floor service. No advantage, no migration.

kranti@laserdata.com · laserdata.com
