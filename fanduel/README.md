# Every wallet event at both in-state sites for FanDuel, on LaserData

Every account and wallet event a state's sportsbook accepts (a deposit, a bet placed, a bet settled, a cash out, a
withdrawal) reaches the second site in the same state in milliseconds, at the same partition and offset, so
rerouting traffic inside the state loses nothing, even at a 5x kickoff. A wallet replica at the second site keeps
every balance current and rebuilds from its own site's log after a restart. Nothing leaves the state. It runs
beside Kafka: the sportsbook, the wallet database, Confluent Cloud, Flink and Tinybird stay where they are.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** (link added when published)
- **One slide:** [`LaserData_FanDuel_InStateWallet.pdf`](LaserData_FanDuel_InStateWallet.pdf)

## Why this use case

From FanDuel's own public sources:

- The Federal Wire Act requires financial transactions to be completed and processed within the same state; a New
  Jersey transaction hitting a data center in Virginia would be prohibited. "That's the core of what was driving
  our hybrid strategy and the need for us to build data centers." (Shane Sweeney, SVP of Technology, AWS case
  study.)
- FanDuel runs on about 40 sites (AWS Local Zones, Wavelength and Outposts), scales capacity 5-10x for events like
  the Super Bowl, reroutes traffic to another Local Zone or Outpost if one location has an outage, and is exploring
  sharing capacity across multiple sites in every state, "which would allow near-real-time data replication" (AWS
  case study).
- Per state, on premises: "Sportsbook, Account & wallet, Casino". Per-state needs: "Data residency requirements",
  "Low latency to AWS Regions", "Local HA and DR in state". Future plans: "Additional sites for scale and DR per
  state" (Shane Sweeney, AWS Summit Atlanta, CMP207, May 2022).
- The data platform streams on Confluent Cloud Kafka with Apache Flink (Tinybird customer story); 2026 job posts
  ask for Kafka, Flink and SNS/SQS.

This kit runs one narrow lane: every account and wallet event in one state, keyed by account, carrying the
account's sequence number and its resulting balance and open bets (never a delta), written at the site taking bets
(`nj-1`) and mirrored to the second site in the same state (`nj-2`). The wallet database stays the system of
record; the lane carries its events. How wallet events travel between sites today is not public and is the first
thing to confirm with your team.

## Run it

Needs Docker and Python 3.10+. Two Apache Iggy brokers stand in for the two sites (ports 8090 and 8091 on one
machine). Defaults: 200,000 accounts, 600 account and wallet events a second on a normal evening, 5x for 20 s from
30 s in. Accounts, amounts and rates are illustrative; pass yours.

```
docker compose up -d                    # Apache Iggy 0.9.0: nj-1 on :8090, nj-2 on :8091
pip install -r requirements.txt         # apache-iggy 0.9.0
python site_mirror.py &                 # nj-1 -> nj-2, NJ only, same partitions and offsets
python standby.py --name replica-1 &    # the wallet replica at nj-2
python sportsbook.py --duration 120     # every event nj-1 accepts
python failover_check.py                # after the sportsbook stops: is everything at nj-2?
```

Each prints a line a second:

```
[replica-1]   600 events/s  accepted-to-replica p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
              at risk if nj-1 went dark now … events  stake placed $…/s
              held at nj-1 for a mirror start 0  accounts …  gaps 0  repeats 0
[mirror nj-1->nj-2]   600 events/s copied  waiting at nj-1 0  offset drift 0  cross-state refused 0
[sportsbook nj-1]   600 events/s accepted  write ack p50 …ms  p99 …ms
[failover-check] nj-2 holds … of … events accepted at nj-1 (drained in …s), byte-identical at the same offsets:
                 yes; balances and open bets match on … of … accounts; events from outside NJ: 0  OK
```

`at risk if nj-1 went dark now` is nj-1's partition heads minus what the replica at nj-2 has applied, sampled once
a second: the events a failover at that moment would have to recover from nj-1 later. `held at nj-1 for a mirror
start` counts events that waited at nj-1 while no mirror ran; they are timed apart, not as live latency.

**What the numbers mean.** The kit times each event from the moment the sportsbook accepted it (including its
wait for the next write), through the nj-1 broker, the mirror and the nj-2 broker, to the moment `standby.py` has
applied it, through Python clients, on whatever machine you run it on. In CI both brokers, the sportsbook, the
mirror and two replicas share one GitHub runner, with no network between the sites. At 300 events a second across
50,000 accounts with a 5x kickoff surge (about 1,500 a second for 15 s), the mirror killed and restarted mid-run
and one replica restarted, three green runs held a median per-second p50 of 5.9 to 8.4 ms, p99 9.4 to 13.5 ms and
p99.9 10.1 to 15.2 ms against the 50 ms budget; the median p99 was 9.0 to 13.3 ms on a normal evening and 10.9 to
18.3 ms during the surge. In each run one second of 60 went over 50 ms (66 to 101 ms), the second the other replica
was replaying the log on the same CPUs. Every run: nj-2 held all 38,992 events nj-1 accepted, byte-identical at the
same offsets, every balance and open-bet count matched on all 17,857 accounts that bet, no gaps, no repeats, no
offset drift, no event from outside the state. The restarted mirror resumed from nj-2's offsets; the 280 to 328
events that waited at nj-1 while it was down (up to 1.1 s) were counted apart. The restarted replica rebuilt about
14,000 accounts from nj-2's log (about 27,600 events) in 0.32 to 0.44 s. Events at risk if nj-1 had gone dark were
a median of 0 to 2 per sampled second, and 96 to 144 while the mirror was down. That is the end-to-end lane
through this demo code at reduced scale, not the broker alone, and it excludes the sportsbook's own processing and
the network round trip between two real sites. The broker's own tail latency is measured with `iggy-bench` in
`bench/PLAN.md`, on dedicated machines.

The CI run (`ci.sh`): two brokers, 50,000 accounts at 300 events a second with a 5x surge from 15 s for 15 s, the
mirror killed at 40 s and restarted, two replicas (one restarted at 34 s), then nj-1 stops and the failover check
runs.

| File | What it is |
|---|---|
| `sportsbook.py` | The site taking bets: one writer per partition, every account and wallet event keyed by account, with the account's sequence number, resulting balance and open bets. A kickoff surge with `--surge`. |
| `site_mirror.py` | The in-state mirror: one reading and one writing connection per partition, byte-for-byte to the same partition at nj-2, offsets checked on every write, resumes from nj-2's offsets, refuses any site or event outside the state. |
| `standby.py` | The wallet replica at nj-2: reads every partition with its own offsets, applies each account's events in sequence, prints latency, events at risk, held events, gaps and repeats. Rebuilds from nj-2's log on start. |
| `failover_check.py` | After nj-1 stops: reads both sites, checks every event is at nj-2 byte-identical at the same offset, every balance matches, and nothing came from outside the state. |
| `kafka_bridge.py` | Reads one state's account and wallet events from Kafka (Confluent Cloud) as consumer group `iggy-shadow` and writes them to the lane at nj-1, for a shadow run. |
| `ci.sh` | The CI run (see above); pass only on 95% PASS seconds per replica, zero gaps, repeats, offset drift and cross-state copies, a full rebuild, a clean mirror resume and a clean failover check. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Four tests

1. **Super Bowl kickoff:** `python sportsbook.py --surge 10 --surge-s 900`. Pass: p99 inside the budget through
   the surge, `at risk` in single digits, `gaps` and `repeats` 0.
2. **The mirror restarts:** stop `site_mirror.py` for ten seconds during the surge and start it again. Pass: it
   resumes at nj-2's offsets with `offset drift 0`; the replica counts the held events apart, no gap, no repeat.
3. **nj-1 goes dark:** stop the sportsbook (then `docker stop iggy-nj-1` if you like) and run
   `python failover_check.py`. Pass: every event at nj-2, byte-identical at the same offsets, every balance matching.
4. **A replica restarts during the surge:** stop a `standby.py` and start it again. Pass: every balance back from
   nj-2's log within a second, then PASS from the next line.

## Shadow your own events

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <your-cluster> --topic '<wallet events topic>' --state NJ \
  -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN -X sasl.username=... -X sasl.password=...
python site_mirror.py &
python standby.py --name shadow
```

The bridge is one more consumer group, `iggy-shadow`, so producers and existing consumers are untouched. Map your
records in `to_event()`: the kit needs the account, what happened, the amount, the resulting balance and open bets,
and the state; events from other states are skipped. Compare the shadow replica's balances with your system of
record. The latency it prints starts at the bridge, not at the bet slip.

## What Apache Iggy 0.9 does not do yet

No built-in mirroring between clusters: the mirror here is client code on the stable SDK, asynchronous, so a
failover can lose up to the lane's p99 (the kit measures it). Clustering with Viewstamped Replication is new
(September 2026); a cluster spanning two in-state sites would make the second synchronous, and the evaluation
tests it rather than this kit. No Kafka wire protocol (a gateway is in progress), so the bridge uses the Iggy SDK.
Delivery is at-least-once, which is why every event carries a per-account sequence number and the resulting state.
The lane is not the ledger: the wallet database stays the system of record.

On Outposts and Local Zones, Apache Iggy runs self-hosted as a single binary, and LaserData supports it. LaserData
Cloud runs as a managed service on AWS or GCP, or inside your own AWS or GCP account (BYOC). On Azure, run Apache
Iggy self-hosted on your own VMs; LaserData supports it.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one state's mirrored wallet events, beside Kafka. No
advantage, no migration.
