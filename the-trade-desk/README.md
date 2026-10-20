# Real-time spend and pacing for The Trade Desk, on LaserData

Every win's spend reaches the pacing controllers, and every pause reaches every bid server, in
milliseconds, even in a live-sports spike. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/5gXaza3Wyw1BSnMDnpKU1F
- **One slide:** [`LaserData_TheTradeDesk_SpendPacing.pdf`](LaserData_TheTradeDesk_SpendPacing.pdf)

## Why this use case

From The Trade Desk's own job posts: the pacing system "decides in real time how to spend it against
20M queries per second", on "every single one" of them, through "the controllers that decide how budget
is spent over time" (Staff Product Manager, Budget and Pacing, Oct 2026). The pacing engineering team
lists ".Net, gRPC, REST, Docker, Kafka, and SQL Server" (Senior Software Engineer, Pacing, 2025). And
The Trade Desk's live-sports pacing spends "faster and more during short campaign flights", "aiming to
mitigate the risk of overspending" (Live sports pacing, 2023).

Pacing is a loop with two hops: a win's spend has to reach the controllers, and the controller's pause
has to reach every bid server before its next bid. Whatever the loop takes, the fleet keeps winning on an
ad group that is already out of budget. This kit runs that one loop on Apache Iggy, prints both hops'
latency every second, and prints the overspend in USD.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 bid servers, 5,000 ad groups, 10,000 wins a second at
$1.50–$12 CPM, and 60-second flights (a compressed campaign flight, so budgets run out while you watch).

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python pacing.py &                   # the pacing controllers, fed by the spend lane
python bidders.py                    # 8 bid servers: spend out, pacing decisions in
```

They print a line a second:

```
[pacing]   10,000 spend events/s  spend-to-pacing p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  decisions sent …
[budget] flight …: spent $…  ad groups paused …/5,000  throttled …  overspend $… (…% of spend)
[bidders]  10,000 wins/s (steady)  spend $…/s  withheld by pacing …  paused in bid-00's view …
           decision-to-bid-servers n … p50 …ms p99 …ms
```

**What the numbers mean.** The kit times each spend record from the moment `bidders.py` creates it to
the moment `pacing.py` has applied it, and each decision from `pacing.py` to every bid server, through
Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all
scripts on one box, at reduced scale (about 3,000 wins/s, 2,000 ad groups, 4 bid servers, 20 s flights),
it held spend-to-pacing p50 12 ms and p99 23 ms (62 of 62 steady-state seconds inside 50 ms), and
decision-to-bid-servers p50 3 ms and p99 5 ms. With 2 MB/s of bid feedback on the same broker, the lane's
p99 stayed at 23 ms. Up to 1,267 of 2,000 ad groups reached their budget and were paused in a flight, and
overspend was 0.04–0.05% of spend. That is the end-to-end loop through this demo code, not the broker alone. The broker's own tail latency is measured with
`iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `bidders.py` | A fleet of bid servers. Every win publishes the server's running spend for that ad group (the resulting total, with a sequence) to `spend`, keyed by ad group. Each server reads `pacing-decisions` on its own connection and stops or slows bidding when told. |
| `pacing.py` | The pacing controllers. Sums the latest total from every bid server per ad group, pauses at the flight budget, throttles ad groups running ahead of an even pace, and publishes each decision. Prints the lane's p99 and overspend. |
| `feedback_flood.py` | A heavy bid-feedback log on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your win or spend topic from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Live-sports spike:** `python bidders.py --peak 5 --peak-at 60 --peak-for 120`.
   Pass: spend-to-pacing p99 under 50 ms, and overspend under 0.1% of spend.
2. **A heavy neighbour:** `python feedback_flood.py --mbps 50` while both run.
   Pass: the spend lane's p99 moves by less than 10%.
3. **Restart pacing:** stop `pacing.py` and start it again; it replays the spend topic and rebuilds every
   ad group's spend from the running totals, without double counting.
   Pass: it catches up within seconds and pauses resume where they were.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <broker> --kafka-topic <wins-topic> \
  --group-field <ad-group-field> --server-field <host-field> --cpm-field <clearing-cpm-field> \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
```

The bridge only reads; bid servers, producers and existing consumers are untouched. Map your record in
`to_win()`: the kit needs the ad group, the bid server, and the clearing price. The bid servers are .NET;
Apache Iggy has an official C# SDK for when they write to the lane directly.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored spend topic, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
