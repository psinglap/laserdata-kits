# Real-time card testing controls for Stripe, on LaserData

Every card attempt reaches the card testing detector, and every intervention reaches every payments API node,
in milliseconds, before the tester's next attempt. It runs beside Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.stripe-kit
- **One slide:** [`LaserData_Stripe_CardTestingControls.pdf`](LaserData_Stripe_CardTestingControls.pdf)

## Why this use case

From Stripe's docs ("Protect yourself from card testing"): on Payment Element and Checkout, Stripe runs "rate
limiters, AI models, CAPTCHA triggers", and "when we detect that you're under a card testing attack, we
dynamically choose interventions to suppress the attack as much as possible, while still allowing legitimate
users to transact". Testers "use scripts to test a large amount of card information at once", and the declines
can hurt a business with issuers "even after card testing ceases".

From "The ML flywheel" (Ryan Drapeau and Inayat Khosla, Dec 2024): a classifier determines "whether an entity is
undergoing an attack in real time"; Stripe intervenes on "a small, precise slice of traffic" and eases controls
once the attack subsides; a spike can be card testing or a flash sale. Successful attacks are down 80% in two years.

From the Replay 2023 talk on Stripe's Kafka control plane: about 700 TB a day of Kafka publish throughput on 50
clusters across three environments.

Card testing defence is a loop with two hops: each attempt has to reach the detector that judges the business, and
the intervention has to reach every payments API node before the script's next attempt. Whatever the loop takes,
attempts keep reaching issuers. This kit runs that one loop on Apache Iggy, prints both hops' latency every second,
and prints the tester attempts that reached an issuer after the business's controls were published, and the stolen
cards they confirmed, next to what a lane one second behind would have let through.

## Run it

Needs Docker and Python 3.10+. Defaults: 12 payments API nodes, 100,000 businesses, 8,000 real buyers' attempts a
second ($5–$300, about 6% declined), a card testing attack every 3 s (150 small or zero-amount attempts a second on
fresh cards for 8 s, about 15% approved by issuers) and a flash sale every 5 s (200 real buyers a second for 4 s).
All rates and shares are illustrative; set yours.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python detector.py &                 # the card testing detector: attempts in, controls out
python api_nodes.py                  # 12 payments API nodes: attempts out, controls in
```

They print a line a second:

```
[detector]  8,000 attempts/s  attempt-to-detector p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
            changes sent … (controls …, eased …)  businesses under controls …
[nodes]  8,000 attempts/s (steady)  tester attempts …  blocked …  buyers challenged …  attacks …  controls …
         (attack detected in …ms median)  eased …  reached issuers after controls … (… cards confirmed;
         at 1000 ms it would be …, … cards)  flash sales …, flagged 0  false controls 0
         control-to-nodes n … p50 …ms p99 …ms
```

**What the numbers mean.** The kit times each attempt record from the moment `api_nodes.py` takes it to the
moment `detector.py` has applied it, and each control from `detector.py` to every node, through Python clients,
on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all scripts on one box, at
reduced scale (2,500 buyer attempts/s plus attacks and flash sales, about 3,050 attempts/s in all, 20,000
businesses, 4 nodes), three green runs held attempt-to-detector p50 7.4–7.5 ms, p99 13.8–13.9 ms and p99.9
15.2–15.4 ms (every steady-state second inside 50 ms, 60–61 per run), and control-to-nodes p50 3.2–3.8 ms and p99
3.7–4.3 ms. With 2 MB/s of API request logs on the same broker, the lane's p99 moved by less than 0.5 ms. Each run
put 20–21 attacks under controls and eased 19–20, and none of its 12 flash sales was flagged. 9–15 tester attempts
reached an issuer after their business's controls were published (0–3 stolen cards confirmed); a lane one second
behind would have let through 3,000–3,002 (445–452 cards), so the lane stopped 99.5–99.7% of them. The kit's
stand-in detector needs about 165 ms (164–169 ms) of attack to call it; that part is your models' and is not what
the lane measures. All of this is the end-to-end loop through this demo code, not the broker alone. The broker's
own tail latency is measured with `iggy-bench` in `bench/PLAN.md`, on dedicated machines. CI runs:
https://github.com/psinglap/laserdata-kits/actions/runs/39884801326,
https://github.com/psinglap/laserdata-kits/actions/runs/39885021157,
https://github.com/psinglap/laserdata-kits/actions/runs/39885186814

| File | What it is |
|---|---|
| `api_nodes.py` | A fleet of payments API nodes. Every attempt publishes one record (node, business, surface, amount, card fingerprint, outcome, sequence) to `card-attempts`, keyed by business. Each node reads `attack-controls` directly on its own connection and challenges a business's attempts the moment its controls arrive. It also plays the outside world: real buyers, card testing scripts and flash sales. |
| `detector.py` | The card testing detector. Reads the 16 attempt partitions directly, judges each business over a 1 s window (`under_attack()` is the stand-in for your models), publishes CONTROLS with the business's sequence, and EASED after 2 s without tester-shaped attempts. |
| `log_flood.py` | A heavy API request-log stream on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your attempt-outcome records from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Black Friday:** `python api_nodes.py --peak 3 --peak-at 60 --peak-for 120`.
   Pass: attempt-to-detector p99 under 50 ms, tester attempts reaching issuers after controls under 1% of what a
   lane one second behind would let through, and no flash sale flagged.
2. **Request logs next door:** `python log_flood.py --mbps 20` while both run.
   Pass: the attempt lane's p99 moves by less than 10%.
3. **Restart the detector:** stop `detector.py` and start it again; it replays the attempt topic and rebuilds its
   windows and the businesses under controls, publishing again only changes younger than `--replay-controls-s`.
   Restart `api_nodes.py` and every node rebuilds its view from `attack-controls`.
   Pass: it catches up within seconds, running attacks stay under controls, and finished ones are eased.

## Design notes

- A control carries the business's resulting state ("controls" or "eased") with a sequence, so at-least-once
  delivery is safe: a repeated or replayed control changes nothing. Apache Iggy 0.9 does not offer exactly-once,
  and this lane doesn't need it.
- The lane carries card fingerprints, never card numbers. Radar, Shepherd and your models stay the source of every
  decision; the log is how every node hears about it, and a durable, replayable record of each intervention and
  its evidence.
- Other interventions (rate limits, a lower block threshold, 3DS) fit the same topic: publish them on
  `attack-controls` with an `action` and the next sequence.
- There is no official Ruby SDK for Apache Iggy; a Ruby API node would publish through a small sidecar or the HTTP API.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <kafka:9092> --kafka-topic <attempt-outcomes-topic> \
  --business-field <account-id-field> --card-field <fingerprint-field> --outcome-field <outcome-field> \
  --amount-divisor 100
```

The bridge only reads, as its own consumer group `iggy-shadow`; the API, your Flink jobs, Shepherd and every
other consumer are untouched. Map your record in `to_attempt()`: the kit needs the business ID, amount, card
fingerprint and outcome, and, if you have them, the host and surface.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored attempt-outcomes topic, alongside Kafka.
No advantage, no migration.

kranti@laserdata.com · laserdata.com
