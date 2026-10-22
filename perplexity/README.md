# Real-time Computer credits for Perplexity, on LaserData

Every agent action's credits reach the credit ledger, and every pause and resume reaches every agent runner,
in milliseconds, even at peak. It runs beside your billing platform and Stripe, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source
engine LaserData builds and runs.

- **One slide:** [`LaserData_Perplexity_ComputerCredits.pdf`](LaserData_Perplexity_ComputerCredits.pdf)

## Why this use case

From the Monetization job post (Member of Technical Staff, Software Engineer, Monetization, Apr 2026):
"every action an agent takes is metered, budgeted, and settled through the billing platform this role owns",
and the usage-based billing system is "foundational to Perplexity Computer and our agentic products: metering
every agent action in real time, enforcing credit budgets, and settling costs". Stack: Python, Go, Stripe,
PostgreSQL, AWS.

From the help center ("How credits work on Perplexity", read Oct 2026): 100 credits = $1; bonus credits are
used first, then monthly, then purchased; when credits run out, active tasks pause, work is saved, and they
resume once credits are added or monthly credits reset; auto-refill at a low balance; Max accounts have a
monthly spending cap.

From the Data Platform job post (Jun 2026): "event-driven and streaming systems (Kafka, Kinesis, PubSub, or
similar) for real-time ingestion, transformation, and delivery".

Settlement can take its time and must be exact; that stays on the billing platform. Enforcement is a loop
with two hops: an action's credits have to reach the ledger that owns the account's balance, and the ledger's
pause has to reach every runner before the account's sub-agents start another action. Whatever the loop
takes, the fleet keeps running agent work the account no longer covers. This kit runs that one loop on
Apache Iggy, prints both hops' latency every second, and prints the credits spent past zero.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 agent runners, 20,000 accounts, 10,000 agent actions a second at
2–40 credits each, Pro, Max and Enterprise plan shapes, and 60-second months (a compressed month, so accounts
run out, refill and reset while you watch).

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python credit_ledger.py &            # the credit ledger, fed by the lane
python agent_runners.py              # 8 agent runners: credits out, pauses in
```

They print a line a second:

```
[ledger]   10,000 action records/s  action-to-ledger p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  decisions sent …
[credits] month …: spent … (bonus … plan … purchased …)  refills …  pauses …  resumes …
          paused now … (pro … max … enterprise …)  past zero … credits $… (…% of credits)
[runners]  10,000 agent actions/s (steady)  credits …/s  held while paused …  paused in runner-00's view …
           decision-to-runners n … p50 …ms p99 …ms
```

**What the numbers mean.** The kit times each action record from the moment `agent_runners.py` creates it to
the moment `credit_ledger.py` has applied it, and each decision from the ledger to every runner, through
Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all
scripts on one box, at reduced scale (3,000 agent actions/s, 2,000 accounts, 4 runners, 20 s months), three
green runs held action-to-ledger p50 3.2–6.8 ms, p99 6.1–12.6 ms and p99.9 7.5–14.0 ms (every steady-state
second inside 50 ms, 61–62 per run), and decision-to-runners p50 2.7–2.8 ms and p99 4.6–5.3 ms. The ledger
reads the 16 agent-actions partitions directly, on two connections. With 2 MB/s of agent trajectories on the
same broker, the lane's p99 did not rise (median per second 7.0 → 6.1, 12.8 → 12.8, 7.8 → 7.7 ms). Each run
paused accounts 1,232–1,299 times, ran 3,182–3,358 auto-refills and resumed 1,055–1,109 times, and credits
spent past zero were 0.012–0.019% of credits spent (438–745 credits, $4.38–$7.45 per run). That is the
end-to-end loop through this demo code, not the broker alone. The broker's own tail latency is measured with
`iggy-bench` in `bench/PLAN.md`, on dedicated machines. CI runs:
https://github.com/psinglap/laserdata-kits/actions/runs/39871619658,
https://github.com/psinglap/laserdata-kits/actions/runs/39871897978,
https://github.com/psinglap/laserdata-kits/actions/runs/39872028516

| File | What it is |
|---|---|
| `agent_runners.py` | A fleet of Computer agent runners. Every agent action publishes the runner's running credits for that account (the resulting total, with a sequence) to `agent-actions`, keyed by account. Busy accounts run more agents than quiet ones. Each runner reads `credit-decisions` on its own connection and holds that account's tasks while paused. |
| `credit_ledger.py` | The credit ledger. Sums the latest total from every runner per account, spends bonus, then monthly, then purchased credits, runs auto-refill under the monthly cap, pauses at zero, resumes on refill or monthly reset, and publishes each decision. Prints the lane's p99 and credits past zero. |
| `common.py` | Topics, keys and the illustrative plan shapes (`plan()`): Pro with a one-time bonus and optional auto-refill, Max with monthly credits, auto-refill and a spending cap, Enterprise with monthly credits. Set yours. |
| `trace_flood.py` | A heavy agent-trajectory stream (task traces for evals) on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies your agent-usage topic from Kafka into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A launch day:** `python agent_runners.py --peak 5 --peak-at 60 --peak-for 120`.
   Pass: action-to-ledger p99 under 50 ms, and credits past zero under 0.1% of credits spent.
2. **Agent trajectories next door:** `python trace_flood.py --mbps 20` while both run.
   Pass: the credit lane's p99 moves by less than 10%.
3. **Restart the ledger:** stop `credit_ledger.py` and start it again; it replays the agent-actions topic and
   rebuilds every balance from the running totals, without double counting.
   Pass: it catches up within seconds and paused accounts stay paused until credits come back.

## Design notes

- Running totals with a sequence make at-least-once delivery idempotent: a repeated or replayed record
  changes nothing. Apache Iggy 0.9 does not offer exactly-once, and this lane doesn't need it: it only
  decides when tasks pause and resume. Settlement and invoices stay on your billing platform and Stripe.
- Credits past zero count only actions that started after the balance had reached zero. The action that
  spends the last credits is let finish; its remainder is written off and not counted.
- Months here are compressed and aligned to the clock. In production, key the month by each account's
  billing cycle. Refills here clear after `--charge-ms`; in production they come from the billing platform
  when the charge succeeds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <kafka:9092> --topic <agent-usage-topic> \
  --account-field <account-id-field> --runner-field <runner-field> --credits-field <credits-field>
```

The bridge only reads, as its own consumer group `iggy-shadow`; the services that emit usage, the billing
platform, PostgreSQL and Stripe are untouched. Map your record in `to_action()`: the kit needs the account,
the runner and the credits charged (or `--usd-field` for a USD cost). If your usage events travel on Kinesis
or Pub/Sub, swap the consumer for that client; the rest stays the same.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored agent-usage topic, beside your billing
platform. No advantage, no migration.

kranti@laserdata.com · laserdata.com
