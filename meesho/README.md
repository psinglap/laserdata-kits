# Real-time order notifications for Meesho, on LaserData

Every order update your services push to Meecom reaches the sender in milliseconds, even while a
170-million-recipient sale campaign goes out. It runs beside Confluent, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/FV1CdVVi38Acfdv86HkWeA
- **One slide:** [`LaserData_Meesho_OrderUpdates.pdf`](LaserData_Meesho_OrderUpdates.pdf)

## Why this use case

From Meesho's own Tech blog post on Meecom (Jun 2025): over 10 billion messages a day across push,
WhatsApp (200M), SMS (50M) and email (40M), for 100+ campaign creators and 20+ microservices.
Meecom offers "API and Kafka-based real-time push support", "Per-client rate limits and failure
isolation", and "Prioritization of critical communications over promotional ones". It fetches and
personalises "170 million recipients in under 10 minutes".

On sale day those two kinds of traffic peak together: the Mega Blockbuster Sale passed 75,000 orders a
minute 15 minutes after launch (2026), and the sale campaigns go out at the same time. This kit puts
the order updates on their own lane, keyed by order ID, so a campaign queues behind its own rate
limit and never in front of a "Confirmed" or "Out for delivery" push. And because Meesho chose
Confluent's usage pricing to stop provisioning Kafka for sale peaks, the kit prints the broker's CPU
next to the lane's p99, and the evaluation reports cost per billion messages.

## Run it

Needs Docker and Python 3.10+. Defaults: 75,000 new orders a minute, five states each (placed,
confirmed, shipped, out for delivery, delivered; compressed to a few seconds apart), so about 6,000
order updates a second, and a 170-million-recipient campaign queued in 10 minutes.

```
docker compose up -d                            # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                 # apache-iggy 0.9.0
python meecom_sender.py --only orders &         # Meecom's order-update sender
python meecom_sender.py --only campaign &       # Meecom's campaign sender, rate-limited
python order_services.py &                      # your order services, and the truth
python campaign.py --at 30                      # the sale-day campaign, 30 s in
```

Each prints a line a second:

```
[meecom] 6,000 order updates/s  change-to-sender p50 …ms  p99 …ms  p99.9 …ms  PASS (<200ms)  campaign topic +…/s
[broker] CPU …% of one core  … messages written/s on all topics
[meecom-campaign] … campaign sends/s (limit 200,000/s)  waited in the queue p50 … s
[orders] 1,250 new orders/s  6,000 order updates/s  receipts checked …  sent within 1 s …%  out of date when sent …%
```

`out of date when sent` is the business number: the share of order notifications Meecom sent after
the order had already moved to its next state, checked by `order_services.py` against the truth from
Meecom's "sent" receipts. A slow or congested lane shows up there first.

One Python `campaign.py` process cannot queue 283,000 sends a second; it prints the rate it reached.
Run several with `--share N` to split the campaign.

**What the numbers mean.** The kit times each order update from the moment `order_services.py`
creates it to the moment Meecom's sender has it, through Python clients, on whatever machine you run
it on. On a shared 2-vCPU GitHub runner, with the broker and four Python processes on one box, six CI
runs at 1,000 order updates a second (12,000 orders a minute) held p50 12 to 18 ms, p99 24 to 41 ms and
p99.9 26 to 48 ms over 62 steady-state seconds each, with at most one second over the 200 ms budget.
While a 100,000-recipient campaign was queued next door at 5,000 sends a second, the median per-second
p99 was 24 to 56 ms (23 to 33 ms without it); the campaign drained at its 2,500/s limit and waited up
to 25 s in its own queue. Every order update was sent within 1 s, and none was out of date when sent.
That is the end-to-end lane through this demo code, not the broker alone. The broker's own tail latency
and cost per billion messages are measured with `iggy-bench` on dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `order_services.py` | Orders moving through five states, each change pushed to `order-updates` keyed by order ID, with a per-order sequence and the resulting state. Checks Meecom's send receipts against the truth. |
| `meecom_sender.py` | Meecom's delivery: order updates sent as they arrive, deduplicated by sequence, with a receipt each; campaign sends drained at the per-client rate limit. `--why` shows where the time goes. |
| `campaign.py` | A sale-day campaign: personalised sends on `campaign-sends`, with Meecom's published channel mix. |
| `confluent_bridge.py` | Copies the Confluent topic your services push to into Iggy, as a read-only consumer group, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you, cost per billion messages included; rules fixed before results. |

## Three tests

1. **Sale opens:** `python order_services.py --sale 5 --sale-at 60 --sale-for 120` while the campaign runs.
   Pass: change-to-sender p99 under 200 ms, and `out of date when sent` under 0.1%.
2. **The campaign at full rate:** `python campaign.py --share 4` in four terminals.
   Pass: the order lane's p99 moves by less than 10%, and the campaign waits only on its own rate limit.
3. **Restart the sender:** stop `meecom_sender.py --only orders` and start it again. Its consumer group
   resumes from the last committed offset, and the per-order sequence drops anything already sent.
   Pass: no duplicate notifications, and it is back under budget within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python confluent_bridge.py --bootstrap <bootstrap-server> --kafka-topic <your-order-notification-topic> \
  -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN -X sasl.username=<api-key> -X sasl.password=<api-secret>
```

The bridge only reads; your services, Meecom and every existing consumer are untouched. Map your record
shape in `to_msg()`: the kit needs `k` (order ID), `state` and `ch` (channel).

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored order-update topic, alongside
Confluent, priced per billion messages. No advantage, no migration.

kranti@laserdata.com · laserdata.com
