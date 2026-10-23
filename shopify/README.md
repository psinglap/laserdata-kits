# Real-time buyer signals for Shopify Inbox, on LaserData

Every cart change and completed order reaches the merchant's conversation in milliseconds, so the next
reply, from staff or the Inbox agent, sees what the buyer just did. It runs beside your Kafka, so nothing
else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **One slide:** [`LaserData_Shopify_BuyerSignals.pdf`](LaserData_Shopify_BuyerSignals.pdf)

## Why this use case

From Shopify's own sources:

- Shopify's Messaging team built a real-time buyer signal pipeline for Shopify Inbox: cart actions and
  order completions shown to the merchant as events inside the conversation. "There are two main
  requirements our system needs to fulfill: low latency and high reliability", and "the information we
  share with merchants should be up-to-date and always relevant to a conversation". In an A/B test,
  merchants who saw the signals had a response rate 2 percentage points higher and a conversion rate 0.7
  percentage points higher (Shopify Engineering, Dec 2021).
- That pipeline reads Kafka through Monorail and CDC (Debezium on MySQL binlogs), runs Apache Beam on
  Google Cloud Dataflow, and writes its output to a Monorail topic. The cart and checkout sources carry
  "tens of thousands of events per second" even off-peak. Because cart and checkout come from different
  sources, events "can't be guaranteed" to arrive in order, so the pipeline compares timestamps with its
  state to discard outdated information (same post).
- Inbox still shows these signals today: "If a customer adds or removes a product from their cart, then
  the event is displayed in the conversation", and the cart subtotal and item quantities appear in the
  conversation header (Shopify Help Center, Oct 2026). Inbox now also has an AI agent that "responds to
  customers automatically" (same page).
- Kafka is Shopify's backbone: it peaked at 66 million messages a second over BFCM 2024 (Tobi Lütke, as
  reported by Nasdaq/TheFly, Nov 2024). BFCM 2025 readiness tests, scaled up to 150% of the prior year's
  peak, "exposed bottlenecks—Kafka, memory, timeouts" that had to be fixed (Shopify Engineering, Nov 2025).

Your Kafka, Monorail, Dataflow and Flink are the right home for this volume, and this kit does not touch
them. The signal behind the next reply is a narrower job with a tighter clock: a buyer asks about a
discount, then places the order, and the Inbox agent answers a second later. If the order is still in a
processing window, the reply sees an open cart and may offer a discount code to a buyer who has already
paid. This kit runs buyer signals on their own lane, keyed by buyer, so a buyer's cart and checkout events
apply in one order, read by the conversation view directly, and counts the outcome: replies written
without the buyer's last signal.

Shopify does not publish the Inbox pipeline's end-to-end latency, its current architecture (the post is
from 2021), or the gap from a buyer's action to the next reply. The lane is a proposal; the kit's flags
let you set those to yours.

## Run it

Needs Docker and Python 3.10+. Defaults: 300,000 buyers with an open conversation, 10,000 cart and
checkout signals a second (cart adds and removes, checkout started, order completed), 40% of them
followed by a reply 0.3 to 3 s later (70% from the Inbox agent), and a 2× Black Friday opening surge for
the first minute. These are illustrative; set them to yours.

```
docker compose up -d                   # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt        # apache-iggy 0.9.0
python inbox.py &                      # the conversation view, checking every reply
python buyers.py &                     # buyers chatting, carting and checking out
python storefront_events.py --at 20    # a heavy storefront analytics stream next door
```

Each prints a line a second:

```
[buyers]  20,000 buyer signals/s (surge)  add …  remove …  checkout …  orders …  replies … (agent …, staff …)  buyers chatting …
[inbox]  20,000 signals/s  signal-to-conversation p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  conversations …
[replies] …  written without the last signal …%  on a wrong cart …  order placed but not shown …
```

`written without the last signal` is the business number. Each signal carries the buyer's resulting
state (status, item quantity, subtotal and, once placed, the order number) and a sequence that only grows
per buyer, so the conversation drops older or repeated signals and a late cart event never reopens a cart
after the order. Each reply carries the sequence of the buyer's last signal and the state the conversation
should show after it. `inbox.py` asks whether it showed that signal when the reply started. If not, the
reply was written from a stale view; if that view showed another status or subtotal, it was `on a wrong
cart`; and if the buyer had already placed the order, it counts as `order placed but not shown`.

What-if: `python inbox.py --hold-ms 1000` applies every signal only once it is 1 s old, as a one-second
processing window would, so you can see how many replies such a path writes from a stale view.

**What the numbers mean.** The kit times each signal from the moment `buyers.py` sends it to the moment
`inbox.py` has applied it to the conversation, through Python clients, on whatever machine you run it on.
On a shared GitHub runner (2 vCPU), with the broker and four Python processes on one box, at 1,500 buyer
signals a second over 20,000 chatting buyers (2x for 15 s as the surge), three green CI runs held p50 5.2 to
7.7 ms, p99 8.8 to 14.1 ms and p99.9 9.9 to 15.5 ms (median per second over 62 steady-state seconds each, no
second over the 50 ms budget). Through the surge (3,000 signals a second) the median per-second p99 was 10.2
to 17.8 ms, worst 11.3 to 20.0 ms, with a 1 MB/s storefront stream next door. Of about 45,600 to 45,900
replies per run, around 6,000 orders completed while chatting, no reply was written without the buyer's last
signal, none on a wrong cart and no placed order went unseen. The what-if with every signal applied 1 s late
wrote 25.8 to 25.9% of replies (median per second) from a stale view: about 5,000 replies per run on a wrong
cart, 181 to 201 of them to a buyer who had already placed the order. That is the end-to-end lane through
this demo code at reduced scale, not the broker alone; the broker's own tail latency is measured with
`iggy-bench` on dedicated machines (`bench/PLAN.md`).

| File | What it is |
|---|---|
| `buyers.py` | Buyers chatting with a store: cart adds and removes, checkout started and order completed on `buyer-signals` (resulting state, sequence only grows per buyer), and the next reply on `inbox-replies`. `--gap-min/--gap-max`, `--order-share`, `--agent-share` and `--surge` shape the load. |
| `inbox.py` | The conversation view: applies signals by sequence, times signal-to-conversation, and checks every reply against the buyer's last signal. `--hold-ms` is the what-if. |
| `common.py` | Topics, partitioning by buyer, the illustrative catalogue and the states the conversation shows. |
| `storefront_events.py` | A bulk, latency-tolerant storefront analytics stream (page and product views) on its own topic on the same broker. |
| `kafka_bridge.py` | Reads your cart, checkout and order events from Kafka as consumer group `iggy-shadow`, read-only, and publishes them to the lane, keyed by buyer, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **Black Friday opens:** `python buyers.py --signals-per-s 10000 --surge 2 --surge-for 60`.
   Pass: signal-to-conversation p99 under 50 ms through the surge, and no reply written without the last signal.
2. **Storefront analytics on the same broker:** `python storefront_events.py --mbps 20 --at 20`.
   Pass: the lane's p99 moves by less than 10% while it runs.
3. **Restart the conversation view:** stop `inbox.py` and start it again. It rebuilds every buyer's
   conversation from the lane, prints "catching up", then times live signals again.
   Pass: no signal applied twice, back under budget within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <your-brokers> --kafka-topic <cart-topic> --kafka-topic <checkout-topic> --kafka-topic <order-topic>
```

The bridge only reads, as its own consumer group (`iggy-shadow`); Monorail, every producer, every existing
consumer and the Dataflow and Flink jobs are untouched. Map your fields in `to_signal()`: Shopify's internal
event schemas are not public, so the defaults read the public webhook payloads for the same events
(`carts/update`, `checkouts/create`, `orders/create`), keyed by a buyer field if present, else by the cart
token. The bridge takes each signal's sequence from the event's own time, so an event that arrives after a
newer one for the same buyer is dropped, and infers adds and removes by comparing quantities with the
buyer's previous cart, as the Inbox pipeline does. In shadow mode the clock starts when the bridge reads
the record. Everything runs in your Google Cloud project.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored buyer-signals stream, beside your
Kafka. No advantage, no migration.

kranti@laserdata.com · laserdata.com
