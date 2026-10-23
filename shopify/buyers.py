"""Buyers who are chatting with a store in Shopify Inbox: they add to and remove from their cart, start checkout
and complete orders while the conversation is open, and the conversation gets its next reply.

Plays the storefront and the chat during a sale. Each buyer signal is sent, keyed by buyer, to:
  * buyer-signals: the lane. The event (cart_add, cart_remove, checkout_started, order_completed), the
    product, and the buyer's resulting state: cart status, item quantity, subtotal and, once placed, the
    order number. A sequence that only grows for each buyer lets the conversation drop older or repeated
    events, so a late cart event never reopens a cart after the order was placed.
  * inbox-replies: a short time after a signal, the next reply in that buyer's conversation, from the Inbox
    agent or the merchant's staff. It carries the sequence of the buyer's last signal, which the
    conversation must already show, and the state it should show (status and subtotal).

Buyers place an order now and then (--order-share of carts that reach checkout); a completed order clears
the cart, as the conversation shows it. --gap-min/--gap-max set the time from a signal to the next reply;
--surge multiplies the rate for --surge-for seconds (the opening minutes of Black Friday).
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (CART, CHECKOUT, ORDERED, PARTITIONS, PRODUCTS, REPLIES, SIGNALS, STREAM, DEFAULT_URL,
                    buyer_id, ensure_topics, msg, part)


async def main(a):
    async def own_client():                 # one connection per producer, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), SIGNALS, REPLIES)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    sig_p = await (await own_client()).producer(STREAM, SIGNALS, topic_partitions_count=PARTITIONS[SIGNALS], mode=cfg)
    rep_p = await (await own_client()).producer(STREAM, REPLIES, topic_partitions_count=PARTITIONS[REPLIES], mode=cfg)
    # Signal sequence: one counter for all buyers, started from the clock, so each buyer's sequence only grows,
    # even across restarts of this script, without keeping a counter per buyer.
    next_seq = iter(range(time.time_ns(), 1 << 63))
    next_order = iter(range(1001 + random.randrange(10_000) * 100, 1 << 40))
    buyers = {}                             # buyer -> [status, {product: qty}, order number]
    pending = []                            # (when, buyer, needs, status, subtotal, moved): the next reply
    stats = defaultdict(int)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def signal(buyer, now_ns):
        b = buyers.get(buyer)
        if b is None or b[0] == ORDERED and random.random() < 0.5:
            b = buyers[buyer] = [CART, {}, ""]          # a new cart (a buyer back after an order starts over)
        status, cart, order = b
        before = (status, sum(PRODUCTS[i][1] * q for i, q in cart.items()))
        r = random.random()
        product = ""
        if status == CHECKOUT and r < a.order_share:
            kind, b[0], b[2] = "order_completed", ORDERED, f"#{next(next_order)}"
            stats["orders"] += 1
            subtotal = before[1]
            b[1] = cart = {}                            # the order clears the cart
        elif cart and status == CART and r < 0.18:
            kind, b[0] = "checkout_started", CHECKOUT
        elif cart and r < 0.45:
            i = random.choice(list(cart))
            kind, product = "cart_remove", PRODUCTS[i][0]
            cart[i] -= 1
            if not cart[i]:
                del cart[i]
            b[0] = CART
        else:
            i = random.randrange(len(PRODUCTS))
            kind, product = "cart_add", PRODUCTS[i][0]
            cart[i] = cart.get(i, 0) + 1
            b[0], b[2] = CART, ""
        if b[0] != ORDERED:
            subtotal = sum(PRODUCTS[i][1] * q for i, q in cart.items())
        items = sum(cart.values())
        seq = next(next_seq)
        stats[kind] += 1
        if random.random() < a.reply_share:
            heapq.heappush(pending, (time.monotonic() + random.uniform(a.gap_min, a.gap_max), buyer, seq, b[0],
                                     subtotal, before != (b[0], subtotal)))
        return SendMessage(msg(buyer=buyer, seq=seq, kind=kind, product=product, status=b[0], items=items,
                               subtotal=subtotal, order=b[2], ns=now_ns))

    def reply(buyer, needs, status, subtotal, moved, now_ns):
        by = "agent" if random.random() < a.agent_share else "staff"
        stats[by] += 1
        return SendMessage(msg(buyer=buyer, needs=needs, status=status, subtotal=subtotal, moved=moved, by=by,
                               ns=now_ns))

    async def send(producer, items, parts):
        # One batch per partition, all sent at once: per-buyer order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for buyer, m in items:
            groups[part(buyer, parts)].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    async def run_loop():
        t0 = due = time.monotonic()
        owed = 0.0
        async with sig_p, rep_p:
            while True:
                now_s = time.monotonic()
                t = now_s - t0
                x = a.surge if a.surge_at <= t < a.surge_at + a.surge_for else 1
                owed += a.signals_per_s * x / 100
                now = time.time_ns()
                sigs = []
                for _ in range(int(owed)):
                    u = buyer_id(random.randrange(a.buyers))
                    sigs.append((u, signal(u, now)))
                owed -= int(owed)
                reps = []
                while pending and pending[0][0] <= now_s:
                    _, u, needs, status, subtotal, moved = heapq.heappop(pending)
                    reps.append((u, reply(u, needs, status, subtotal, moved, time.time_ns())))
                await asyncio.gather(send(sig_p, sigs, PARTITIONS[SIGNALS]), send(rep_p, reps, PARTITIONS[REPLIES]))
                stats["surge"] = x > 1
                due += 0.01                 # from the due time, so a slow tick doesn't drift the schedule
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        kinds = ["cart_add", "cart_remove", "checkout_started", "order_completed"]
        while True:
            await asyncio.sleep(1)
            gc.freeze()                     # long-lived buyers out of the collector's reach: no full-heap pauses
            phase = "surge" if stats["surge"] else "steady"
            n = sum(stats[k] for k in kinds)
            print(f"[buyers] {n:>7,} buyer signals/s ({phase})  add {stats['cart_add']:,}  remove {stats['cart_remove']:,}  "
                  f"checkout {stats['checkout_started']:,}  orders {stats['order_completed']:,}  "
                  f"replies {stats['agent'] + stats['staff']:,} (agent {stats['agent']:,}, staff {stats['staff']:,})  "
                  f"buyers chatting {len(buyers):,}", flush=True)
            for k in kinds + ["agent", "staff", "orders"]:
                stats[k] = 0

    await asyncio.gather(run_loop(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--buyers", type=int, default=300_000, help="buyers with an open conversation (illustrative)")
    p.add_argument("--signals-per-s", type=float, default=10_000, help="cart and checkout events a second for those buyers")
    p.add_argument("--order-share", type=float, default=0.5, help="chance a buyer in checkout completes the order next")
    p.add_argument("--reply-share", type=float, default=0.4, help="share of signals followed by a reply in the conversation")
    p.add_argument("--agent-share", type=float, default=0.7, help="share of those replies written by the Inbox agent")
    p.add_argument("--gap-min", type=float, default=0.3, help="seconds from a signal to the next reply, at least")
    p.add_argument("--gap-max", type=float, default=3.0, help="seconds from a signal to the next reply, at most")
    p.add_argument("--surge", type=float, default=2.0, help="rate multiplier as Black Friday opens")
    p.add_argument("--surge-at", type=float, default=0, help="seconds after start")
    p.add_argument("--surge-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
