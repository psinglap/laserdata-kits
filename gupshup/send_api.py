"""A fleet of send API nodes: every message's fees onto the wallet lane, and every wallet state applied before the next send.

Plays --nodes send API nodes in one process. Each node accepts outbound WhatsApp messages for apps it still
sees as funded and, for every message, publishes to the charges topic, keyed by app:
  * at accept: the node's running Gupshup fee and message count for that app (resulting totals, never only
    the last message's fee), with a per-(node, app) sequence;
  * at delivery, 0.3-3 s later for most messages: the node's running WhatsApp (Meta) fee for that app,
    by the message's category (marketing, utility, authentication, service).
A node that sees an app as blocked rejects its messages with 1003 (Wallet Balance Low) instead.
Each node reads wallet-state directly, on its own connection, and applies it to its own view; a node never
sees another's view, so a block or a recharge reaches every node through the log.
A payments step stands in for top-ups (Razorpay or Stripe): two apps in three recharge a few seconds after
they are blocked, as a credit on the same lane.

Prints messages accepted per second, 1003 rejections, apps blocked in one node's view, and wallet-state-to-node
p50/p99: the second hop of the loop.
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (CATEGORIES, CHARGES, DEFAULT_URL, DELIVERED, GS_FEE, PARTITIONS, STATE, STREAM, Window, app,
                    ensure_topics, load, msg, part, shares, wallet)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), CHARGES, STATE)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, CHARGES, topic_partitions_count=PARTITIONS[CHARGES], mode=cfg)
    start_ns = time.time_ns()
    nodes = [f"send-{i:02d}" for i in range(a.nodes)]
    apps = [app(i) for i in range(a.apps)]
    cw, acc = [], 0.0
    for s in shares(a.apps):
        acc += s
        cw.append(acc)
    pid = {x: part(x, PARTITIONS[CHARGES]) for x in apps}
    info = {app(i): wallet(i, a.apps, a.msgs_per_s, a.runway_s) for i in range(a.apps)}
    cats = list(CATEGORIES)
    cat_w = [CATEGORIES[c][0] for c in cats]
    view = {n: {} for n in nodes}           # node -> {app: (state, time the wallet decided it)}
    seq = defaultdict(int)                  # (node, app) -> sequence
    tot = defaultdict(lambda: [0, 0.0, 0.0, 0.0])   # (node, app) -> [messages, gs fee, wa fee, credits]
    due_dlr = []                            # (due time, node, app, Meta fee): deliveries still to come
    recharge = {}                           # app -> due time of its top-up
    lat, stats = Window(), defaultdict(float)

    def record(n, x, kind):
        seq[(n, x)] += 1
        m, gs, wa, cr = tot[(n, x)]
        return msg(n=n, a=x, seq=seq[(n, x)], k=kind, m=m, gs=round(gs, 6), wa=round(wa, 6), cr=round(cr, 4))

    async def state_reader(n):
        # Partition 0 of wallet-state, directly, from the start of the log, then from our own position.
        c = await own_client()
        who = Consumer.Single(f"{n}-{os.getpid()}")
        nxt = None
        while True:
            ms = await c.poll_messages(STREAM, STATE, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            for m in ms:
                d = load(m.payload())
                v = view[n].get(d["a"])
                if v and d["ns"] <= v[1]:
                    continue                # older or repeated state: the newest wins (also after a wallet restart)
                view[n][d["a"]] = (d["state"], d["ns"])
                if n == nodes[0]:           # payments watch send-00's view: a blocked app may top up
                    if d["state"] == "blocked" and info[d["a"]]["recharges"]:
                        recharge.setdefault(d["a"], time.monotonic() + a.recharge_s)
                    elif d["state"] != "blocked":
                        recharge.pop(d["a"], None)
                if d["ns"] > start_ns:
                    lat.add((time.time_ns() - d["ns"]) / 1e6)   # live states only, not ones replayed at start-up
            if ms:
                nxt = ms[-1].offset() + 1
            if len(ms) < 1000:
                await asyncio.sleep(0.0005)

    gc.freeze()                             # keep start-up objects out of the collector's way

    async def send_loop():
        t0 = due = time.monotonic()
        owed = 0.0
        async with producer:
            while True:
                now = time.monotonic()
                t = now - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed += a.msgs_per_s * x / 100
                batches = defaultdict(list)
                k = int(owed)
                owed -= k
                for n, ap in zip(random.choices(nodes, k=k), random.choices(apps, cum_weights=cw, k=k)):
                    v = view[n].get(ap)
                    if v and v[0] == "blocked":
                        stats["rejected"] += 1      # 1003 Wallet Balance Low
                        continue
                    tt = tot[(n, ap)]
                    tt[0] += 1
                    tt[1] += GS_FEE
                    batches[pid[ap]].append(record(n, ap, "accept"))
                    stats["accepted"] += 1
                    if random.random() < DELIVERED:
                        fee = CATEGORIES[random.choices(cats, weights=cat_w)[0]][1]
                        heapq.heappush(due_dlr, (now + random.uniform(0.3, 3.0), n, ap, fee))
                while due_dlr and due_dlr[0][0] <= now:
                    _, n, ap, fee = heapq.heappop(due_dlr)
                    tot[(n, ap)][2] += fee
                    batches[pid[ap]].append(record(n, ap, "delivered"))
                    stats["delivered"] += 1
                # Payments: an app blocked in send-00's view tops up after --recharge-s, once per block.
                for ap, at in list(recharge.items()):
                    if at is not None and at <= now:
                        tot[("pay", ap)][3] += info[ap]["opening"]
                        batches[pid[ap]].append(record("pay", ap, "recharge"))
                        recharge[ap] = None
                        stats["recharges"] += 1
                # At-least-once: now and then a record goes out twice; the wallet must charge it once.
                if a.dup:
                    for ms in batches.values():
                        ms.extend([r for r in ms if random.random() < a.dup])
                # One batch per partition, all sent at once: per-app order is kept, about one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning([SendMessage(r) for r in ms],
                                                                           Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                stats["peak"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            blocked = sum(1 for v in view[nodes[0]].values() if v[0] == "blocked")
            low = sum(1 for v in view[nodes[0]].values() if v[0] == "low")
            phase = "peak" if stats["peak"] else "steady"
            print(f"[send] {int(stats['accepted']):>6,} msgs accepted/s ({phase})  delivered {int(stats['delivered']):,}  "
                  f"rejected 1003 {int(stats['rejected']):,}  recharges {int(stats['recharges'])}  "
                  f"in {nodes[0]}'s view: blocked {blocked:,} low {low:,}  "
                  f"state-to-send-nodes n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("accepted", "delivered", "rejected", "recharges"):
                stats[k] = 0

    await asyncio.gather(send_loop(), report(), *(state_reader(n) for n in nodes))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--nodes", type=int, default=8, help="send API nodes, each with its own wallet-state reader")
    p.add_argument("--apps", type=int, default=5_000, help="must match wallet.py")
    p.add_argument("--msgs-per-s", type=float, default=4_000, help="outbound messages per second, all nodes; must match wallet.py")
    p.add_argument("--runway-s", type=float, default=30, help="median seconds an app's opening balance lasts; must match wallet.py")
    p.add_argument("--recharge-s", type=float, default=5, help="seconds after a block before a top-up arrives")
    p.add_argument("--dup", type=float, default=0.01, help="share of records sent twice (at-least-once)")
    p.add_argument("--peak", type=float, default=1, help="send-rate multiplier during the peak (a festival campaign)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
