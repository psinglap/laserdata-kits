"""The item status service: every stock-out, restock and kill-switch block of an advertised item, written
once to the lane.

Stands in for whatever turns a retailer's stock changes, incremental catalog feeds and Item Kill Switch
calls (UpdateItemStatusBulk) into item status for Moloco Commerce Media. For each item in a live campaign it
keeps a stock count and a block reason, and writes a message whenever either changes:

  * stock updates: an item sells or is topped up and stays in stock (most messages; eligibility unchanged);
  * stock-outs: an item's stock reaches 0 and it must stop being advertised (step OUT);
  * restocks: an out-of-stock item is back and may be advertised again (step BACK);
  * kill switch: every --kill-every seconds one bulk call blocks --kill-items items at once, as
    UpdateItemStatusBulk does for a recall or a fraudulent product report (step OUT for each one still
    eligible). A blocked item stays blocked; a restock does not unblock it.

Flash sale: from --sale-at for --sale-for seconds, one category (--sale-cat) sees --sale-x times its usual
stock updates and stock-outs, as when a deal drop sends shoppers to the same items at once.

One writer per partition, each with its own connection, writes every --tick-ms, so each item's changes stay
in order. On start it writes one snapshot message per item (sequence 1). After that it re-writes every item's
current status once per --snapshot-every seconds, spread over the period, with the item's current sequence
(live readers skip it as a repeat), so a Decision API server that starts, or that the GKE autoscaler adds,
only replays the last minute or so of the log to know the status of every item.
"""
import argparse
import asyncio
import gc
import random
import time

from apache_iggy import SendMessage

from common import BACK, CATEGORIES, DEFAULT_URL, NONE, OUT, PARTITIONS, STATUS, STREAM, Window, connect, \
    eligible, ensure_topic, items, msg, part


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, STATUS)
    n_parts = PARTITIONS[STATUS]
    keys = items(a.items_per_cat, a.cats)
    by_part = [[k for k in keys if part(k) == p] for p in range(n_parts)]
    hot = [[k for k in mine if k.split("|", 1)[0] == a.sale_cat] for mine in by_part]
    rng = random.Random(7)
    state = {}                                       # item -> [stock, blocked reason, seq]
    out_of_stock = [[] for _ in range(n_parts)]      # per partition: items at 0, for restocks
    for p, mine in enumerate(by_part):
        for k in mine:
            n = 0 if rng.random() < a.oos_share else rng.randint(1, a.max_stock)
            state[k] = [n, "", 0]
            if n == 0:
                out_of_stock[p].append(k)
    kills = [[] for _ in range(n_parts)]             # per partition: (item, time the bulk call was made)
    acks, t0 = Window(), time.monotonic()
    cnt = dict(sent=0, outs=0, backs=0, blocked=0, calls=0)
    snapped = [0]
    gc.freeze()                                      # keep start-up tables out of the collector's way

    def change(k, step=NONE, waited_ns=0, repeat=False):
        st = state[k]
        if not repeat:
            st[2] += 1
        return SendMessage(msg(k, st[2], st[0], st[1], step, waited_ns))

    def selling():
        el = time.monotonic() - t0
        return a.sale_for > 0 and a.sale_at <= el < a.sale_at + a.sale_for

    def update(pool, waited):
        # A sale or a top-up that leaves the item in stock; some items far busier than others.
        for _ in range(4):
            k = pool[int(len(pool) * rng.random() ** 2)]
            st = state[k]
            if st[0] > 1:
                st[0] += -rng.randint(1, min(3, st[0] - 1)) if rng.random() < 0.85 else rng.randint(5, 40)
                return change(k, waited_ns=waited)
        return None

    def stock_out(p, pool, waited):
        for _ in range(8):
            k = pool[rng.randrange(len(pool))]
            st = state[k]
            if st[0] > 0:
                was = eligible(st[0], st[1])
                st[0] = 0
                out_of_stock[p].append(k)
                cnt["outs"] += was
                return change(k, step=OUT if was else NONE, waited_ns=waited)
        return None

    def restock(p, waited):
        oos = out_of_stock[p]
        if not oos:
            return None
        i = rng.randrange(len(oos))
        k = oos[i]
        oos[i] = oos[-1]
        oos.pop()
        st = state[k]
        st[0] = rng.randint(5, a.max_stock)
        back = eligible(st[0], st[1])
        cnt["backs"] += back
        return change(k, step=BACK if back else NONE, waited_ns=waited)

    async def kill_switch():
        # One UpdateItemStatusBulk call: a batch of items blocked at once, written by each partition's writer.
        if not a.kill_every:
            return
        await started.wait()
        due = time.monotonic() + a.kill_every / 2
        while True:
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            due += a.kill_every
            now = time.time_ns()
            picked = 0
            while picked < a.kill_items:
                k = keys[rng.randrange(len(keys))]
                if not state[k][1]:
                    kills[part(k)].append((k, now))
                    picked += 1
            cnt["calls"] += 1

    async def writer(p):
        c = await connect(a.url)
        mine, mine_hot = by_part[p], hot[p]
        for i in range(0, len(mine), 5000):                          # snapshot: every item, sequence 1
            batch = []
            for k in mine[i:i + 5000]:
                batch.append(change(k))
            await c.send_messages(STREAM, STATUS, p, batch)
        snapped[0] += 1
        await started.wait()
        share, hot_share = len(mine) / len(keys), len(mine_hot) / len(keys)
        due, cu, co, cb, hu, ho, cursor, snap = time.monotonic(), 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0
        snap_per_write = len(mine) * a.tick_ms / (a.snapshot_every * 1000) if a.snapshot_every else 0
        tick = a.tick_ms / 1000
        while True:
            cu += a.rate * tick * share
            co += a.outs * tick * share
            cb += a.backs * tick * share
            if selling() and mine_hot:                               # the flash sale on one category
                hu += a.rate * tick * hot_share * (a.sale_x - 1)
                ho += a.outs * tick * hot_share * (a.sale_x - 1)
            nu, cu = int(cu), cu - int(cu)
            no, co = int(co), co - int(co)
            nb, cb = int(cb), cb - int(cb)
            hn, hu = int(hu), hu - int(hu)
            hs, ho = int(ho), ho - int(ho)
            batch = []
            snap += snap_per_write                                   # rolling snapshot: a slice of items,
            if nu or no or nb or hn or hs or snap >= 10 * snap_per_write:   # riding on writes with live changes
                for _ in range(int(snap)):
                    batch.append(change(mine[cursor], repeat=True))
                    cursor = (cursor + 1) % len(mine)
                snap -= int(snap)
            # A change happens at some point in the last --tick-ms and waits for this write; that wait counts.
            w = lambda: int(rng.random() * a.tick_ms * 1e6)
            made = [update(mine, w()) for _ in range(nu)] + [update(mine_hot, w()) for _ in range(hn)]
            made += [stock_out(p, mine, w()) for _ in range(no)] + [stock_out(p, mine_hot, w()) for _ in range(hs)]
            made += [restock(p, w()) for _ in range(nb)]
            if kills[p]:
                now = time.time_ns()
                for k, at in kills[p]:
                    st = state[k]
                    was = eligible(st[0], st[1])
                    st[1] = "kill_switch"
                    cnt["blocked"] += was
                    made.append(change(k, step=OUT if was else NONE, waited_ns=now - at))
                kills[p].clear()
            made = [m for m in made if m is not None]
            batch += made
            cnt["sent"] += len(made)
            if batch:
                s0 = time.perf_counter()
                await c.send_messages(STREAM, STATUS, p, batch)
                acks.add((time.perf_counter() - s0) * 1e3)
            due += tick                                              # from the due time, so a slow write doesn't drift
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        await started.wait()
        while True:
            await asyncio.sleep(1)
            r = acks.flush()
            tag = f"  flash sale: {a.sale_cat} x{a.sale_x:g}" if selling() else ""
            print(f"[status] {cnt['sent']:>7,} status changes/s  stock-outs {cnt['outs']:,}  restocks {cnt['backs']:,}  "
                  f"kill-switch blocks {cnt['blocked']:,}  {r['n']:,} writes  write ack p50 {r['p50']:.1f}ms  "
                  f"p99 {r['p99']:.1f}ms{tag}", flush=True)
            cnt.update(sent=0, outs=0, backs=0, blocked=0)

    started = asyncio.Event()

    async def go():
        nonlocal t0
        while snapped[0] < n_parts:
            await asyncio.sleep(0.05)
        oos = sum(map(len, out_of_stock))
        print(f"[status] snapshot: {len(keys):,} items in live campaigns ({a.cats} categories x "
              f"{a.items_per_cat:,}), {oos:,} out of stock", flush=True)
        t0 = time.monotonic()
        started.set()

    await asyncio.gather(go(), report(), kill_switch(), *(writer(p) for p in range(n_parts)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--cats", type=int, default=len(CATEGORIES), help="catalog categories (up to 16)")
    p.add_argument("--items-per-cat", type=int, default=12500, help="items in live campaigns per category")
    p.add_argument("--max-stock", type=int, default=200, help="highest starting stock count")
    p.add_argument("--oos-share", type=float, default=0.05, help="share of items out of stock at start")
    p.add_argument("--rate", type=int, default=5000, help="stock updates per second that keep an item in stock")
    p.add_argument("--outs", type=float, default=60, help="stock-outs per second")
    p.add_argument("--backs", type=float, default=50, help="restocks per second")
    p.add_argument("--kill-every", type=float, default=10, help="seconds between Item Kill Switch bulk calls; 0 = off")
    p.add_argument("--kill-items", type=int, default=100, help="items blocked per bulk call")
    p.add_argument("--sale-cat", default="electronics", help="the category the flash sale lands on")
    p.add_argument("--sale-at", type=float, default=60, help="the flash sale starts this many seconds in")
    p.add_argument("--sale-for", type=float, default=30, help="seconds; 0 = no sale")
    p.add_argument("--sale-x", type=float, default=5, help="that category's stock updates and stock-outs, times usual")
    p.add_argument("--tick-ms", type=float, default=2, help="each writer writes once per this many ms")
    p.add_argument("--snapshot-every", type=float, default=60, help="seconds per rolling snapshot of every item; 0 = off")
    asyncio.run(main(p.parse_args()))
