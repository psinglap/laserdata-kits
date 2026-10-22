"""A heavy neighbour on the same broker: the retailer's shopper user events, at a steady rate with a burst.

User events in the shape the MCM Event service takes (HOME, SEARCH, ITEM_PAGE_VIEW, ADD_TO_CART, PURCHASE,
with items, page, channel and a persistent device ID): the bulk, latency-tolerant stream that feeds model
training and reporting, on its own topic. Run it while status.py and decision_node.py run, and watch
whether the item status lane's p99 moves when shopper traffic bursts, as it does when a flash sale opens.

Sends one batch per --tick-ms with balanced partitioning, as a real high-volume producer batches; one tiny
send per partition per tick would load the broker with requests rather than bytes.
"""
import argparse
import asyncio
import json
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import CATEGORIES, DEFAULT_URL, STREAM, USER_EVENTS, connect, ensure_topic, item

TYPES = ["HOME"] * 2 + ["SEARCH"] * 3 + ["ITEM_PAGE_VIEW"] * 8 + ["ADD_TO_CART"] * 2 + ["PURCHASE"]


def record(rng, n):
    t = rng.choice(TYPES)
    cat = rng.choice(CATEGORIES)
    d = {"id": f"ev-{n:016x}", "event_type": t, "timestamp": str(int(time.time() * 1000)),
         "channel_type": rng.choice(("APP", "APP", "SITE")), "user_id": f"u{rng.getrandbits(40):010x}",
         "page_id": f"category/{cat}", "session_id": f"s{rng.getrandbits(48):012x}",
         "device": {"os": rng.choice(("ANDROID", "IOS", "WEB")), "persistent_id": f"{rng.getrandbits(64):016x}"}}
    if t == "SEARCH":
        d["search_query"] = rng.choice(("tv 65 inch", "paper towels", "air fryer", "protein bars", "running shoes"))
    if t in ("ITEM_PAGE_VIEW", "ADD_TO_CART", "PURCHASE"):
        d["items"] = [{"id": item(cat, rng.randrange(12500)).split("|")[1], "quantity": rng.randint(1, 3),
                       "price": {"currency": "USD", "amount": round(rng.uniform(3, 900), 2)}}
                      for _ in range(rng.randint(1, 4) if t == "PURCHASE" else 1)]
    return json.dumps(d, separators=(",", ":")).encode()


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, USER_EVENTS)
    rng = random.Random(1)
    pool = [record(rng, n) for n in range(2000)]     # pre-built records, so the generator spends its CPU on sending
    size = sum(map(len, pool)) / len(pool)
    print(f"[user-events] records of ~{size:.0f} B, {a.mbps:g} MB/s, burst {a.x:g}x at {a.at:g}s for {a.for_s:g}s",
          flush=True)
    t0 = time.monotonic()
    due, carry, sent, nbytes, last, i = t0, 0.0, 0, 0, t0, 0
    while True:
        el = time.monotonic() - t0
        if el >= a.duration:
            break
        x = a.x if a.at <= el < a.at + a.for_s else 1
        carry += a.mbps * x * 1e6 / size * a.tick_ms / 1000
        k, carry = int(carry), carry - int(carry)
        if k:
            batch = [SendMessage(pool[(i + j) % len(pool)]) for j in range(k)]
            i += k
            await c.send_messages(STREAM, USER_EVENTS, Partitioning.balanced(), batch)
            sent += k
            nbytes += int(k * size)
        if time.monotonic() - last >= 1:
            tag = f"  burst {a.x:g}x" if x > 1 else ""
            print(f"[user-events] {sent:>7,} records/s  ~{nbytes / 1e6:.1f} MB/s{tag}", flush=True)
            sent, nbytes, last = 0, 0, time.monotonic()
        due += a.tick_ms / 1000
        await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[user-events] done", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--mbps", type=float, default=20, help="MB/s of user events before the burst")
    p.add_argument("--x", type=float, default=4, help="burst multiplier")
    p.add_argument("--at", type=float, default=56, help="burst starts this many seconds in")
    p.add_argument("--for", dest="for_s", type=float, default=30)
    p.add_argument("--tick-ms", type=float, default=20, help="one batch per this many ms")
    p.add_argument("--duration", type=float, default=3600, help="stop after this many seconds")
    asyncio.run(main(p.parse_args()))
