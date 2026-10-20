"""A busy neighbour on the same broker: order postbacks, at a steady rate with a burst.

Order updates (placed, open, complete) shaped like Kite Connect's order postback JSON, on
their own topic. Run it while feed.py and ticker.py run, and watch whether the tick lane's
p99 moves when the neighbour bursts.
"""
import argparse
import asyncio
import json
import random
import time

from apache_iggy import SendMessage

from common import DEFAULT_URL, POSTBACKS, STREAM, connect, ensure_topic


def postback(rng, n):
    return json.dumps({
        "order_id": f"2510200{n:09d}", "exchange_order_id": f"1300000{n:011d}", "status": rng.choice(("OPEN", "COMPLETE", "UPDATE")),
        "tradingsymbol": f"SYM{rng.randrange(5000)}", "exchange": rng.choice(("NSE", "BSE", "NFO")), "transaction_type": rng.choice(("BUY", "SELL")),
        "order_type": rng.choice(("LIMIT", "MARKET")), "product": rng.choice(("CNC", "MIS", "NRML")), "quantity": rng.choice((1, 10, 50, 75)),
        "price": rng.randint(100, 400000) / 100, "filled_quantity": 0, "pending_quantity": 0, "average_price": 0,
        "order_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"), "variety": "regular", "validity": "DAY", "tag": None,
        "ns": time.time_ns()}, separators=(",", ":")).encode()


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, POSTBACKS, 4)
    rng, n, t0 = random.Random(1), 0, time.monotonic()
    due, carry, sent, last = time.monotonic(), 0.0, 0, time.monotonic()
    while True:
        el = time.monotonic() - t0
        x = a.x if a.at <= el < a.at + a.for_s else 1
        carry += a.rate * x / 100
        k, carry = int(carry), carry - int(carry)
        if k:
            ms = []
            for _ in range(k):
                n += 1
                ms.append(SendMessage(postback(rng, n)))
            await c.send_messages(STREAM, POSTBACKS, n % 4, ms)
            sent += k
        if time.monotonic() - last >= 1:
            tag = f"  burst {a.x:g}x" if x > 1 else ""
            print(f"[postbacks] {sent:,} order updates/s{tag}", flush=True)
            sent, last = 0, time.monotonic()
        due += 0.01
        await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rate", type=float, default=2000, help="order updates per second before the burst")
    p.add_argument("--x", type=float, default=10, help="burst multiplier")
    p.add_argument("--at", type=float, default=30, help="burst starts this many seconds in")
    p.add_argument("--for", dest="for_s", type=float, default=20)
    asyncio.run(main(p.parse_args()))
