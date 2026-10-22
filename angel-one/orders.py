"""A busy neighbour on the same broker: order lifecycle events, at a steady rate with a burst.

Order updates shaped like a SmartAPI order-book row (orderid, symboltoken, transactiontype,
producttype, status), on their own topic. Default rate: Angel One reported 122.29 million orders
in September 2026 (monthly business update), about 5.8 million a trading day, or ~260 orders a
second over a 6 h 15 min session; at about three lifecycle events per order that is ~800 events a
second, and the open runs far above the average. Run it while capture.py and candles.py run, and
watch whether the candle lane's p99 moves when the neighbour bursts.
One batch per 10 ms tick, so the neighbour is busy without flooding the broker with tiny requests.
"""
import argparse
import asyncio
import json
import random
import time

from apache_iggy import SendMessage

from common import DEFAULT_URL, ORDERS, STREAM, connect, ensure_topic


def event(rng, n):
    return json.dumps({
        "orderid": f"2610220{n:09d}", "uniqueorderid": f"{n:08x}-ao", "symboltoken": str(10_000 + rng.randrange(40_000)),
        "exchange": rng.choice(("NSE", "BSE", "NFO", "MCX")), "transactiontype": rng.choice(("BUY", "SELL")),
        "ordertype": rng.choice(("LIMIT", "MARKET", "STOPLOSS_LIMIT")), "producttype": rng.choice(("DELIVERY", "INTRADAY", "CARRYFORWARD")),
        "variety": "NORMAL", "duration": "DAY", "quantity": rng.choice((1, 10, 50, 75)), "price": rng.randint(100, 400000) / 100,
        "status": rng.choice(("open pending", "open", "complete", "complete", "rejected")),
        "updatetime": time.strftime("%d-%b-%Y %H:%M:%S"), "ns": time.time_ns()}, separators=(",", ":")).encode()


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, ORDERS, 4)
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
                ms.append(SendMessage(event(rng, n)))
            await c.send_messages(STREAM, ORDERS, n % 4, ms)
            sent += k
        if time.monotonic() - last >= 1:
            tag = f"  burst {a.x:g}x" if x > 1 else ""
            print(f"[orders] {sent:,} order events/s{tag}", flush=True)
            sent, last = 0, time.monotonic()
        due += 0.01
        await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rate", type=float, default=800, help="order events per second before the burst")
    p.add_argument("--x", type=float, default=10, help="burst multiplier, as at the 09:15 open")
    p.add_argument("--at", type=float, default=30, help="burst starts this many seconds in")
    p.add_argument("--for", dest="for_s", type=float, default=20)
    asyncio.run(main(p.parse_args()))
