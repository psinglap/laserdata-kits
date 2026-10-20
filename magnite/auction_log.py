"""A heavy neighbour on the same broker: the auction log, at a steady rate with a burst.

Auction records (the request, which DSPs were called, their responses, the clearing price), the kind of
bulk, latency-tolerant feed that Spark Structured Streaming jobs read for reporting and model training,
on their own topic. Run it while exchange.py and shaper.py run, and watch whether the appetite lane's
p99 moves when the auction log bursts.

Sends one batch per --tick-ms with balanced partitioning, as a real high-volume producer batches; one
tiny send per partition per tick would load the broker with requests rather than bytes.
"""
import argparse
import asyncio
import json
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import AUCTION_LOG, DEFAULT_URL, STREAM, connect, ensure_topic


def record(rng, n):
    callouts = rng.randint(4, 24)
    return json.dumps({
        "auction_id": f"a-{n:012x}", "ts": int(time.time() * 1000),
        "seg": f"seg-{rng.randrange(5000):05d}", "format": rng.choice(("ctv", "olv", "display", "audio")),
        "device": rng.choice(("ctv", "mobile_app", "desktop", "mobile_web")), "geo": f"us-{rng.randrange(500, 882)}",
        "floor_cpm": round(rng.uniform(0.5, 25.0), 2),
        "responses": [{"dsp": f"dsp-{rng.randrange(60):03d}", "bid": rng.random() < 0.25,
                       "cpm": round(rng.uniform(0.5, 40.0), 2), "ms": rng.randint(8, 180)} for _ in range(callouts)],
        "winner": f"dsp-{rng.randrange(60):03d}", "clear_cpm": round(rng.uniform(0.5, 30.0), 2),
    }, separators=(",", ":")).encode()


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, AUCTION_LOG)
    rng = random.Random(1)
    pool = [record(rng, n) for n in range(2000)]     # pre-built records, so the generator spends its CPU on sending
    size = sum(map(len, pool)) / len(pool)
    print(f"[auction-log] records of ~{size:.0f} B, {a.mbps:g} MB/s, burst {a.x:g}x at {a.at:g}s for {a.for_s:g}s",
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
            await c.send_messages(STREAM, AUCTION_LOG, Partitioning.balanced(), batch)
            sent += k
            nbytes += int(k * size)
        if time.monotonic() - last >= 1:
            tag = f"  burst {a.x:g}x" if x > 1 else ""
            print(f"[auction-log] {sent:>7,} records/s  ~{nbytes / 1e6:.1f} MB/s{tag}", flush=True)
            sent, nbytes, last = 0, 0, time.monotonic()
        due += a.tick_ms / 1000
        await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[auction-log] done", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--mbps", type=float, default=20, help="MB/s of auction records before the burst")
    p.add_argument("--x", type=float, default=4, help="burst multiplier")
    p.add_argument("--at", type=float, default=30, help="burst starts this many seconds in")
    p.add_argument("--for", dest="for_s", type=float, default=20)
    p.add_argument("--tick-ms", type=float, default=20, help="one batch per this many ms")
    p.add_argument("--duration", type=float, default=3600, help="stop after this many seconds")
    asyncio.run(main(p.parse_args()))
