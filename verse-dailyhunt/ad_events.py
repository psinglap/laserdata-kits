"""A heavy neighbour on the same broker: the Dailyhunt ad event log, at a steady rate with a burst.

Ad events (the request, the bid response or 204 no-bid, the win notice fired when the ad renders, the
clearing price), the kind of bulk, latency-tolerant feed that is streamed into BigQuery for reporting and
model training, on their own topic. Run it while floors.py and bidnode.py run, and watch whether the floor
lane's p99 moves when the event log bursts, as it does when breaking news sends readers to the app at once.

Sends one batch per --tick-ms with balanced partitioning, as a real high-volume producer batches; one
tiny send per partition per tick would load the broker with requests rather than bytes.
"""
import argparse
import asyncio
import json
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import AD_EVENTS, DEFAULT_URL, LANGS, STREAM, connect, ensure_topic, placement


def record(rng, n):
    lang = rng.choice(LANGS)
    bid = rng.random() < 0.6
    return json.dumps({
        "event_id": f"e-{n:012x}", "ts": int(time.time() * 1000), "app": "com.eterno",
        "placement": placement(lang, rng.randrange(2000)), "lang": lang,
        "state": rng.choice(("UP", "BR", "MH", "WB", "RJ", "TN", "KA", "AP", "TG", "GJ", "MP", "OD")),
        "os": rng.choice(("android", "android", "android", "ios")), "tmax": 120,
        "floor_cpm_rs": round(rng.uniform(20, 180), 2),
        "outcome": "win_notice" if bid else "no_bid_204",
        "bids": [{"seat": f"dsp-{rng.randrange(40):02d}", "cpm_rs": round(rng.uniform(15, 260), 2),
                  "ms": rng.randint(8, 115)} for _ in range(rng.randint(1, 8) if bid else 0)],
        "clear_cpm_rs": round(rng.uniform(20, 220), 2) if bid else None,
    }, separators=(",", ":")).encode()


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, AD_EVENTS)
    rng = random.Random(1)
    pool = [record(rng, n) for n in range(2000)]     # pre-built records, so the generator spends its CPU on sending
    size = sum(map(len, pool)) / len(pool)
    print(f"[ad-events] records of ~{size:.0f} B, {a.mbps:g} MB/s, burst {a.x:g}x at {a.at:g}s for {a.for_s:g}s",
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
            await c.send_messages(STREAM, AD_EVENTS, Partitioning.balanced(), batch)
            sent += k
            nbytes += int(k * size)
        if time.monotonic() - last >= 1:
            tag = f"  burst {a.x:g}x" if x > 1 else ""
            print(f"[ad-events] {sent:>7,} records/s  ~{nbytes / 1e6:.1f} MB/s{tag}", flush=True)
            sent, nbytes, last = 0, 0, time.monotonic()
        due += a.tick_ms / 1000
        await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[ad-events] done", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--mbps", type=float, default=20, help="MB/s of ad events before the burst")
    p.add_argument("--x", type=float, default=4, help="burst multiplier")
    p.add_argument("--at", type=float, default=56, help="burst starts this many seconds in")
    p.add_argument("--for", dest="for_s", type=float, default=30)
    p.add_argument("--tick-ms", type=float, default=20, help="one batch per this many ms")
    p.add_argument("--duration", type=float, default=3600, help="stop after this many seconds")
    asyncio.run(main(p.parse_args()))
