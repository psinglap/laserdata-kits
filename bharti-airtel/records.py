"""A heavy neighbour on the same broker: call and SMS records, at a steady rate with a burst.

Records of the kind Airtel's spam detection looks at (caller or sender, usage pattern, call or SMS frequency,
call duration, links in the SMS), the bulk, latency-tolerant feed that analytics and model training read, on
their own topic. Run it while verdicts.py and resolver.py run, and watch whether the verdict lane's p99 moves
when the records burst, as they do when an SMS blast goes out.

Sends one batch per --tick-ms with balanced partitioning, as a real high-volume producer batches; one tiny
send per partition per tick would load the broker with requests rather than bytes.
"""
import argparse
import asyncio
import json
import random
import time

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, RECORDS, STREAM, connect, domain, ensure_topic

CIRCLES = ("HR", "DL", "MH", "MU", "KA", "TN", "AP", "UPE", "UPW", "WB", "RJ", "GJ", "PB", "KO", "OR", "AS")


def record(rng, n):
    sms = rng.random() < 0.4
    r = {"id": f"r-{n:012x}", "ts": int(time.time() * 1000), "kind": "sms" if sms else "call",
         "a": f"91{rng.randrange(6_000_000_000, 9_999_999_999)}", "b": f"91{rng.randrange(6_000_000_000, 9_999_999_999)}",
         "circle": rng.choice(CIRCLES), "cell": f"{rng.randrange(1 << 28):07x}", "imei_changes_7d": rng.choice((0, 0, 0, 1, 2)),
         "a_calls_1h": rng.randint(0, 400), "a_distinct_b_1h": rng.randint(0, 300)}
    if sms:
        r.update(header=rng.choice(("", "", "AX-NOTICE", "VM-ALERTS")), len=rng.randint(40, 160),
                 links=[domain(rng.randrange(500_000))] if rng.random() < 0.3 else [])
    else:
        r.update(dur_s=rng.choice((0, 0, 3, 8, 25, 60, 180, 600)), answered=rng.random() < 0.6)
    return json.dumps(r, separators=(",", ":")).encode()


async def main(a):
    c = await connect(a.url)
    await ensure_topic(c, RECORDS)
    rng = random.Random(1)
    pool = [record(rng, n) for n in range(2000)]     # pre-built records, so the generator spends its CPU on sending
    size = sum(map(len, pool)) / len(pool)
    print(f"[records] records of ~{size:.0f} B, {a.mbps:g} MB/s, burst {a.x:g}x at {a.at:g}s for {a.for_s:g}s",
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
            await c.send_messages(STREAM, RECORDS, Partitioning.balanced(), batch)
            sent += k
            nbytes += int(k * size)
        if time.monotonic() - last >= 1:
            tag = f"  burst {a.x:g}x" if x > 1 else ""
            print(f"[records] {sent:>7,} records/s  ~{nbytes / 1e6:.1f} MB/s{tag}", flush=True)
            sent, nbytes, last = 0, 0, time.monotonic()
        due += a.tick_ms / 1000
        await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[records] done", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--mbps", type=float, default=20, help="MB/s of call and SMS records before the burst")
    p.add_argument("--x", type=float, default=4, help="burst multiplier")
    p.add_argument("--at", type=float, default=56, help="burst starts this many seconds in")
    p.add_argument("--for", dest="for_s", type=float, default=30)
    p.add_argument("--tick-ms", type=float, default=20, help="one batch per this many ms")
    p.add_argument("--duration", type=float, default=3600, help="stop after this many seconds")
    asyncio.run(main(p.parse_args()))
