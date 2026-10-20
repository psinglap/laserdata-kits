"""The send side: reads every text the decision workers queued and checks the subscriber's view.

Stands in for the send service that reads the decision engine's output. It reads every partition
of the send-intents topic directly and, per subscriber, keeps the last text and the worker that
decided it. Prints a line a second:
  * texts queued per second, and event-to-send-intent latency (shopping event written to text
    queued, through the decision worker);
  * duplicate texts: a second text to the same subscriber inside --cooldown-s (the outcome
    Attentive's post says "may increase subscription opt-outs"); 0 is the pass line;
  * subscribers decided by two or more workers: the same signal as the post's mutual-exclusivity
    counter, measured on the output; 0 is the pass line.
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, SENDS_TOPIC, STREAM, Window, connect, ensure_topic, load


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, STREAM, SENDS_TOPIC, a.send_partitions)
    parts = (await admin.get_topic(STREAM, SENDS_TOPIC)).partitions_count
    c = await connect(a.url)
    consumers = {p: Consumer.Single(f"send-audit-p{p}") for p in range(parts)}
    nxt = {}
    cool_ns = int(a.cooldown_s * 1e9)
    last_text, decided_by = {}, {}
    multi = set()
    lat = Window()
    st, total = defaultdict(int), defaultdict(int)
    gc.freeze()

    def check(t, now):
        sub = t["u"]
        prev = last_text.get(sub)
        if prev is not None and t["ns"] - prev < cool_ns:
            total["dup"] += 1                             # a second text to this subscriber inside the cooldown
            st["dup"] += 1
        last_text[sub] = t["ns"] if prev is None else max(prev, t["ns"])
        if decided_by.setdefault(sub, t["w"]) != t["w"]:
            multi.add(sub)
        lat.add((now - t["ns"]) / 1e6)
        st["texts"] += 1
        st[t["kind"]] += 1

    async def read():
        while True:
            more = False
            for p in range(parts):
                start = PollingStrategy.Offset(nxt[p]) if p in nxt else PollingStrategy.First()
                ms = await c.poll_messages(STREAM, SENDS_TOPIC, consumer=consumers[p], partition_id=p,
                                           count=a.batch, auto_commit=False, polling_strategy=start)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        check(load(m.payload()), now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            print(f"[send] {st['texts']:>5,} texts/s queued ({st['cart_reminder']:,} cart, "
                  f"{st['checkout_reminder']:,} checkout)  event-to-send-intent p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  duplicate texts {st['dup']:,}/s ({total['dup']:,} total)  "
                  f"subscribers decided by 2+ workers {len(multi):,}  subscribers texted {len(last_text):,}",
                  flush=True)
            st.clear()

    print(f"[send] reading '{STREAM}/{SENDS_TOPIC}' ({parts} partitions), cooldown {a.cooldown_s:g} s", flush=True)
    await asyncio.gather(read(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--send-partitions", type=int, default=16, help="partitions for a new send-intents topic")
    p.add_argument("--cooldown-s", type=float, default=3600, help="same as decision.py")
    p.add_argument("--poll-ms", type=float, default=5)
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
