"""The send side: reads every reminder the journey workers queued and checks it against the user.

Stands in for the channel service that reads the journeys' output. It reads every partition of the
journey-sends topic, and the exit events (Policy Renewed, Payment Successful, Added To Cart) from the
tracked-events topic, both directly. Each reminder is judged --grace-s after it arrives, so an exit
that is still on its way is counted. Prints a line a second:
  * reminders queued per second, by journey, and how late each went out after its due time;
  * reminders sent after the trip had exited: the user renewed the policy, paid or added to cart
    before the reminder was due, and still got "your renewal is due". 0 is the pass line;
  * trips reminded twice, and users whose trips were advanced by two or more workers. 0 and 0;
  * exit events seen so far (so a 0 above is measured against real exits, not an empty table).
"""
import argparse
import asyncio
import gc
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, EVENTS_TOPIC, EXIT_NAMES, JOURNEYS, SENDS_TOPIC, STREAM, Window, connect, ensure_topic, load

EXIT_BYTES = tuple(b'"e":"%s"' % n.encode() for n in EXIT_NAMES)


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, STREAM, EVENTS_TOPIC, a.partitions)
    await ensure_topic(admin, STREAM, SENDS_TOPIC, a.send_partitions)
    ev_parts = (await admin.get_topic(STREAM, EVENTS_TOPIC)).partitions_count
    send_parts = (await admin.get_topic(STREAM, SENDS_TOPIC)).partitions_count
    exited = {}                                           # (user, context) -> time the exit event was written
    reminded = set()
    decided_by = {}
    multi = set()
    pending = deque()                                     # (judge at, reminder)
    late = Window()
    st, total = defaultdict(int), defaultdict(int)
    grace_ns = int(a.grace_s * 1e9)
    gc.freeze()

    async def read(topic, parts, tag, handle, first):
        c = await connect(a.url)
        consumers = {p: Consumer.Single(f"send-audit-{tag}-p{p}") for p in range(parts)}
        nxt = {}
        while True:
            more = False
            for p in range(parts):
                start = PollingStrategy.Offset(nxt[p]) if p in nxt else first
                ms = await c.poll_messages(STREAM, topic, consumer=consumers[p], partition_id=p,
                                           count=a.batch, auto_commit=False, polling_strategy=start)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        handle(m.payload())
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    def on_event(b):
        if any(x in b for x in EXIT_BYTES):               # only exits matter here; skip parsing the rest
            e = load(b)
            exited[e["u"], e["c"]] = e["ns"]
            total["exits"] += 1

    def on_send(b):
        r = load(b)
        pending.append((time.time_ns() + grace_ns, r))
        late.add((r["ns"] - r["due"]) / 1e6)
        st["sent"] += 1
        st[r["j"]] += 1

    def judge():
        now = time.time_ns()
        while pending and pending[0][0] <= now:
            _, r = pending.popleft()
            key = (r["u"], r["c"])
            if key in reminded:
                total["twice"] += 1
            reminded.add(key)
            x = exited.get(key)
            if x is not None and r["entry"] < x < r["due"]:
                total["stale"] += 1                       # the user had already exited this trip
            if decided_by.setdefault(r["u"], r["w"]) != r["w"]:
                multi.add(r["u"])
            total["judged"] += 1

    async def report():
        while True:
            await asyncio.sleep(1)
            judge()
            r = late.flush()
            by = ", ".join(f"{st[j]:,} {j}" for j in JOURNEYS)
            share = 100 * total["stale"] / total["judged"] if total["judged"] else 0.0
            print(f"[send] {st['sent']:>5,} reminders/s queued ({by})  sent after due p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  reminders sent after the trip had exited {total['stale']:,} "
                  f"({share:.3f}% of {total['judged']:,} judged)  trips reminded twice {total['twice']:,}  "
                  f"users advanced by 2+ workers {len(multi):,}  exit events seen {total['exits']:,}", flush=True)
            st.clear()

    print(f"[send] reading '{STREAM}/{SENDS_TOPIC}' ({send_parts} partitions) and the exits on "
          f"'{STREAM}/{EVENTS_TOPIC}' ({ev_parts} partitions), judging each reminder {a.grace_s:g} s after it",
          flush=True)
    await asyncio.gather(read(EVENTS_TOPIC, ev_parts, "ev", on_event, PollingStrategy.First()),
                         read(SENDS_TOPIC, send_parts, "send", on_send, PollingStrategy.First()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--partitions", type=int, default=64, help="partitions for a new events topic")
    p.add_argument("--send-partitions", type=int, default=16, help="partitions for a new journey-sends topic")
    p.add_argument("--grace-s", type=float, default=3.0, help="wait for exits still on their way before judging")
    p.add_argument("--poll-ms", type=float, default=10, help="the send side is not on the timed path")
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
