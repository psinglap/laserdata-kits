"""Shadow your existing tick subject from NATS into Apache Iggy, read-only.

Subscribes to the subject your feed handlers publish on today, like any other ticker node
does, so publishers and existing subscribers are untouched. Writes each segment's ticks to
its partition once per --tick-ms. Point one ticker.py at Iggy and compare it with a node on NATS.

Map your packets in to_tick(): the kit expects Kite's 44-byte quote layout (token first,
prices in paise). A per-instrument sequence is added here; the created time is when the bridge
received the tick, so the latency ticker.py prints starts at the bridge, not at the exchange.
Needs: pip install nats-py apache-iggy==0.9.0
"""
import argparse
import asyncio
from collections import defaultdict

from apache_iggy import SendMessage

from common import DEFAULT_URL, QUOTE, SEGMENTS, STREAM, TICKS, connect, ensure_topic, pack, segment_of

seq = defaultdict(int)


def to_tick(subject: str, data: bytes) -> tuple[int, bytes] | None:
    if len(data) < QUOTE.size:
        return None                         # heartbeats and other short packets
    f = QUOTE.unpack_from(data)
    seq[f[0]] += 1
    return f[0], pack(*f, seq[f[0]])


async def main(a):
    import nats

    iggy = await connect(a.url)
    await ensure_topic(iggy, TICKS, len(SEGMENTS))
    writers = [await connect(a.url) for _ in SEGMENTS]
    pending = defaultdict(list)

    async def on_msg(m):
        t = to_tick(m.subject, m.data)
        if t:
            pending[segment_of(t[0])].append(SendMessage(t[1]))

    nc = await nats.connect(a.servers)
    await nc.subscribe(a.subject, cb=on_msg)
    print(f"[bridge] {a.subject} on {a.servers} -> Iggy {STREAM}/{TICKS}", flush=True)

    async def write(s):
        while True:
            if pending[s]:
                batch, pending[s] = pending[s], []
                await writers[s].send_messages(STREAM, TICKS, s, batch)
            await asyncio.sleep(a.tick_ms / 1000)

    await asyncio.gather(*(write(s) for s in range(len(SEGMENTS))))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--servers", default="nats://127.0.0.1:4222")
    p.add_argument("--subject", required=True, help="the subject your ticks are published on, e.g. 'ticks.>'")
    p.add_argument("--tick-ms", type=float, default=1)
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
