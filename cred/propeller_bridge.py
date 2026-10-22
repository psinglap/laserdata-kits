"""Shadow real Propeller events into Apache Iggy, read-only, from the broker Propeller already uses.

Propeller (github.com/CRED-CLUB/propeller) publishes every event a backend sends with SendEventToClientChannel to
a broker channel named after the member (the x-user-id value), SendEventToClientDeviceChannel to
"<member>--<device>", and SendEventToTopic to the topic's name; the payload is a marshalled push.v1.Event. This
bridge subscribes to those channels as one more listener (Redis PSUBSCRIBE, or a NATS subscription on ">"). It only
listens: it publishes nothing to Redis or NATS and calls no Propeller API.

Each event is written to the lane keyed by member, with a per-member seq assigned here and the event's name
decoded from the push.v1.Event header, so propeller_node.py reads real event names and rates. The kit's status and
coin fields stay empty unless you map them in to_event() from your own payloads. ns is when the bridge received
the event, so the latency printed is bridge-to-node. With Propeller's persistence option on, events go to Redis
streams ("<channel>-stream") that Propeller deletes after delivery; shadow the pub/sub path, or the backend that
calls Propeller, instead.

Needs: pip install apache-iggy==0.9.0 redis   (or nats-py for --nats)
"""
import argparse
import asyncio
import re
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, EVENTS, STREAM, connect, ensure_topics, msg, part_of

seq = defaultdict(int)


def event_name(b: bytes) -> str:
    """push.v1.Event field 1 (name, a string) without generated code: tag 0x0a, a varint length, the bytes."""
    i = 0
    while i < len(b):
        tag, i = b[i], i + 1
        n = shift = 0
        while i < len(b):
            x, i = b[i], i + 1
            n |= (x & 0x7F) << shift
            shift += 7
            if x < 0x80:
                break
        if tag == 0x0A:
            return b[i:i + n].decode(errors="replace")
        if tag & 7 != 2:
            return ""                         # not a length-delimited field first: not an Event header
        i += n
    return ""


def to_event(channel: str, data: bytes) -> tuple[str, bytes]:
    """Map one Propeller publish onto the lane's record. Adjust here to decode your own payloads."""
    k, _, device = channel.partition("--")
    seq[k] += 1
    return k, msg(k=k, seq=seq[k], device=device, ref="", flow="propeller", status=event_name(data) or "EVENT",
                  amount=0, reason="", bank="", coins=0, size=len(data))


async def main(a):
    iggy = await connect(a.url)
    await ensure_topics(iggy, EVENTS)
    keep = re.compile(a.members)
    buf, n, t0 = [], 0, time.monotonic()

    def take(channel: str, data: bytes):
        if keep.fullmatch(channel.partition("--")[0]):
            buf.append(to_event(channel, data))

    async def flush():
        # One batch per partition every 2 ms, never one awaited send per event.
        nonlocal n, t0
        while True:
            await asyncio.sleep(0.002)
            if buf:
                out = defaultdict(list)
                for k, payload in buf:
                    out[part_of(k)].append(SendMessage(payload))
                n += len(buf)
                buf.clear()
                await asyncio.gather(*(iggy.send_messages(STREAM, EVENTS, Partitioning.partition_id(p), ms)
                                       for p, ms in out.items()))
            if time.monotonic() - t0 >= 10:
                print(f"[propeller-bridge] {n / (time.monotonic() - t0):,.1f} events/s copied to '{EVENTS}'",
                      flush=True)
                n, t0 = 0, time.monotonic()

    sender = asyncio.create_task(flush())
    if a.nats:
        import nats
        nc = await nats.connect(a.nats)

        async def on(m):
            take(m.subject, m.data)
        await nc.subscribe(">", cb=on)
        await asyncio.Event().wait()
    else:
        import redis.asyncio as redis
        r = redis.from_url(a.redis)
        ps = r.pubsub()
        await ps.psubscribe("*")
        async for m in ps.listen():
            if m["type"] == "pmessage":
                take(m["channel"].decode(), m["data"])
    sender.cancel()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--redis", default="redis://localhost:6379", help="Propeller's Redis (broker = \"redis\")")
    p.add_argument("--nats", help="Propeller's NATS URL (broker = \"nats\"), e.g. nats://localhost:4222")
    p.add_argument("--members", default=r"[^.]+",
                   help="regex a member channel name must match; use it to leave custom topics out")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
