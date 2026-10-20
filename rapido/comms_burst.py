"""A campaign burst next door: comms events on their own topic, then --x times the rate.

Rapido's message transporter (RCE) sends push notifications, banners and alerts to customer
and captain apps and publishes events such as message_created and message_pushed_to_mobile
for analysis. A city-wide campaign multiplies those events in seconds. This script plays that
neighbour on the same broker: a steady comms-event stream, then --x times that rate for --for
seconds. Watch match.py while it runs; the captain lane's p99 should barely move.
"""
import argparse
import asyncio
import os
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import COMMS, DEFAULT_URL, STREAM, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    p = await client.producer(STREAM, COMMS, topic_partitions_count=a.partitions,
                              mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=100)))
    body = os.urandom(a.size // 2).hex()          # stands in for a notification event with its payload
    t0 = due = mark = time.monotonic()
    sent = 0
    async with p:
        while True:
            t = time.monotonic() - t0
            x = a.x if a.at <= t < a.at + a.for_s else 1
            per_tick = max(1, int(a.mbps * x * 1e6 / a.size / 50))
            ms = [SendMessage(msg(msg_id=f"M{random.randrange(10**10):010d}",
                                  ev=random.choice(("message_created", "message_pushed_to_mobile")), v=body))
                  for _ in range(per_tick)]
            # spread the tick over partitions and send the chunks at once, so a burst is not paced by one round trip
            chunks = [ms[i::a.partitions] for i in range(a.partitions)]
            await asyncio.gather(*(p.send_with_partitioning(c, Partitioning.partition_id(pid))
                                   for pid, c in enumerate(chunks) if c))
            sent += per_tick
            if time.monotonic() - mark >= 1:
                state = f"burst {x:g}x" if x > 1 else "steady"
                print(f"[comms] {state}  {sent:,} msgs/s  ~{sent * a.size / 1e6:.1f} MB/s on '{COMMS}'", flush=True)
                sent, mark = 0, time.monotonic()
            due = max(due + 0.02, time.monotonic() - 0.1)        # never bank more than 100 ms of catch-up
            await asyncio.sleep(max(0.0, due - time.monotonic()))   # behind schedule: send back to back


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--mbps", type=float, default=0.5, help="steady ingress, megabytes per second")
    ap.add_argument("--x", type=float, default=20, help="ingress multiplier during the campaign burst")
    ap.add_argument("--at", type=float, default=30, help="seconds before the burst starts")
    ap.add_argument("--for", dest="for_s", type=float, default=60, help="burst length in seconds")
    ap.add_argument("--size", type=int, default=1024, help="bytes per message")
    ap.add_argument("--partitions", type=int, default=16)
    asyncio.run(main(ap.parse_args()))
