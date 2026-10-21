"""A slow consumer on the lane itself: a reader of link-samples that processes at most --rate samples a second
(a research capture, a backfill job, a dashboard that stalls), so it falls further behind every second.

Jump has listed "tolerance of slow consumers" among the gaps it found in earlier messaging systems. On Apache Iggy
each consumer group keeps its own offset on the log; a reader that falls behind reads older data from the log and
does not hold back the producers or the other readers. Run it next to link_rules.py and watch the lane's p99 while
this reader's lag grows.
"""
import argparse
import asyncio
import gc
import time
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import DEFAULT_URL, SAMPLES, STREAM, ensure_topics, load


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    await ensure_topics(client, SAMPLES)
    if a.at:
        print(f"[slow-reader] starting in {a.at:g} s", flush=True)
        await asyncio.sleep(a.at)
    reader = await client.consumer_group(
        a.name, STREAM, SAMPLES, polling_strategy=PollingStrategy.Next(), batch_length=500,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(milliseconds=5))
    gc.freeze()
    st = {"n": 0, "behind": 0.0, "budget": 0.0, "t": time.monotonic()}

    async def on_sample(m):
        d = load(m.payload())
        st["n"] += 1
        st["behind"] = (time.time_ns() - d["ns"]) / 1e9
        # The deliberate slowness: this reader only gets through --rate samples a second.
        st["budget"] += 1 / a.rate
        if st["budget"] >= 0.01:
            await asyncio.sleep(st["budget"])
            st["budget"] = 0.0

    async def report():
        end = time.monotonic() + (a.duration or 1e12)
        while time.monotonic() < end:
            await asyncio.sleep(1)
            print(f"[slow-reader] {st['n']:,} samples/s read, behind the lane by {st['behind']:.1f} s", flush=True)
            st["n"] = 0
        print("[slow-reader] done", flush=True)

    task = asyncio.ensure_future(reader.consume_messages(on_sample, asyncio.Event()))
    await report()
    task.cancel()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--name", default="research-capture")
    ap.add_argument("--rate", type=float, default=1000, help="samples a second this reader gets through")
    ap.add_argument("--at", type=float, default=0, help="seconds to wait before starting")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
