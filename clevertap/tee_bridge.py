"""Shadow your delivered decisions and goal events into the lane, read-only, from JSON lines on stdin.

CleverTap does not publish what carries events between the Log Collector, TesseractDB and Experience
Builder, so this bridge takes the simplest copy any of them can give: one JSON record per line on stdin.
Pipe in whatever you already have, for example a log tail, an export file being written, or a CLI
consumer of your internal queue:
    tail -F /var/log/collector/events.jsonl | python tee_bridge.py
Nothing upstream changes; the bridge only reads its input. For each record it publishes one message,
keyed by user, with a sequence that only grows (the read time in ns, plus one per record), with the same
partitioning as the kit. Point learner.py at Iggy and compare its next decision with the one your engine
made.

Map your record in to_record(). The defaults accept CleverTap's Upload Events API shape
({"identity" | "objectId", "type": "event", "evtName", "evtData"}) and map event names to the kit's
kinds: "Notification Sent" to delivered (with evtData "action", "Campaign id" or "wzrk_id" as the action),
"Added To Cart" to add_to_cart, "Charged" to checkout, "App Uninstalled" to uninstall, and
"Subscription Cancelled" to subscription_cancel. Rename them to your brands' event names. In a shadow run
only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install apache-iggy==0.9.0
"""
import argparse
import asyncio
import json
import sys
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, OUTCOMES, PARTITIONS, STREAM, ensure_topics, msg, part

KIND = {"notification sent": "delivered", "added to cart": "add_to_cart", "add to cart": "add_to_cart",
        "charged": "checkout", "app uninstalled": "uninstall",
        "subscription cancelled": "subscription_cancel", "subscription canceled": "subscription_cancel"}


def to_record(line: str):
    d = json.loads(line)
    user = str(d.get("identity") or d.get("objectId") or d["user"])
    kind = KIND.get(str(d.get("evtName") or d.get("event") or "").lower())
    data = d.get("evtData") or {}
    action = str(data.get("action") or data.get("Campaign id") or data.get("wzrk_id") or "") if kind == "delivered" else ""
    return user, kind, action


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)       # one connection for the one producer
    await iggy.connect()
    await ensure_topics(iggy, OUTCOMES)
    producer = await iggy.producer(STREAM, OUTCOMES, topic_partitions_count=PARTITIONS[OUTCOMES])
    queue = asyncio.Queue(maxsize=100_000)
    print("[bridge] reading JSON lines from stdin, read-only", flush=True)

    async def reader():
        loop = asyncio.get_running_loop()
        while True:
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                await queue.put(None)
                return
            await queue.put(line)

    async def sender():
        done, sent = False, 0
        async with producer:
            while not done:
                lines = [await queue.get()]
                while not queue.empty() and len(lines) < 1000:
                    lines.append(queue.get_nowait())
                groups = defaultdict(list)
                seq = time.time_ns()        # newer than any sequence before, even across bridge restarts
                for line in lines:
                    if line is None:
                        done = True
                        continue
                    try:
                        user, kind, action = to_record(line)
                    except (KeyError, ValueError, TypeError):
                        continue            # not a record this mapping understands
                    if not kind:
                        continue            # an event the learner does not use
                    seq += 1
                    extra = {"action": action} if kind == "delivered" else {}
                    groups[part(user, PARTITIONS[OUTCOMES])].append(SendMessage(msg(user=user, seq=seq, kind=kind, **extra)))
                # One batch per partition, all sent at once, so a user's records stay in order.
                await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                       for p, ms in groups.items()))
                sent += sum(len(ms) for ms in groups.values())
        print(f"[bridge] input ended: {sent:,} records published to {OUTCOMES}", flush=True)

    await asyncio.gather(reader(), sender())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
