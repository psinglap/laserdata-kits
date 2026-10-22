"""Shadow your conversation start and end events into the Apache Iggy concurrency lane, read-only, from JSON lines on
stdin.

Sarvam does not publish what carries call lifecycle events between telephony, the agent runtime and the
campaign service, so this bridge takes the simplest copy any of them can give: one JSON record per line on
stdin. Pipe in whatever you already have, for example a log tail from the workers that host conversations, or
a CLI consumer of an internal queue:
    tail -F /var/log/agent-runtime/conversations.jsonl | python call_events_bridge.py
Nothing upstream changes; the bridge only reads its input. For each record it keeps a running count of live
conversations per (worker, organization) and publishes the resulting count with a sequence, keyed by
organization, the same shape call_workers.py writes, plus one beat a second for every worker it has seen.
Point ledger.py at Iggy (with your plan ceilings in plan()) and compare its headroom changes with the
admissions and rejections your platform made.

The post-call webhook payload is not enough on its own: it arrives after the call ends, so it cannot say how
many conversations are live now. Use the start and end of each conversation.

Map your record in to_event(). The defaults read JSON with fields event ("conversation_started" or
"conversation_ended"), org_id, host (the worker) and source ("campaign", "instant" or "inbound"); the
interaction_id is used to ignore a repeated start or end. In a shadow run only the lane is timed; the clock
starts when the bridge reads the record.
Needs: pip install apache-iggy==0.9.0
"""
import argparse
import asyncio
import json
import sys
import time
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, PARTITIONS, SLOTS, STREAM, ensure_topics, msg, part

EVENT = {"conversation_started": "start", "conversation_ended": "end", "start": "start", "end": "end",
         "call_started": "start", "call_ended": "end"}


def to_event(line: str, a):
    d = json.loads(line)
    ev = EVENT.get(str(d.get(a.event_field, "")).lower())
    if ev is None:
        raise ValueError("not a start or end")
    return (ev, str(d[a.org_field]), str(d.get(a.worker_field, "unknown")), str(d.get(a.source_field, "campaign")),
            str(d.get(a.id_field, "")))


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, SLOTS)
    producer = await iggy.producer(STREAM, SLOTS, topic_partitions_count=PARTITIONS[SLOTS])
    loop = asyncio.get_running_loop()
    lines: asyncio.Queue = asyncio.Queue()
    loop.add_reader(sys.stdin.fileno(), lambda: lines.put_nowait(sys.stdin.readline()))
    seq, live, open_ids, workers = defaultdict(int), defaultdict(int), set(), set()
    next_beat = time.monotonic()
    async with producer:
        while True:
            groups = defaultdict(list)
            try:
                first = await asyncio.wait_for(lines.get(), timeout=0.2)
            except asyncio.TimeoutError:
                first = None
            batch = [first] if first is not None else []
            while not lines.empty() and len(batch) < 1000:
                batch.append(lines.get_nowait())
            for line in batch:
                if line == "":
                    return                  # end of input
                try:
                    ev, o, w, src, cid = to_event(line, a)
                except (KeyError, ValueError):
                    continue                # not a record this mapping understands
                if cid:                     # a repeated start or end changes nothing
                    if (ev == "start") == (cid in open_ids):
                        continue
                    (open_ids.add if ev == "start" else open_ids.discard)(cid)
                live[(w, o)] = max(0, live[(w, o)] + (1 if ev == "start" else -1))
                seq[(w, o)] += 1
                workers.add(w)
                groups[part(o, PARTITIONS[SLOTS])].append(
                    SendMessage(msg(w=w, o=o, seq=seq[(w, o)], live=live[(w, o)], ev=ev, src=src)))
            if time.monotonic() >= next_beat:  # the ledger releases a silent worker's slots after its lease
                next_beat += 1
                for w in workers:
                    groups[part(w, PARTITIONS[SLOTS])].append(SendMessage(msg(w=w, beat=1)))
            # One batch per partition, all sent at once, so an organization's events stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                   for q, ms in groups.items()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--event-field", default="event")
    ap.add_argument("--org-field", default="org_id")
    ap.add_argument("--worker-field", default="host")
    ap.add_argument("--source-field", default="source")
    ap.add_argument("--id-field", default="interaction_id")
    ap.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(ap.parse_args()))
