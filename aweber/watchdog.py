"""The lane's watchdog: flags a reader whose position stops moving while notifications wait for it.

In apache/iggy#4273 a consumer group kept a member that was gone; the live members polled, got nothing, and saw no
error, so the stop looked like an idle topic for six days. #4279 asks for group offsets over HTTP so lag can be
watched from a dashboard. This script watches lag from outside the readers:

  * every --check-ms it reads each lane topic's current offset per partition (get_topic);
  * each reader publishes its own position once a second (notify.py), read here from notification_readers;
  * lag = notifications on the log past the reader's position. A reader with lag whose position has not moved for
    --stall-s is flagged STALL (once per episode); one that stops reporting for --stall-s is flagged SILENT.
    RECOVERED when it moves again.

Prints each reader's lag every second, and every flag with how long after the position stopped it was raised.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, PollingStrategy

from common import DEFAULT_URL, LANE, NOTIFY, POSITIONS, PROGRESS, ensure, load


class Reader:
    __slots__ = ("pos", "seen", "moved", "stuck_since", "flagged", "flags")

    def __init__(self, now):
        self.pos, self.seen, self.moved = None, now, now
        self.stuck_since, self.flagged, self.flags = None, None, 0


async def main(a):
    async def own_client():
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    lane = await own_client()
    await ensure(lane, NOTIFY, *LANE)
    prog = await own_client()
    await ensure(prog, PROGRESS, POSITIONS)
    readers = {}
    gc.freeze()

    async def ends():
        out = {}
        for t in LANE:
            topic = await lane.get_topic(NOTIFY, t)
            for i, p in enumerate(sorted(topic.partitions, key=lambda p: p.id)):
                out[f"{t}/{i}"] = p.current_offset + 1 if p.messages_count else 0
        return out

    async def read_positions():
        who, nxt = Consumer.Single(f"watchdog-{os.getpid()}"), None
        while True:
            ms = await prog.poll_messages(PROGRESS, POSITIONS, consumer=who, partition_id=0, count=1000,
                                          auto_commit=False,
                                          polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                          else PollingStrategy.Last())
            for m in ms:
                d = load(m.payload())
                now = time.monotonic()
                r = readers.setdefault(d["reader"], Reader(now))
                r.seen = now
                if d["pos"] != r.pos:
                    r.pos, r.moved, r.stuck_since = d["pos"], now, None
                nxt = m.offset() + 1
            await asyncio.sleep(0.05)

    def lag(r, end):
        return sum(max(0, o - (r.pos or {}).get(k, 0)) for k, o in end.items())

    async def check():
        t0, line = time.monotonic(), defaultdict(int)
        while True:
            await asyncio.sleep(a.check_ms / 1000)
            end = await ends()
            now = time.monotonic()
            for name, r in sorted(readers.items()):
                n = lag(r, end)
                line[name] = n
                if now - r.seen >= a.stall_s:
                    state = "SILENT"
                elif n:
                    r.stuck_since = r.stuck_since or max(r.moved, now - a.check_ms / 1000)
                    state = "STALL" if now - r.stuck_since >= a.stall_s else None
                else:
                    r.stuck_since, state = None, None
                if state and not r.flagged:
                    r.flagged, r.flags = now, r.flags + 1
                    since = r.stuck_since if state == "STALL" else r.seen
                    print(f"[watchdog] {state} {name}: position unchanged with {n:,} notifications waiting; flagged "
                          f"{now - since:.1f} s after it stopped", flush=True)
                elif r.flagged and not state:
                    print(f"[watchdog] RECOVERED {name}: moving again after {now - r.flagged:.1f} s flagged", flush=True)
                    r.flagged = None
            if now - t0 >= 1:
                parts = "  ".join(f"{k} lag {v:,} {'FLAGGED' if readers[k].flagged else 'moving'}"
                                  for k, v in sorted(line.items()))
                total = sum(r.flags for r in readers.values())
                print(f"[watchdog] {parts or 'no reader reported yet'}  stalls flagged {total}", flush=True)
                t0 = now

    await asyncio.gather(read_positions(), check())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--check-ms", type=float, default=250, help="how often lag is checked")
    p.add_argument("--stall-s", type=float, default=3, help="seconds without movement (with lag) before a flag")
    asyncio.run(main(p.parse_args()))
