"""The CocoIndex app: an Apache Iggy topic of review-thread changes in, a LanceDB table of every open thread out.

It is CocoIndex's own Iggy connector, unchanged: `iggy.topic_as_map` treats the topic as a live keyed map (key = the
thread id in the payload, `{"_deleted": id}` = delete), `coco.mount_each` runs `process_thread` once per thread,
and `declare_row` upserts the row; a resolved thread's row is removed. The connector stores the Iggy offset only after
CocoIndex has written the row, so a crash replays from the last durable write and never skips a change.

    python index_app.py              # catch up on the topic, then keep the table current (live mode)
"""
import argparse
import asyncio
import gc
import json
import os
import pathlib
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta

os.environ.setdefault("COCOINDEX_DB", "./cocoindex_state")   # CocoIndex's own state: what each thread declared

import cocoindex as coco
from apache_iggy import IggyClient
from cocoindex.connectors import iggy, lancedb

from common import DEFAULT_URL, EVENTS, HEARTBEAT, LANCEDB_URI, STREAM, TABLE, is_deletion, key_of, topic_end

ap = argparse.ArgumentParser()
ap.add_argument("--url", default=DEFAULT_URL)
ap.add_argument("--topic", default=EVENTS)
ap.add_argument("--table", default=TABLE)
ap.add_argument("--batch", type=int, default=5000, help="Iggy messages per poll (the connector's default is 100)")
ap.add_argument("--group", default="cocoindex-review-context", help="Iggy consumer group the connector stores offsets in")
A = ap.parse_args()

IGGY = coco.ContextKey[IggyClient]("iggy")
LANCE_DB = coco.ContextKey[lancedb.LanceAsyncConnection]("review_context_db")


@dataclass
class ReviewThread:
    thread_id: str
    repo: str
    pr: int
    path: str
    line: int
    author: str
    comments: int
    body: str
    seq: int        # the thread's change number on the topic
    ns: int         # when the change was written to Iggy; context_check.py times freshness from it


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    client = IggyClient.from_connection_string(A.url)
    await client.connect()
    builder.provide(IGGY, client)
    builder.provide(LANCE_DB, await lancedb.connect_async(LANCEDB_URI))
    yield


applied = 0


@coco.fn
async def process_thread(msg, table: lancedb.TableTarget[ReviewThread]) -> None:
    global applied
    d = json.loads(msg.payload())
    table.declare_row(row=ReviewThread(
        thread_id=d["thread_id"], repo=d.get("repo", ""), pr=int(d.get("pr", 0)), path=d.get("path", ""),
        line=int(d.get("line", 0)), author=d.get("author", ""), comments=int(d.get("comments", 0)),
        body=d.get("body", ""), seq=int(d["seq"]), ns=int(d["ns"])))
    applied += 1


first_offset = None
DBG = {"delivered": 0, "last": -1, "applies": [], "rows": 0}
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logging.getLogger("lance").setLevel(logging.ERROR)
from cocoindex.connectors.lancedb import _target as _lt
_orig_apply = _lt._RowHandler._apply_actions
async def _timed_apply(self, ctx, actions):
    t0 = time.perf_counter()
    await _orig_apply(self, ctx, actions)
    DBG["applies"].append((time.perf_counter() - t0) * 1000)
    DBG["rows"] += len(actions)
_lt._RowHandler._apply_actions = _timed_apply


def key(message):
    global first_offset
    DBG["delivered"] += 1
    DBG["last"] = message.offset()
    if first_offset is None:
        first_offset = message.offset()
    return key_of(message)


@coco.fn
async def app_main() -> None:
    table = await lancedb.mount_table_target(
        LANCE_DB, table_name=A.table,
        table_schema=await lancedb.TableSchema.from_class(ReviewThread, primary_key=["thread_id"]))
    # batch_length: the connector stores an offset only after the row is written, and under PollingStrategy.Next an
    # Iggy consumer stalls once it reads batch_length past the stored offset (apache-iggy 0.9 consumer docs), so the
    # batch must cover every change in flight; the connector's default of 100 caps it near 100 per write latency.
    # poll_interval: without one the consumer re-polls in a busy loop while it waits for offsets to be stored.
    threads = iggy.topic_as_map(coco.use_context(IGGY), A.group, STREAM, A.topic, key=key, is_deletion=is_deletion,
                                batch_length=A.batch, poll_interval=timedelta(milliseconds=5))
    await coco.mount_each(process_thread, threads, table)


app = coco.App(coco.AppConfig(name="IggyReviewContext"), app_main)


async def heartbeat():
    p = pathlib.Path(HEARTBEAT)
    while True:
        p.touch()
        await asyncio.sleep(0.1)


async def report(end: int):
    global applied
    said = False
    while True:
        await asyncio.sleep(1)
        if first_offset is not None and not said:
            said = True
            print(f"[index] resumed at offset {first_offset:,} (the last offset stored after a durable write); "
                  f"{max(0, end - first_offset):,} changes to replay from Iggy", flush=True)
        ap_ = sorted(DBG["applies"]) or [0]
        print(f"[index] {applied:>6,} changes/s through CocoIndex into LanceDB  [dbg delivered {DBG['delivered']} last "
              f"{DBG['last']} lance applies {len(DBG['applies'])} rows {DBG['rows']} max {ap_[-1]:.0f}ms "
              f"sum {sum(ap_):.0f}ms]", flush=True)
        DBG["delivered"] = DBG["rows"] = 0
        DBG["applies"] = []
        applied = 0


async def main():
    probe = IggyClient.from_connection_string(A.url)
    await probe.connect()
    while not await probe.get_topic(STREAM, A.topic):     # the review service creates it; CocoIndex never does
        await asyncio.sleep(0.5)
    end = await topic_end(probe, A.topic)
    print(f"[index] topic {STREAM}/{A.topic} ends at offset {end:,}; table {A.table} in {LANCEDB_URI}", flush=True)
    tasks = [asyncio.create_task(heartbeat()), asyncio.create_task(report(end))]
    handle = app.update(live=True)
    t0 = time.monotonic()
    gc.freeze()
    ready = False
    async for snap in handle.watch():
        if not ready and snap.status == coco.UpdateStatus.READY:
            ready = True
            print(f"[index] caught up with the topic after {time.monotonic() - t0:.1f} s; live", flush=True)
    for t in tasks:
        t.cancel()


if __name__ == "__main__":
    asyncio.run(main())
