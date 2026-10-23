"""The CocoIndex app: an Apache Iggy topic of review-thread changes in, a Postgres table of every open thread out.

It is CocoIndex's own Iggy connector: `iggy.topic_as_map` treats the topic as a live keyed map (key = the thread id
in the payload, `{"_deleted": id}` = delete), `coco.mount_each` runs `process_thread` once per thread, and
`declare_row` upserts the row; a resolved thread's row is removed. The connector stores the Iggy offset only after
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

import asyncpg
import cocoindex as coco
from apache_iggy import IggyClient
from cocoindex.connectors import iggy, postgres

from common import DEFAULT_URL, EVENTS, HEARTBEAT, PG_URL, STREAM, TABLE, is_deletion, key_of, topic_end

ap = argparse.ArgumentParser()
ap.add_argument("--url", default=DEFAULT_URL)
ap.add_argument("--pg-url", default=PG_URL)
ap.add_argument("--topic", default=EVENTS)
ap.add_argument("--table", default=TABLE)
ap.add_argument("--batch", type=int, default=2000, help="Iggy messages per poll (the connector's default is 100)")
ap.add_argument("--group", default="cocoindex-review-context", help="Iggy consumer group the connector stores offsets in")
A = ap.parse_args()

IGGY = coco.ContextKey[IggyClient]("iggy")
PG_DB = coco.ContextKey[asyncpg.Pool]("review_context_db")


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
    async with asyncpg.create_pool(A.pg_url) as pool:
        builder.provide(PG_DB, pool)
        yield


stats = {"applied": 0, "read": 0, "offset": -1, "first": None}


@coco.fn
async def process_thread(msg, table: postgres.TableTarget[ReviewThread]) -> None:
    d = json.loads(msg.payload())
    table.declare_row(row=ReviewThread(
        thread_id=d["thread_id"], repo=d.get("repo", ""), pr=int(d.get("pr", 0)), path=d.get("path", ""),
        line=int(d.get("line", 0)), author=d.get("author", ""), comments=int(d.get("comments", 0)),
        body=d.get("body", ""), seq=int(d["seq"]), ns=int(d["ns"])))
    stats["applied"] += 1


def key(message):
    stats["read"] += 1
    stats["offset"] = message.offset()
    if stats["first"] is None:
        stats["first"] = message.offset()
    return key_of(message)


@coco.fn
async def app_main() -> None:
    table = await postgres.mount_table_target(
        PG_DB, table_name=A.table,
        table_schema=await postgres.TableSchema.from_class(ReviewThread, primary_key=["thread_id"]))
    # batch_length: the connector stores an offset only after the row is written, and under PollingStrategy.Next an
    # Iggy consumer stalls once it reads batch_length past the stored offset (apache-iggy 0.9 consumer docs), so the
    # batch must cover every change in flight; the connector's default of 100 caps it near 100 per write latency.
    # poll_interval: without one the consumer re-polls in a busy loop while it waits for offsets to be stored.
    threads = iggy.topic_as_map(coco.use_context(IGGY), A.group, STREAM, A.topic, key=key, is_deletion=is_deletion,
                                batch_length=A.batch, poll_interval=timedelta(milliseconds=10))
    await coco.mount_each(process_thread, threads, table)


app = coco.App(coco.AppConfig(name="IggyReviewContext"), app_main)


async def heartbeat():
    p = pathlib.Path(HEARTBEAT)
    while True:
        p.touch()
        await asyncio.sleep(0.1)


async def report(end: int):
    said = False
    while True:
        await asyncio.sleep(1)
        if stats["first"] is not None and not said:
            said = True
            print(f"[index] resumed at offset {stats['first']:,} (the last offset stored after a durable write); "
                  f"{max(0, end - stats['first']):,} changes to replay from Iggy", flush=True)
        print(f"[index] {stats['read']:>6,} read from Iggy  {stats['applied']:>6,} declared through CocoIndex  "
              f"at offset {stats['offset']:,}", flush=True)
        stats["read"] = stats["applied"] = 0


async def main():
    probe = IggyClient.from_connection_string(A.url)
    await probe.connect()
    while not await probe.get_topic(STREAM, A.topic):     # the review service creates it; CocoIndex never does
        await asyncio.sleep(0.5)
    end = await topic_end(probe, A.topic)
    print(f"[index] topic {STREAM}/{A.topic} ends at offset {end:,}; table {A.table} in Postgres", flush=True)
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
