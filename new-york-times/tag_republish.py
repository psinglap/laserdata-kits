"""A republish burst on the same lane: one tag changes, and every article that references it is republished.

The Publishing Pipeline publishes assets normalized (an image, a byline, a tag is its own asset) and the
denormalizer bundles each top-level asset with everything it references. When a referenced asset changes, every
top-level asset that references it is republished. A tag on thousands of older articles therefore turns one edit
into thousands of new versions, on the same lane, while editors keep publishing.

This script writes version 1 of --articles archive articles that carry one tag, then at --at seconds republishes
every one of them over --for seconds (version 2, then 3 on the next burst if --every is set). One writer per
partition, one batch per partition per tick. Run it while newsroom.py and serving_node.py run, and watch whether
editor publishes stay inside their budget while the burst drains.
"""
import argparse
import asyncio
import gc
import time

from apache_iggy import SendMessage

from common import DEFAULT_URL, LANE, PARTITIONS, STREAM, archive_uris, connect, ensure_topic, pack, partition_of


async def main(a):
    admin = await connect(a.url)
    await ensure_topic(admin, LANE, PARTITIONS)
    uris = archive_uris(a.articles)
    by_part = [[u for u in uris if partition_of(u) == p] for p in range(PARTITIONS)]
    version = {u: 1 for u in uris}
    writers = [await connect(a.url) for _ in range(PARTITIONS)]
    head = lambda u: f"Archive article {u[-6:]} (tag: {a.tag})"
    async def first(pid):
        mine = by_part[pid]
        for i in range(0, len(mine), 500):                      # version 1 in batches of 500
            await writers[pid].send_messages(STREAM, LANE, pid, [SendMessage(pack(u, 1, "publish", head(u), a.body_bytes))
                                                                for u in mine[i:i + 500]])

    await asyncio.gather(*(first(p) for p in range(PARTITIONS)))
    print(f"[tag-republish] version 1 of {len(uris):,} archive articles tagged '{a.tag}'; the tag changes at {a.at:g}s",
          flush=True)
    gc.freeze()
    t0 = time.monotonic()
    queue = []                                                  # (partition, uri) still to republish
    sent, last, next_burst, carry = 0, t0, a.at, 0.0
    due = t0
    while True:
        el = time.monotonic() - t0
        if el >= next_burst and not queue:
            queue = [(partition_of(u), u) for u in uris]
            next_burst = next_burst + a.every if a.every else float("inf")
            print(f"[tag-republish] tag '{a.tag}' updated: republishing {len(uris):,} articles over {a.for_s:g}s",
                  flush=True)
        if queue:
            carry += len(uris) / a.for_s * a.tick_ms / 1000
            k, carry = int(carry), carry - int(carry)
            take, queue = queue[:k], queue[k:]
            groups = {}
            for pid, u in take:
                version[u] += 1
                groups.setdefault(pid, []).append(SendMessage(pack(u, version[u], "republish", head(u), a.body_bytes)))
            if groups:                                          # one batch per partition, sent together
                await asyncio.gather(*(writers[pid].send_messages(STREAM, LANE, pid, ms) for pid, ms in groups.items()))
                sent += len(take)
        if time.monotonic() - last >= 1:
            tag = "  burst" if queue else ""
            print(f"[tag-republish] {sent:,} republishes/s{tag}", flush=True)
            sent, last = 0, time.monotonic()
        due += a.tick_ms / 1000                                 # from the due time, so a slow tick doesn't drift
        await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--articles", type=int, default=20000, help="archive articles that reference the tag")
    p.add_argument("--tag", default="Elections")
    p.add_argument("--at", type=float, default=30, help="the tag changes this many seconds in")
    p.add_argument("--for", dest="for_s", type=float, default=20, help="the denormalizer republishes over this long")
    p.add_argument("--every", type=float, default=0, help="change the tag again every this many seconds (0 = once)")
    p.add_argument("--body-bytes", type=int, default=4000)
    p.add_argument("--tick-ms", type=float, default=20)
    asyncio.run(main(p.parse_args()))
