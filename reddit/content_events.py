"""Preliminary enriched content and the ML signals about it, keyed by content ID, into the signal lane.

Plays two sides at once so the kit runs on a laptop:
  * the enrichment service, which writes every new post and comment, with its basic context, to the
    preliminary stream (content-preliminary): content ID, kind, subreddit, whether it has media, and the
    ML signals expected for it;
  * the independent ML services, which score that content after their own inference time and each write
    a signal to their own topic (signal-text-harm, signal-spam, signal-media-safety).
Each signal is written once its inference delay (lognormal around the median in common.SIGNALS) has
passed. A share of signals comes late (--late-share: a model backed up for 6 to 15 s) and a share never
comes (--drop-share: a scoring call that failed), which is what the joiner's wait limit is for. A share of
content (--bad-share) gets one high score, which the toy REV2 rule acts on.

Every message carries the content ID, the time it was written (ns) and, for content, seq 1; a signal is
identified by (content ID, signal name), so a repeat is dropped by the joiner. All topics use the same
partition function (crc32 of the content ID), so content and its signals share a partition number.
"""
import argparse
import asyncio
import gc
import heapq
import math
import random
import time
from collections import defaultdict

from apache_iggy import Partitioning, SendMessage

from common import (DEFAULT_URL, PRELIM_TOPIC, SIGNALS, STREAM, connect, content_id, ensure_all, expected, msg,
                    part, signal_topic)

SUBREDDITS = [f"r/sub{n:04d}" for n in range(2000)]     # illustrative


async def main(a):
    admin = await connect(a.url)
    parts = await ensure_all(admin, a.partitions)
    w_content = await connect(a.url)                     # one connection for content, one for signals
    w_signals = await connect(a.url)
    rnd = random.Random(a.seed)
    due = []                                             # (due time, tie, signal, content ID, score)
    stats = defaultdict(int)
    n, tie, owed = 0, 0, 0.0
    peak_until = time.monotonic() + a.peak_s
    print(f"[content] writing to '{STREAM}' ({parts} partitions per topic): {a.rate:,.0f} posts and comments/s"
          f"{f' ({a.peak:g}x for the first {a.peak_s:g} s)' if a.peak != 1 else ''}, signals "
          f"{', '.join(SIGNALS)}; {a.late_share:.1%} of signals late, {a.drop_share:.1%} never sent", flush=True)
    gc.freeze()                                          # keep the collector from pausing the sender

    def infer_s(sig):
        d = SIGNALS[sig]["median_ms"] / 1000 * math.exp(rnd.gauss(0, a.sigma))
        return min(d, a.max_infer_s)

    async def run():
        nonlocal n, tie, owed
        tick = time.monotonic()
        while True:
            now = time.monotonic()
            rate = a.rate * (a.peak if now < peak_until else 1)
            owed += rate * a.tick_ms / 1000
            content = defaultdict(list)
            ns = time.time_ns()
            for _ in range(int(owed)):
                owed -= 1
                n += 1
                kind = "post" if rnd.random() < a.post_share else "comment"
                media = int(kind == "post" and rnd.random() < a.media_share)
                cid = content_id(n, kind)
                exp = expected(kind, media)
                content[part(cid, parts)].append(SendMessage(msg(
                    ns=ns, id=cid, seq=1, kind=kind, sr=rnd.choice(SUBREDDITS), media=media, exp=exp)))
                stats["content"] += 1
                bad = rnd.choice(exp) if rnd.random() < a.bad_share else None
                for sig in exp:
                    r = rnd.random()
                    if r < a.drop_share:
                        stats["dropped"] += 1
                        continue                         # this scoring call failed: the signal never comes
                    delay = rnd.uniform(6, 15) if r < a.drop_share + a.late_share else infer_s(sig)
                    score = round(rnd.uniform(0.975, 0.999) if sig == bad else rnd.betavariate(1.2, 12), 3)
                    tie += 1
                    heapq.heappush(due, (now + delay, tie, sig, cid, score))
            signals = defaultdict(lambda: defaultdict(list))
            while due and due[0][0] <= now:
                _, _, sig, cid, score = heapq.heappop(due)
                signals[sig][part(cid, parts)].append(SendMessage(msg(ns=ns, id=cid, sig=sig, score=score)))
                stats["signals"] += 1
            # One batch per partition per topic per tick, content and signals on their own connections.
            sends = [w_content.send_messages(STREAM, PRELIM_TOPIC, Partitioning.partition_id(p), ms)
                     for p, ms in content.items()]
            sends += [w_signals.send_messages(STREAM, signal_topic(s), Partitioning.partition_id(p), ms)
                      for s, by in signals.items() for p, ms in by.items()]
            if sends:
                await asyncio.gather(*sends)
            tick = max(tick + a.tick_ms / 1000, time.monotonic() - 0.1)   # never bank more than 100 ms
            await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            peak = " (peak)" if time.monotonic() < peak_until else ""
            print(f"[content] {stats['content']:>6,} posts and comments/s{peak}  {stats['signals']:>6,} signals/s  "
                  f"{len(due):>6,} signals still being scored  {stats['dropped']:>3,} scoring calls failed",
                  flush=True)
            stats.clear()

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rate", type=float, default=2_000,
                   help="posts and comments a second (Reddit's enrichment service: ~600/s in normal traffic, 2024)")
    p.add_argument("--peak", type=float, default=3.0, help="multiplier for the first --peak-s seconds")
    p.add_argument("--peak-s", type=float, default=60)
    p.add_argument("--post-share", type=float, default=0.3, help="share of content that is posts (the rest comments)")
    p.add_argument("--media-share", type=float, default=0.45, help="share of posts with an image or video")
    p.add_argument("--bad-share", type=float, default=0.02, help="share of content with one high ML score")
    p.add_argument("--late-share", type=float, default=0.01, help="share of signals that come 6 to 15 s late")
    p.add_argument("--drop-share", type=float, default=0.002, help="share of signals that never come")
    p.add_argument("--sigma", type=float, default=0.5, help="spread of inference delay (lognormal sigma)")
    p.add_argument("--max-infer-s", type=float, default=4.0, help="cap on a normal inference delay")
    p.add_argument("--partitions", type=int, default=32, help="partitions for new lane topics")
    p.add_argument("--tick-ms", type=float, default=50, help="write a batch per partition this often")
    p.add_argument("--seed", type=int, default=7)
    asyncio.run(main(p.parse_args()))
