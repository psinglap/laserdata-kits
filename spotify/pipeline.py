"""The video podcast publishing pipeline, simulated, as the source of truth for the lane.

Creators upload new episodes (the medium-priority queue) and updates to older episodes arrive (the low-priority
queue). A transcoding pool starts --capacity episodes a second; each takes about --transcode-s, then content
analysis takes about --analysis-s, then the episode goes live. At --spike-at a bulk delivery multiplies new
uploads by --spike-x for --spike-for seconds, more than the pool can start, so a backlog builds, as it did on
June 24, 2026. Creators whose episode is still not live after --patience-s re-upload the same file with
probability --reupload-share (once each).

Every stage change of every episode is written to the lane, keyed by episode, with the resulting stage and a
per-episode event number. One batch per partition per 10 ms tick, all sent at once on one connection.

The pipeline also prints the truth the status service is judged against: each second the real queue depths and
the real wait of the oldest queued episode in each queue, and the exact moment the oldest new episode's wait
crossed --threshold-s (and fell back under half of it), with the same rule the status service's watch uses.
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict, deque

from apache_iggy import Partitioning, SendMessage

from common import DEFAULT_URL, NEW, PARTITIONS, STATUS, STREAM, UPDATE, connect, ensure_topics, episode, msg, part


async def main(a):
    await ensure_topics(await connect(a.url), STATUS)
    writer = await connect(a.url)
    P = PARTITIONS[STATUS]
    rng = random.Random(a.seed)
    eps = {}                                  # key -> [priority, queued ns, n, stage]
    queues = {NEW: deque(), UPDATE: deque()}  # waiting for the transcoding pool, oldest first
    due_q = []                                # (due ns, key, next stage) for running work
    patience = deque()                        # (queued ns, key) of new uploads, to decide re-uploads
    uploads = 0
    acc = {NEW: 0.0, UPDATE: 0.0, "start": 0.0}
    st = defaultdict(int)
    over = [False]
    t0 = time.monotonic()
    gc.freeze()                               # keep start-up objects out of the collector's way

    def emit(key, kind, batches, dup=0):
        r = eps[key]
        r[2] += 1
        m = msg(e=key, st=r[3], pr=r[0], q=r[1], n=r[2], k=kind, dup=dup)
        p = part(key, P)
        batches[p].append(SendMessage(m))
        st["events"] += 1
        if a.dup_rate and rng.random() < a.dup_rate:
            batches[p].append(SendMessage(m))  # delivered twice, same event number
            st["dups"] += 1

    def upload(pr, now_ns, batches):
        nonlocal uploads
        uploads += 1
        key = episode(rng.randrange(a.shows), uploads)
        while key in eps:                     # a digest collision in the illustration; real digests don't collide
            uploads += 1
            key = episode(rng.randrange(a.shows), uploads)
        eps[key] = [pr, now_ns, 0, 0]
        queues[pr].append(key)
        if pr == NEW:
            patience.append((now_ns, key))
        emit(key, "stage", batches)          # received and queued
        st[pr] += 1

    def oldest(pr, now_ns):
        q = queues[pr]
        return (now_ns - eps[q[0]][1]) / 1e9 if q else 0.0

    due = time.monotonic()
    mark = time.monotonic()
    while True:
        now_ns = time.time_ns()
        el = time.monotonic() - t0
        if a.duration and el > a.duration:
            break
        batches = defaultdict(list)
        dt = a.tick_ms / 1000
        spike = a.spike_at <= el < a.spike_at + a.spike_for
        acc[NEW] += a.new_per_s * (a.spike_x if spike else 1) * dt
        acc[UPDATE] += a.update_per_s * dt
        for pr in (NEW, UPDATE):
            while acc[pr] >= 1:
                acc[pr] -= 1
                upload(pr, now_ns, batches)
        # The transcoding pool starts up to --capacity episodes a second.
        acc["start"] = min(acc["start"] + a.capacity * dt, a.capacity * dt * 2)
        turn = 0
        while acc["start"] >= 1 and (queues[NEW] or queues[UPDATE]):
            if a.policy == "new-first":
                pr = NEW if queues[NEW] else UPDATE
            else:                             # share: take from each queue in turn
                pr = (NEW, UPDATE)[turn % 2] if queues[NEW] and queues[UPDATE] else (NEW if queues[NEW] else UPDATE)
                turn += 1
            key = queues[pr].popleft()
            acc["start"] -= 1
            eps[key][3] = 1
            emit(key, "stage", batches)      # transcoding
            st["started"] += 1
            heapq.heappush(due_q, (now_ns + int(a.transcode_s * rng.uniform(0.6, 1.4) * 1e9), key, 2))
        while due_q and due_q[0][0] <= now_ns:
            _, key, stage = heapq.heappop(due_q)
            eps[key][3] = stage
            emit(key, "stage", batches)
            if stage == 2:                    # transcoded -> content analysis
                heapq.heappush(due_q, (now_ns + int(a.analysis_s * rng.uniform(0.6, 1.4) * 1e9), key, 3))
            elif stage == 3:                  # analysed -> live
                heapq.heappush(due_q, (now_ns + int(0.2e9), key, 4))
            else:
                st["live"] += 1
                del eps[key]
        # Creators whose new episode is still not live after --patience-s re-upload the same file, once.
        while patience and patience[0][0] + int(a.patience_s * 1e9) <= now_ns:
            _, key = patience.popleft()
            if key in eps and eps[key][3] < 4 and rng.random() < a.reupload_share:
                emit(key, "reupload", batches, dup=1)
                st["reuploads"] += 1
        # Truth: the oldest new episode's wait against the line, with the watch's rule (over above the line,
        # clear below half of it). The crossing moment is exact: queued time of the oldest plus the line.
        w = oldest(NEW, now_ns)
        if not over[0] and w > a.threshold_s:
            over[0] = True
            cross = eps[queues[NEW][0]][1] / 1e9 + a.threshold_s
            print(f"[truth] new-episode backlog crossed {a.threshold_s:g}s at {cross:.3f}, {len(queues[NEW]):,} waiting",
                  flush=True)
        elif over[0] and w < a.threshold_s / 2:
            over[0] = False
            print(f"[truth] new-episode backlog back under {a.threshold_s / 2:g}s at {now_ns / 1e9:.3f}", flush=True)
        if batches:
            await asyncio.gather(*(writer.send_messages(STREAM, STATUS, Partitioning.partition_id(p), ms)
                                   for p, ms in batches.items()))
        if time.monotonic() - mark >= 1:
            mark += 1
            print(f"[pipeline] uploads {st[NEW] + st[UPDATE]:>4,}/s (new {st[NEW]:,}, updates {st[UPDATE]:,})"
                  f"{'  BULK DELIVERY' if spike else ''}  re-uploads {st['reuploads']:,}/s  started {st['started']:,}/s  "
                  f"live {st['live']:,}/s  waiting new {len(queues[NEW]):,} (oldest {oldest(NEW, now_ns):.1f}s) "
                  f"updates {len(queues[UPDATE]):,} (oldest {oldest(UPDATE, now_ns):.1f}s)  events {st['events']:,}/s  "
                  f"sent twice {st['dups']:,}", flush=True)
            st.clear()
        due += dt                             # from the due time, so a slow tick doesn't drift the schedule
        await asyncio.sleep(max(0.0, due - time.monotonic()))
    print("[pipeline] done", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--shows", type=int, default=200_000, help="shows uploading (illustrative)")
    p.add_argument("--new-per-s", type=float, default=400, help="new episode uploads a second, before the spike")
    p.add_argument("--update-per-s", type=float, default=150, help="updates to older episodes a second (low priority)")
    p.add_argument("--capacity", type=float, default=800, help="episodes the transcoding pool starts a second")
    p.add_argument("--policy", choices=("new-first", "share"), default="new-first",
                   help="new-first: the pool takes new episodes before updates; share: from each queue in turn")
    p.add_argument("--transcode-s", type=float, default=120, help="mean transcoding time per episode")
    p.add_argument("--analysis-s", type=float, default=30, help="mean content analysis time per episode")
    p.add_argument("--spike-at", type=float, default=60, help="seconds after start of the bulk delivery")
    p.add_argument("--spike-for", type=float, default=300, help="seconds the bulk delivery lasts")
    p.add_argument("--spike-x", type=float, default=4, help="new uploads multiplied by this during the bulk delivery")
    p.add_argument("--patience-s", type=float, default=900, help="a creator re-uploads after this long without going live")
    p.add_argument("--reupload-share", type=float, default=0.1, help="share of waiting creators who re-upload")
    p.add_argument("--threshold-s", type=float, default=300, help="the line for the oldest new episode's wait")
    p.add_argument("--dup-rate", type=float, default=0.005, help="share of stage changes delivered twice")
    p.add_argument("--tick-ms", type=float, default=10)
    p.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    p.add_argument("--seed", type=int, default=7)
    asyncio.run(main(p.parse_args()))
