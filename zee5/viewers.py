"""The source of truth: viewers watching ZEE5 titles, each player sending a playback heartbeat.

Every playing viewer sends a heartbeat every --every seconds (ZEE5's clients send one every
60 s today), keyed by viewer ID, with a per-viewer sequence number and the position itself
(never a delta). When a viewer stops, the player sends a final "stop" heartbeat with the exact
position, unless the app is closed or the network drops first (--no-stop-p): then the last
periodic heartbeat is all the platform has.

A few seconds later the viewer opens the same title on another device (TV to phone, phone to
web) and asks the Continue Watching service where to resume. viewers.py knows exactly where the
viewer stopped, so it checks every answer: did playback resume at the stop point, or from an
earlier heartbeat (and how many seconds the viewer has to watch again)? And was the answer built
on the latest heartbeat the player had sent, or on one the lane had not delivered yet?
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, IggyExpiry, Partitioning, SendMessage

from common import (DEFAULT_URL, DEVICES, HEARTBEATS, RESUME_ANS, RESUME_REQ, STREAM, Window, load, msg, part,
                    read_partitions, viewer)

TICK = 0.02   # seconds; each viewer still beats on its own schedule, stamped when sent
IDLE, PLAYING, SWITCHING = 0, 1, 2


class Viewer:
    __slots__ = ("id", "idx", "state", "asset", "dev", "seq", "start_t", "start_pos", "stopping")

    def __init__(self, i, first=0):
        self.id, self.idx = viewer(first + i), i
        self.state, self.asset, self.dev = IDLE, None, random.choice(DEVICES)
        self.seq, self.start_t, self.start_pos, self.stopping = 0, 0.0, 0.0, False

    def pos(self, now):
        return round(self.start_pos + (now - self.start_t), 1)


async def main(a):
    async def connected():
        # One connection per writer and reader, so a busy one never queues behind another.
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    admin = await connected()
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=500))
    hours = IggyExpiry.ExpireDuration(timedelta(hours=a.keep_hours)) if a.keep_hours > 0 else None
    hb_p = await (await connected()).producer(STREAM, HEARTBEATS, topic_partitions_count=a.partitions, mode=cfg,
                                              topic_message_expiry=hours)
    req_p = await (await connected()).producer(STREAM, RESUME_REQ, topic_partitions_count=8, mode=cfg)
    if await admin.get_topic(STREAM, RESUME_ANS) is None:   # continue_watching.py may not have created it yet
        try:
            await admin.create_topic(STREAM, RESUME_ANS, 8)
        except RuntimeError:
            pass                                             # created by continue_watching.py in the meantime
    answers = await connected()             # direct partition reads, offsets stored once a second
    answer_parts = range((await admin.get_topic(STREAM, RESUME_ANS)).partitions_count)

    # A catalogue with a long tail: a few titles carry most of the viewing, like a weekly top-shows list.
    titles = [f"T{t:05d}" for t in range(a.titles)]
    weights = [1 / (r + 1) for r in range(a.titles)]
    pool = [Viewer(i, a.first) for i in range(int(a.viewers * max(1.0, a.peak)))]
    slots = max(1, round(a.every / TICK))
    ring = [[] for _ in range(slots)]                       # each viewer beats in its own slot, staggered
    for v in pool:
        ring[v.idx % slots].append(v.idx)
    events, n_ev = [], 0                                    # (due, n, kind, idx): stops and resumes
    pending = {}                                            # request ID -> (viewer idx, true stop position, seq sent)
    stats, answer_ms = defaultdict(float), Window()
    online, playing = [a.viewers], [0]
    gc.freeze()                                             # the audience lives all run; keep it out of later GC passes

    def schedule(due, kind, idx):
        nonlocal n_ev
        n_ev += 1
        heapq.heappush(events, (due, n_ev, kind, idx))

    async def send_by_partition(producer, items, partitions):
        # One batch per partition, all sent at once: per-viewer order kept, about one round trip per tick.
        batches = defaultdict(list)
        for key, m in items:
            batches[part(key, partitions)].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in batches.items()))

    def beat(v, now, ev, now_ns):
        v.seq += 1
        return v.id, SendMessage(msg(k=v.id, seq=v.seq, ev=ev, asset=v.asset, pos=v.pos(now), dev=v.dev, ns=now_ns))

    async def play():
        n_req = 0
        async with hb_p, req_p:
            t0 = due = time.monotonic()
            tick = 0
            while True:
                now, now_ns = time.monotonic(), time.time_ns()
                peak = a.peak if a.peak_at and a.peak_at <= now - t0 < a.peak_at + a.peak_for else 1.0
                online[0] = int(a.viewers * peak)
                beats, reqs = [], []
                while events and events[0][0] <= now:
                    _, _, kind, idx = heapq.heappop(events)
                    v = pool[idx]
                    if kind == "stop":
                        true_pos, v.stopping = v.pos(now), False
                        if random.random() >= a.no_stop_p:
                            beats.append(beat(v, now, "stop", now_ns))      # the player's last word
                        v.state = SWITCHING if idx < online[0] else IDLE   # after the peak, some just log off
                        playing[0] -= 1
                        if v.state == SWITCHING:
                            schedule(now + random.uniform(*a.switch_s), "resume", idx)
                            v.start_pos = true_pos
                    else:                                                   # opens the title on another device
                        n_req += 1
                        rid = f"R{n_req:010d}"
                        v.dev = random.choice([d for d in DEVICES if d != v.dev])
                        pending[rid] = (idx, v.start_pos, v.seq, now_ns)
                        reqs.append((v.id, SendMessage(msg(k=v.id, rid=rid, asset=v.asset, dev=v.dev, ns=now_ns))))
                for idx in ring[tick % slots]:
                    v = pool[idx]
                    if v.state == IDLE:
                        if idx >= online[0]:
                            continue
                        v.state, v.asset = PLAYING, random.choices(titles, weights)[0]
                        v.start_t, v.start_pos = now, float(random.randrange(0, 2400))
                        playing[0] += 1
                    if v.state != PLAYING:
                        continue
                    beats.append(beat(v, now, "hb", now_ns))
                    if not v.stopping and random.random() < a.end_p:        # stops somewhere before the next beat
                        v.stopping = True
                        schedule(now + random.uniform(0, a.every), "stop", idx)
                stats["beats"] += len(beats)
                await asyncio.gather(send_by_partition(hb_p, beats, a.partitions), send_by_partition(req_p, reqs, 8))
                tick += 1
                due += TICK
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def on_answer(m):
        r = load(m.payload())
        p = pending.pop(r["rid"], None)
        if p is None:
            return                                          # an answer to an earlier run
        idx, true_pos, seq_sent, req_ns = p
        answer_ms.add((time.time_ns() - req_ns) / 1e6)
        stats["resumes"] += 1
        if r.get("pos") is None:
            stats["missing"] += 1
            served = 0.0
        else:
            served = r["pos"]
            if r["seq"] < seq_sent:
                stats["lane_late"] += 1                     # the player had sent a newer heartbeat than the one used
        rewatch = max(0.0, true_pos - served)
        stats["exact" if rewatch < 0.5 else "earlier"] += 1
        stats["rewatch_sum"] += rewatch
        stats["rewatch_max"] = max(stats["rewatch_max"], rewatch)
        v = pool[idx]
        v.state, v.start_t, v.start_pos = PLAYING, time.monotonic(), served   # playback picks up where it was told
        playing[0] += 1

    async def report():
        while True:
            await asyncio.sleep(1)
            n, r = stats["resumes"], answer_ms.flush()
            pct = lambda k: 100 * stats[k] / n if n else 0.0
            print(f"[viewers] {int(stats['beats']):>7,} heartbeats/s  watching {playing[0]:,}  resumes {int(n):,}  "
                  f"at the stop point {pct('exact'):6.2f}%  from an earlier heartbeat {pct('earlier'):5.2f}%  "
                  f"rewatch avg {stats['rewatch_sum'] / n if n else 0:4.1f}s max {stats['rewatch_max']:4.1f}s  "
                  f"lane-late {int(stats['lane_late'])}  no position {int(stats['missing'])}  "
                  f"resume answer p99 {r['p99']:5.1f}ms", flush=True)
            stats.clear()

    await asyncio.gather(play(), read_partitions(answers, RESUME_ANS, answer_parts, "viewers-resume-check", on_answer,
                                                 commit_s=1, batch=2000, poll_ms=1), report())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--viewers", type=int, default=200_000, help="viewers playing at once, for this process")
    ap.add_argument("--every", type=float, default=10.0, help="seconds between heartbeats per viewer (60 today)")
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--first", type=int, default=0, help="first viewer number, to run several processes side by side")
    ap.add_argument("--titles", type=int, default=5000)
    ap.add_argument("--end-p", type=float, default=0.005, help="chance a viewer stops before the next heartbeat")
    ap.add_argument("--no-stop-p", type=float, default=0.15, help="share of stops with no final heartbeat (app closed)")
    ap.add_argument("--switch-s", type=float, nargs=2, default=(2, 20), help="seconds before resuming on another device")
    ap.add_argument("--keep-hours", type=float, default=24, help="heartbeat retention on the lane")
    ap.add_argument("--peak", type=float, default=1.0, help="multiplier for viewers during the peak")
    ap.add_argument("--peak-at", type=float, default=0)
    ap.add_argument("--peak-for", type=float, default=120)
    asyncio.run(main(ap.parse_args()))
