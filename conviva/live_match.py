"""Live channels with ad breaks, the viewer sessions watching them, and the consumers that read every ad alert.

Plays --sessions viewer sessions across --channels live channels in one process. Every session on a channel enters
the channel's ad break together (a live break is the same moment for everyone watching), and each session's
player or ad stitcher publishes to ad-events, keyed by session:
  * break_start, then for each of --slots slots an ad attempt and its outcome: an impression (the creative
    started), an ad start failure (response failure, creative media failure) or a slate, then break_end.
Every record carries the session's ad server, market and device, the break and slot, the break's counts so far
and the session's event sequence. Outcome mix and delays are illustrative (set them to yours in outcome()).

The fault: from --fault-at for --fault-for seconds, one ad server stops answering in one market
(--fault-server x --fault-market): a share of its attempts end in a response failure when the player's ad request
times out after --ad-timeout-s.

--consumers readers play the services that act on ad-alerts (the ad ops console, webhooks, agents), each on its
own connection, reading the alerts partition directly from its own position.

The business numbers: of the breaks in the failing cohort that lost a slot to the fault, how many were flagged at
the ad ops console while the break was still running; and of the failing cohort's ad requests during the fault,
how many came after the console had the alert, so they could have gone to a backup ad server. Prints them every
second, next to event-to-consumers p50/p99 (the whole loop: ad event, checker,
alert, consumer).
"""
import argparse
import asyncio
import gc
import heapq
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (AD_EVENTS, ALERTS, CREATIVE_FAILURE, DEFAULT_URL, PARTITIONS, RESPONSE_FAILURE, SLATE, STREAM,
                    UNFILLED, Stalls, Window, cohort_of, ensure_topics, load, msg, part, session)

CONSUMERS = ["ad-ops-console", "webhooks", "agents", "reports", "partner-feed"]


def outcome(a, faulted: bool):
    """(outcome, seconds after the attempt). Illustrative mix and delays; set them to yours."""
    if faulted and random.random() < a.fault_share:
        return RESPONSE_FAILURE, a.ad_timeout_s + random.uniform(0, 0.1)     # the ad request timed out
    r = random.random()
    if r < 0.03:
        return SLATE, random.uniform(0.1, 0.4)
    if r < 0.05:
        return CREATIVE_FAILURE, random.uniform(0.5, 1.5)
    if r < 0.06:
        return RESPONSE_FAILURE, a.ad_timeout_s + random.uniform(0, 0.1)
    return "impression", random.uniform(0.2, 0.8)


class Session:
    __slots__ = ("s", "srv", "mkt", "dev", "ch", "seq", "br", "att", "imp", "unf", "hit")

    def __init__(self, i, channels):
        self.s = session(i)
        self.srv, self.mkt, self.dev = cohort_of(self.s)
        self.ch = i % channels
        self.seq = self.br = self.att = self.imp = self.unf = 0
        self.hit = False                    # this break lost a slot to the fault


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), AD_EVENTS, ALERTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, AD_EVENTS, topic_partitions_count=PARTITIONS[AD_EVENTS],
                                                   mode=cfg)
    start_ns = time.time_ns()
    sessions = [Session(i, a.channels) for i in range(a.sessions)]
    pid = [part(x.s, PARTITIONS[AD_EVENTS]) for x in sessions]
    by_ch = defaultdict(list)
    for i, x in enumerate(sessions):
        by_ch[x.ch].append(i)
    t0 = time.monotonic()
    # Each channel's breaks come every --cycle-s seconds; channels are staggered, as live events on different
    # channels are, so some channel is always in a break.
    due = [(t0 + 1 + c * a.cycle_s / a.channels, -1, "pod", c) for c in range(a.channels)]
    heapq.heapify(due)
    fault_cohort = (a.fault_server, a.fault_market)
    fault_ns = start_ns + int(a.fault_at * 1e9)
    alert = {"rx": 0, "src": 0}             # first fire for the failing cohort at the console: received, raised by
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    seen = set()                            # (cohort, alert, state) the console already has

    def reader(name):
        def on_alert(m):
            d = load(m.payload())
            now = time.time_ns()
            if d["src_ns"] > start_ns:
                lat.add((now - d["src_ns"]) / 1e6)  # live alerts only, not ones replayed at start-up
            key = (d["srv"], d["mkt"], d["alert"], d["state"])
            if name != "ad-ops-console" or key in seen:
                return                      # repeated alert (at-least-once): already counted
            seen.add(key)
            stats[d["state"]] += 1
            c = (d["srv"], d["mkt"])
            if c != fault_cohort:
                stats["other_fire"] += d["state"] == "fire"
                return
            if d["state"] == "fire" and not alert["rx"] and d["src_ns"] >= fault_ns:
                alert["rx"], alert["src"] = now, d["src_ns"]
                print(f"[live-match] ALERT {c[0]} x {c[1]} at the ad ops console "
                      f"{(now - fault_ns) / 1e6:,.0f} ms after the fault began: unfilled share {d['share']:.0%} of "
                      f"{d['slots']} slots, causes {d['causes']}, devices {', '.join(d['devices'])}; "
                      f"checker-to-console {(now - d['src_ns']) / 1e6:.1f} ms after the event that tipped it",
                      flush=True)
            elif d["state"] == "clear":
                print(f"[live-match] CLEAR {c[0]} x {c[1]} at the ad ops console: unfilled share "
                      f"{d['share']:.0%} of {d['slots']} slots", flush=True)
        return on_alert

    async def consume(name):
        # One connection per consumer, reading the alerts partition directly from its own position (no consumer
        # group, nothing stored on the broker per poll); a full batch means more is waiting, so no pause.
        c, cb = await own_client(), reader(name)
        who, nxt = Consumer.Single(f"{name}-{os.getpid()}"), None
        while True:
            ms = await c.poll_messages(STREAM, ALERTS, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    cb(m)
            if len(ms) < 1000:
                await asyncio.sleep(a.poll_ms / 1000)

    gc.freeze()                             # keep start-up objects out of the collector's way

    def event(x, k, **extra):
        x.seq += 1
        return SendMessage(msg(s=x.s, seq=x.seq, k=k, srv=x.srv, mkt=x.mkt, dev=x.dev, ch=x.ch, br=x.br,
                               att=x.att, imp=x.imp, unf=x.unf, **extra))

    async def players():
        tick = time.monotonic()
        async with producer:
            while True:
                now = time.monotonic()
                now_ns = time.time_ns()
                faulting = a.fault_at <= now - t0 < a.fault_at + a.fault_for
                batches = defaultdict(list)
                while due and due[0][0] <= now:
                    t, i, kind, arg = heapq.heappop(due)
                    if kind == "pod":       # channel `arg` goes to an ad break: every session on it, together
                        heapq.heappush(due, (t + a.cycle_s, -1, "pod", arg))
                        for j in by_ch[arg]:
                            heapq.heappush(due, (t + random.uniform(0, 0.5), j, "break_start", 0))
                            for sl in range(a.slots):
                                heapq.heappush(due, (t + sl * a.slot_s + random.uniform(0, 1.5), j, "attempt", sl))
                            heapq.heappush(due, (t + a.slots * a.slot_s + random.uniform(0, 0.5), j, "break_end", 0))
                        stats["pods"] += 1
                        continue
                    x = sessions[i]
                    if kind == "break_start":
                        x.br += 1
                        x.att = x.imp = x.unf = 0
                        x.hit = False
                        batches[pid[i]].append(event(x, "break_start"))
                    elif kind == "attempt":
                        x.att += 1
                        stats["attempts"] += 1
                        faulted = faulting and (x.srv, x.mkt) == fault_cohort
                        if faulted:         # could this request go to a backup ad server yet?
                            stats["fc_att"] += 1
                            stats["ahead"] += bool(alert["rx"]) and alert["rx"] <= now_ns
                            stats["ahead_cmp"] += bool(alert["src"]) and alert["src"] + a.compare_s * 1e9 <= now_ns
                        out, after = outcome(a, faulted)
                        heapq.heappush(due, (t + after, i, out, (arg, faulted)))
                        batches[pid[i]].append(event(x, "attempt", sl=arg))
                    elif kind == "break_end":
                        batches[pid[i]].append(event(x, "break_end"))
                        if x.hit:           # a break in the failing cohort that lost a slot to the fault
                            stats["fault_breaks"] += 1
                            if alert["rx"] and alert["rx"] <= now_ns:
                                stats["live"] += 1          # flagged while this break was still running
                    else:                   # a slot outcome
                        sl, faulted = arg
                        if kind == "impression":
                            x.imp += 1
                        else:
                            x.unf += 1
                            stats[kind] += 1
                            if faulted and kind == RESPONSE_FAILURE:
                                x.hit = True
                                stats["fault_slots"] += 1
                        batches[pid[i]].append(event(x, kind, sl=sl))
                    stats["events"] += 1
                # One batch per partition, all sent at once: per-session order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["faulting"] = faulting
                tick += 0.01                # from its due time, so a slow tick doesn't drift the schedule
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[live-match] {a.sessions:,} sessions on {a.channels} channels, a break of {a.slots} x {a.slot_s:g} s "
              f"slots every {a.cycle_s:g} s per channel; fault: {a.fault_server} x {a.fault_market} from "
              f"{a.fault_at:g} s for {a.fault_for:g} s; {a.consumers} consumers", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "fault" if stats["faulting"] else "normal"
            unf = int(stats[RESPONSE_FAILURE] + stats[CREATIVE_FAILURE] + stats[SLATE])
            print(f"[live-match] {int(stats['events']):>6,} events/s ({phase})  ad attempts {int(stats['attempts']):,}  "
                  f"unfilled {unf:,} (response failures {int(stats[RESPONSE_FAILURE]):,}, of them the fault "
                  f"{int(stats['fault_slots']):,})  failing-cohort breaks that lost a slot, ended "
                  f"{int(stats['fault_breaks']):,}: flagged while the break was still running {int(stats['live']):,}  "
                  f"failing-cohort ad requests "
                  f"{int(stats['fc_att']):,}, after the console had the alert {int(stats['ahead']):,} (at "
                  f"{a.compare_s:g} s: {int(stats['ahead_cmp']):,})  alerts at the console: fire "
                  f"{int(stats['fire']):,}, clear {int(stats['clear']):,}, other cohorts {int(stats['other_fire']):,}  "
                  f"event-to-consumers n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  {stalls.read()}",
                  flush=True)
            stats.clear()

    await asyncio.gather(players(), report(), stalls.watch(), *(consume(n) for n in CONSUMERS[: a.consumers]))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--sessions", type=int, default=40_000, help="viewer sessions played by this process")
    p.add_argument("--channels", type=int, default=8, help="live channels, breaks staggered across them")
    p.add_argument("--slots", type=int, default=4, help="ad slots per break")
    p.add_argument("--slot-s", type=float, default=30, help="seconds per slot")
    p.add_argument("--cycle-s", type=float, default=600, help="seconds from one break to the next on a channel")
    p.add_argument("--ad-timeout-s", type=float, default=2, help="player's ad request timeout")
    p.add_argument("--fault-server", default="ad-server-b")
    p.add_argument("--fault-market", default="dallas")
    p.add_argument("--fault-at", type=float, default=60, help="seconds after start")
    p.add_argument("--fault-for", type=float, default=300, help="seconds")
    p.add_argument("--fault-share", type=float, default=0.7, help="share of the failing cohort's attempts that time out")
    p.add_argument("--consumers", type=int, default=3, help="services reading ad-alerts, each with its own reader")
    p.add_argument("--poll-ms", type=float, default=1, help="consumer wait between polls when nothing is waiting")
    p.add_argument("--compare-s", type=float, default=30,
                   help="also count the failing cohort's ad requests a path this many seconds slower would have flagged")
    asyncio.run(main(p.parse_args()))
