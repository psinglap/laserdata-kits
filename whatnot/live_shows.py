"""Clients joining livestreams and bidding, the story events they emit, and the consumers that read every SLO alert.

Plays --clients clients (ios, android and web) in one process. Each second, --joins clients tap a livestream tile
and --bids clients tap Bid; each action is one story with a random story key, and every event of it goes to
story-events, keyed by story key, the way the events ingestion service forwards client and backend events:
  * livestream_join: livestream_tile_tap (trigger), join_authorized (step), then video_playing, auction_shown and
    chat_visible (outcomes), or seller_blocked (an expected failure: the seller blocked this user);
  * place_bid: bid_tap (trigger), bid_accepted (step), bid_placed_shown (outcome), or auction_ended (expected).
Outcome mix and delays are illustrative (set them to yours in join() and bid()). A share of events (--dup-share)
is delivered twice, as an at-least-once pipeline does.

The fault: from --fault-at for --fault-for seconds, newly loaded web clients stop playing video (a black screen):
--fault-share of web joins never emit video_playing, while auctions and chat still arrive and every backend
component stays healthy.

--consumers readers play the services that act on slo-alerts (the pager, the Datadog exporter, the incident bot),
each on its own connection, reading the alerts partition directly from its own position.

The business numbers, printed every second: web joins with no video during the fault, and how many of them came
before the pager had the alert; when the pager had it, measured from the fault's start; and when a judge that runs
once a minute over the previous full minute of deadlines would have had it, from the same stories.
"""
import argparse
import asyncio
import gc
import heapq
import itertools
import os
import random
import time
import zlib
from collections import defaultdict
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, PARTITIONS, STORY_EVENTS, STREAM, SURFACES, VERDICTS, Stalls, Window,
                    ensure_topics, load, msg, part)

CONSUMERS = ["pager", "datadog-exporter", "incident-bot", "status-page", "eng-dashboard"]


def surface_of(client: str) -> str:
    """The client's surface. The same in every script, so the kit needs no lookup (illustrative mix)."""
    h = zlib.crc32(client.encode()) % 100
    return "ios" if h < 45 else "android" if h < 75 else "web"


def join(a, broken: bool):
    """[(role, seconds after the tap)] for one livestream join. Illustrative mix and delays; set them to yours."""
    if random.random() < 0.002:
        return [("seller_blocked", random.uniform(0.1, 0.3))]
    ev = [("join_authorized", random.uniform(0.05, 0.3)), ("auction_shown", random.uniform(0.3, 1.2)),
          ("chat_visible", random.uniform(0.3, 1.0))]
    r = random.random()
    if broken or r < 0.003:
        pass                                # black screen: video never starts, so no outcome event for it
    elif r < 0.005:
        ev.append(("video_playing", a.deadline_s + random.uniform(1, 4)))      # a slow start, past the deadline
    else:
        ev.append(("video_playing", random.uniform(0.6, 2.5)))
    if random.random() < 0.001:
        ev = [e for e in ev if e[0] != "chat_visible"]
    return ev


def bid(a):
    """[(role, seconds after the tap)] for one bid. Illustrative; set it to yours."""
    if random.random() < 0.01:
        return [("auction_ended", random.uniform(0.05, 0.2))]
    ev = [("bid_accepted", random.uniform(0.03, 0.15))]
    if random.random() > 0.002:
        ev.append(("bid_placed_shown", random.uniform(0.08, 0.4)))
    return ev


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), STORY_EVENTS, VERDICTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, STORY_EVENTS,
                                                   topic_partitions_count=PARTITIONS[STORY_EVENTS], mode=cfg)
    start_ns = time.time_ns()
    clients = [f"cl-{i:07d}" for i in range(a.clients)]
    surf = {c: surface_of(c) for c in clients}
    fault_ns = start_ns + int(a.fault_at * 1e9)
    alert = {"rx": 0}                       # when the pager first had the alert for livestream_join x web
    lat, stats, stalls = Window(), defaultdict(float), Stalls()
    seen = set()                            # (cohort, alert, state) the pager already has
    minute = defaultdict(lambda: [0, 0])    # wall minute of the deadline -> [web joins judged, without video]
    minute_page = {"at": 0.0}               # when a once-a-minute judge would page, s after the fault began

    def reader(name):
        def on_alert(m):
            d = load(m.payload())
            if d["state"] == "sli":
                stats["snapshots"] += name == "datadog-exporter"
                return
            now = time.time_ns()
            if d["due_ns"] > start_ns:
                lat.add((now - d["due_ns"]) / 1e6)  # knowable-to-consumer, live alerts only
            key = (d["story"], d["sf"], d["alert"], d["state"])
            if name != "pager" or key in seen:
                return                      # repeated alert (at least once): already counted
            seen.add(key)
            stats[d["state"]] += 1
            if (d["story"], d["sf"]) != ("livestream_join", "web"):
                stats["other_fire"] += d["state"] == "fire"
                return
            if d["state"] == "fire" and not alert["rx"] and now >= fault_ns:
                alert["rx"] = now
                print(f"[live-shows] ALERT livestream_join x web at the pager {(now - fault_ns) / 1e6:,.0f} ms after "
                      f"the fault began: SLI {d['sli']:.1%} of {d['clients']} clients, missing {d['missing']}; "
                      f"deadline-to-pager {(now - d['due_ns']) / 1e6:.1f} ms", flush=True)
            elif d["state"] == "clear":
                print(f"[live-shows] CLEAR livestream_join x web at the pager: SLI {d['sli']:.1%} of "
                      f"{d['clients']} clients", flush=True)
        return on_alert

    async def consume(name):
        # One connection per consumer, reading the alerts partition directly from its own position (no consumer
        # group, nothing stored on the broker per poll); a full batch means more is waiting, so no pause.
        c, cb = await own_client(), reader(name)
        who, nxt = Consumer.Single(f"{name}-{os.getpid()}"), None
        while True:
            ms = await c.poll_messages(STREAM, VERDICTS, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    cb(m)
            if len(ms) < 1000:
                await asyncio.sleep(a.poll_ms / 1000)

    gc.freeze()                             # keep start-up objects out of the collector's way

    def minute_judge(dl_s, broken):
        # A judge that runs once a minute: at the top of minute t it judges the stories whose deadline fell in
        # minute t-2, so a deadline at :05 is judged up to two minutes later. It pages at the first run whose
        # minute of web joins is below the SLO. The same stories, judged on that clock instead of as they land.
        m = int(dl_s // 60)
        cell = minute[m]
        cell[0] += 1
        cell[1] += broken
        if (not minute_page["at"] and cell[0] >= a.min_clients and 1 - cell[1] / cell[0] < a.fire_sli
                and dl_s * 1e9 >= fault_ns):
            minute_page["at"] = (m + 2) * 60 - fault_ns / 1e9

    async def clients_loop():
        due = []                            # (send at, tiebreak, story key, story type, client, role, repeat)
        seqno = itertools.count()
        tick = time.monotonic()
        acc_j = acc_b = 0.0
        async with producer:
            while True:
                now = time.monotonic()
                now_ns = time.time_ns()
                faulting = a.fault_at <= (now_ns - start_ns) / 1e9 < a.fault_at + a.fault_for
                acc_j += a.joins * 0.01
                acc_b += a.bids * 0.01
                batches = defaultdict(list)

                def emit(key, st, c, r, repeat=False):
                    batches[part(key, PARTITIONS[STORY_EVENTS])].append(
                        SendMessage(msg(k=key, st=st, r=r, c=c, sf=surf[c])))
                    stats["events"] += 1
                    if repeat:
                        stats["dups"] += 1
                    elif random.random() < a.dup_share:         # delivered again a little later (at least once)
                        heapq.heappush(due, (now + random.uniform(0.05, 2), next(seqno), key, st, c, r, True))

                while acc_j >= 1:                               # new livestream joins this tick
                    acc_j -= 1
                    c = random.choice(clients)
                    key = os.urandom(8).hex()
                    broken = faulting and surf[c] == "web" and random.random() < a.fault_share
                    emit(key, "livestream_join", c, "livestream_tile_tap")
                    stats["joins"] += 1
                    plan = join(a, broken)
                    for r, after in plan:
                        heapq.heappush(due, (now + after, next(seqno), key, "livestream_join", c, r, False))
                    if surf[c] == "web" and plan[0][0] != "seller_blocked":
                        got_video = any(r == "video_playing" and after <= a.deadline_s for r, after in plan)
                        minute_judge(now_ns / 1e9 + a.deadline_s, not got_video)
                        if faulting:
                            stats["web_joins"] += 1
                            if broken:
                                stats["black"] += 1
                                stats["black_before"] += not alert["rx"] or alert["rx"] > now_ns
                while acc_b >= 1:                               # new bids this tick
                    acc_b -= 1
                    c = random.choice(clients)
                    key = os.urandom(8).hex()
                    emit(key, "place_bid", c, "bid_tap")
                    stats["bids"] += 1
                    for r, after in bid(a):
                        heapq.heappush(due, (now + after, next(seqno), key, "place_bid", c, r, False))
                while due and due[0][0] <= now:
                    _, _, key, st, c, r, repeat = heapq.heappop(due)
                    emit(key, st, c, r, repeat)
                # One batch per partition, all sent at once: per-story order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["faulting"] = faulting
                tick += 0.01                # from its due time, so a slow tick doesn't drift the schedule
                await asyncio.sleep(max(0.0, tick - time.monotonic()))

    async def report():
        print(f"[live-shows] {a.clients:,} clients, {a.joins:g} livestream joins/s and {a.bids:g} bids/s, story "
              f"deadline {a.deadline_s:g} s; fault: black screen on {a.fault_share:.0%} of web joins from "
              f"{a.fault_at:g} s for {a.fault_for:g} s; {a.consumers} consumers", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "fault" if stats["faulting"] else "normal"
            paged = f"{(alert['rx'] - fault_ns) / 1e9:.1f} s" if alert["rx"] else "not yet"
            mpage = f"{minute_page['at']:.0f} s" if minute_page["at"] else "not yet"
            print(f"[live-shows] {int(stats['events']):>6,} events/s ({phase})  joins {int(stats['joins']):,}  bids "
                  f"{int(stats['bids']):,}  repeats sent {int(stats['dups']):,}  web joins during the fault "
                  f"{int(stats['web_joins']):,}: no video {int(stats['black']):,}, of them before the pager had the "
                  f"alert {int(stats['black_before']):,}  pager had it after the fault began: {paged}; a "
                  f"once-a-minute judge: {mpage}  alerts at the pager: fire {int(stats['fire']):,}, clear "
                  f"{int(stats['clear']):,}, other cohorts {int(stats['other_fire']):,}  SLI snapshots "
                  f"{int(stats['snapshots']):,}  knowable-to-consumers n {r['n']:,} p50 {r['p50']:.1f}ms p99 "
                  f"{r['p99']:.1f}ms  {stalls.read()}", flush=True)
            stats.clear()

    await asyncio.gather(clients_loop(), report(), stalls.watch(), *(consume(n) for n in CONSUMERS[: a.consumers]))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--clients", type=int, default=200_000, help="clients played by this process")
    p.add_argument("--joins", type=float, default=300, help="livestream joins per second")
    p.add_argument("--bids", type=float, default=600, help="bids per second")
    p.add_argument("--deadline-s", type=float, default=60, help="the story deadline the judge uses")
    p.add_argument("--dup-share", type=float, default=0.01, help="share of events delivered twice")
    p.add_argument("--fault-at", type=float, default=120, help="seconds after start")
    p.add_argument("--fault-for", type=float, default=300, help="seconds")
    p.add_argument("--fault-share", type=float, default=0.8, help="share of web joins with a black screen")
    p.add_argument("--min-clients", type=int, default=50, help="as the judge: clients needed to judge a minute")
    p.add_argument("--fire-sli", type=float, default=0.95, help="as the judge: SLI below which it pages")
    p.add_argument("--consumers", type=int, default=3, help="services reading slo-alerts, each with its own reader")
    p.add_argument("--poll-ms", type=float, default=1, help="consumer wait between polls when nothing is waiting")
    asyncio.run(main(p.parse_args()))
