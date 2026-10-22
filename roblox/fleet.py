"""The source of truth: every game server in every edge data center, and the players pressing Play.

Each game server (one engine instance running one place) publishes its state to the server-state topic, keyed by
server, with a per-server sequence number and the resulting state, never only a delta:
  * join, leave: a player arrives or leaves; direct: a player follows a friend straight into a server, a join
    matchmaking did not pick;
  * open: a new server starts for a place in an edge data center when that place runs short of free slots there;
  * closing, closed: the creator publishes an update, the place's old servers stop taking joins and shut down;
  * heartbeat: every engine instance also re-sends its state every --heartbeat-s seconds.

It also plays the players: join requests go to join-requests, keyed by place. A matchmaker (matchmaker.py) picks
a server from its index of the lane and sends the placement back. This script checks each placement against the
server's true state at that moment: a join to a server that is full or shutting down bounces, and the player has
to be matched again. Each placement also carries the pick the same matchmaker would have made from an index
refreshed every few seconds (--refresh-s on matchmaker.py), and that pick is checked the same way.

--herd starts a thundering herd: at --herd-at the creator of the most popular place publishes an update (its
servers start closing) and join requests jump --herd times, all of the extra joins for that place.
"""
import argparse
import asyncio
import gc
import itertools
import random
import time
from collections import defaultdict
from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, JOINS, PARTITIONS, PLACEMENTS, STATE, STREAM, Window, connect, dc_id,
                    ensure_topics, load, msg, part_of, place_id)


async def main(a):
    await ensure_topics(await connect(a.url), STATE, JOINS, PLACEMENTS)
    lane, join_w = await connect(a.url), await connect(a.url)          # one connection per writer and reader
    pl_reader = await connect(a.url)

    rng = random.Random(11)
    places = [place_id(i) for i in range(a.places)]
    dcs = [dc_id(i) for i in range(a.regions)]
    weight = [1.0 / (i + 1) ** 0.9 for i in range(a.places)]        # a few places draw most of the players
    cap_of = {p: rng.choice((8, 12, 20, 30, 50)) for p in places}   # max players per server, set by the creator
    srv = {}                       # server -> [place, dc, cap, players, status, seq, seen, close_at]
    free = defaultdict(int)        # (place, dc) -> free slots on open servers
    live = []                      # servers that are not closed yet, for leaves and friend joins
    outbox = []                    # (server, message) waiting for the next tick's send
    ids = itertools.count(1)
    st, lat, run = defaultdict(int), Window(), {"herd": False, "updated": False}
    pending = {}                   # request id -> (created ns, place, dc)

    def emit(k, kind):
        v = srv[k]
        v[5] += 1
        outbox.append((k, SendMessage(msg(k=k, seq=v[5], place=v[0], dc=v[1], cap=v[2], n=v[3], st=v[4],
                                          seen=v[6], kind=kind))))
        st["changes"] += 1

    def open_server(p, d, players=0):
        k = f"S{next(ids):07d}"
        srv[k] = [p, d, cap_of[p], players, "open", 0, 0, 0.0]
        free[(p, d)] += cap_of[p] - players
        live.append(k)
        return k

    def players_move(k, dn, kind):
        v = srv[k]
        v[3] += dn
        if v[4] == "open":
            free[(v[0], v[1])] -= dn
        emit(k, kind)

    def set_status(k, status, kind):
        v = srv[k]
        if v[4] == "open":
            free[(v[0], v[1])] -= v[2] - v[3]          # a closing server takes no more joins
        v[4] = status
        if status == "closed":
            v[3] = 0
        emit(k, kind)

    # The fleet at start: servers per place and edge data center in proportion to the place's players.
    total_w = sum(weight)
    for i, p in enumerate(places):
        per_dc = max(2, round(a.servers * weight[i] / total_w / a.regions))
        for d in dcs:
            for _ in range(per_dc):
                open_server(p, d, int(cap_of[p] * rng.uniform(0.5, 0.95)))

    async def send(writer, topic, items):
        out = defaultdict(list)            # one batch per partition: a key's messages stay in order in one partition
        for key, m in items:
            out[part_of(key, PARTITIONS[topic])].append(m)
        await asyncio.gather(*(writer.send_messages(STREAM, topic, Partitioning.partition_id(p), ms)
                               for p, ms in out.items()))

    async def snapshot():
        # Every server's starting state (seq 1, not timed as a live change), like a full index load.
        for k in list(srv):
            emit(k, "snapshot")
        items, outbox[:] = list(outbox), []
        for i in range(0, len(items), 5000):
            await send(lane, STATE, items[i:i + 5000])
        print(f"[fleet] snapshot: {len(srv):,} game servers, {sum(v[3] for v in srv.values()):,} players, "
              f"{a.places} places in {a.regions} edge data centers", flush=True)

    def any_live(cond):
        for _ in range(4):                 # a few tries; closed servers are pruned lazily
            k = live[rng.randrange(len(live))]
            if cond(srv[k]):
                return k
        return None

    def tick_players(n_join, n_direct, n_leave, herd_joins, now_ns):
        reqs, touched = [], set()
        for j in range(n_join + herd_joins):
            p = places[0] if j >= n_join else rng.choices(places, weight)[0]
            d = dcs[rng.randrange(len(dcs))]
            rid = f"j{next(ids)}"
            pending[rid] = (now_ns, p, d)
            touched.add((p, d))
            reqs.append((p, SendMessage(msg(id=rid, place=p, dc=d, ns=now_ns))))
            st["requests"] += 1
        for _ in range(n_direct):          # following a friend into their server: matchmaking does not pick it
            k = any_live(lambda v: v[4] == "open" and 0 < v[3] < v[2])
            if k:
                players_move(k, 1, "direct")
                st["direct"] += 1
        for _ in range(n_leave):
            k = any_live(lambda v: v[4] != "closed" and v[3] > 0)
            if k:
                players_move(k, -1, "leave")
                v = srv[k]
                if v[3] == 0 and v[4] == "open" and free[(v[0], v[1])] > (a.headroom + 2) * v[2]:
                    set_status(k, "closed", "closed")      # the orchestrator stops an empty server it doesn't need
                    st["stopped"] += 1
        for p, d in touched:               # the orchestrator: start a server where a place runs short of slots
            if free[(p, d)] < a.headroom * cap_of[p]:
                open_server(p, d)
                emit(live[-1], "open")
                st["opened"] += 1
        return reqs

    def creator_update():
        # The creator publishes an update: the place's running servers stop taking joins and shut down.
        p = places[0]
        now = time.monotonic()
        for k in live:
            v = srv[k]
            if v[0] == p and v[4] == "open":
                set_status(k, "closing", "closing")
                v[7] = now + rng.uniform(2, a.drain_s)
                st["closing"] += 1
        run["updated"] = True
        print(f"[fleet] creator update on {p}: {st['closing']:,} servers closing, joins x{a.herd:g} for {a.herd_for:g} s",
              flush=True)

    def close_drained():
        now = time.monotonic()
        for k in live:
            v = srv[k]
            if v[4] == "closing" and v[7] <= now:
                set_status(k, "closed", "closed")
        live[:] = [k for k in live if srv[k][4] != "closed"]

    def heartbeats(share):
        for _ in range(int(share)):
            k = live[rng.randrange(len(live))]
            emit(k, "heartbeat")

    async def run_fleet():
        await snapshot()
        gc.freeze()                        # keep start-up tables out of the collector's way
        t0 = due = time.monotonic()
        owed = defaultdict(float)
        next_drain = t0
        while True:
            t = time.monotonic() - t0
            herd = a.herd > 1 and a.herd_at <= t < a.herd_at + a.herd_for
            if herd and not run["updated"]:
                creator_update()
            run["herd"] = herd
            owed["j"] += a.joins / 100
            owed["h"] += (a.herd - 1) * a.joins / 100 if herd else 0
            owed["d"] += a.direct / 100
            owed["l"] += a.leaves / 100
            owed["b"] += len(live) / a.heartbeat_s / 100
            n = {x: int(v) for x, v in owed.items()}
            for x in n:
                owed[x] -= n[x]
            now_ns = time.time_ns()
            reqs = tick_players(n["j"], n["d"], n["l"], n["h"], now_ns)
            heartbeats(n["b"])
            if time.monotonic() >= next_drain:
                close_drained()
                next_drain += 0.1
            changes, outbox[:] = list(outbox), []
            # The tick's server changes and join requests go out together, each writer on its own connection.
            t_send = time.monotonic()
            await asyncio.gather(send(lane, STATE, changes), send(join_w, JOINS, reqs))
            st["send_ms"] = max(st["send_ms"], 1000 * (time.monotonic() - t_send))
            st["late_ms"] = max(st["late_ms"], 1000 * (t_send - due))   # how late this tick started
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    def verdict(k):
        v = srv.get(k)
        if v is None:
            return "none"
        if v[4] != "open":
            return "closing"
        return "full" if v[3] >= v[2] else "ok"

    async def read_placements():
        # Every partition read directly, from start-up on, on its own connection.
        who = {p: Consumer.Single(f"fleet-check-{p}") for p in range(PARTITIONS[PLACEMENTS])}
        since = PollingStrategy.Timestamp(time.time_ns() // 1000)
        nxt = {}
        while True:
            more = False
            for p in who:
                ms = await pl_reader.poll_messages(STREAM, PLACEMENTS, consumer=who[p], partition_id=p, count=1000,
                                                   auto_commit=False,
                                                   polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_placement(m)
                    more = more or len(ms) == 1000
            if not more:
                await asyncio.sleep(0.001)

    def on_placement(m):
        pl = load(m.payload())
        req = pending.pop(pl["id"], None)
        if req is None:
            return                         # placed twice (at-least-once) or already timed out
        lat.add((time.time_ns() - req[0]) / 1e6)
        st["placed"] += 1
        live_v = verdict(pl["s"]) if pl["s"] else "none"
        if pl["s"] in srv:
            srv[pl["s"]][6] += 1           # the server has processed this placement (joined or bounced)
        if live_v == "ok":
            players_move(pl["s"], 1, "join")
            st["joined"] += 1
        else:
            st[f"live_{live_v}"] += 1
            if pl["s"] in srv:
                emit(pl["s"], "bounce")    # the engine instance turned the player away; its state says so
        alt_v = verdict(pl["alt"]) if pl["alt"] else "none"
        if alt_v != "ok":
            st[f"alt_{alt_v}"] += 1

    async def report():
        while True:
            await asyncio.sleep(1)
            cutoff = time.time_ns() - 5_000_000_000
            for rid in [r for r, p in pending.items() if p[0] < cutoff]:
                del pending[rid]
                st["unplaced"] += 1        # no placement within 5 s: the player sees an error
            r = lat.flush()
            c = st["placed"]
            bounced = st["live_full"] + st["live_closing"]
            alt = st["alt_full"] + st["alt_closing"]
            print(f"[fleet] {st['changes']:>6,} changes/s  {len(live):,} servers  joins requested {st['requests']:,} "
                  f"placed {c:,} ({'herd' if run['herd'] else 'steady'})  bounced {bounced:,} "
                  f"(full {st['live_full']:,} shutting down {st['live_closing']:,}) no server {st['live_none']:,}  "
                  f"an index refreshed every {a.refresh_s:g}s would bounce {alt:,} "
                  f"(full {st['alt_full']:,} shutting down {st['alt_closing']:,}) no server {st['alt_none']:,}  "
                  f"friend joins {st['direct']:,}  servers opened {st['opened']:,} stopped {st['stopped']:,}  unplaced {st['unplaced']:,}  "
                  f"press-to-placement p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  "
                  f"tick late max {st['late_ms']:.0f}ms  send max {st['send_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(run_fleet(), read_placements(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--places", type=int, default=500, help="places (experiences) with players")
    p.add_argument("--servers", type=int, default=10_000, help="game servers at start")
    p.add_argument("--regions", type=int, default=24, help="edge data centers")
    p.add_argument("--joins", type=float, default=400, help="players pressing Play per second (matchmade joins)")
    p.add_argument("--direct", type=float, default=150, help="players following a friend into a server per second")
    p.add_argument("--leaves", type=float, default=550, help="players leaving a server per second")
    p.add_argument("--heartbeat-s", type=float, default=10, help="each engine instance re-sends its state this often")
    p.add_argument("--headroom", type=float, default=1, help="start a server when free slots fall under this many servers' worth")
    p.add_argument("--herd", type=float, default=1, help="join-rate multiplier in the thundering herd (1 = off)")
    p.add_argument("--herd-at", type=float, default=60, help="seconds after start")
    p.add_argument("--herd-for", type=float, default=30)
    p.add_argument("--drain-s", type=float, default=10, help="old servers shut down within this many seconds of the update")
    p.add_argument("--refresh-s", type=float, default=3, help="the comparison index's refresh, as set on matchmaker.py (label only)")
    asyncio.run(main(p.parse_args()))
