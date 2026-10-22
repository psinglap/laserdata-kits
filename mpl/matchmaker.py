"""A matchmaker: decides how to match every player who taps Play again, from its own view of every player's form.

Each process is one matchmaker; run several. Every matchmaker reads every partition of player-form directly (its
own offsets, no consumer group, nothing committed) on its own connection, so each one holds the latest form of
every player: rating, win or loss streak, games played. On start it rebuilds that view from the log before it
takes a single request.

Match requests arrive on match-requests, keyed by lobby. Each matchmaker owns a share of its partitions (--shard
of --shards) and reads them directly too, so a lobby's requests are decided by one matchmaker; on start it takes
requests from that moment on, since older ones have timed out. The decision: match at the player's rating, and
for a player on a losing streak of --protect-after or more, streak protection (an opponent --protect-by rating
points easier). Decisions go to matches from a separate task every 5 ms, so the read loops never wait on a send.

For comparison, each matchmaker also keeps a second view that applies every result --stale-s seconds after the
game server created it, and records the decision it would have made from that view; players.py checks both.

Prints result-to-matchmaker latency every second (p50, p99, p99.9) with PASS or FAIL against --budget-ms.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (DEFAULT_URL, FORM, MATCHES, PARTITIONS, REQUESTS, START_RATING, STREAM, Window, connect,
                    ensure_topics, load, msg)


class View:
    """The latest form of every player: player -> [seq, rating, streak, games]."""

    def __init__(self):
        self.form = {}

    def apply(self, c) -> bool:
        v = self.form.get(c["k"])
        if v is not None and c["seq"] <= v[0]:
            return False                 # older or repeated result (replay, at-least-once): newest form wins
        self.form[c["k"]] = [c["seq"], c["r"], c["streak"], c["games"]]
        return True

    def decide(self, p, protect_after, protect_by):
        seq, rating, streak, _ = self.form.get(p, (0, START_RATING, 0, 0))
        protect = streak <= -protect_after
        return seq, protect, round(rating - (protect_by if protect else 0), 1)


async def main(a):
    me = f"{a.name}.{os.getpid()}"
    await ensure_topics(await connect(a.url), FORM, REQUESTS, MATCHES)
    reader, writer = await connect(a.url), await connect(a.url)       # one connection each way
    P = PARTITIONS[FORM]
    start_ns = time.time_ns()

    view, behind = View(), View()
    delayed = deque()                    # (due ns, result) for the comparison view
    outbox = []                          # decisions waiting for the sender task
    lat, st, run = Window(), defaultdict(int), defaultdict(int)
    caught = [None]
    delay_ns = int(a.stale_s * 1e9)

    def to_behind(now):
        while delayed and delayed[0][0] <= now:
            behind.apply(delayed.popleft()[1])

    async def read_lane():
        who = {p: Consumer.Single(f"{me}-{p}") for p in range(P)}
        nxt, done = {}, set()
        last = time.monotonic()
        while True:
            now_m = time.monotonic()
            st["gap_ms"] = max(st["gap_ms"], 1000 * (now_m - last))    # longest pass over all partitions
            last, more = now_m, False
            for p in range(P):
                ms = await reader.poll_messages(STREAM, FORM, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                                else PollingStrategy.First())
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        c = load(m.payload())
                        if caught[0] is not None and c["seq"] > 1 and c["ns"] >= start_ns:
                            lat.add((now - c["ns"]) / 1e6)      # a live result, not the rebuild
                            st["changes"] += 1
                        else:
                            run["replayed"] += 1
                        if not view.apply(c):
                            run["repeats"] += 1
                        if c["ns"] + delay_ns <= now:
                            behind.apply(c)                     # already older than the comparison delay
                        else:
                            delayed.append((c["ns"] + delay_ns, c))
                if len(ms) == a.batch:
                    more = True             # a rebuild or a burst: read on at once
                elif p not in done:
                    done.add(p)             # this partition has been read to the end of the log once
                    if len(done) == P:
                        caught[0] = time.monotonic()
                        gc.freeze()         # keep the rebuilt view out of the collector's way
                        print(f"[{a.name}] rebuilt player form from the log in {(now - start_ns) / 1e9:.2f}s: "
                              f"{len(view.form):,} players, {run['replayed']:,} results replayed", flush=True)
            to_behind(time.time_ns())
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    def on_request(m):
        r = load(m.payload())
        if time.time_ns() - r["ns"] > a.expire_s * 1e9:
            st["expired"] += 1              # the player's tap has already timed out
            return
        seq, protect, opp = view.decide(r["p"], a.protect_after, a.protect_by)
        alt_seq, alt_protect, _ = behind.decide(r["p"], a.protect_after, a.protect_by)
        st["decided"] += 1
        st["protected"] += protect
        outbox.append(SendMessage(msg(id=r["id"], seq=seq, protect=protect, opp=opp,
                                      alt_seq=alt_seq, alt_protect=alt_protect, by=me)))

    async def send_decisions():
        while True:
            await asyncio.sleep(0.005)
            if outbox:
                out, outbox[:] = list(outbox), []
                await writer.send_messages(STREAM, MATCHES, Partitioning.balanced(), out)

    async def take_requests():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # never decide before the view is rebuilt
        rr = await connect(a.url)           # its own connection, so requests never queue behind the lane
        mine = [p for p in range(PARTITIONS[REQUESTS]) if p % a.shards == a.shard]
        who = {p: Consumer.Single(f"{me}-req-{p}") for p in mine}
        since = PollingStrategy.Timestamp(time.time_ns() // 1000)     # requests from now on
        nxt = {}
        while True:
            more = False
            for p in mine:
                ms = await rr.poll_messages(STREAM, REQUESTS, consumer=who[p], partition_id=p, count=a.batch,
                                            auto_commit=False,
                                            polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_request(m)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding player form from the log: {run['replayed']:,} results so far", flush=True)
                lat.flush()
                st.clear()
                continue
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {st['changes']:>6,} results/s  result-to-matchmaker p50 {r['p50']:5.1f}ms  "
                  f"p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  "
                  f"decided {st['decided']:,}  streak protected {st['protected']:,}  expired {st['expired']:,}  "
                  f"view {len(view.form):,} players  read pass max {st['gap_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(read_lane(), take_requests(), send_decisions(), report())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--name", default="mm-1")
    ap.add_argument("--budget-ms", type=float, default=50, help="result-to-matchmaker p99 budget")
    ap.add_argument("--shard", type=int, default=0, help="this matchmaker's share of the lobbies: 0 .. shards-1")
    ap.add_argument("--shards", type=int, default=1, help="how many matchmakers share the lobbies")
    ap.add_argument("--protect-after", type=int, default=3, help="losses in a row that call for streak protection")
    ap.add_argument("--protect-by", type=float, default=150, help="how many rating points easier a protected match is")
    ap.add_argument("--stale-s", type=float, default=10, help="delay of the comparison view, seconds")
    ap.add_argument("--expire-s", type=float, default=2.0, help="a request older than this has timed out")
    ap.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    ap.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(ap.parse_args()))
