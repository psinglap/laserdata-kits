"""The key enforcement service, fed by the request lane and by leak reports.

Reads every partition of api-requests directly (no consumer group: a group reader visits one partition per
poll, so with 16 partitions a request waited for its partition's turn), on --readers connections of its own,
tracking its own position. Each record is one served request: request router, API key, project, the
network it came from, a per-(router, key) sequence and its cost. A replayed or repeated record is recognised
by its sequence and changes nothing.

It disables a key in two cases:
  * HIJACK: in the last --window-ms the key served at least --min-requests requests from at least
    --min-networks different networks. A key's own traffic comes from its owner's one or two networks; a
    stolen key is used from many at once. hijacked() is a stand-in for your abuse models; put yours there.
  * LEAK: a leak report on key-reports (a key found on the public internet or in an app), disabled at once.
Each disable goes out on key-status, which every request router reads (routers.py), with the key's own
sequence and the resulting state (never "+1"), so a restarted service or router rebuilds it from the log.

Prints request-to-enforcement p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, the
disables sent, and report-to-disable time for leaked keys.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import DEFAULT_URL, PARTITIONS, REPORTS, REQUESTS, STATUS, STREAM, Window, ensure_topics, load, msg


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), REQUESTS, REPORTS, STATUS)
    out = await (await own_client()).producer(STREAM, STATUS, topic_partitions_count=PARTITIONS[STATUS])
    lat, leak_lat, stats = Window(), Window(), defaultdict(float)
    seq = {}                                # (router, key) -> last sequence applied
    recent = defaultdict(deque)             # key -> (ns, network) of requests inside the window
    disabled = {}                           # key -> reason
    dseq = defaultdict(int)                 # key -> status sequence
    pending = []                            # disables waiting for the next send (one batch every 2 ms)
    caught_up = [False]                     # a new reader first replays requests from before it joined
    window_ns = a.window_ms * 1_000_000
    gc.freeze()                             # keep start-up objects out of the collector's way

    def hijacked(rs) -> tuple[int, int] | None:
        """Stands in for your abuse detection. Here: many requests from many networks inside the window."""
        if len(rs) < a.min_requests:
            return None
        nets = len({n for _, n in rs})
        return (len(rs), nets) if nets >= a.min_networks else None

    def disable(k, reason, age_ms, **evidence):
        if k in disabled:
            return
        disabled[k] = reason
        stats[reason] += 1
        if age_ms > a.replay_disable_s * 1000:
            return                          # replayed from long ago: its disable is already in key-status
        dseq[k] += 1
        pending.append(SendMessage(msg(k=k, seq=dseq[k], state="disabled", reason=reason, **evidence)))

    def on_request(m, now):
        d = load(m.payload())
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest request seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        sk = (d["s"], d["k"])
        if d["seq"] <= seq.get(sk, 0):
            stats["dropped"] += 1           # older or repeated record: already counted
            return
        seq[sk] = d["seq"]
        stats["requests"] += 1
        k = d["k"]
        if k in disabled:
            return
        rs = recent[k]
        rs.append((d["ns"], d["net"]))
        while rs and rs[0][0] < d["ns"] - window_ns:
            rs.popleft()
        hit = hijacked(rs)
        if hit:
            disable(k, "hijack", age, requests=hit[0], networks=hit[1])
            del recent[k]
        elif len(rs) == 1 and len(recent) > 200_000:
            for q in [q for q, v in recent.items() if v[-1][0] < d["ns"] - window_ns]:
                del recent[q]               # forget quiet keys

    async def read(pids):
        # Each partition in turn, from the start of the log (the windows are rebuilt from it), then from our
        # own position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, REQUESTS, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_request(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def read_reports():
        c = await own_client()
        who, nxt = Consumer.Single(f"{a.name}-{os.getpid()}-reports"), None
        while True:
            ms = await c.poll_messages(STREAM, REPORTS, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    d = load(m.payload())
                    age = (time.time_ns() - d["ns"]) / 1e6
                    if age < a.replay_disable_s * 1000:
                        leak_lat.add(age)
                    disable(d["k"], "leak", age, found=d["where"])
            else:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send_disables():
        # Disables are small and come in bursts; one batch every 2 ms keeps them off a send-per-disable loop.
        async with out:
            while True:
                await asyncio.sleep(0.002)
                if pending:
                    batch = pending[:]
                    pending.clear()
                    await out.send(batch)
                    stats["sent"] += len(batch)

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught_up[0]:
                # caught up once a whole second of requests arrived inside the budget; partitions interleave,
                # so one fresh request alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier requests this second, "
                      f"{len(disabled):,} keys disabled so far", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r, lk = lat.flush(), leak_lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            leak = f"report-to-disable max {lk['max']:.1f}ms" if lk["n"] else "report-to-disable -"
            print(f"[{a.name}] {r['n']:>7,} requests/s  request-to-enforcement p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"disables sent {int(stats['sent']):,} (hijack {int(stats['hijack']):,}, leak {int(stats['leak']):,})  "
                  f"{leak}  keys disabled {len(disabled):,}", flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[REQUESTS]))
    await asyncio.gather(send_disables(), report(), read_reports(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="enforcement")
    p.add_argument("--budget-ms", type=int, default=50, help="request-to-enforcement p99 budget for the lane")
    p.add_argument("--readers", type=int, default=2, help="connections reading the request partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--window-ms", type=float, default=500, help="hijack window")
    p.add_argument("--min-requests", type=int, default=20, help="requests inside the window to call a hijack")
    p.add_argument("--min-networks", type=int, default=5, help="distinct networks inside the window to call a hijack")
    p.add_argument("--replay-disable-s", type=float, default=30,
                   help="after a restart, still publish disables for evidence younger than this")
    asyncio.run(main(p.parse_args()))
