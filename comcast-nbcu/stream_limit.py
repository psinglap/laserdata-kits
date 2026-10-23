"""A playback node with its stream-limit view: it grants or refuses each new stream under the account's
three-stream limit, and shares every session start, heartbeat and stop with every other playback node through
the stream-limit lane.

Each process is one playback node; run several (ci.sh runs three) with --index 0, 1, 2 and the same --nodes.
A device's requests always land on the same node (a sticky load balancer), so the devices of one household
are spread over the nodes, and a stream stopped on one node is often replaced by a stream started on another.

The households follow a schedule every node computes from the wall clock, one live-event block of --cycle-s
seconds after another (each block's streams end just before the next block's kickoff, as viewers move from
one live event to the next). A share of accounts tunes in within --ramp-ms of kickoff. Per account and block:
  * full house, then a switch (--switch-share): three devices stream, then one stops and a fourth device
    starts 0.5 to 4 s later, as a viewer moves from the TV to a phone. The fourth device must be let in.
  * a fourth device (--fourth-share): three devices stream and a fourth tries to start. It must be refused.
  * one or two devices (the rest): well under the limit.
Each running stream sends a heartbeat every --hb-s. A stream with no heartbeat for --lease-hb heartbeats is
released (a crashed app). Only starts the node grants are written to the lane, so the lane holds the
account's real sessions.

The node reads every partition of the lane directly (its own offsets, no consumer group) on its own connection
and applies the other nodes' events to its view; its own at once. On start it rebuilds its view from the log,
from two blocks back, before it decides any stream.

Every decision is judged afterwards against the account's sessions as they really were at the moment of the
decision (every event published before it, including ones still on their way): a refusal while the account
had fewer than --limit streams is a viewer wrongly blocked; a grant while it already had --limit is a stream
over the limit.

Prints a line a second: session-event-to-view latency (published on another node -> applied here), PASS or
FAIL against --budget-ms, starts granted and refused, active streams, repeats dropped, sequence gaps. When a
block's decisions are judged: viewers wrongly blocked, streams over the limit, fourth devices refused, device
switches let in, sessions released by lease.
"""
import argparse
import asyncio
import gc
import os
import random
import time
import zlib
from collections import defaultdict, deque

from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, DEVICES, PARTITIONS, SESSIONS, STREAM, Window, account, connect, ensure_topics, load, msg, part

VARIANTS = 4    # four household schedules, rotating by block, so every block is not the same


def schedule(a, owned):
    """Per variant, this node's tasks in a block, sorted: (ms into the block, 'S' start | 'E' stop, account, device, tag).
    Tags: 'w' a switch's new device (must be let in), 'f' a fourth device (must be refused), 'n' anything else."""
    C = a.cycle_s * 1000
    out = []
    for v in range(VARIANTS):
        tasks = []
        for i in range(a.accounts):
            r = random.Random(i * 7919 + v * 104729)
            s1 = r.random() * a.ramp_ms if r.random() < a.kickoff_share else r.random() * 0.25 * C
            devs = list(range(len(DEVICES)))
            r.shuffle(devs)
            end = C - 500 - r.random() * 1000                  # every stream ends before the next block's kickoff
            u = r.random()
            if u < a.switch_share:
                stop_c = s1 + 0.25 * C + r.random() * 0.1 * C
                start_d = stop_c + min(500 + r.random() * 3500, 0.1 * C)
                plan = [(s1, "S", devs[0], "n"), (s1 + 0.03 * C, "S", devs[1], "n"), (s1 + 0.06 * C, "S", devs[2], "n"),
                        (stop_c, "E", devs[2], "n"), (start_d, "S", devs[3], "w"),
                        (end, "E", devs[0], "n"), (end, "E", devs[1], "n"), (end, "E", devs[3], "n")]
            elif u < a.switch_share + a.fourth_share:
                plan = [(s1 + k * 0.03 * C, "S", devs[k], "f" if k == 3 else "n") for k in range(4)]
                plan += [(end, "E", devs[k], "n") for k in range(4)]
            else:
                k = 1 + (r.random() < 0.4)
                plan = [(s1 + j * 0.03 * C, "S", devs[j], "n") for j in range(k)] + [(end, "E", devs[j], "n") for j in range(k)]
            for t, kind, d, tag in plan:
                if owned(i, d):
                    tasks.append((t, kind, i, d, tag))
        tasks.sort()
        out.append(tasks)
    return out


async def main(a):
    assert 0 <= a.index < a.nodes, "--index must be in 0..--nodes-1"
    me = f"{a.name}.{os.getpid()}"          # a restarted node is a new writer, so its sequences start again
    await ensure_topics(await connect(a.url), SESSIONS)
    writer, reader = await connect(a.url), await connect(a.url)     # one connection each way
    P = PARTITIONS[SESSIONS]
    accts = [account(i) for i in range(a.accounts)]
    apart = [part(x, P) for x in accts]
    owned = lambda i, d: zlib.crc32(f"{i}/{d}".encode()) % a.nodes == a.index
    tasks = schedule(a, owned) if not a.read_only else [[] for _ in range(VARIANTS)]
    lease_ns = int(a.lease_hb * a.hb_s * 1e9)
    rng = random.Random(me)
    start_ns = time.time_ns()

    view = defaultdict(dict)                # account -> {session: [last event ns, device]}
    last_seq = {}                           # (writer, account) -> last sequence applied
    my_seq = defaultdict(int)               # account -> this node's sequence
    pending = defaultdict(list)             # account -> decisions waiting for every earlier event to arrive
    judged = deque()                        # decisions in time order, for judging
    blocks = defaultdict(lambda: defaultdict(int))   # block -> business counters
    hb_due = deque()                        # (next heartbeat ns, account index, device, session) for own streams
    lat, st, run = Window(), defaultdict(int), defaultdict(int)
    live, caught, held = set(), [None], deque()
    gc.freeze()                             # keep start-up tables out of the collector's way

    def active(acct, now):
        sess = view.get(acct)
        if not sess:
            return []
        dead = [s for s, (ns, _) in sess.items() if ns < now - lease_ns]
        for s in dead:
            del sess[s]
            run["released"] += 1            # no heartbeat for the lease: a crashed app or a lost stop
            blocks[int(now / 1e9 // a.cycle_s)]["released"] += 1
        return list(sess)

    def apply(e):
        k = (e["s"], e["a"])
        prev = last_seq.get(k)
        if prev is not None and e["seq"] <= prev:
            run["repeats"] += 1             # same event ID seen before (retry, redelivery, replay): apply it once
            return
        if prev is not None and e["seq"] > prev + 1:
            run["gaps"] += 1                # an event from this writer never arrived
        last_seq[k] = e["seq"]
        acct, sid, kind = e["a"], e["sid"], e["k"]
        if kind == "stop":
            view[acct].pop(sid, None)
        else:
            view[acct][sid] = [e["ns"], e["d"]]
        if e["s"] != me:
            # A decision made before this event was published did not see it: correct its judgement.
            for d in pending.get(acct, ()):
                if e["ns"] < d["ns"]:
                    if kind == "start" and sid not in d["seen"]:
                        d["late"].add(sid)
                    elif kind == "stop":
                        d["seen"].discard(sid)
                        d["late"].discard(sid)

    def release(now):
        # --hold-ms: apply other nodes' events only once they are this old (a slower path)
        while held and now - held[0]["ns"] >= a.hold_ms * 1_000_000:
            e = held.popleft()
            lat.add((now - e["ns"]) / 1e6)
            apply(e)

    def judge(now):
        # A decision is judged once every event published before it has had time to arrive.
        while judged and now - judged[0]["ns"] > (a.hold_ms + 2500) * 1_000_000:
            d = judged.popleft()
            lst = pending[d["a"]]
            for j, x in enumerate(lst):
                if x is d:
                    del lst[j]
                    break
            if not lst:
                del pending[d["a"]]
            really = len(d["seen"]) + len(d["late"])
            b = blocks[d["b"]]
            if d["ok"]:
                b["granted"] += 1
                b["switch_in"] += d["tag"] == "w"
                if really >= a.limit:
                    b["over"] += 1          # let in while the account already had its full limit
            else:
                b["refused"] += 1
                b["fourth_refused"] += d["tag"] == "f" and really >= a.limit
                if really < a.limit:
                    b["wrong_block"] += 1   # a viewer told the limit is reached when it was not
                    b["wrong_switch"] += d["tag"] == "w"

    async def read():
        who = {p: Consumer.Single(f"{me}-{p}") for p in range(P)}
        since = PollingStrategy.Timestamp(int((time.time() - 2 * a.cycle_s) * 1e6))
        nxt = {}
        while True:
            more = False
            for p in range(P):
                ms = await reader.poll_messages(STREAM, SESSIONS, consumer=who[p], partition_id=p, count=a.batch,
                                                auto_commit=False,
                                                polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt else since)
                now = time.time_ns()
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    for m in ms:
                        e = load(m.payload())
                        if e["s"] == me:
                            continue        # this node's own events were applied when it wrote them
                        if caught[0] is None or e["ns"] < start_ns:
                            run["replayed"] += 1    # rebuilding: not live latency
                            apply(e)
                        elif a.hold_ms:
                            held.append(e)
                        else:
                            lat.add((now - e["ns"]) / 1e6)
                            apply(e)
                if len(ms) == a.batch:
                    more = True             # a replay or a burst: read on at once
                elif p not in live:
                    live.add(p)             # this partition has been read to the end of the log once
                    if len(live) == P:
                        caught[0] = time.monotonic()
                        n = sum(len(s) for s in view.values())
                        print(f"[{a.name}] rebuilt the stream-limit view from the log in {(time.time_ns() - start_ns) / 1e9:.2f}s: "
                              f"{n:,} active streams on {sum(1 for s in view.values() if s):,} accounts, "
                              f"{run['replayed']:,} session events", flush=True)
            if held:
                release(time.time_ns())
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    def emit(i, d, sid, kind, batches):
        acct = accts[i]
        my_seq[acct] += 1
        e = {"s": me, "a": acct, "d": d, "sid": sid, "k": kind, "seq": my_seq[acct], "ns": time.time_ns()}
        apply(e)                            # this node knows its own event at once
        m = msg(**e)
        batches[apart[i]].append(SendMessage(m))
        if a.dup_rate and rng.random() < a.dup_rate:
            batches[apart[i]].append(SendMessage(m))    # delivered twice, same event ID
            st["dups"] += 1

    def mine(i, d):
        return [s for s, (_, dev) in view.get(accts[i], {}).items() if dev == d]

    async def serve():
        while caught[0] is None:
            await asyncio.sleep(0.05)       # never decide a stream before the view is rebuilt
        now_ns = time.time_ns()
        for i, acct in enumerate(accts):    # streams this node's devices were running before a restart
            for s, (ns, d) in view.get(acct, {}).items():
                if owned(i, d):
                    hb_due.append((ns + int(a.hb_s * 1e9), i, d, s))
        hb_due_sorted = sorted(hb_due)
        hb_due.clear()
        hb_due.extend(hb_due_sorted)
        due, cur_b, ptr, cur = time.monotonic(), None, 0, []
        while True:
            now = time.time()
            now_ns = time.time_ns()
            batches = defaultdict(list)
            b = int(now // a.cycle_s)
            if b != cur_b:
                into = (now - b * a.cycle_s) * 1000
                late = cur_b is None        # joined mid-block (a start or a restart)
                cur_b, cur = b, tasks[b % VARIANTS]
                ptr = 0
                if late:
                    # Starts already past are skipped. Stops already past are sent now, as a player that stopped
                    # while its node was away reports it on reconnect, so no stream is left running in the view.
                    past = [(b - 1, i, d) for _, k, i, d, _ in tasks[(b - 1) % VARIANTS] if k == "E"]
                    while ptr < len(cur) and cur[ptr][0] <= into - 50:
                        if cur[ptr][1] == "E":
                            past.append((b, cur[ptr][2], cur[ptr][3]))
                        ptr += 1
                    for blk, i, d in past:
                        for s in mine(i, d):
                            if s.endswith(f".{blk}"):   # only the stream that stop was for
                                emit(i, d, s, "stop", batches)
            into_ms = (now - b * a.cycle_s) * 1000
            while ptr < len(cur) and cur[ptr][0] <= into_ms:
                _, kind, i, d, tag = cur[ptr]
                ptr += 1
                acct = accts[i]
                if kind == "E":
                    for s in mine(i, d):
                        emit(i, d, s, "stop", batches)
                    continue
                if mine(i, d):
                    continue                # this device is already streaming
                seen = active(acct, now_ns)
                ok = len(seen) < a.limit
                d_ = {"a": acct, "ns": time.time_ns(), "seen": set(seen), "late": set(), "ok": ok, "tag": tag, "b": b}
                pending[acct].append(d_)
                judged.append(d_)
                if ok:
                    sid = f"{acct}.{d}.{b}"
                    emit(i, d, sid, "start", batches)
                    hb_due.append((time.time_ns() + int(a.hb_s * 1e9), i, d, sid))
                    st["granted"] += 1
                else:
                    st["refused"] += 1      # "stream limit reached" on that device
            while hb_due and hb_due[0][0] <= now_ns:
                _, i, d, s = hb_due.popleft()
                if s in view.get(accts[i], {}):
                    emit(i, d, s, "hb", batches)
                    hb_due.append((now_ns + int(a.hb_s * 1e9), i, d, s))
            # One batch per partition, all sent at once: an account's events stay in order, one round trip a tick.
            if batches:
                await asyncio.gather(*(writer.send_messages(STREAM, SESSIONS, Partitioning.partition_id(p), ms)
                                       for p, ms in batches.items()))
            judge(time.time_ns())
            due += a.tick_ms / 1000
            await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        closed = int(time.time() // a.cycle_s) - 1      # blocks summarised so far (none before start)
        while True:
            await asyncio.sleep(1)
            if caught[0] is None:
                print(f"[{a.name}] rebuilding the stream-limit view from the log: {run['replayed']:,} session events so far",
                      flush=True)
                lat.flush()
                continue
            if a.read_only:
                judge(time.time_ns())
            r = lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            streams = sum(len(s) for s in view.values())
            print(f"[{a.name}] starts {st['granted'] + st['refused']:>4,}/s (granted {st['granted']:,}, refused {st['refused']:,})  "
                  f"from other nodes {r['n']:>6,}/s  session-event-to-view p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  "
                  f"p99.9 {r['p999']:5.1f}ms  {ok} (<{a.budget_ms:g}ms)  active streams {streams:,}  "
                  f"repeats dropped {run['repeats']:,}  gaps {run['gaps']}", flush=True)
            st.clear()
            b = int(time.time() // a.cycle_s)
            # A block is summarised once its last decision has been judged.
            if b - 1 > closed and time.time() - b * a.cycle_s >= (a.hold_ms + 3500) / 1000:
                c = closed = b - 1
                x = blocks.pop(c, defaultdict(int))
                n = x["granted"] + x["refused"]
                print(f"[limit] block {c % 10000} judged: start decisions {n:,}  granted {x['granted']:,}  refused "
                      f"{x['refused']:,} (fourth devices refused {x['fourth_refused']:,})  device switches let in "
                      f"{x['switch_in']:,}  viewers wrongly blocked {x['wrong_block']:,} ({100 * x['wrong_block'] / n if n else 0:.2f}%, "
                      f"switches {x['wrong_switch']:,})  streams over the limit {x['over']:,} "
                      f"({100 * x['over'] / n if n else 0:.2f}%)  released by lease {x['released']:,}", flush=True)
                for k in [k for k in blocks if k < c]:
                    del blocks[k]

    jobs = [read(), report()] + ([] if a.read_only else [serve()])
    await asyncio.gather(*jobs)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="pb-1")
    p.add_argument("--index", type=int, default=0, help="this node's index, 0..--nodes-1")
    p.add_argument("--nodes", type=int, default=3, help="playback nodes sharing the households; same on every node")
    p.add_argument("--accounts", type=int, default=12_000, help="accounts watching; same on every node")
    p.add_argument("--limit", type=int, default=3, help="streams at once per account")
    p.add_argument("--cycle-s", type=float, default=60, help="one live-event block (compressed); same on every node")
    p.add_argument("--kickoff-share", type=float, default=0.4, help="share of accounts tuning in at kickoff")
    p.add_argument("--ramp-ms", type=float, default=2_000, help="kickoff viewers start within this time")
    p.add_argument("--switch-share", type=float, default=0.5, help="share of accounts on three streams that switch a device")
    p.add_argument("--fourth-share", type=float, default=0.2, help="share of accounts where a fourth device tries to start")
    p.add_argument("--hb-s", type=float, default=10, help="seconds between a stream's heartbeats")
    p.add_argument("--lease-hb", type=float, default=3, help="a stream with no heartbeat for this many intervals is released")
    p.add_argument("--budget-ms", type=float, default=50, help="session-event-to-view p99 budget")
    p.add_argument("--dup-rate", type=float, default=0.005, help="share of session events delivered twice")
    p.add_argument("--hold-ms", type=float, default=0,
                   help="what-if: apply other nodes' events only after this delay, like a consumer stuck in a rebalance")
    p.add_argument("--read-only", action="store_true", help="read the lane only (a shadow run behind kafka_bridge.py)")
    p.add_argument("--tick-ms", type=float, default=10, help="how often the node sends its events")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads when the lane is idle")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
