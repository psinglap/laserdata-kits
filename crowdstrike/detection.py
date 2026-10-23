"""The identity detection engine, fed by the authentication lane and by account events.

Reads every partition of auth-events directly (no consumer group: a group reader visits one partition per poll, so
with 16 partitions an authentication waited for its partition's turn), on --readers connections of its own,
tracking its own position. Each record is one authentication a domain controller let through: DC, account, source
host, destination host, protocol and a per-(DC, account) sequence. A replayed or repeated record is recognised by
its sequence and changes nothing.

It enforces on an account in two cases:
  * LATERAL: in the last --window-ms the account authenticated to at least --min-hosts different hosts, at least
    --min-auths times. An account's own logons go to its own handful of servers; a stolen credential used to move
    through the network touches host after host. lateral() is a stand-in for your identity detections; put yours
    there. Action: block.
  * COMPROMISED: an account event on account-events reports the password exposed. Action: require MFA, at once.
Each enforcement goes out on identity-enforcement, which every DC sensor reads (dc_sensors.py), with the account's
own sequence and the resulting state (never "+1"), so a restarted engine or DC rebuilds it from the log.

Prints auth-to-detection p50/p99/p99.9 every second, with PASS/FAIL against --budget-ms, the enforcements sent, and
event-to-enforcement time for compromised passwords.
"""
import argparse
import asyncio
import gc
import os
import time
from collections import defaultdict, deque

from apache_iggy import Consumer, IggyClient, PollingStrategy, SendMessage

from common import ACCOUNT, AUTH, DEFAULT_URL, ENFORCE, PARTITIONS, STREAM, Window, ensure_topics, load, msg


async def main(a):
    async def own_client():                 # one connection per reader and per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), AUTH, ACCOUNT, ENFORCE)
    out = await (await own_client()).producer(STREAM, ENFORCE, topic_partitions_count=PARTITIONS[ENFORCE])
    lat, event_lat, stats = Window(), Window(), defaultdict(float)
    seq = {}                                # (DC, account) -> last sequence applied
    recent = defaultdict(deque)             # account -> (ns, destination) of authentications inside the window
    enforced = {}                           # account -> reason
    eseq = defaultdict(int)                 # account -> enforcement sequence
    pending = []                            # enforcements waiting for the next send (one batch every 2 ms)
    caught_up = [False]                     # a new reader first replays authentications from before it joined
    window_ns = a.window_ms * 1_000_000
    gc.freeze()                             # keep start-up objects out of the collector's way

    def lateral(rs) -> tuple[int, int] | None:
        """Stands in for your identity detections. Here: one account reaching many hosts inside the window."""
        if len(rs) < a.min_auths:
            return None
        hosts = len({d for _, d in rs})
        return (len(rs), hosts) if hosts >= a.min_hosts else None

    def enforce(x, reason, action, age_ms, **evidence):
        if x in enforced:
            return
        enforced[x] = reason
        stats[reason] += 1
        if age_ms > a.replay_enforce_s * 1000:
            return                          # replayed from long ago: its enforcement is already in the log
        eseq[x] += 1
        pending.append(SendMessage(msg(a=x, seq=eseq[x], state=action, reason=reason, **evidence)))

    def on_auth(m, now):
        d = load(m.payload())
        age = (now - d["ns"]) / 1e6
        if caught_up[0]:
            lat.add(age)
        else:                               # still replaying: track the oldest authentication seen this second
            stats["replayed"] += 1
            stats["max_age"] = max(stats["max_age"], age)
        sk = (d["s"], d["a"])
        if d["seq"] <= seq.get(sk, 0):
            stats["dropped"] += 1           # older or repeated record: already counted
            return
        seq[sk] = d["seq"]
        stats["auths"] += 1
        x = d["a"]
        if x in enforced:
            return
        rs = recent[x]
        rs.append((d["ns"], d["dst"]))
        while rs and rs[0][0] < d["ns"] - window_ns:
            rs.popleft()
        hit = lateral(rs)
        if hit:
            enforce(x, "lateral", "blocked", age, auths=hit[0], hosts=hit[1], src=d["src"])
            del recent[x]
        elif len(rs) == 1 and len(recent) > 300_000:
            for q in [q for q, v in recent.items() if v[-1][0] < d["ns"] - window_ns]:
                del recent[q]               # forget quiet accounts

    async def read(pids):
        # Each partition in turn, from the start of the log (the windows are rebuilt from it), then from our own
        # position; nothing is stored on the broker. A full batch means more is waiting, so no pause.
        c = await own_client()
        who = {p: Consumer.Single(f"{a.name}-{os.getpid()}-p{p}") for p in pids}
        nxt = {}
        while True:
            more = False
            for p in pids:
                ms = await c.poll_messages(STREAM, AUTH, consumer=who[p], partition_id=p, count=a.batch,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt[p]) if p in nxt
                                           else PollingStrategy.First())
                if ms:
                    nxt[p] = ms[-1].offset() + 1
                    now = time.time_ns()
                    for m in ms:
                        on_auth(m, now)
                    more = more or len(ms) == a.batch
            if not more:
                await asyncio.sleep(a.poll_ms / 1000)

    async def read_account_events():
        c = await own_client()
        who, nxt = Consumer.Single(f"{a.name}-{os.getpid()}-account"), None
        while True:
            ms = await c.poll_messages(STREAM, ACCOUNT, consumer=who, partition_id=0, count=1000, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                       else PollingStrategy.First())
            if ms:
                nxt = ms[-1].offset() + 1
                for m in ms:
                    d = load(m.payload())
                    age = (time.time_ns() - d["ns"]) / 1e6
                    if age < a.replay_enforce_s * 1000:
                        event_lat.add(age)
                    enforce(d["a"], "compromised", "mfa_required", age, source=d["source"])
            else:
                await asyncio.sleep(a.poll_ms / 1000)

    async def send_enforcements():
        # Enforcements are small and come in bursts; one batch every 2 ms keeps them off a send-per-block loop.
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
                # caught up once a whole second of authentications arrived inside the budget; partitions
                # interleave, so one fresh record alone doesn't mean the replay is finished
                if stats["replayed"] and stats["max_age"] < a.budget_ms:
                    caught_up[0] = True
                print(f"[{a.name}] catching up: replayed {int(stats['replayed']):,} earlier authentications this "
                      f"second, {len(enforced):,} accounts enforced so far", flush=True)
                stats["replayed"] = stats["max_age"] = 0
                lat.flush()
                continue
            r, ev = lat.flush(), event_lat.flush()
            ok = "idle" if not r["n"] else "PASS" if r["p99"] < a.budget_ms else "FAIL"
            evs = f"event-to-enforcement max {ev['max']:.1f}ms" if ev["n"] else "event-to-enforcement -"
            print(f"[{a.name}] {r['n']:>7,} auths/s  auth-to-detection p50 {r['p50']:6.1f}ms  "
                  f"p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                  f"enforcements sent {int(stats['sent']):,} (lateral {int(stats['lateral']):,}, "
                  f"compromised {int(stats['compromised']):,})  {evs}  accounts enforced {len(enforced):,}",
                  flush=True)
            stats.clear()

    parts = list(range(PARTITIONS[AUTH]))
    await asyncio.gather(send_enforcements(), report(), read_account_events(),
                         *(read(parts[r::a.readers]) for r in range(a.readers)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="detection")
    p.add_argument("--budget-ms", type=int, default=50,
                   help="auth-to-detection p99 budget for the lane, a small share of a sub-second detection")
    p.add_argument("--readers", type=int, default=2, help="connections reading the authentication partitions")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between rounds when nothing more is waiting")
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--window-ms", type=float, default=1000, help="lateral movement window")
    p.add_argument("--min-auths", type=int, default=12, help="authentications inside the window to call it lateral")
    p.add_argument("--min-hosts", type=int, default=10, help="distinct destination hosts inside the window")
    p.add_argument("--replay-enforce-s", type=float, default=30,
                   help="after a restart, still publish enforcements for evidence younger than this")
    asyncio.run(main(p.parse_args()))
