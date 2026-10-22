"""The source of truth: every agent across Freshdesk, Freshchat and Freshcaller, and the customers writing in.

Each agent's state goes to the agent-state topic, keyed by agent, with a per-agent sequence number and the
resulting state, never only a delta. The product that made the change is on each record:
  * freshdesk: a ticket assigned to the agent, a ticket resolved, or a ticket a supervisor assigned by hand
    (load the routing engine did not pick);
  * freshchat: a chat assigned or closed;
  * freshcaller: a call starts or ends; an agent on a call gets no other work;
  * omniroute: the agent sets a status: available, away (a break) or offline (end of shift);
  * heartbeat: every agent's state is also re-sent every --heartbeat-s seconds.

It also plays the customers: new tickets and chats go to inquiries, keyed by group. A routing node (router.py)
picks an agent from its view of the lane and sends the assignment back. This script checks each assignment
against the agent's true state at that moment: an inquiry sent to an agent who is away or offline, on a call, or
already at capacity for that channel is misrouted; it waits, and in the product it would have to be routed again.
Each assignment also carries the pick the same routing node would have made from a view refreshed every few
seconds (--refresh-s on router.py), and that pick is checked the same way.

--surge starts a shift change with a spike: at --surge-at, part of the busiest group's agents log off over
--drain-s seconds, the next shift (offline until then) logs on, and that group's tickets and chats jump --surge
times, the way an outage at a large customer lands on one support team.
"""
import argparse
import asyncio
import gc
import heapq
import itertools
import random
import time
from collections import defaultdict
from apache_iggy import Consumer, Partitioning, PollingStrategy, SendMessage

from common import (ASSIGNMENTS, DEFAULT_URL, INQUIRIES, PARTITIONS, STATE, STREAM, Window, connect, ensure_topics,
                    group_id, load, msg, part_of)

G, ST, T, CT, C, CC, CALL, SEQ, SEEN = range(9)      # an agent's fields


async def main(a):
    await ensure_topics(await connect(a.url), STATE, INQUIRIES, ASSIGNMENTS)
    lane, inq_w = await connect(a.url), await connect(a.url)          # one connection per writer and reader
    as_reader = await connect(a.url)

    rng = random.Random(7)
    groups = [group_id(i) for i in range(a.groups)]
    weight = [1.0 / (i + 1) ** 0.8 for i in range(a.groups)]        # a few groups take most of the work
    total_w = sum(weight)
    ag = {}                        # agent -> [group, status, tickets, ticket cap, chats, chat cap, on call, seq, seen]
    members = defaultdict(list)    # group -> agents
    names = []                     # every agent, for random picks
    next_shift = []                # the busiest group's next shift: offline until the shift change
    outbox = []                    # (agent, message) waiting for the next tick's send
    due = []                       # heap of (when, n, agent, what): ticket and chat completions, call and break ends
    ids = itertools.count(1)
    st, lat, run = defaultdict(int), Window(), {"surge": False, "shifted": False}
    pending = {}                   # inquiry id -> (created ns, group, channel)

    def emit(k, kind, src):
        v = ag[k]
        v[SEQ] += 1
        outbox.append((k, SendMessage(msg(k=k, seq=v[SEQ], g=v[G], st=v[ST], t=v[T], ct=v[CT], c=v[C], cc=v[CC],
                                          call=v[CALL], seen=v[SEEN], src=src, kind=kind))))
        st["changes"] += 1

    def later(s, k, what):
        heapq.heappush(due, (time.monotonic() + s, next(ids), k, what))

    def new_agent(g, status):
        k = f"A{next(ids):07d}"
        ag[k] = [g, status, 0, rng.choice((4, 6, 8)), 0, rng.choice((2, 3, 4)), 0, 0, 0]
        members[g].append(k)
        names.append(k)
        return k

    # The agents at start: groups sized by their share of the work, most agents available, loads near --util.
    for i, g in enumerate(groups):
        for _ in range(max(3, round(a.agents * weight[i] / total_w))):
            new_agent(g, "available" if rng.random() < 0.88 else "away")
    for _ in range(round(len(members[groups[0]]) * a.shift_share)):
        next_shift.append(new_agent(groups[0], "offline"))
    on = [k for k in names if ag[k][ST] != "offline"]
    ticket_rate = a.inquiries * a.ticket_share + a.manual
    chat_rate = a.inquiries * (1 - a.ticket_share)
    handle_t = a.util * sum(ag[k][CT] for k in on) / ticket_rate     # time a ticket stays open, so load holds at --util
    handle_c = a.util * sum(ag[k][CC] for k in on) / chat_rate
    for k in on:
        v = ag[k]
        v[T] = min(v[CT], round(v[CT] * a.util * rng.uniform(0.6, 1.3)))
        v[C] = min(v[CC], round(v[CC] * a.util * rng.uniform(0.6, 1.3)))
        for _ in range(v[T]):
            later(rng.uniform(0, 2 * handle_t), k, "ticket")
        for _ in range(v[C]):
            later(rng.uniform(0, 2 * handle_c), k, "chat")
        if v[ST] == "away":
            later(rng.uniform(1, a.break_s), k, "back")

    async def send(writer, topic, items):
        out = defaultdict(list)            # one batch per partition: a key's messages stay in order in one partition
        for key, m in items:
            out[part_of(key, PARTITIONS[topic])].append(m)
        await asyncio.gather(*(writer.send_messages(STREAM, topic, Partitioning.partition_id(p), ms)
                               for p, ms in out.items()))

    async def snapshot():
        # Every agent's starting state (seq 1, not timed as a live change), like a full load of the routing view.
        for k in names:
            emit(k, "snapshot", "omniroute")
        items, outbox[:] = list(outbox), []
        for i in range(0, len(items), 5000):
            await send(lane, STATE, items[i:i + 5000])
        n_on = sum(1 for v in ag.values() if v[ST] == "available")
        print(f"[agents] snapshot: {len(ag):,} agents in {a.groups} groups, {n_on:,} available, "
              f"{len(next_shift)} of the busiest group's next shift offline; tickets stay open ~{handle_t:.0f} s, "
              f"chats ~{handle_c:.0f} s at {a.util:.0%} load", flush=True)

    def any_agent(cond):
        for _ in range(6):                 # a few tries
            k = names[rng.randrange(len(names))]
            if cond(ag[k]):
                return k
        return None

    def tick(n_inq, n_surge, n_calls, n_breaks, n_manual, now_ns):
        reqs = []
        for j in range(n_inq + n_surge):
            g = groups[0] if j >= n_inq else rng.choices(groups, weight)[0]
            ch = "ticket" if rng.random() < a.ticket_share else "chat"
            rid = f"q{next(ids)}"
            pending[rid] = (now_ns, g, ch)
            reqs.append((g, SendMessage(msg(id=rid, g=g, ch=ch, ns=now_ns))))
            st["requests"] += 1
        for _ in range(n_calls):           # Freshcaller routes its own calls; the agent is busy until it ends
            k = any_agent(lambda v: v[ST] == "available" and not v[CALL])
            if k:
                ag[k][CALL] = 1
                emit(k, "call_start", "freshcaller")
                later(rng.expovariate(1 / a.call_s), k, "call_end")
                st["calls"] += 1
        for _ in range(n_breaks):
            k = any_agent(lambda v: v[ST] == "available" and not v[CALL] and v[G] != groups[0])
            if k:
                ag[k][ST] = "away"
                emit(k, "status_away", "omniroute")
                later(rng.uniform(0.5, 1.5) * a.break_s, k, "back")
                st["breaks"] += 1
        for _ in range(n_manual):          # a supervisor assigns a ticket by hand: load the router did not pick
            k = any_agent(lambda v: v[ST] == "available" and v[T] < v[CT])
            if k:
                ag[k][T] += 1
                emit(k, "ticket_assigned_by_hand", "freshdesk")
                later(rng.expovariate(1 / handle_t), k, "ticket")
                st["manual"] += 1
        return reqs

    def complete_due():
        now = time.monotonic()
        while due and due[0][0] <= now:
            _, _, k, what = heapq.heappop(due)
            v = ag[k]
            if what == "ticket" and v[T] > 0:
                v[T] -= 1
                emit(k, "ticket_resolved", "freshdesk")
            elif what == "chat" and v[C] > 0:
                v[C] -= 1
                emit(k, "chat_closed", "freshchat")
            elif what == "call_end" and v[CALL]:
                v[CALL] = 0
                emit(k, "call_end", "freshcaller")
            elif what == "back" and v[ST] == "away":
                v[ST] = "available"
                emit(k, "status_available", "omniroute")
            elif what == "log_off" and v[ST] != "offline":
                v[ST] = "offline"
                emit(k, "status_offline", "omniroute")
            elif what == "log_on" and v[ST] == "offline":
                v[ST] = "available"
                emit(k, "status_available", "omniroute")

    def shift_change():
        # The busiest group's shift ends while its tickets spike: part of it logs off, the next shift logs on.
        g = groups[0]
        leaving = [k for k in members[g] if ag[k][ST] != "offline" and k not in next_shift]
        rng.shuffle(leaving)
        leaving = leaving[:len(next_shift)]
        for k in leaving:
            later(rng.uniform(0, a.drain_s), k, "log_off")
        for k in next_shift:
            later(rng.uniform(0, a.drain_s), k, "log_on")
        run["shifted"] = True
        print(f"[agents] shift change in {g}: {len(leaving)} agents logging off and {len(next_shift)} logging on "
              f"within {a.drain_s:g} s; its tickets and chats x{a.surge:g} for {a.surge_for:g} s", flush=True)

    def heartbeats(n):
        for _ in range(n):
            k = names[rng.randrange(len(names))]
            emit(k, "heartbeat", "omniroute")

    async def run_agents():
        await snapshot()
        gc.freeze()                        # keep start-up tables out of the collector's way
        t0 = nxt = time.monotonic()
        owed = defaultdict(float)
        g0_share = weight[0] / total_w
        while True:
            t = time.monotonic() - t0
            surge = a.surge > 1 and a.surge_at <= t < a.surge_at + a.surge_for
            if surge and not run["shifted"]:
                shift_change()
            run["surge"] = surge
            owed["q"] += a.inquiries / 100
            owed["s"] += (a.surge - 1) * a.inquiries * g0_share / 100 if surge else 0
            owed["c"] += a.calls / 100
            owed["b"] += a.breaks / 100
            owed["m"] += a.manual / 100
            owed["h"] += len(names) / a.heartbeat_s / 100
            n = {x: int(v) for x, v in owed.items()}
            for x in n:
                owed[x] -= n[x]
            now_ns = time.time_ns()
            reqs = tick(n["q"], n["s"], n["c"], n["b"], n["m"], now_ns)
            heartbeats(n["h"])
            complete_due()
            changes, outbox[:] = list(outbox), []
            # The tick's agent changes and new inquiries go out together, each writer on its own connection.
            t_send = time.monotonic()
            await asyncio.gather(send(lane, STATE, changes), send(inq_w, INQUIRIES, reqs))
            st["send_ms"] = max(st["send_ms"], 1000 * (time.monotonic() - t_send))
            st["late_ms"] = max(st["late_ms"], 1000 * (t_send - nxt))   # how late this tick started
            nxt += 0.01
            await asyncio.sleep(max(0.0, nxt - time.monotonic()))

    def verdict(k, ch):
        v = ag.get(k)
        if v is None:
            return "none"
        if v[ST] != "available":
            return "away"
        if v[CALL]:
            return "call"
        if ch == "ticket":
            return "full" if v[T] >= v[CT] else "ok"
        return "full" if v[C] >= v[CC] else "ok"

    async def read_assignments():
        # Every partition read directly, from start-up on, on its own connection.
        who = {p: Consumer.Single(f"agents-check-{p}") for p in range(PARTITIONS[ASSIGNMENTS])}
        since = PollingStrategy.Timestamp(time.time_ns() // 1000)
        nx = {}
        while True:
            more = False
            for p in who:
                ms = await as_reader.poll_messages(STREAM, ASSIGNMENTS, consumer=who[p], partition_id=p, count=1000,
                                                   auto_commit=False,
                                                   polling_strategy=PollingStrategy.Offset(nx[p]) if p in nx else since)
                if ms:
                    nx[p] = ms[-1].offset() + 1
                    for m in ms:
                        on_assignment(m)
                    more = more or len(ms) == 1000
            if not more:
                await asyncio.sleep(0.001)

    def on_assignment(m):
        r = load(m.payload())
        req = pending.pop(r["id"], None)
        if req is None:
            return                         # assigned twice (at-least-once) or already timed out
        lat.add((time.time_ns() - req[0]) / 1e6)
        ch = req[2]
        st["routed"] += 1
        k = r["a"]
        live_v = verdict(k, ch) if k else "none"
        alt_v = verdict(r["alt"], ch) if r["alt"] else "none"   # both picks judged on the same state, before either lands
        if k in ag:
            ag[k][SEEN] += 1               # the product has processed this assignment (taken or sent back)
        if live_v == "ok":
            v = ag[k]
            if ch == "ticket":
                v[T] += 1
                emit(k, "ticket_assigned", "freshdesk")
                later(rng.expovariate(1 / handle_t), k, "ticket")
            else:
                v[C] += 1
                emit(k, "chat_assigned", "freshchat")
                later(rng.expovariate(1 / handle_c), k, "chat")
            st["taken"] += 1
        else:
            st[f"live_{live_v}"] += 1
            if k in ag:
                emit(k, "sent_back", "freshdesk" if ch == "ticket" else "freshchat")
        if alt_v != "ok":
            st[f"alt_{alt_v}"] += 1

    async def report():
        while True:
            await asyncio.sleep(1)
            cutoff = time.time_ns() - 5_000_000_000
            for rid in [q for q, p in pending.items() if p[0] < cutoff]:
                del pending[rid]
                st["unrouted"] += 1        # no assignment within 5 s
            r = lat.flush()
            c = st["routed"]
            mis = st["live_away"] + st["live_call"] + st["live_full"]
            alt = st["alt_away"] + st["alt_call"] + st["alt_full"]
            print(f"[agents] {st['changes']:>6,} changes/s  inquiries {st['requests']:,} "
                  f"routed {c:,} ({'surge' if run['surge'] else 'steady'})  misrouted {mis:,} "
                  f"(away {st['live_away']:,} on a call {st['live_call']:,} at capacity {st['live_full']:,}) "
                  f"no agent {st['live_none']:,}  a view refreshed every {a.refresh_s:g}s would misroute {alt:,} "
                  f"(away {st['alt_away']:,} on a call {st['alt_call']:,} at capacity {st['alt_full']:,}) "
                  f"no agent {st['alt_none']:,}  calls {st['calls']:,} breaks {st['breaks']:,} "
                  f"by hand {st['manual']:,}  unrouted {st['unrouted']:,}  "
                  f"inquiry-to-assignment p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms  "
                  f"tick late max {st['late_ms']:.0f}ms  send max {st['send_ms']:.0f}ms", flush=True)
            st.clear()

    await asyncio.gather(run_agents(), read_assignments(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--agents", type=int, default=20_000, help="agents across Freshdesk, Freshchat and Freshcaller")
    p.add_argument("--groups", type=int, default=2_000, help="agent groups with automatic routing on")
    p.add_argument("--inquiries", type=float, default=300, help="new tickets and chats per second")
    p.add_argument("--ticket-share", type=float, default=0.6, help="share of inquiries that are tickets (rest: chats)")
    p.add_argument("--util", type=float, default=0.75, help="average agent load as a share of capacity")
    p.add_argument("--calls", type=float, default=40, help="Freshcaller calls starting per second")
    p.add_argument("--call-s", type=float, default=240, help="mean call length in seconds")
    p.add_argument("--breaks", type=float, default=20, help="agents going away (a break) per second")
    p.add_argument("--break-s", type=float, default=600, help="typical break length in seconds")
    p.add_argument("--manual", type=float, default=20, help="tickets assigned by hand per second")
    p.add_argument("--heartbeat-s", type=float, default=10, help="every agent's state is re-sent this often")
    p.add_argument("--surge", type=float, default=1, help="inquiry multiplier for the busiest group (1 = off)")
    p.add_argument("--surge-at", type=float, default=60, help="seconds after start")
    p.add_argument("--surge-for", type=float, default=30)
    p.add_argument("--shift-share", type=float, default=0.5, help="share of the busiest group that changes shift")
    p.add_argument("--drain-s", type=float, default=10, help="the shift change happens within this many seconds")
    p.add_argument("--refresh-s", type=float, default=3, help="the comparison view's refresh, as set on router.py (label only)")
    asyncio.run(main(p.parse_args()))
