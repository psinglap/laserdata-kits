"""A fleet of payments API nodes: every card attempt onto the lane, and every card testing intervention applied
before the business's next attempt.

Plays --nodes payments API nodes in one process. Each node checks every card setup or payment attempt against
its own view of businesses under card testing controls. With no controls the attempt goes on to the issuer;
under controls it gets a challenge (here, a CAPTCHA): a tester's script fails it and is blocked before the
issuer, a real buyer passes it and goes on. Either way the node publishes one record to card-attempts, keyed
by business: node, business, surface, amount, card fingerprint, outcome, a per-(node, business) sequence.
Each node also reads attack-controls directly on its own connection and applies every intervention and every
easing to its own view. A node never sees another's view, so a change reaches every node through the log.

The same process plays the outside world, on fixed schedules (all illustrative):
  * the businesses' own buyers: --attempts-per-s over --businesses, a tenth of the businesses carrying most
    of it, $5-$300 each, about 6% declined by the issuer;
  * a CARD TESTING attack every --attack-every s on one business: a script runs --attack-rps small or
    zero-amount attempts a second on fresh cards for --attack-for s; issuers decline about 85% of them, and
    each approval confirms a stolen card for the tester. The script keeps going after it is challenged.
  * a FLASH SALE every --sale-every s on another business: --sale-rps real buyers a second for --sale-for s.
    It is a spike too, and must not be put under controls.

The business number: tester attempts that reached an issuer after the business's controls were published,
because the controls had not reached that node yet, and the stolen cards they confirmed; and the same for a
lane --compare-ms behind. Prints it every second, with control-to-nodes p50/p99: the second hop of the loop.
"""
import argparse
import asyncio
import gc
import os
import random
import statistics
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import Consumer, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (ATTEMPTS, CONTROLS, DEFAULT_URL, PARTITIONS, STREAM, SURFACES, Window, business,
                    ensure_topics, load, msg, part)


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), ATTEMPTS, CONTROLS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, ATTEMPTS, topic_partitions_count=PARTITIONS[ATTEMPTS],
                                                   mode=cfg)
    start_ns = time.time_ns()
    nodes = [f"api-node-{i:02d}" for i in range(a.nodes)]
    accts = [business(i) for i in range(a.businesses)]
    busy = accts[: max(1, a.businesses // 10)]
    pid = {b: part(b, PARTITIONS[ATTEMPTS]) for b in accts}
    view = {s: {} for s in nodes}           # node -> {business: controls ns}
    last = {s: {} for s in nodes}           # node -> {business: last control sequence applied}
    seq = defaultdict(int)                  # (node, business) -> attempt sequence
    attacks, sales = {}, {}                 # business -> (start ns, end ns)
    kind = {}                               # business -> "attack" | "sale", for businesses the outside world picked
    recent = defaultdict(lambda: deque(maxlen=4096))   # attacked business -> (ns, reached, confirmed) attempts
    truth = {}                              # business -> controls ns, once the first node has it
    detect = []                             # attack: first tester attempt to controls published, ms
    lat, stats = Window(), defaultdict(float)
    cmp_ns = a.compare_ms * 1_000_000

    def account(b, t, reached, confirmed):
        """One tester attempt on an attacked business, counted against its controls once they are known."""
        d = truth.get(b)
        if d is None:
            recent[b].append((t, reached, confirmed))
            return
        if t <= d:
            return
        if t - d < cmp_ns:
            stats["would"] += 1             # a lane --compare-ms behind would have let it reach the issuer
            stats["would_cards"] += confirmed
        if reached:
            stats["after"] += 1             # reached the issuer after the controls were published
            stats["after_cards"] += confirmed

    def reader(s):
        async def run():
            c = await own_client()
            who, nxt = Consumer.Single(f"{s}-{os.getpid()}"), None
            while True:
                ms = await c.poll_messages(STREAM, CONTROLS, consumer=who, partition_id=0, count=1000,
                                           auto_commit=False,
                                           polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None
                                           else PollingStrategy.First())
                if not ms:
                    await asyncio.sleep(a.poll_ms / 1000)
                    continue
                nxt = ms[-1].offset() + 1
                for m in ms:
                    d = load(m.payload())
                    b = d["b"]
                    if d["seq"] <= last[s].get(b, 0):
                        continue            # older or repeated change: already applied
                    last[s][b] = d["seq"]
                    if d["state"] == "controls":
                        view[s][b] = d["ns"]
                    else:
                        view[s].pop(b, None)
                    if d["ns"] <= start_ns:
                        continue            # replayed at start-up: applied, not timed
                    lat.add((time.time_ns() - d["ns"]) / 1e6)
                    if d["state"] == "controls" and b not in truth:
                        truth[b] = d["ns"]
                        stats["controls"] += 1
                        if kind.get(b) != "attack":
                            stats["unplanted"] += 1     # controls on a business nobody attacked: a false positive
                            stats["sale_flagged"] += kind.get(b) == "sale"
                        else:
                            detect.append((d["ns"] - attacks.get(b, (d["ns"],))[0]) / 1e6)
                        for t, reached, confirmed in recent.pop(b, ()):
                            account(b, t, reached, confirmed)
                    elif d["state"] == "eased" and s == nodes[0]:
                        stats["eased"] += 1
        return run

    async def outside_world():
        # Attacks and flash sales on their own schedules, from their due times so a slow tick doesn't drift them.
        t0 = time.monotonic()
        next_a, next_s = t0 + a.first_at, t0 + a.first_at + a.sale_every / 2
        while True:
            now = time.monotonic()
            free = [b for b in random.sample(busy, 20) if b not in kind]
            if now >= next_a and free:
                b = free[0]
                kind[b] = "attack"
                at = time.time_ns()
                attacks[b] = (at, at + int(a.attack_for * 1e9))
                stats["attacks"] += 1
                next_a += a.attack_every
            if now >= next_s and len(free) > 1:
                b = free[1]
                kind[b] = "sale"
                at = time.time_ns()
                sales[b] = (at, at + int(a.sale_for * 1e9))
                stats["sales"] += 1
                next_s += a.sale_every
            await asyncio.sleep(0.05)

    def attempt(s, b, batches, tester):
        t = time.time_ns()
        controlled = b in view[s]
        if tester:
            amt = random.choice((0.0, 0.0, round(random.uniform(0.5, 1.0), 2)))
            surf = random.choice(("setup_intent", "payment_element"))
            ok = random.random() < a.tester_approve
            account(b, t, not controlled, ok)
            stats["tester"] += 1
            if controlled:
                out = "challenge_failed"    # the script cannot pass the challenge: blocked before the issuer
                stats["blocked"] += 1
            else:
                out = "approved" if ok else "declined"
                stats["reached"] += 1
        else:
            amt = round(random.uniform(5, 300), 2)
            surf = random.choice(SURFACES)
            out = "approved" if random.random() < a.buyer_approve else "declined"
            if controlled:
                stats["challenged"] += 1    # a real buyer passes the challenge and goes on to the issuer
        seq[(s, b)] += 1
        batches[pid[b]].append(SendMessage(msg(s=s, b=b, surf=surf, amt=amt, card=f"{random.getrandbits(48):012x}",
                                               out=out, ch=int(controlled), seq=seq[(s, b)], ns=t)))
        stats["attempts"] += 1

    async def attempt_loop():
        t0 = due = time.monotonic()
        owed = 0.0
        spike_owed = defaultdict(float)
        async with producer:
            while True:
                t = time.monotonic() - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed += a.attempts_per_s * x / 100
                batches = defaultdict(list)
                for _ in range(int(owed)):
                    b = random.choice(busy) if random.random() < 0.7 else random.choice(accts)
                    attempt(random.choice(nodes), b, batches, False)
                owed -= int(owed)
                now_ns = time.time_ns()
                for spikes, rps, tester in ((attacks, a.attack_rps, True), (sales, a.sale_rps, False)):
                    for b, (st, end) in list(spikes.items()):
                        if now_ns >= end:
                            del spikes[b]
                            spike_owed.pop(b, None)
                            continue
                        spike_owed[b] += rps / 100
                        for _ in range(int(spike_owed[b])):
                            attempt(random.choice(nodes), b, batches, tester)
                        spike_owed[b] -= int(spike_owed[b])
                # One batch per partition, all sent at once: per-business order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(q))
                                           for q, ms in batches.items()))
                stats["peak"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        print(f"[nodes] {a.nodes} payments API nodes, {a.businesses:,} businesses, a card testing attack every "
              f"{a.attack_every:g} s at {a.attack_rps:g} attempts/s and a flash sale every {a.sale_every:g} s "
              f"at {a.sale_rps:g} attempts/s", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "peak" if stats["peak"] else "steady"
            win = f"{statistics.median(detect):.0f}ms" if detect else "-"
            print(f"[nodes] {int(stats['attempts']):>7,} attempts/s ({phase})  tester attempts {int(stats['tester']):,}  "
                  f"blocked {int(stats['blocked']):,}  buyers challenged {int(stats['challenged']):,}  "
                  f"attacks {int(stats['attacks'])}  controls {int(stats['controls'])} (attack detected in {win} median)  "
                  f"eased {int(stats['eased'])}  "
                  f"reached issuers after controls {int(stats['after']):,} ({int(stats['after_cards'])} cards "
                  f"confirmed; at {a.compare_ms:g} ms it would be {int(stats['would']):,}, "
                  f"{int(stats['would_cards'])} cards)  flash sales {int(stats['sales'])}, flagged {int(stats['sale_flagged'])}  "
                  f"false controls {int(stats['unplanted'])}  "
                  f"control-to-nodes n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("attempts", "attacks", "sales", "tester", "blocked", "reached", "challenged", "controls",
                      "eased", "after", "after_cards", "would", "would_cards", "sale_flagged", "unplanted"):
                stats[k] = 0
            del detect[:-200]

    readers = [reader(s)() for s in nodes]
    gc.freeze()                             # keep start-up objects out of the collector's way
    await asyncio.gather(attempt_loop(), report(), outside_world(), *readers)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--nodes", type=int, default=12, help="payments API nodes, each with its own attack-controls reader")
    p.add_argument("--businesses", type=int, default=100_000, help="businesses taking payments")
    p.add_argument("--attempts-per-s", type=float, default=8_000, help="real buyers' attempts per second, all nodes")
    p.add_argument("--buyer-approve", type=float, default=0.94, help="share of real buyers' attempts issuers approve")
    p.add_argument("--attack-every", type=float, default=3, help="seconds between card testing attacks")
    p.add_argument("--attack-rps", type=float, default=150, help="tester attempts per second in one attack")
    p.add_argument("--attack-for", type=float, default=8, help="seconds a tester's script keeps going")
    p.add_argument("--tester-approve", type=float, default=0.15, help="share of tester attempts issuers approve")
    p.add_argument("--sale-every", type=float, default=5, help="seconds between flash sales")
    p.add_argument("--sale-rps", type=float, default=200, help="real buyers' attempts per second in one flash sale")
    p.add_argument("--sale-for", type=float, default=4, help="seconds a flash sale lasts")
    p.add_argument("--first-at", type=float, default=5, help="seconds before the first attack")
    p.add_argument("--compare-ms", type=float, default=1000, help="also count what a lane this far behind would let through")
    p.add_argument("--poll-ms", type=float, default=1, help="attack-controls poll wait when nothing is waiting")
    p.add_argument("--peak", type=float, default=1, help="attempt-rate multiplier during the peak (Black Friday)")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
