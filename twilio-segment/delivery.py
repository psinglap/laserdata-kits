"""The delivery fleet and the alert service: every delivery attempt onto the lane, and every alert received.

Plays --workers delivery workers in one process, delivering --rate attempts a second to --keys workspace
destinations (one workspace's connection to one destination). Volume follows a power law (--skew), as customer
traffic does: a few workspace destinations carry most attempts and most carry a few a second. Each workspace
destination is owned by one worker. Every attempt is published to delivery-outcomes, keyed by
workspace:destination:
  * worker, the worker's sequence for that key, HTTP status, delivered or failed, attempt number.
A small --base-fail share of attempts fails anywhere (timeouts, a 500 now and then) and should never alert.

Destination incidents are planted here (the kit's ground truth): every --incident-every seconds one workspace
destination starts failing for --incident-for seconds, in turn: 401 (credentials revoked, every attempt fails),
429 (rate limited by the partner, half fail) and 503 (partner API down, 80% fail).

The alert service reads delivery-alerts on its own connection and consumer group, as the notifier behind email,
Slack and in-app alerts would.

The business number, per incident: time from the first failing attempt to the alert reaching the alert service,
and the failed attempts on that destination before the customer could be told. Each second it prints attempts,
failures, alerts received (and any for a destination that had no incident), and alert-to-service p50/p99.
"""
import argparse
import asyncio
import gc
import itertools
import os
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (ALERT, ALERTS, DEFAULT_URL, DESTINATIONS, INCIDENTS, OUTCOMES, PARTITIONS, STREAM, Window,
                    ensure_topics, key, load, msg, part, workspace_id)


def build_keys(n: int, rnd: random.Random):
    """n workspace destinations: each workspace connects 1-4 destinations."""
    keys, ws = [], 0
    while len(keys) < n:
        for dest in rnd.sample(DESTINATIONS, rnd.randint(1, 4)):
            keys.append(key(workspace_id(ws), dest))
        ws += 1
    keys = keys[:n]
    rnd.shuffle(keys)                       # rank (volume) is independent of workspace number
    return keys


async def main(a):
    async def own_client():                 # one connection per producer and per reader
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), OUTCOMES, ALERTS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    producer = await (await own_client()).producer(STREAM, OUTCOMES, topic_partitions_count=PARTITIONS[OUTCOMES],
                                                   mode=cfg)
    start_ns = time.time_ns()
    rnd = random.Random(11)
    keys = build_keys(a.keys, rnd)
    cw = list(itertools.accumulate(1.0 / (i + 1) ** a.skew for i in range(len(keys))))
    share = {k: (cw[i] - (cw[i - 1] if i else 0)) / cw[-1] for i, k in enumerate(keys)}
    workers = [f"delivery-{i:02d}" for i in range(a.workers)]
    owner = {k: workers[part(k, len(workers))] for k in keys}
    pid = {k: part(k, PARTITIONS[OUTCOMES]) for k in keys}
    lo, hi = (int(x) for x in a.target_ranks.split("-"))
    targets = keys[lo:hi + 1]
    rnd.shuffle(targets)

    seq = defaultdict(int)                  # (worker, key) -> sequence
    incident = {"k": None}                  # the open incident (ground truth), see start_incident()
    planted = set()                         # every key that had an incident
    alerted = {}                            # key -> state as the alert service last heard it
    vns = {}                                # key -> ns of the newest alert applied
    lat, stats = Window(), defaultdict(float)

    def on_alert(m):
        d = load(m.payload())
        k = d["k"]
        if d["ns"] <= vns.get(k, 0):
            return                          # older or repeated alert: the newest wins
        vns[k] = d["ns"]
        alerted[k] = d["state"]
        if d["ns"] < start_ns:
            return                          # replayed from before this run: state only
        now = time.time_ns()
        lat.add((now - d["ns"]) / 1e6)
        stats["received"] += 1
        if d["state"] != ALERT:
            stats["resolved"] += 1
            return
        if k not in planted:
            stats["unplanned"] += 1
            print(f"[delivery] alert for {k} with no planted incident (delivered {d['rate']}%)", flush=True)
            return
        if incident["k"] == k and not incident["alert_ns"]:
            incident["alert_ns"] = d["ns"]
            print(f"[delivery] incident {k} {incident['status']} ({incident['why']}, ~{incident['rate']:.0f} attempts/s): "
                  f"first failure to alert {(d['ns'] - incident['first_fail']) / 1e6:,.0f} ms, "
                  f"alert to alert service {(now - d['ns']) / 1e6:.1f} ms, "
                  f"failed attempts before the alert {incident['failed']:,}, delivered {d['rate']}% "
                  f"(threshold {d['thr'] * 100:.0f}%)", flush=True)
            stats["incidents"] += 1

    async def on_alert_async(m):
        on_alert(m)

    alert_reader = await (await own_client()).consumer_group(
        f"alert-service-{os.getpid()}", STREAM, ALERTS, polling_strategy=PollingStrategy.First(), batch_length=1000,
        auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500))
    gc.freeze()                             # keep start-up objects out of the collector's way

    def start_incident(n):
        k = targets[n % len(targets)]
        status, fail, why = INCIDENTS[n % len(INCIDENTS)]
        planted.add(k)
        incident.update(k=k, status=status, fail=fail, why=why, end=time.monotonic() + a.incident_for,
                        first_fail=0, alert_ns=0, failed=0, rate=a.rate * share[k])

    async def deliver_loop():
        t0 = last = next_t = time.monotonic()
        owed, n_inc = 0.0, 0
        idx = range(len(keys))
        async with producer:
            while True:
                now = time.monotonic()
                el = now - t0
                x = a.peak if a.peak_at <= el < a.peak_at + a.peak_for else 1
                if incident["k"] and now >= incident["end"]:
                    incident["k"] = None
                if a.incident_every and el >= a.incident_at + n_inc * a.incident_every and not incident["k"]:
                    start_incident(n_inc)
                    n_inc += 1
                owed += a.rate * x * (now - last)   # what this tick owes, so a slow tick is made up in the next
                last, n = now, int(owed)
                owed -= n
                now_ns = time.time_ns()
                batches = defaultdict(list)
                for i in rnd.choices(idx, cum_weights=cw, k=n):
                    k = keys[i]
                    w = owner[k]
                    if k == incident["k"] and random.random() < incident["fail"]:
                        ok, status = False, incident["status"]
                        incident["first_fail"] = incident["first_fail"] or now_ns
                        if not incident["alert_ns"]:
                            incident["failed"] += 1
                            stats["before_alert"] += 1
                    elif random.random() < a.base_fail:
                        ok, status = False, random.choice(("500", "504", "timeout"))
                    else:
                        ok, status = True, "200"
                    seq[(w, k)] += 1
                    batches[pid[k]].append(SendMessage(msg(
                        k=k, w=w, seq=seq[(w, k)], ok=ok, status=status,
                        attempt=1 if random.random() < 0.97 else random.randint(2, 6))))
                    stats["attempts"] += 1
                    stats["failed"] += not ok
                # One batch per partition, all sent at once: per-key order is kept, one round trip per tick.
                if batches:
                    await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                           for p, ms in batches.items()))
                stats["peak"] = x > 1
                next_t += 0.01
                await asyncio.sleep(max(0.0, next_t - time.monotonic()))

    async def report():
        print(f"[delivery] {a.workers} delivery workers, {len(keys):,} workspace destinations, ~{a.rate:,.0f} attempts/s, "
              f"incident every {a.incident_every:g} s for {a.incident_for:g} s on destinations carrying "
              f"{a.rate * share[keys[hi]]:,.0f}-{a.rate * share[keys[lo]]:,.0f} attempts/s", flush=True)
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            phase = "surge" if stats["peak"] else "steady"
            open_now = sum(1 for v in alerted.values() if v == ALERT)
            print(f"[delivery] {int(stats['attempts']):>7,} attempts/s ({phase})  failed {int(stats['failed']):,}  "
                  f"failed on an incident before its alert {int(stats['before_alert']):,}  "
                  f"alerts received {int(stats['received']):,} (resolved {int(stats['resolved']):,}, "
                  f"no incident {int(stats['unplanned']):,})  open {open_now}  "
                  f"alert-to-service n {r['n']:,} p50 {r['p50']:.1f}ms p99 {r['p99']:.1f}ms", flush=True)
            for k in ("attempts", "failed", "before_alert", "received", "resolved", "unplanned"):
                stats[k] = 0

    await asyncio.gather(deliver_loop(), report(), alert_reader.consume_messages(on_alert_async, asyncio.Event()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--workers", type=int, default=8, help="delivery workers; each workspace destination has one owner")
    p.add_argument("--keys", type=int, default=20_000, help="workspace destinations (workspace x destination)")
    p.add_argument("--rate", type=float, default=20_000, help="delivery attempts per second, all keys")
    p.add_argument("--skew", type=float, default=1.1, help="power-law exponent of volume per workspace destination")
    p.add_argument("--base-fail", type=float, default=0.002, help="share of attempts that fail with no incident")
    p.add_argument("--target-ranks", default="3-20", help="volume ranks of the workspace destinations given incidents")
    p.add_argument("--incident-at", type=float, default=15, help="seconds after start of the first incident")
    p.add_argument("--incident-every", type=float, default=10, help="seconds between incidents (0 = none)")
    p.add_argument("--incident-for", type=float, default=6, help="seconds each incident lasts")
    p.add_argument("--peak", type=float, default=1, help="attempt-rate multiplier during a traffic peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
