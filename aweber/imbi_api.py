"""Imbi's services as producers on the notifications lane: deployments finishing (or rolling back) and users being
mentioned in comments, the two examples Imbi's ADR 0019 gives for the future `notifications` stream.

Each notification is keyed (deployment by project, mention by the mentioned user), carries that key's sequence
number and the time it was published, and goes to partition crc32(key) % partitions, so a project's deployments
reach every reader in order. One batch per partition per 10 ms tick, sent together.

Delivery on Iggy is at-least-once, as the ADR says. --resend re-sends a share of notifications a second time, the
way a retried publish does; the readers must post each one once.

--wave-at / --wave-x / --wave-s: a release wave, many projects deploying at once, for a while.

Prints notifications/s by topic every second.
"""
import argparse
import asyncio
import gc
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import (DEFAULT_URL, DEPLOYMENT, ENTRY_TYPES, ENVIRONMENTS, LANE, MENTION, NOTIFY, PARTITIONS, ensure,
                    msg, part, project, user)


async def main(a):
    async def own_client():                 # one connection per producer
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure(await own_client(), NOTIFY, *LANE)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    prod = {t: await (await own_client()).producer(NOTIFY, t, topic_partitions_count=PARTITIONS[t], mode=cfg)
            for t in LANE}
    seq = defaultdict(int)                  # per (topic, key)
    version = defaultdict(int)              # per project: the version it deploys next
    resend = []                             # (topic, pid, bytes) to publish again next tick
    stats = defaultdict(int)
    gc.freeze()

    def make():
        if random.random() < a.deploy_share:
            t, p = DEPLOYMENT, project(random.randrange(a.projects))
            version[p] += 1
            seq[(t, p)] += 1
            body = msg(id=f"d-{p}-{seq[(t, p)]}", k=p, seq=seq[(t, p)], project_slug=p,
                       environment_slug=random.choice(ENVIRONMENTS), entry_type=random.choice(ENTRY_TYPES),
                       version=f"2.{version[p] // 100}.{version[p] % 100}", performed_by=user(random.randrange(a.users)))
            return t, part(p, PARTITIONS[t]), body
        t, u = MENTION, user(random.randrange(a.users))
        seq[(t, u)] += 1
        body = msg(id=f"m-{u}-{seq[(t, u)]}", k=u, seq=seq[(t, u)], user=u,
                   project_slug=project(random.randrange(a.projects)), comment_id=f"c-{random.getrandbits(40):x}",
                   by=user(random.randrange(a.users)))
        return t, part(u, PARTITIONS[t]), body

    async def run():
        start = time.monotonic()
        end = start + (a.duration or 1e12)
        nxt, t0, owed, last = start, start, 0.0, start
        while time.monotonic() < end:
            now = time.monotonic()
            el = now - start
            rate = a.rate * (a.wave_x if a.wave_at <= el < a.wave_at + a.wave_s else 1)
            owed += rate * (now - last)     # what this tick owes, so a slow send is made up in the next one
            last, n = now, int(owed)
            owed -= n
            batches = defaultdict(list)
            for t, pid, body in resend:     # last tick's retries go out again, unchanged
                batches[(t, pid)].append(SendMessage(body))
                stats["resent"] += 1
            resend.clear()
            for _ in range(n):
                t, pid, body = make()
                batches[(t, pid)].append(SendMessage(body))
                stats[t] += 1
                if random.random() < a.resend:
                    resend.append((t, pid, body))
            if batches:
                await asyncio.gather(*(prod[t].send_with_partitioning(ms, Partitioning.partition_id(pid))
                                       for (t, pid), ms in batches.items()))
            if now - t0 >= 1:
                wave = "  (release wave)" if a.wave_at <= el < a.wave_at + a.wave_s else ""
                print(f"[imbi-api] {stats[DEPLOYMENT] + stats[MENTION]:>6,} notifications/s  deployment "
                      f"{stats[DEPLOYMENT]:,}  mention {stats[MENTION]:,}  resent {stats['resent']:,}{wave}", flush=True)
                stats.clear()
                t0 = now
            nxt += 0.01                     # scheduled from the due time, so a slow tick doesn't drift
            await asyncio.sleep(max(0.0, nxt - time.monotonic()))
        print("[imbi-api] done", flush=True)

    await run()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--rate", type=float, default=200, help="notifications a second (deployments + mentions)")
    ap.add_argument("--deploy-share", type=float, default=0.6, help="share of notifications that are deployments")
    ap.add_argument("--projects", type=int, default=2000)
    ap.add_argument("--users", type=int, default=600)
    ap.add_argument("--resend", type=float, default=0.01, help="share of notifications published twice")
    ap.add_argument("--wave-at", type=float, default=20, help="seconds in when the release wave starts")
    ap.add_argument("--wave-x", type=float, default=5, help="rate multiplier during the wave")
    ap.add_argument("--wave-s", type=float, default=10, help="how long the wave lasts")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until stopped)")
    asyncio.run(main(ap.parse_args()))
