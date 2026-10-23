"""Account activity: logins and profile changes as account-to-asset links, and the payments that follow them.

Plays the event side of PayPal's real-time graph. PayPal's graph post (Dec 2021) says production
data sources are "abstracted as events/messages (such as login, transaction ...)", and that consuming
them creates "new account vertices, and new edge links". This script publishes, keyed as below:
  * asset-links: the lane. Each link between an account and an asset (device ID, IP, VID or
    address), keyed by asset, with a sequence number and the resulting state of that edge;
  * payment-checks: when an account pays a few seconds after logging in, the two-hop graph check
    on that payment. It lists the links from that login, which the graph must already hold.

At start-up it loads a snapshot, as the offline channel does: restricted accounts, seven to each
known-bad profile asset (the post's repeat-fraudster example). A small share of new logins are
repeat fraudsters: a new account that adds one of those known-bad assets and pays within seconds.
--peak multiplies the login rate for --peak-for seconds.
"""
import argparse
import asyncio
import gc
import heapq
import random
import time
import zlib
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage

from common import CHECKS, DEFAULT_URL, LINKS, PARTITIONS, STREAM, account_id, ensure_topics, msg, part


async def main(a):
    async def own_client():                 # one connection per producer, so neither waits on the other
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    await ensure_topics(await own_client(), LINKS, CHECKS)
    cfg = DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=200))
    link_p = await (await own_client()).producer(STREAM, LINKS, topic_partitions_count=PARTITIONS[LINKS], mode=cfg)
    chk_p = await (await own_client()).producer(STREAM, CHECKS, topic_partitions_count=PARTITIONS[CHECKS], mode=cfg)
    seq = [0]                               # one counter for every edge: always higher than the edge's last
    pending = []                            # (due, n, account, links, login ns, fraud): payments to come
    stats = defaultdict(int)
    gc.freeze()                             # keep start-up objects out of the collector's way

    def h(s: str) -> str:
        return f"{zlib.crc32(s.encode()):08x}"

    def link(acct, asset, kind, now, **extra):
        seq[0] += 1
        return asset, SendMessage(msg(asset=asset, kind=kind, acct=acct, seq=seq[0], linked=1, ns=now, **extra)), seq[0]

    async def send(producer, items, parts):
        # One batch per partition, all sent at once: per-key order is kept, about one round trip per tick.
        groups = defaultdict(list)
        for key, m in items:
            groups[part(key, parts)].append(m)
        await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(pid))
                               for pid, ms in groups.items()))

    bad_assets = [f"{'vid' if i % 2 else 'addr'}:{h(f'bad{i}')}" for i in range(a.bad_assets)]

    async def snapshot():
        # The offline channel's graph snapshot: restricted accounts and the profile assets they share.
        items, now, n = [], time.time_ns(), 0
        for i, asset in enumerate(bad_assets):
            for _ in range(7):
                n += 1
                acct = f"R{n:010d}"
                for key, m, _ in (link(acct, asset, asset.split(":")[0], now, snap=1, restricted=1),
                                  link(acct, f"dev:{h(acct)}", "dev", now, snap=1, restricted=1)):
                    items.append((key, m))
        for i in range(0, len(items), 5000):
            await send(link_p, items[i:i + 5000], PARTITIONS[LINKS])
        print(f"[activity] snapshot: {n:,} restricted accounts on {len(bad_assets):,} known-bad profile assets", flush=True)

    def login(acct, now, fraud):
        # A login links the account to its device and IP; a profile change adds an address or VID.
        dev = f"dev:{h(acct)}" if not fraud and random.random() < 0.9 else f"dev:{random.getrandbits(32):08x}"
        out = [link(acct, dev, "dev", now), link(acct, f"ip:{h(str(random.randrange(a.ips)))}", "ip", now)]
        if fraud:
            asset = random.choice(bad_assets)                  # the known-bad profile asset, added again
            out.append(link(acct, asset, asset.split(":")[0], now))
        elif random.random() < a.profile_share:
            kind = random.choice(("addr", "vid"))
            out.append(link(acct, f"{kind}:{random.getrandbits(32):08x}", kind, now))
        return out

    async def run():
        t0 = due = time.monotonic()
        owed, n_acct, n_pay = 0.0, 0, 0
        async with link_p, chk_p:
            await snapshot()
            while True:
                now_s = time.monotonic()
                t = now_s - t0
                x = a.peak if a.peak_at <= t < a.peak_at + a.peak_for else 1
                owed += a.logins_per_s * x / 100
                links, checks, now = [], [], time.time_ns()
                for _ in range(int(owed)):
                    fraud = random.random() < a.fraud_share
                    if fraud:
                        n_acct += 1
                        acct = f"N{n_acct:010d}"                  # a brand-new account
                    else:
                        acct = account_id(random.randrange(a.accounts))
                    made = login(acct, now, fraud)
                    links += [(key, m) for key, m, _ in made]
                    stats["logins"] += 1
                    stats["fraud"] += fraud
                    if fraud or random.random() < a.pay_share:
                        lo, hi = (a.fraud_gap_min, a.fraud_gap_max) if fraud else (a.gap_min, a.gap_max)
                        n_pay += 1
                        heapq.heappush(pending, (now_s + random.uniform(lo, hi), n_pay, acct,
                                                 [[key, s] for key, _, s in made], now, int(fraud)))
                owed -= int(owed)
                while pending and pending[0][0] <= now_s:          # payments whose time has come
                    _, n, acct, need, login_ns, fraud = heapq.heappop(pending)
                    pay_ns = time.time_ns()
                    checks.append((acct, SendMessage(msg(acct=acct, txn=f"T{n:014d}", links=need, login_ns=login_ns,
                                                         fraud=fraud, ns=pay_ns))))
                    stats["checks"] += 1
                stats["links"] += len(links)
                if links or checks:
                    await asyncio.gather(send(link_p, links, PARTITIONS[LINKS]), send(chk_p, checks, PARTITIONS[CHECKS]))
                stats["peak"] = x > 1
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            phase = "peak" if stats["peak"] else "steady"
            print(f"[activity] {stats['logins']:>7,} logins/s ({phase})  links {stats['links']:,}  "
                  f"repeat fraudsters {stats['fraud']:,}  payment checks {stats['checks']:,}  "
                  f"payments waiting {len(pending):,}", flush=True)
            for k in ("logins", "links", "fraud", "checks"):
                stats[k] = 0

    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--accounts", type=int, default=50_000_000, help="accounts in the ordinary pool")
    p.add_argument("--ips", type=int, default=20_000_000, help="IP addresses the ordinary pool logs in from")
    p.add_argument("--logins-per-s", type=float, default=5_000)
    p.add_argument("--pay-share", type=float, default=0.6, help="share of ordinary logins followed by a payment")
    p.add_argument("--profile-share", type=float, default=0.05, help="share of ordinary logins that add an address or VID")
    p.add_argument("--fraud-share", type=float, default=0.005, help="share of logins that are repeat fraudsters")
    p.add_argument("--bad-assets", type=int, default=2_000, help="known-bad profile assets, seven restricted accounts each")
    p.add_argument("--gap-min", type=float, default=0.2, help="seconds from login to payment, at least")
    p.add_argument("--gap-max", type=float, default=8.0, help="seconds from login to payment, at most")
    p.add_argument("--fraud-gap-min", type=float, default=0.2, help="a scripted repeat fraudster pays sooner")
    p.add_argument("--fraud-gap-max", type=float, default=2.0)
    p.add_argument("--peak", type=float, default=3, help="login rate multiplier during the peak")
    p.add_argument("--peak-at", type=float, default=0, help="seconds after start")
    p.add_argument("--peak-for", type=float, default=60, help="seconds")
    asyncio.run(main(p.parse_args()))
