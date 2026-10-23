"""The failover check: after nj-1 stops taking bets, does nj-2 hold every event nj-1 accepted?

Run it when the sportsbook at nj-1 has stopped (a planned failover, or the end of a DR drill). It waits up to
--wait-s for the mirror to drain, then reads the whole wallet lane at both sites, partition by partition, and checks:
  * nj-2 holds every offset nj-1 holds, and each event is byte-identical at the same offset;
  * every account's final balance and open bets are the same at both sites;
  * every event was accepted in the lane's state.
Prints one summary line and exits 1 if anything differs.
"""
import argparse
import asyncio
import sys
import time

from apache_iggy import Consumer, PollingStrategy

from common import LANE, PARTITIONS, PRIMARY, PRIMARY_URL, SECOND, SECOND_URL, STATE, STREAM, connect, heads, unpack


async def read_all(url: str, pid: int, upto: int) -> list:
    c = await connect(url)
    me, nxt, out = Consumer.Single(f"failover-check-{pid}"), 0, []
    while nxt < upto:
        ms = await c.poll_messages(STREAM, LANE, consumer=me, partition_id=pid, count=1000, auto_commit=False,
                                   polling_strategy=PollingStrategy.Offset(nxt))
        if not ms:
            break
        out.extend((m.offset(), m.payload()) for m in ms)
        nxt = ms[-1].offset() + 1
    return out


async def main(a):
    p, s = await connect(a.primary_url), await connect(a.second_url)
    t0 = time.monotonic()
    hp, hs = await heads(p), await heads(s)
    while hs != hp and time.monotonic() - t0 < a.wait_s:
        await asyncio.sleep(0.2)
        hp, hs = await heads(p), await heads(s)
    drained = time.monotonic() - t0
    accepted, held, mismatched, foreign = sum(hp), 0, 0, 0
    final = {PRIMARY: {}, SECOND: {}}
    for pid in range(PARTITIONS):
        rp, rs = await asyncio.gather(read_all(a.primary_url, pid, hp[pid]), read_all(a.second_url, pid, hp[pid]))
        at_s = dict(rs)
        for off, body in rp:
            other = at_s.get(off)
            if other is None:
                continue
            held += 1
            if other != body:
                mismatched += 1
        for site, rows in ((PRIMARY, rp), (SECOND, rs)):
            for _, body in rows:
                v = unpack(body)
                if v["st"] != STATE:
                    foreign += 1
                w = final[site].get(v["a"])
                if w is None or v["s"] > w[0]:
                    final[site][v["a"]] = (v["s"], v["bal"], v["o"])
    accts = len(final[PRIMARY])
    differ = sum(1 for k, w in final[PRIMARY].items() if final[SECOND].get(k) != w)
    ok = held == accepted and mismatched == 0 and differ == 0 and foreign == 0
    print(f"[failover-check] {SECOND} holds {held:,} of {accepted:,} events accepted at {PRIMARY} "
          f"(drained in {drained:.1f}s), byte-identical at the same offsets: {'yes' if mismatched == 0 else 'NO'}; "
          f"balances and open bets match on {accts - differ:,} of {accts:,} accounts; events from outside {STATE}: "
          f"{foreign}  {'OK' if ok else 'FAILED'}", flush=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--primary-url", default=PRIMARY_URL)
    ap.add_argument("--second-url", default=SECOND_URL)
    ap.add_argument("--wait-s", type=float, default=10)
    asyncio.run(main(ap.parse_args()))
