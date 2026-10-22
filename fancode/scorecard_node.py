"""A scorecard node: reads every score update from the lane and answers scorecardPoll from the newest one.

FanCode's web app polls scorecardPoll(matchId, lastKey) every 5 s on a live match page and every 10 s beside
the commentary; the answer carries the match's current key, refresh (true when the key moved past lastKey),
the delay before the next poll, and the score. This node is the part behind that answer: it reads every
partition with its own connection and its own offsets (no shared consumer group), so adding nodes before a
big match rebalances nothing, and it keeps every live match's newest key and score in memory.

On start it rebuilds every live match from the log (every update since the opening scores, kept for a match
day), then times live updates. Prints a line a second:
  * update-to-node latency (update created at the score feed -> answerable here), PASS or FAIL against
    --budget-ms;
  * viewer polls answered behind the newest score: polls that arrive between an update's creation and the
    moment this node holds it would get the older key. Estimated from --viewers spread over the live matches
    by popularity, each polling every --poll-s, times how late each update was;
  * gaps: a missing key for a match (must stay 0).
"""
import argparse
import asyncio
import gc
import time

from apache_iggy import Consumer, PollingStrategy

from common import DEFAULT_URL, LANE, PARTITIONS, STREAM, Window, connect, match_ids, popularity, unpack


async def main(a):
    admin = await connect(a.url)
    while not await admin.get_topic(STREAM, LANE):
        print(f"[{a.name}] waiting for score_feed.py to create the scorecard lane…", flush=True)
        await asyncio.sleep(1)

    matches = match_ids(a.matches)
    w = [popularity(i) for i in range(len(matches))]
    polls_per_s = {m: a.viewers * w[i] / sum(w) / a.poll_s for i, m in enumerate(matches)}   # viewer polls a second
    total_polls = sum(polls_per_s.values())
    card = {}                                               # match -> (key, sport, event, score): what a poll gets
    gc.freeze()                                             # keep startup tables out of the collector's way

    def answer(match: str, last_key: int) -> dict:
        # The scorecardPoll answer, from the newest update this node holds for the match.
        key, sport, event, score = card.get(match, (0, None, None, None))
        return {"key": key, "refresh": key != last_key, "delay": a.poll_s, "data": {"sport": sport, "score": score}}

    lat, st = Window(), dict(live=0, behind=0.0, gaps=0, replayed=0, rebuilt=0, max_age=0.0)
    started, head, caught = time.monotonic(), set(), [None, False]   # partitions at the log head; [rebuilt at, timing live]

    def apply(ms, now):
        for msg in ms:
            v = unpack(msg.payload())
            m, key = v["m"], v["k"]
            last = card.get(m)
            if last is not None:
                if key <= last[0]:
                    continue                                # a repeat (at-least-once redelivery): skip it
                if key > last[0] + 1:
                    st["gaps"] += 1                         # an update for this match never arrived
            card[m] = (key, v["sp"], v["e"], v["sc"])  # newest wins: a poll never sees the key go backwards
            if key == 1:
                continue                                    # the opening score is not a live update
            age = now - v["ns"]
            if caught[1]:
                lat.add(age / 1e6)
                st["live"] += 1
                st["behind"] += polls_per_s.get(m, 0.0) * age / 1e9   # polls that got the older key meanwhile
            else:
                st["replayed"] += 1
                st["rebuilt"] += 1
                st["max_age"] = max(st["max_age"], age / 1e6)

    async def read(pid):
        c = await connect(a.url)
        me, nxt = Consumer.Single(f"{a.name}-{pid}"), None
        while True:
            ms = await c.poll_messages(STREAM, LANE, consumer=me, partition_id=pid, count=a.batch, auto_commit=False,
                                       polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else PollingStrategy.First())
            if ms:
                apply(ms, time.time_ns())
                nxt = ms[-1].offset() + 1
                if len(ms) == a.batch:
                    continue                                # more is waiting (the rebuild): read on at once
            if pid not in head:
                head.add(pid)                               # this partition's replay reached the head of the log
                if len(head) == PARTITIONS:
                    caught[0] = time.monotonic()
                    live = [m for m in matches if m in card]
                    print(f"[{a.name}] rebuilt from the log in {caught[0] - started:.2f}s: scorecards for {len(live)} of "
                          f"{len(matches)} live matches, {st['rebuilt']:,} updates replayed; without the log "
                          f"{total_polls:,.0f} viewer polls a second would get no score until each match's next update",
                          flush=True)
            await asyncio.sleep(a.poll_ms / 1000)           # the feed writes once per --tick-ms; wait for the next write

    async def report():
        while True:
            await asyncio.sleep(1)
            if not caught[1]:
                # Live once the log is replayed and a whole second of updates arrived inside the budget;
                # partitions interleave, so one fresh update alone doesn't mean the replay is finished.
                if caught[0] is not None and st["replayed"] and st["max_age"] < a.budget_ms:
                    caught[1] = True
                print(f"[{a.name}] catching up: {st['replayed']:,} updates replayed this second, "
                      f"{len(head)} of {PARTITIONS} partitions at the head of the log", flush=True)
                st.update(replayed=0, max_age=0.0)
                lat.flush()
                continue
            r, n = lat.flush(), st["live"]
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            pct = 100 * st["behind"] / total_polls if total_polls else 0.0
            top = answer(matches[0], 0)                     # what a poll for the headline match gets right now
            print(f"[{a.name}] {n:>5,} updates/s  update-to-node p50 {r['p50']:5.2f}ms  p99 {r['p99']:5.2f}ms  "
                  f"p99.9 {r['p999']:5.2f}ms  {ok} (<{a.budget_ms:g}ms)  viewer polls answered behind the newest score "
                  f"{st['behind']:,.0f} of {total_polls:,.0f}/s ({pct:.4f}%)  gaps {st['gaps']}  {matches[0]} key {top['key']}",
                  flush=True)
            st.update(live=0, behind=0.0)

    await asyncio.gather(report(), *(read(p) for p in range(PARTITIONS)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="scorecard-1")
    p.add_argument("--matches", type=int, default=40, help="same as score_feed.py")
    p.add_argument("--viewers", type=int, default=6_000_000, help="concurrent viewers across the live matches")
    p.add_argument("--poll-s", type=float, default=5, help="seconds between a viewer's scorecard polls (web app default: 5)")
    p.add_argument("--budget-ms", type=float, default=20, help="p99 budget for update-to-node")
    p.add_argument("--poll-ms", type=float, default=1, help="wait between reads of a partition, unless the last read was a full batch")
    p.add_argument("--batch", type=int, default=5000)
    asyncio.run(main(p.parse_args()))
