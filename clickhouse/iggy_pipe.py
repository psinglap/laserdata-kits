"""An Apache Iggy source for ClickHouse, with the exactly-once rules of Kafka ClickPipes.

Every --flush-s seconds (0.5 s) the pipe reads each partition of otel-logs directly from its high-water mark,
records the block it is about to insert (offset range per partition, and a token derived from those ranges) in its
own Iggy topic, pipe-state, and inserts the block into otel_logs as one INSERT with that token as the
insert_deduplication_token. Only then do the high-water marks move.

  * ClickHouse down or throttling: the same block is retried with the same token until it lands. Nothing is
    dropped and nothing is buffered in the pipe's memory; collectors keep writing and the events wait in Iggy.
  * Pipe killed at any moment: on start it reads the last pipe-state record, re-inserts the block that was in
    flight with its original token (ClickHouse drops it if the first insert had landed) and resumes from there.
  * --repeat-every N re-sends every Nth block after it landed, as if its acknowledgement had been lost, to show
    that ClickHouse drops the repeat.

Prints, every second, event-to-ClickHouse freshness (from the collector's timestamp to the insert being
acknowledged, when the rows are queryable), how many events are held in Iggy, and the exactly-once counters.
"""
import argparse
import asyncio
import gc
import hashlib
import json
import time

from apache_iggy import Consumer, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import (DDL, DEFAULT_CH, DEFAULT_URL, LOGS, PARTITIONS, STATE, STREAM, TABLE, ClickHouse, Window,
                    ends, ensure_topics, event_ns)


def token_for(ranges: dict) -> str:
    # Deterministic: the same offset ranges always give the same token, in this process or after a restart.
    spec = LOGS + "|" + ",".join(f"{p}:{f}-{l}" for p, (f, l) in sorted(ranges.items()))
    return "iggy-" + hashlib.sha1(spec.encode()).hexdigest()[:24]


def rows_of(batch) -> bytes:
    # The collectors' JSON already has the table's columns; add where each row sits in Iggy.
    return b"\n".join(m.payload()[:-1] + b',"IggyPartition":%d,"IggyOffset":%d}' % (p, m.offset())
                      for p, ms in sorted(batch.items()) for m in ms)


async def main(a):
    async def own_client():                    # one connection per role, so none waits on another
        c = IggyClient.from_connection_string(a.url)
        await c.connect()
        return c

    client, state_client, admin = await own_client(), await own_client(), await own_client()
    await ensure_topics(client, LOGS, STATE)
    ch = ClickHouse(a.ch_url, a.database)
    while True:
        try:
            await asyncio.to_thread(ch.query, DDL)
            break
        except Exception as e:                 # ClickHouse still starting
            print(f"[pipe] waiting for ClickHouse at {ch.base}: {type(e).__name__}", flush=True)
            await asyncio.sleep(1)

    parts = range(PARTITIONS[LOGS])
    readers = {p: Consumer.Single(f"{a.name}-p{p}") for p in parts}
    hwm = {p: -1 for p in parts}               # last offset known to be in ClickHouse, per partition
    lat, st = Window(), {"rows": 0, "inserts": 0, "held_rows": 0, "held_max": 0.0}
    tot = {"inserted": 0, "deduped": 0, "leaked": 0, "retried": 0, "replayed": 0, "outage_s": 0.0}
    mode = {"catching": True, "down_since": None, "block_rows": 0}

    async def poll(p, start, count):
        return await client.poll_messages(STREAM, LOGS, consumer=readers[p], partition_id=p, count=count,
                                          auto_commit=False, polling_strategy=PollingStrategy.Offset(start))

    async def insert(body: bytes, token: str, n: int) -> int:
        """Insert one block; on failure retry the same block with the same token until ClickHouse takes it."""
        while True:
            try:
                written = await asyncio.to_thread(ch.insert, TABLE, body, token)
                if mode["down_since"] is not None:
                    tot["outage_s"] += time.monotonic() - mode["down_since"]
                    print(f"[pipe] ClickHouse is back after {time.monotonic() - mode['down_since']:.1f} s; "
                          f"block of {n:,} rows landed with its original token", flush=True)
                    mode["down_since"] = None
                return written
            except Exception as e:
                if mode["down_since"] is None:
                    mode["down_since"] = time.monotonic()
                    mode["block_rows"] = n
                    print(f"[pipe] insert failed ({type(e).__name__}): retrying the same block with the same "
                          f"token; collectors keep writing to Iggy", flush=True)
                mode["catching"] = True
                tot["retried"] += 1
                await asyncio.sleep(a.retry_s)

    async def record(pending: dict, token: str):
        # Written before the insert: if the pipe dies after this, a restart knows exactly which block was in flight.
        doc = {"pipe": a.name, "hwm": hwm, "pending": pending, "token": token, "ns": time.time_ns()}
        await state_client.send_messages(STREAM, STATE, Partitioning.partition_id(0),
                                          [SendMessage(json.dumps(doc, separators=(",", ":")).encode())])

    # Restore from the pipe's own log: high-water marks, then the block that was in flight, with its token.
    end = (await ends(client, STATE))[0]
    if end:
        last = await client.poll_messages(STREAM, STATE, consumer=Consumer.Single(f"{a.name}-restore"), partition_id=0,
                                          count=1, auto_commit=False, polling_strategy=PollingStrategy.Offset(end - 1))
        doc = json.loads(last[-1].payload())
        hwm.update({int(p): o for p, o in doc["hwm"].items()})
        pending = {int(p): r for p, r in doc["pending"].items()}
        batch = {}
        for p, (f, l) in pending.items():
            got = []
            while len(got) < l - f + 1:
                ms = await poll(p, f + len(got), l - f + 1 - len(got))
                if not ms:
                    break
                got += ms
            batch[p] = got
        n = sum(len(v) for v in batch.values())
        written = await insert(rows_of(batch), doc["token"], n) if n else 0
        for p, (f, l) in pending.items():
            hwm[p] = l
        tot["replayed"] += n
        tot["deduped"] += max(0, n - written)
        print(f"[pipe] restored from {STATE}: high-water marks for {len(hwm)} partitions; re-inserted the block in "
              f"flight ({n:,} rows) with its original token: ClickHouse wrote {max(written, 0):,} "
              f"({'it had already landed' if written == 0 else 'it had not landed'})", flush=True)
    else:
        print(f"[pipe] no {STATE} yet: reading every partition from the first event", flush=True)
    nxt = {p: hwm[p] + 1 for p in parts}

    async def run():
        cycle, blocks = 0, 0
        next_t = time.monotonic()
        while True:
            batch, ranges, full = {}, {}, False
            for p in parts:
                ms = await poll(p, nxt[p], a.max_rows)
                if ms:
                    batch[p], ranges[p] = ms, (ms[0].offset(), ms[-1].offset())
                    full = full or len(ms) == a.max_rows
            if batch:
                n = sum(len(v) for v in batch.values())
                token = token_for(ranges)
                await record({p: list(r) for p, r in ranges.items()}, token)
                body = rows_of(batch)
                written = await insert(body, token, n)
                ack = time.time_ns()
                for p, (f, l) in ranges.items():
                    hwm[p], nxt[p] = l, l + 1
                tot["inserted"] += max(written, 0)
                tot["deduped"] += max(0, n - written) if written >= 0 else 0
                st["rows"] += n
                st["inserts"] += 1
                for ms in batch.values():
                    for m in ms:
                        age = (ack - event_ns(m.payload())) / 1e6
                        if mode["catching"]:          # held in Iggy during an outage or before a (re)start
                            st["held_rows"] += 1
                            st["held_max"] = max(st["held_max"], age)
                        else:
                            lat.add(age)
                blocks += 1
                if a.repeat_every and blocks % a.repeat_every == 0:
                    again = await insert(body, token, n)  # as if the first acknowledgement had been lost
                    tot["deduped"] += n - max(again, 0)
                    tot["leaked"] += max(again, 0)
            cycle += 1
            if full:
                continue                              # a backlog: keep reading without waiting
            next_t += a.flush_s                       # schedule from the due time, not from now
            now = time.monotonic()
            if next_t < now - a.flush_s:
                next_t = now
            await asyncio.sleep(max(0.0, next_t - now))

    async def report():
        while True:
            await asyncio.sleep(1)
            e = await ends(admin, LOGS)
            held = sum(max(0, e[p] - (hwm[p] + 1)) for p in parts)
            if mode["down_since"] is not None:
                print(f"[pipe] ClickHouse unavailable for {time.monotonic() - mode['down_since']:.0f} s: block of "
                      f"{mode['block_rows']:,} rows retried with the same token; {held:,} events held in Iggy", flush=True)
                lat.flush()
            elif mode["catching"]:
                if st["held_rows"] and st["held_max"] < a.budget_ms:
                    mode["catching"] = False          # a whole second inside the budget: caught up
                print(f"[pipe] catching up: inserted {st['held_rows']:,} held events this second, oldest "
                      f"{st['held_max'] / 1000:.1f} s; {held:,} still in Iggy", flush=True)
                lat.flush()
            else:
                r = lat.flush()
                ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
                print(f"[pipe] {st['rows']:>7,} rows/s in {st['inserts']} inserts  event-to-ClickHouse p50 "
                      f"{r['p50']:6.1f}ms p99 {r['p99']:6.1f}ms p99.9 {r['p999']:6.1f}ms  {ok} (<{a.budget_ms}ms)  "
                      f"held in Iggy {held:,}", flush=True)
            print(f"[pipe] totals: inserted {tot['inserted']:,}  dropped by ClickHouse as repeats {tot['deduped']:,}  "
                  f"written twice {tot['leaked']:,}  insert retries {tot['retried']:,}  "
                  f"replayed on restart {tot['replayed']:,}  outage {tot['outage_s']:.1f} s", flush=True)
            st.update(rows=0, inserts=0, held_rows=0, held_max=0.0)

    gc.freeze()                                       # no collector pauses in the hot path once started
    await asyncio.gather(run(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--ch-url", default=DEFAULT_CH, help="http(s)://user:password@host:port of the ClickHouse service")
    p.add_argument("--database", default="default")
    p.add_argument("--name", default="otel-logs-pipe")
    p.add_argument("--flush-s", type=float, default=0.5, help="seconds between inserts (one INSERT per cycle)")
    p.add_argument("--max-rows", type=int, default=5000, help="rows per partition per insert")
    p.add_argument("--retry-s", type=float, default=1.0)
    p.add_argument("--repeat-every", type=int, default=0, help="re-send every Nth block after it landed (0: off)")
    p.add_argument("--budget-ms", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
