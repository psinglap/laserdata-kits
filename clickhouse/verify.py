"""Check that every event in Iggy is in ClickHouse exactly once.

Waits until the pipe has drained the topic (or --wait-s passes), then compares:
  * events in Iggy (every partition's message count),
  * rows in otel_logs, and distinct (Source, Seq) pairs: missing = Iggy - distinct, duplicates = rows - distinct,
  * sequence gaps: each source numbers its events 1, 2, 3, ..., so the sum of every source's highest Seq must equal
    the distinct count (this check does not trust Iggy's count at all).
Exits 1 on any missing row, duplicate or gap.
"""
import argparse
import asyncio

from apache_iggy import IggyClient

from common import DEFAULT_CH, DEFAULT_URL, LOGS, TABLE, ClickHouse, ends


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    ch = ClickHouse(a.ch_url, a.database)
    q = f"SELECT count(), uniqExact(Source, Seq) FROM {TABLE} FORMAT TSV"
    for i in range(int(a.wait_s)):
        in_iggy = sum((await ends(client, LOGS)).values())
        rows, distinct = map(int, (await asyncio.to_thread(ch.query, q)).split())
        if distinct >= in_iggy:
            break
        if i % 5 == 0:
            print(f"[verify] waiting for the pipe: {in_iggy - distinct:,} events not in ClickHouse yet", flush=True)
        await asyncio.sleep(1)
    top = int((await asyncio.to_thread(ch.query, f"SELECT sum(m) FROM (SELECT max(Seq) AS m FROM {TABLE} "
                                                 f"GROUP BY Source) FORMAT TSV")).strip() or 0)
    sources = int((await asyncio.to_thread(ch.query, f"SELECT uniqExact(Source) FROM {TABLE} FORMAT TSV")).strip())
    missing, dups, gaps = in_iggy - distinct, rows - distinct, top - distinct
    print(f"[verify] events in Iggy {in_iggy:,}  rows in ClickHouse {rows:,}  distinct {distinct:,}  "
          f"missing {missing:,}  duplicates {dups:,}  sequence gaps {gaps:,} across {sources:,} sources", flush=True)
    if missing or dups or gaps:
        raise SystemExit("[verify] FAIL: not exactly once")
    print("[verify] OK: every event in ClickHouse exactly once", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--ch-url", default=DEFAULT_CH)
    p.add_argument("--database", default="default")
    p.add_argument("--wait-s", type=float, default=60)
    asyncio.run(main(p.parse_args()))
