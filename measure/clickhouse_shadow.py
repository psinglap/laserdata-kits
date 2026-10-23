"""Shadow your own traffic into the crash spike lane, read-only, from the ClickHouse `events` table your ingest
worker already writes.

Every --every-s seconds it reads the rows inserted since its last read (by `inserted_at`, a few seconds behind now
so late inserts are not missed), one row per app and session: the session, and its fatal crashes and ANRs with
their fingerprints, using the same predicates as backend/alerts (type = 'exception' with
config.FatalExceptionExpr, and type = 'anr'). Each row becomes one record on issue-occurrences, keyed by app ID,
the shape ingest_worker.py writes. Nothing in Measure changes: the SDKs, the ingest service, the bus, the ingest
worker and the alerts service are untouched, and the query is a plain SELECT. Point spike_judge.py at Iggy and
compare its alerts, and when it raised them, with the `alerts` table your hourly job fills.

This is the shadow path only: it adds the poll interval and ClickHouse's insert path to every record, so it shows
which spikes the lane would catch and how much earlier, not the lane's own latency. In production the record is
published by the ingest worker itself, right after step 7 (see the README).
Needs: pip install apache-iggy==0.9.0 (ClickHouse is read over its HTTP interface with the standard library)
"""
import argparse
import asyncio
import base64
import json
import time
import urllib.parse
import urllib.request
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, ISSUES, PARTITIONS, STREAM, ensure_topics, msg, part

FATAL = "(`exception.severity` = 'fatal' OR (`exception.severity` = '' AND `exception.handled` = false))"
QUERY = """
SELECT toString(app_id) AS a, toString(session_id) AS s,
       groupArrayIf(tuple(if(type = 'anr', 'anr', 'crash'),
                          if(type = 'anr', `anr.fingerprint`, `exception.fingerprint`)),
                    type = 'anr' OR (type = 'exception' AND {fatal})) AS x
FROM events
WHERE inserted_at > fromUnixTimestamp64Milli({lo}) AND inserted_at <= fromUnixTimestamp64Milli({hi})
GROUP BY a, s
FORMAT JSONEachRow
"""


def select(a, lo_ms: int, hi_ms: int):
    q = QUERY.format(fatal=FATAL, lo=lo_ms, hi=hi_ms)
    req = urllib.request.Request(f"{a.clickhouse}/?{urllib.parse.urlencode({'database': a.database})}",
                                 data=q.encode(), method="POST")
    if a.user:
        tok = base64.b64encode(f"{a.user}:{a.password}".encode()).decode()
        req.add_header("Authorization", f"Basic {tok}")
    with urllib.request.urlopen(req, timeout=60) as r:
        for line in r:
            if line.strip():
                yield json.loads(line)


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, ISSUES)
    producer = await iggy.producer(STREAM, ISSUES, topic_partitions_count=PARTITIONS[ISSUES])
    hi = int((time.time() - a.lag_s) * 1000)
    async with producer:
        while True:
            await asyncio.sleep(a.every_s)
            lo, hi = hi, int((time.time() - a.lag_s) * 1000)
            groups, n_rows, n_occ = defaultdict(list), 0, 0
            for row in await asyncio.to_thread(lambda: list(select(a, lo, hi))):
                x = {}
                for kind, fp in row["x"]:
                    if fp:
                        x[(kind, fp)] = x.get((kind, fp), 0) + 1
                rec = [[k, f, n] for (k, f), n in x.items()]
                # One record per app and session per read; the batch ID makes a re-read of the same range a repeat.
                groups[part(row["a"], PARTITIONS[ISSUES])].append(
                    SendMessage(msg(b=f"ch:{row['s']}:{hi}", a=row["a"], s=row["s"], x=rec)))
                n_rows += 1
                n_occ += sum(rec_n for _, _, rec_n in rec)
            # One batch per partition, all sent at once, so an app's records stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            print(f"[clickhouse-shadow] {n_rows:,} app sessions, {n_occ:,} fatal crashes and ANRs, rows inserted up "
                  f"to {time.strftime('%H:%M:%S', time.gmtime(hi / 1000))} UTC", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--clickhouse", default="http://localhost:8123", help="ClickHouse HTTP URL")
    p.add_argument("--database", default="measure")
    p.add_argument("--user", default="")
    p.add_argument("--password", default="")
    p.add_argument("--every-s", type=float, default=5, help="how often to read new rows")
    p.add_argument("--lag-s", type=float, default=5, help="stay this far behind now, for late inserts")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
