"""Shadow bridge: Imbi's own operations_log rows, as they reach Iggy today, into deployment notifications.

Read-only. Polls `operations_log/deployments` on Imbi's existing Iggy server (--source-url) as its own plain
consumer, `notifications-shadow`, holding its position in memory. It never joins the ClickHouse sink's consumer
group and never stores an offset there, so the sink and its offsets are untouched. Each finished deployment or
rollback becomes one notification on the lane (--url, the same server or a separate one), keyed by project with
that project's sequence number, in the shape notify.py reads.

Row fields follow imbi-api's OperationLogResponse (project_slug, environment_slug, entry_type, version,
performed_by, completed_at). operations_log rows can be published twice (the open row, then the completed row with
a bumped version), so a row becomes a notification only once it is complete, and only once per id. Check the rule
against your own rows before trusting the counts.

    python iggy_bridge.py --source-url iggy+tcp://USER:PASS@imbi-iggy:8090?nodelay=true --from now
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict

from apache_iggy import Consumer, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, DEPLOYMENT, NOTIFY, PARTITIONS, ensure, msg, part

NOTIFY_ON = {"Deployed", "Rolled Back"}


async def main(a):
    src = IggyClient.from_connection_string(a.source_url)
    await src.connect()
    dst = IggyClient.from_connection_string(a.url)
    await dst.connect()
    await ensure(dst, NOTIFY, DEPLOYMENT)
    prod = await dst.producer(NOTIFY, DEPLOYMENT, topic_partitions_count=PARTITIONS[DEPLOYMENT])
    who = Consumer.Single("notifications-shadow")
    seq, seen, nxt = defaultdict(int), set(), None
    stats, t0 = defaultdict(int), time.monotonic()
    first = PollingStrategy.First() if a.from_ == "first" else PollingStrategy.Last()
    async with prod:
        while True:
            ms = await src.poll_messages(a.stream, a.topic, consumer=who, partition_id=0, count=1000,
                                         auto_commit=False,
                                         polling_strategy=PollingStrategy.Offset(nxt) if nxt is not None else first)
            batches = defaultdict(list)
            for m in ms:
                nxt = m.offset() + 1
                stats["rows"] += 1
                row = json.loads(m.payload())
                if row.get("entry_type") not in NOTIFY_ON or not row.get("completed_at") or row["id"] in seen:
                    continue
                seen.add(row["id"])
                p = row["project_slug"]
                seq[p] += 1
                batches[part(p, PARTITIONS[DEPLOYMENT])].append(SendMessage(msg(
                    id=row["id"], k=p, seq=seq[p], project_slug=p, environment_slug=row["environment_slug"],
                    entry_type=row["entry_type"], version=row.get("version") or "",
                    performed_by=row.get("performed_by") or "")))
            if batches:
                await asyncio.gather(*(prod.send_with_partitioning(b, Partitioning.partition_id(pid))
                                       for pid, b in batches.items()))
                stats["notified"] += sum(len(b) for b in batches.values())
            if time.monotonic() - t0 >= 1:
                print(f"[bridge] {stats['rows']:,} operations_log rows/s read  {stats['notified']:,} deployment "
                      f"notifications/s published", flush=True)
                stats.clear()
                t0 = time.monotonic()
            if not ms:
                await asyncio.sleep(0.01)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-url", required=True, help="Imbi's existing Iggy server (read-only use)")
    ap.add_argument("--url", default=DEFAULT_URL, help="where the notifications lane runs")
    ap.add_argument("--stream", default="operations_log")
    ap.add_argument("--topic", default="deployments")
    ap.add_argument("--from", dest="from_", choices=["first", "now"], default="now",
                    help="start at the oldest retained row, or at the newest")
    asyncio.run(main(ap.parse_args()))
