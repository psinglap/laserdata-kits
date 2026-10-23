"""Shadow your own config and pricing tables from Postgres into Apache Iggy, read-only.

Opens a TEMPORARY logical replication slot (wal2json, format 2) for the tables you name, so Postgres
drops the slot as soon as the bridge disconnects and never holds WAL for it. Point it at a standby
(Postgres 16+ can decode on a standby) to keep it off the primary entirely. Clients keep writing to
Postgres, PG feed keeps its own slot and subscribers, and nothing here writes to the database.

On start it reads the current rows of each table inside a Repeatable Read transaction (op "S"), then
forwards each committed change with the resulting row, a per-table sequence, the commit LSN and the
table's state hash, in the layout pg_writer.py uses, so subscriber.py, checkpoint.py and rejoin.py
run unchanged on your data. The snapshot and the slot are not stitched by transaction visibility the
way PG feed does it, so a change committed during the start-up read can be applied twice; rows carry
their full value, so a repeat is harmless, and subscriber.py's hash check flags any table that drifts.

The latency subscriber.py prints then starts at the commit time wal2json reports.
Needs: pip install psycopg2-binary apache-iggy==0.9.0, and wal2json on the server.
"""
import argparse
import asyncio
import json
import time
from collections import defaultdict
from datetime import datetime

from apache_iggy import SendMessage

from common import CHANGES, DEFAULT_URL, STREAM, connect, encode, ensure_topics, partition_of, row_hash


def lsn_int(s: str) -> int:
    hi, lo = s.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


async def main(a):
    import psycopg2
    import psycopg2.extras

    names = [t.strip() for t in a.tables.split(",") if t.strip()]
    iggy = await connect(a.url)
    await ensure_topics(iggy, a.partitions)
    writers = [await connect(a.url) for _ in range(a.partitions)]
    pending = defaultdict(list)
    seq, h, rows = defaultdict(int), defaultdict(int), defaultdict(dict)

    def change(t, key, op, row, lsn, ns):
        old = rows[t].pop(key, None) if op == "D" else rows[t].get(key)
        if old is not None:
            h[t] = (h[t] - row_hash(key, old)) & 0xFFFFFFFF
        if op != "D":
            rows[t][key] = row
            h[t] = (h[t] + row_hash(key, row)) & 0xFFFFFFFF
        seq[t] += 1
        pending[partition_of(t, a.partitions)].append(SendMessage(encode(t, key, op, row, seq[t], lsn, ns, h[t])))

    # 1. The slot first, so no change committed after the start-up read is lost.
    rep = psycopg2.connect(a.dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
    cur = rep.cursor()
    cur.execute(f"CREATE_REPLICATION_SLOT {a.slot} TEMPORARY LOGICAL wal2json")
    # 2. The current rows of each table, in one consistent read.
    db = psycopg2.connect(a.dsn)
    db.set_session(isolation_level="REPEATABLE READ", readonly=True)
    with db.cursor() as q:
        for t in names:
            q.execute(f"SELECT {a.key}::text, row_to_json(x)::text FROM {t} x")
            for key, row in q.fetchall():
                change(t, key, "S", row, 0, time.time_ns())
    db.rollback()
    print(f"[bridge] slot {a.slot} (temporary); start-up rows for {len(names)} tables -> Iggy {STREAM}/{CHANGES}", flush=True)

    # 3. Committed changes from the slot.
    cur.start_replication(slot_name=a.slot, decode=True, options={
        "format-version": "2", "include-lsn": "1", "include-timestamp": "1",
        "add-tables": ",".join(names)})
    commit_ns = [time.time_ns()]

    def on_msg(msg):
        m = json.loads(msg.payload)
        act = m.get("action")
        if act == "B":
            ts = m.get("timestamp")
            commit_ns[0] = int(datetime.fromisoformat(ts).timestamp() * 1e9) if ts else time.time_ns()
        elif act in ("I", "U", "D"):
            t = f"{m['schema']}.{m['table']}"
            if t not in names and m["table"] not in names:
                return
            t = t if t in names else m["table"]
            cols = {c["name"]: c["value"] for c in m.get("columns", [])}
            ident = {c["name"]: c["value"] for c in m.get("identity", [])}
            key = str(cols.get(a.key, ident.get(a.key)))
            row = json.dumps(cols, separators=(",", ":")) if act != "D" else ""
            change(t, key, act, row, lsn_int(m.get("lsn", "0/0")), commit_ns[0])
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    async def write(pid):
        while True:
            if pending[pid]:
                batch, pending[pid] = pending[pid], []
                for i in range(0, len(batch), 2000):
                    await writers[pid].send_messages(STREAM, CHANGES, pid, batch[i:i + 2000])
            await asyncio.sleep(a.tick_ms / 1000)

    async def pump():
        while True:
            m = cur.read_message()                 # non-blocking: None when nothing is waiting
            while m is not None:
                on_msg(m)
                m = cur.read_message()
            await asyncio.sleep(a.tick_ms / 1000)

    await asyncio.gather(pump(), *(write(p) for p in range(a.partitions)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", required=True, help="e.g. 'host=<standby> dbname=<db> user=<replication user>'")
    p.add_argument("--tables", required=True, help="comma-separated, e.g. 'pricing.vol_params,config.quote_widths'")
    p.add_argument("--key", default="id", help="primary key column (the same name in every table)")
    p.add_argument("--slot", default="iggy_shadow")
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--tick-ms", type=float, default=1)
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
