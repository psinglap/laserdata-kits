"""Count every record the collectors accepted in ClickHouse: nothing lost, replays folded away.

Reads the totals collectors.py wrote when it stopped, then polls ClickHouse until the writer has
drained the log (or --wait seconds pass). Distinct (tenant, batch_seq, idx) keys must equal the
records accepted (reconnect_flood.py rows, tenant agent-*, are left out); extra raw rows are replays after a writer restart, which ReplacingMergeTree folds.
Exit code 0 only if no accepted record is missing.
"""
import argparse
import json
import time

import clickhouse_connect

from common import CH, TABLE


def main(a):
    want = json.load(open(a.totals))["records"]
    ch = clickhouse_connect.get_client(**CH)
    t0 = time.monotonic()
    while True:
        rows, keys = ch.query(f"SELECT count(), uniqExact(tenant, batch_seq, idx) FROM {TABLE} WHERE tenant LIKE 'tenant-%'").result_rows[0]
        if keys >= want or time.monotonic() - t0 > a.wait:
            break
        print(f"[verify] waiting for the writer to drain: {keys:,} of {want:,} accepted records in ClickHouse", flush=True)
        time.sleep(2)
    lost = max(0, want - keys)
    print(f"[verify] accepted by collectors {want:,}  in ClickHouse {keys:,}  lost {lost:,}  "
          f"replayed rows folded by ReplacingMergeTree {rows - keys:,}  (checked after {time.monotonic() - t0:.0f}s)", flush=True)
    raise SystemExit(1 if lost or keys > want else 0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--totals", default="collectors.totals.json")
    p.add_argument("--wait", type=float, default=120)
    main(p.parse_args())
