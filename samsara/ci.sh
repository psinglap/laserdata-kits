#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the Data Connector stream lane at small scale for about a minute:
# 40 orgs and 40,000 vehicles with 92 stream subscriptions on their own topics, two connector shards, raw device
# uploads next door, and one customer broker outage: org-0002-gps stops streaming for 15 s, then backfills from the
# last streamed datapoint at a 1,200 datapoints/s destination limit. Pass only if each shard holds the 100 ms p99
# budget in 95% of its steady-state seconds, only org-0002-gps is ever down or at its limit, it catches up inside
# the recovery window afterwards, and no datapoint is lost.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst (30-35 s after the
# heavy neighbour starts) stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py datapoints.py connector.py ingest_flood.py kinesis_bridge.py
N=40
# Two connector shards start first and wait for the subscription topics; datapoints.py creates them.
# org-0002-gps (shard 1) loses its customer's broker from its second 25 to 40.
for s in 0 1; do
  timeout 85 python -u connector.py --shard $s --shards 2 --orgs $N --rate-limit 1200 \
    --outage org-0002-gps --outage-at 25 --outage-for 15 > conn-$s.log 2>&1 &
done
sleep 2
timeout 81 python -u datapoints.py --orgs $N --vehicles 40000 --on-share 0.3 > datapoints.log 2>&1 &
timeout 79 python -u ingest_flood.py --mbps 4 --orgs $N --partitions 8 > ingest.log 2>&1 &
wait || true
echo "--- datapoints.log"; head -n 2 datapoints.log; tail -n 2 datapoints.log
echo "--- ingest.log";     tail -n 2 ingest.log
for s in 0 1; do echo "--- conn-$s.log"; head -n 3 conn-$s.log; tail -n 6 conn-$s.log; done
echo "--- outage (first, last), backfill (first, last) and catch-up"
(grep "broker down" conn-1.log | sed -n '1p;$p'; grep "backfilling at destination limit" conn-1.log | sed -n '1p;$p'
 grep "caught up:" conn-*.log) || true
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" conn-*.log | cut -c1-160 || true
status=0
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
for s in 0 1; do
  steady=$(grep -E "\[conn-$s\] +[1-9][0-9,]* datapoints/s streamed" conn-$s.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "conn-$s steady-state seconds with live datapoints: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
done
[ "$status" -eq 0 ] || { echo "latency check failed"; exit 1; }
grep -q "broker down: org-0002-gps" conn-1.log || { echo "the broker outage never happened"; exit 1; }
if grep -E "broker down|backfilling at destination limit" conn-*.log | grep -vq "org-0002-gps"; then
  echo "a subscription other than org-0002-gps was down or at its limit"; exit 1; fi
grep -q "org-0002-gps caught up: .* inside the recovery window, 0 datapoints lost" conn-1.log \
  || { echo "org-0002-gps did not catch up cleanly"; exit 1; }
if grep -E "datapoints/s streamed" conn-*.log | grep -vq " lost 0 "; then echo "datapoints were lost"; exit 1; fi
python - <<'PY'
import re, statistics as st
row = r"\[conn-[01]\] +([0-9,]+) datapoints/s streamed"
def num(pat, ls):
    out = []
    for l in ls:                                  # skip lines the pattern doesn't match instead of failing
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
for s in (0, 1):
    t = [l for l in open(f"conn-{s}.log") if re.search(row, l) and not re.search(r"\] +0 datapoints", l)][10:]
    p99 = num(r"p99 +([0-9.]+)ms", t)
    if not p99:
        continue
    print("conn-%d steady-state medians over %d s: datapoints/s %.0f  datapoint-to-connector p50 %.1f ms  p99 %.1f ms  "
          "p99.9 %.1f ms  worst-second p99 %.1f ms" % (
          s, len(t), st.median(num(row, t)), st.median(num(r"p50 +([0-9.]+)ms", t)), st.median(p99),
          st.median(num(r"p99.9 +([0-9.]+)ms", t)), max(p99)))
lines = open("conn-1.log").read().splitlines()
rows = []
for l in lines:                                   # tag each per-second row: was org-0002-gps in trouble that second?
    if ("broker down: org-0002" in l or "limit: org-0002" in l) and rows:
        rows[-1] = (rows[-1][0], True)
    else:
        m = re.search(r"\[conn-1\] +[1-9][0-9,]* datapoints/s streamed.*p99 +([0-9.]+)ms", l)
        if m:
            rows.append((float(m.group(1)), False))
rows = rows[10:]
calm = [p for p, h in rows if not h]
busy = [p for p, h in rows if h]
if calm and busy:
    print("conn-1 p99 for the other subscriptions, median per second: org-0002-gps healthy %.1f ms (%d s), "
          "org-0002-gps down or backfilling %.1f ms (%d s)" % (st.median(calm), len(calm), st.median(busy), len(busy)))
down = [l for l in lines if "broker down: org-0002" in l]
lim = [l for l in lines if "limit: org-0002" in l]
back = num(r"backlog ([0-9,]+)", down + lim)
waits = num(r"oldest waiting +([0-9.]+) s", lim)
print("org-0002-gps outage: down %d s, backfilled at its limit %d s, largest backlog %s on its own topic, longest wait %s; %s" % (
    len(down), len(lim), f"{max(back):,.0f}" if back else "n/a", f"{max(waits):.1f} s" if waits else "n/a",
    next((l.split("] ", 1)[1] for l in lines if "caught up:" in l), "no catch-up line")))
PY
grep -Eq "\[ingest\] [1-9]" ingest.log || { echo "the ingest traffic did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
