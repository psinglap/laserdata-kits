#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the Mux Data streaming export lane at small scale for about a minute:
# 40 environments with a streaming export each on their own topics (Kinesis or Pub/Sub), about 1,500 finished views a
# second, two export worker shards, 1% of destination writes failing once and retried in place, raw player beacons
# next door, and one destination outage: env-0001-kinesis stops for 15 s, then backfills from the last exported view
# at its Kinesis limit. Pass only if each shard holds the 100 ms p99 budget in 95% of its steady-state seconds, only
# env-0001-kinesis is ever down or at its limit, it catches up afterwards with every view inside the one-minute
# promise, and no view is lost.
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
python -m py_compile common.py views.py exporter.py beacon_flood.py kafka_bridge.py
N=40
# Two export shards start first and wait for the export topics; views.py creates them.
# env-0001-kinesis (shard 1) loses its customer's Kinesis stream from its second 25 to 40.
for s in 0 1; do
  timeout 85 python -u exporter.py --shard $s --shards 2 --envs $N \
    --outage env-0001-kinesis --outage-at 25 --outage-for 15 > exp-$s.log 2>&1 &
done
sleep 2
timeout 81 python -u views.py --envs $N --views-per-s 1500 > views.log 2>&1 &
timeout 79 python -u beacon_flood.py --mbps 4 --envs $N --partitions 8 > beacons.log 2>&1 &
wait || true
echo "--- views.log";   head -n 2 views.log; tail -n 2 views.log
echo "--- beacons.log"; tail -n 2 beacons.log
for s in 0 1; do echo "--- exp-$s.log"; head -n 3 exp-$s.log; tail -n 6 exp-$s.log; done
echo "--- outage (first, last), backfill (first, last) and catch-up"
(grep "destination down" exp-1.log | sed -n '1p;$p'; grep "backfilling at destination limit" exp-1.log | sed -n '1p;$p'
 grep "caught up:" exp-*.log) || true
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" exp-*.log | cut -c1-160 || true
status=0
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
for s in 0 1; do
  steady=$(grep -E "\[exp-$s\] +[1-9][0-9,]* views/s exported" exp-$s.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "exp-$s steady-state seconds with live views: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
done
[ "$status" -eq 0 ] || { echo "latency check failed"; exit 1; }
grep -q "destination down: env-0001-kinesis" exp-1.log || { echo "the destination outage never happened"; exit 1; }
if grep -E "destination down|backfilling at destination limit" exp-*.log | grep -vq "env-0001-kinesis"; then
  echo "an export other than env-0001-kinesis was down or at its limit"; exit 1; fi
grep -q "env-0001-kinesis caught up: .* 0 views past the one-minute promise, 0 views lost" exp-1.log \
  || { echo "env-0001-kinesis did not catch up cleanly"; exit 1; }
if grep -E "views/s exported" exp-*.log | grep -vq " lost 0 "; then echo "views were lost"; exit 1; fi
grep -Eq "updates [1-9]" exp-0.log exp-1.log || { echo "no updated views were exported"; exit 1; }
grep -Eq "writes retried [1-9]" exp-0.log exp-1.log || { echo "no destination write was retried"; exit 1; }
python - <<'PY'
import re, statistics as st
row = r"\[exp-[01]\] +([0-9,]+) views/s exported"
def num(pat, ls):
    out = []
    for l in ls:                                  # skip lines the pattern doesn't match instead of failing
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
for s in (0, 1):
    t = [l for l in open(f"exp-{s}.log") if re.search(row, l) and not re.search(r"\] +0 views", l)][10:]
    p99 = num(r"p99 +([0-9.]+)ms", t)
    if not p99:
        continue
    print("exp-%d steady-state medians over %d s: views/s %.0f  view-to-exporter p50 %.1f ms  p99 %.1f ms  "
          "p99.9 %.1f ms  worst-second p99 %.1f ms  writes retried %d  updates %d" % (
          s, len(t), st.median(num(row, t)), st.median(num(r"p50 +([0-9.]+)ms", t)), st.median(p99),
          st.median(num(r"p99.9 +([0-9.]+)ms", t)), max(p99), sum(num(r"retried ([0-9,]+)", t)),
          sum(num(r"updates ([0-9,]+)", t))))
lines = open("exp-1.log").read().splitlines()
rows = []
for l in lines:                                   # tag each per-second row: was env-0001-kinesis in trouble that second?
    if ("destination down: env-0001" in l or "limit: env-0001" in l) and rows:
        rows[-1] = (rows[-1][0], True)
    else:
        m = re.search(r"\[exp-1\] +[1-9][0-9,]* views/s exported.*p99 +([0-9.]+)ms", l)
        if m:
            rows.append((float(m.group(1)), False))
rows = rows[10:]
calm = [p for p, h in rows if not h]
busy = [p for p, h in rows if h]
if calm and busy:
    print("exp-1 p99 for the other exports, median per second: env-0001-kinesis healthy %.1f ms (%d s), "
          "env-0001-kinesis down or backfilling %.1f ms (%d s)" % (st.median(calm), len(calm), st.median(busy), len(busy)))
down = [l for l in lines if "destination down: env-0001" in l]
lim = [l for l in lines if "limit: env-0001" in l]
back = num(r"backlog ([0-9,]+)", down + lim)
waits = num(r"oldest waiting +([0-9.]+) s", lim)
print("env-0001-kinesis outage: down %d s, backfilled at its limit %d s, largest backlog %s on its own topic, longest wait %s; %s" % (
    len(down), len(lim), f"{max(back):,.0f}" if back else "n/a", f"{max(waits):.1f} s" if waits else "n/a",
    next((l.split("] ", 1)[1] for l in lines if "caught up:" in l), "no catch-up line")))
PY
grep -Eq "\[beacons\] [1-9]" beacons.log || { echo "the beacon traffic did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
