#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the Smart Trigger lane at small scale for about a minute:
# 40 workspaces on their own topics, two evaluator shards, bulk ingestion next door, and one brand's
# flash sale (4,000 extra events a second for 10 s) against a 1,500 events/s per-workspace quota.
# Pass only if each shard holds the 100 ms p99 budget in 95% of its steady-state seconds, triggers fire,
# only the flash-sale workspace is ever held at its quota, and its backlog clears afterwards.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst (150-180 MB, 30-35 s
# after the heavy neighbour starts) stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py workspaces.py trigger_eval.py ingest_flood.py kafka_bridge.py
N=40
# Two evaluator shards start first and wait for the workspace topics; workspaces.py creates them.
timeout 80 python -u trigger_eval.py --shard 0 --shards 2 --workspaces $N --quota 1500 > eval-0.log 2>&1 &
timeout 80 python -u trigger_eval.py --shard 1 --shards 2 --workspaces $N --quota 1500 > eval-1.log 2>&1 &
sleep 2
# ws-0002 (evaluated by shard 1) runs a flash sale from its second 20 to 30.
timeout 76 python -u workspaces.py --workspaces $N --rate 3000 --users 400000 \
  --flash-ws 2 --flash-rate 4000 --flash-at 20 --flash-for 10 > workspaces.log 2>&1 &
timeout 74 python -u ingest_flood.py --mbps 4 --workspaces $N --partitions 8 > ingest.log 2>&1 &
wait || true
echo "--- workspaces.log"; head -n 2 workspaces.log; grep -m 2 "flash sale" workspaces.log || true; tail -n 2 workspaces.log
echo "--- ingest.log";     tail -n 2 ingest.log
for s in 0 1; do echo "--- eval-$s.log"; head -n 2 eval-$s.log; tail -n 6 eval-$s.log; done
echo "--- held at quota (first, last) and drain"; (grep "held at quota" eval-1.log | sed -n '1p;$p'; grep "back inside" eval-*.log) || true
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" eval-*.log | cut -c1-150 || true
status=0
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
for s in 0 1; do
  steady=$(grep -E "\[eval-$s\] +[1-9][0-9,]* trigger events/s" eval-$s.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "eval-$s steady-state seconds with live events: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
done
[ "$status" -eq 0 ] || { echo "latency check failed"; exit 1; }
grep -q "held at quota: ws-0002" eval-1.log || { echo "the flash sale never reached its quota"; exit 1; }
if grep "held at quota" eval-*.log | grep -vq "ws-0002"; then echo "a workspace other than the flash sale was held"; exit 1; fi
grep -q "ws-0002 back inside its quota" eval-1.log || { echo "the flash-sale backlog did not clear"; exit 1; }
python - <<'PY'
import re, statistics as st
row = r"\[eval-[01]\] +([0-9,]+) trigger events/s"
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
fired = []
for s in (0, 1):
    t = [l for l in open(f"eval-{s}.log") if re.search(row, l) and not re.search(r"\] +0 trigger", l)][10:]
    fired += t
    p99 = num(r"p99 +([0-9.]+)ms", t)
    print("eval-%d steady-state medians over %d s: events/s %.0f  event-to-evaluator p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  "
          "worst-second p99 %.1f ms  fired/s %.0f  within 1 s %.2f%%" % (
          s, len(t), st.median(num(row, t)), st.median(num(r"p50 +([0-9.]+)ms", t)), st.median(p99),
          st.median(num(r"p99.9 +([0-9.]+)ms", t)), max(p99), st.median(num(r"fired ([0-9,]+)/s", t)),
          st.median(num(r"within 1 s +([0-9.]+)%", t))))
lines = open("eval-1.log").read().splitlines()
rows, held = [], False
for l in lines:                                   # tag each per-second row: was ws-0002 held that second?
    if "held at quota: ws-0002" in l and rows:
        rows[-1] = (rows[-1][0], True)
    elif re.search(r"\[eval-1\] +[1-9][0-9,]* trigger events/s", l):
        rows.append((float(re.search(r"p99 +([0-9.]+)ms", l).group(1)), False))
rows = rows[10:]
quiet = [p for p, h in rows if not h]
busy = [p for p, h in rows if h]
if quiet and busy:
    print("eval-1 p99 for the other workspaces, median per second: ws-0002 inside its quota %.1f ms (%d s), "
          "ws-0002 held at its quota %.1f ms (%d s)" % (st.median(quiet), len(quiet), st.median(busy), len(busy)))
h = [l for l in lines if "held at quota: ws-0002" in l]
print("ws-0002 flash sale: held for %d s, largest backlog %s on its own topic, longest wait %.1f s; %s" % (
    len(h), f"{max(num(r'backlog ([0-9,]+)', h)):,.0f}", max(num(r"oldest waiting +([0-9.]+) s", h)),
    next(l.split("] ", 1)[1] for l in lines if "back inside" in l)))
if st.median(num(r"within 1 s +([0-9.]+)%", fired)) < 99.9:
    raise SystemExit("too few campaign triggers fired within 1 s")
if st.median(num(r"fired ([0-9,]+)/s", fired)) < 1:
    raise SystemExit("no campaign triggers fired")
PY
grep -Eq "\[ingest\] [1-9]" ingest.log || { echo "the ingestion firehose did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
