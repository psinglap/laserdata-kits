#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the live points lane at small scale for about a minute:
# 60 live matches (match time compressed to a ball every 0.5 s each, about 120 balls a second), two points
# nodes with 20,000 teams each, a toss surge of team saves and contest joins next door, and one node
# restarted mid-match.
# Pass only if each node holds the 20 ms p99 budget in 95% of its steady-state seconds, with no sequence
# gaps and no team total off a full recount, and the restarted node rebuilds every live match from the log.
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
python -m py_compile common.py scoring.py points_node.py toss_surge.py kafka_bridge.py
M=60
timeout 88 python -u scoring.py --matches $M --ball-s 0.5 > scoring.log 2>&1 &
sleep 4
timeout 76 python -u toss_surge.py --rate 250 --x 4 --at 40 --for 15 > surge.log 2>&1 &
timeout 76 python -u points_node.py --name points-1 --matches $M --teams 20000 > points-1.log 2>&1 &
# points-2 runs, stops at 34 s and starts again: on restart it must rebuild every live match from the log.
timeout 34 python -u points_node.py --name points-2 --matches $M --teams 20000 > points-2.log 2>&1 || true
sleep 1
timeout 40 python -u points_node.py --name points-2 --matches $M --teams 20000 > points-2-restart.log 2>&1 || true
wait || true
echo "--- scoring.log"; grep -m 1 "line-ups" scoring.log || true; tail -n 3 scoring.log
echo "--- surge.log";   grep -m 1 "toss surge" surge.log || true; tail -n 2 surge.log
for f in points-1 points-2 points-2-restart; do
  echo "--- $f.log"; grep -E "rebuilt" $f.log || true; tail -n 4 $f.log
done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" points-*.log | cut -c1-160 || true
status=0
# Skip each run's first 10 s (warm-up, or the rebuild right after the restart); then require 95% PASS.
steady() { for f in "$@"; do grep -E '\[points-[12]\] +[1-9][0-9,]* balls/s' $f | tail -n +11 || true; done; }
for node in points-1 points-2; do
  if [ $node = points-1 ]; then s=$(steady points-1.log); else s=$(steady points-2.log points-2-restart.log); fi
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$node steady-state seconds with live balls: PASS $live, FAIL $fail"
  if [ "$live" -lt 40 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then status=1; fi
done
for f in points-1.log points-2.log points-2-restart.log; do
  gaps=$(grep -Eo 'gaps [0-9]+' $f | tail -n 1 | cut -d' ' -f2)
  off=$(grep -Eo 'totals off [0-9]+' $f | sort -t' ' -k3 -n | tail -n 1 | cut -d' ' -f3)
  echo "$f sequence gaps: ${gaps:-none}  worst totals off: ${off:-none}"
  [ "${gaps:-1}" = "0" ] || status=1
  [ "${off:-1}" = "0" ] || status=1
done
[ "$status" -eq 0 ] || { echo "points lane check failed"; exit 1; }
grep -Eq "rebuilt from the log in [0-9.]+s: points for $M of $M live matches, [0-9,]+ balls replayed, totals off 0" points-2-restart.log \
  || { echo "the restarted node did not rebuild every live match"; exit 1; }
grep -q "pushed after a newer ball existed" points-1.log || { echo "no business metric"; exit 1; }
python - <<'PY'
import re, statistics as st
live = r"\[points-[12]\] +([0-9,]+) balls/s"
def num(rows, pat):
    out = []
    for l in rows:
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
for name in ("points-1", "points-2-restart"):
    t = [l for l in open(f"{name}.log") if re.search(live, l)][10:]
    p99 = num(t, r"p99 +([0-9.]+)ms")
    if p99:
        print("%s steady-state medians over %d s: balls/s %.0f  ball-to-node p50 %.2f ms  p99 %.2f ms  p99.9 %.2f ms  "
              "worst-second p99 %.2f ms  team totals updated/s %.0f  pushed after a newer ball existed %.3f%%" % (
              name, len(t), st.median(num(t, live)), st.median(num(t, r"p50 +([0-9.]+)ms")), st.median(p99),
              st.median(num(t, r"p99\.9 +([0-9.]+)ms")), max(p99), st.median(num(t, r"updated ([0-9,]+)/s")),
              st.median(num(t, r"newer ball existed ([0-9.]+)%"))))
rows = num([l for l in open("points-1.log") if re.search(live, l)], r"p99 +([0-9.]+)ms")
if len(rows) > 56:   # the toss surge runs from its second 40 to 55, about points-1's seconds 40 to 55
    print("points-1 p99, median per second: before the toss surge %.2f ms (s 20-38), during it %.2f ms (s 42-54)" % (
        st.median(rows[20:39]), st.median(rows[42:55])))
for f in ("points-1", "points-2", "points-2-restart"):
    for l in open(f"{f}.log"):
        if "rebuilt from the log" in l:
            print(l.strip())
PY
echo "Kit OK on Apache Iggy 0.9.0"
