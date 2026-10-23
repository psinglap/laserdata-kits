#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the table change log at small scale for about a minute:
# 16 config and pricing tables of 500 rows on 4 partitions, 1,000 row changes a second, one live subscriber node
# serving 500 simulated processes, checkpoints every 2 s, and from second 12 a storm of 40 processes every 8 s that
# start at once and rebuild their tables from the log.
# Pass only if the subscriber holds the 10 ms p99 budget in 95% of its steady-state seconds, every table it holds
# matches the writer's state hash, there are no sequence gaps anywhere, and every rejoined process's tables match.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst stalled every
# partition for 0.2-0.8 s at once (found on other kits); it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py pg_writer.py subscriber.py checkpoint.py rejoin.py pg_bridge.py
T=16
timeout 84 python -u pg_writer.py --tables $T --rows 500 --partitions 4 --rate 1000 > writer.log 2>&1 &
sleep 3
timeout 80 python -u checkpoint.py --every-s 2 > checkpoint.log 2>&1 &
timeout 80 python -u subscriber.py --name sub-1 --tables $T --procs 500 > sub-1.log 2>&1 &
timeout 78 python -u rejoin.py --tables $T --storm 40 --every-s 8 --start-s 12 --conns 4 > rejoin.log 2>&1 &
wait || true
echo "--- writer.log";     head -n 2 writer.log; tail -n 2 writer.log
echo "--- checkpoint.log"; head -n 2 checkpoint.log; tail -n 2 checkpoint.log
echo "--- sub-1.log";      grep -m 1 "caught up" sub-1.log || true; tail -n 3 sub-1.log
echo "--- rejoin.log";     head -n 2 rejoin.log; tail -n 2 rejoin.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" sub-1.log | cut -c1-200 || true
status=0
# Skip the first 10 live seconds (warm-up); a shared CI runner may blip, so require 95% of the rest to pass.
steady=$(grep -E '\[sub-1\] +[1-9][0-9,]* changes/s' sub-1.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "sub-1 steady-state seconds with live changes: PASS $live, FAIL $fail"
if [ "$live" -lt 30 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then status=1; fi
last=$(grep -E '\[sub-1\] +[0-9,]+ changes/s' sub-1.log | tail -n 1)
if ! grep -Eq "tables matching the writer $T/$T  gaps 0" <<<"$last"; then
  echo "sub-1: a table drifted from the writer or a change went missing"; status=1
fi
if grep -Eq 'hash mismatches [1-9]|gaps [1-9]' checkpoint.log; then echo "checkpoint: hash mismatch or gap"; status=1; fi
so_far=$(grep 'so far:' rejoin.log | tail -n 1)
echo "rejoin: $so_far"
if ! python - "$so_far" <<'PY'
import re, sys
m = re.search(r"so far: ([0-9,]+) rejoins, ([0-9,]+) matching, ([0-9]+) gaps", sys.argv[1])
n = lambda i: int(m.group(i).replace(",", ""))
sys.exit(0 if m and n(1) >= 150 and n(1) == n(2) and n(3) == 0 else 1)
PY
then echo "rejoin: too few rejoins, a rejoined copy did not match, or a gap"; status=1; fi
[ "$status" -eq 0 ] || { echo "table change log check failed"; exit 1; }
python - <<'PY'
import re, statistics as st
live = r"\[sub-1\] +([0-9,]+) changes/s"
t = [l for l in open("sub-1.log") if re.search(live, l) and not re.search(r"\] +0 changes/s", l)][10:]
def num(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
if t:
    p99 = num(r"p99 +([0-9.]+)ms", t)
    print("sub-1 steady-state medians over %d s: changes/s %.0f  commit-to-subscriber p50 %.2f ms  p99 %.2f ms  "
          "p99.9 %.2f ms  worst-second p99 %.2f ms  deliveries/s %.0f" % (
          len(t), st.median(num(live, t)), st.median(num(r"p50 +([0-9.]+)ms", t)), st.median(p99),
          st.median(num(r"p99.9 +([0-9.]+)ms", t)), max(p99), st.median(num(r"processes ([0-9,]+)/s", t))))
r = [l for l in open("rejoin.log") if "processes rejoined" in l]
if r:
    print("rejoin, over %d seconds with rejoins: rejoin time p50 median %.1f ms, p99 median %.1f ms, worst %.1f ms; "
          "rows from the log per second median %.0f" % (
          len(r), st.median(num(r"p50 +([0-9.]+)ms", r)), st.median(num(r"p99 +([0-9.]+)ms", r)),
          max(num(r"max +([0-9.]+)ms", r)), st.median(num(r"rows from the log ([0-9,]+)", r))))
PY
grep -m 1 "caught up" sub-1.log || true
echo "Kit OK on Apache Iggy 0.9.0"
