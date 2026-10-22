#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the DSP appetite lane at small scale for about a minute:
# two traffic-shaping nodes, an auction-log burst next door, and one shaping node restarted mid-run.
# Pass only if each node holds the 20 ms p99 budget in 95% of its steady-state seconds with no sequence gaps,
# flips reach the nodes and are counted as wasted callouts, and the restarted node rebuilds every pair from the log.
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
python -m py_compile common.py exchange.py shaper.py auction_log.py kafka_bridge.py
# 20 DSPs x 600 supply segments = 12,000 pairs; 2,000 appetite changes a second, 50 of them flips;
# 1 MB/s of auction log next door, 4x from its second 40 to 55.
D=20; S=600
# Record the runner's disk every second (blocked processes, IO wait, writes), to tell a slow runner disk from the lane.
vmstat -n -t 1 88 > vmstat.log 2>&1 &
timeout 88 python -u exchange.py --dsps $D --segments $S --rate 2000 --flips 50 --snapshot-every 15 > exchange.log 2>&1 &
sleep 4
timeout 76 python -u auction_log.py --mbps 1 --x 4 --at 40 --for 15 --duration 70 > auction-log.log 2>&1 &
timeout 76 python -u shaper.py --name shaper-1 --dsps $D --segments $S --requests-per-s 20000 --replay-s 0 > shaper-1.log 2>&1 &
# shaper-2 runs, stops at 34 s and starts again, as a pod rescheduled mid-run would: on restart it must
# rebuild every pair from the last 20 s of the log (one 15 s rolling snapshot plus the changes since).
timeout 34 python -u shaper.py --name shaper-2 --dsps $D --segments $S --requests-per-s 20000 --replay-s 0 > shaper-2.log 2>&1 || true
sleep 1
timeout 40 python -u shaper.py --name shaper-2 --dsps $D --segments $S --requests-per-s 20000 --replay-s 20 > shaper-2-restart.log 2>&1 || true
wait || true
echo "--- exchange.log";    grep -m 1 snapshot exchange.log || true; tail -n 3 exchange.log
echo "--- auction-log.log"; head -n 1 auction-log.log; grep -m 1 -E 'records/s .*burst' auction-log.log || true; tail -n 2 auction-log.log
for f in shaper-1 shaper-2 shaper-2-restart; do
  echo "--- $f.log"; grep -E "rebuilt|without the log" $f.log || true; tail -n 4 $f.log | cut -c1-260
done
echo "--- exchange seconds with write ack p99 of 50 ms or more (line: output)"
grep -nE "write ack p50 +[0-9.]+ms +p99 +([5-9][0-9]|[0-9]{3,})\.[0-9]ms" exchange.log | head -n 20 || true
echo "--- runner disk, per second (vmstat): seconds with IO wait of 10% or more"
awk 'NR==2{for(i=1;i<=NF;i++)h[$i]=i; next} NR>2{ if($h["b"]>0)n++; if($h["wa"]>w)w=$h["wa"]; if($h["bo"]>o)o=$h["bo"];
  if($h["wa"]>=10 && k++<30) print} END{printf "seconds with a process blocked on IO %d, max IO wait %d%%, max writes %d KB/s\n", n, w, o}' vmstat.log || true
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" shaper-*.log | cut -c1-160 || true
status=0
# Skip each run's first 10 s (warm-up, or the replay right after the restart); a shared CI runner may blip,
# so require 95% of each node's remaining seconds to pass, and no sequence gap anywhere.
steady() { for f in "$@"; do grep -E '\[shaper-[12]\] +[1-9][0-9,]* changes/s' $f | tail -n +11 || true; done; }
for node in shaper-1 shaper-2; do
  if [ $node = shaper-1 ]; then s=$(steady shaper-1.log); else s=$(steady shaper-2.log shaper-2-restart.log); fi
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$node steady-state seconds with live changes: PASS $live, FAIL $fail"
  if [ "$live" -lt 40 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then status=1; fi
done
for f in shaper-1.log shaper-2.log shaper-2-restart.log; do
  gaps=$(grep -Eo 'gaps [0-9]+' $f | tail -n 1 | cut -d' ' -f2 || true)
  echo "$f sequence gaps: ${gaps:-none}"
  [ "${gaps:-1}" = "0" ] || status=1
done
[ "$status" -eq 0 ] || { echo "appetite lane check failed"; exit 1; }
grep -Eq 'rebuilt from the last 20 s of the log in [0-9.]+s: appetite for ([0-9,]+) of \1 pairs' shaper-2-restart.log \
  || { echo "the restarted node did not rebuild every pair"; exit 1; }
if ! grep -Eq 'flips [1-9][0-9,]*  wasted callouts' shaper-1.log; then echo "no flips reached the shaping node"; exit 1; fi
grep -q "\[auction-log\] done" auction-log.log || { echo "the auction-log neighbour did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
live = r"\[shaper-[12]\] +([0-9,]+) changes/s"
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
for name in ("shaper-1", "shaper-2-restart"):
    t = [l for l in open(f"{name}.log") if re.search(live, l) and not re.search(r"\] +0 changes/s", l)][10:]
    if not t:
        continue
    p99 = nums(r"p99 +([0-9.]+)ms", t)
    med = lambda pat: st.median(nums(pat, t) or [float("nan")])
    print("%s steady-state medians over %d s: changes/s %.0f  exchange-to-shaper p50 %.2f ms  p99 %.2f ms  p99.9 %.2f ms  "
          "worst-second p99 %.2f ms  flips/s %.0f  wasted callouts/s %.2f (%.3f per 1M)  1 s behind %.0f/s" % (
          name, len(t), med(live), med(r"p50 +([0-9.]+)ms"), med(r"p99 +([0-9.]+)ms"), med(r"p99.9 +([0-9.]+)ms"),
          max(p99 or [float("nan")]), med(r"flips ([0-9,]+)"), med(r"wasted callouts ([0-9,.]+)/s"),
          med(r"\(([0-9.]+) per 1M\)"), med(r"1 s behind ([0-9,]+)/s")))
rows = nums(r"p99 +([0-9.]+)ms", [l for l in open("shaper-1.log") if re.search(live, l)])
if len(rows) > 56:   # the auction log bursts 4x from its second 40 to 55, about shaper-1's seconds 40 to 55
    print("shaper-1 p99, median per second: before the auction-log burst %.2f ms (s 20-38), during it %.2f ms (s 42-54)" % (
        st.median(rows[20:39]), st.median(rows[42:55])))
for f in ("shaper-2-restart.log", "shaper-1.log"):
    for l in open(f):
        if "rebuilt" in l or "without the log" in l:
            print(l.strip())
e = nums(r"~([0-9.]+) MB/s", [l for l in open("auction-log.log") if re.search(r"\] +[1-9][0-9,]* records/s", l)])
if e:
    print("auction-log neighbour: %d s, median %.1f MB/s, peak %.1f MB/s on its own topic" % (len(e), st.median(e), max(e)))
PY
echo "Kit OK on Apache Iggy 0.9.0"
