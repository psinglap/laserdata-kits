#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the ball-by-ball commentary lane at small scale for about a minute:
# 60 live matches (match time 60x real time, about 100 commentary lines a second with corrections), two commentary
# nodes and match alerts reading the same lane, ad impressions next door with a 4x innings-break surge, and one
# commentary node and match alerts each restarted mid-match.
# Pass only if each commentary node holds the 20 ms p99 budget in 95% of its steady-state seconds with no gaps and
# no line held at an old version, the restarted node rebuilds every live match from the log, corrections reach the
# nodes, match alerts holds its 50 ms budget in 95% of its seconds with alerts, takes overturned wickets back, and
# the alerts-sent record holds no alert twice across the restart.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst; on shared runners that burst stalled every
# reader for 0.2-0.8 s at once. It is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py commentary_desk.py commentary_node.py match_alerts.py impressions.py commentary_bridge.py
M=60
timeout 88 python -u commentary_desk.py --matches $M --speed 60 > desk.log 2>&1 &
sleep 4
timeout 76 python -u impressions.py --rate 250 --x 4 --at 40 --for 15 > impressions.log 2>&1 &
timeout 76 python -u commentary_node.py --name commentary-1 --matches $M > commentary-1.log 2>&1 &
# commentary-2 and match alerts run, stop at 34 s and start again: on restart the node must rebuild every live
# match from the log, and match alerts must send nothing twice.
( timeout 34 python -u match_alerts.py > alerts.log 2>&1 || true; sleep 1
  timeout 40 python -u match_alerts.py > alerts-restart.log 2>&1 || true ) &
timeout 34 python -u commentary_node.py --name commentary-2 --matches $M > commentary-2.log 2>&1 || true
sleep 1
timeout 40 python -u commentary_node.py --name commentary-2 --matches $M > commentary-2-restart.log 2>&1 || true
wait || true
python -u match_alerts.py --audit > audit.log 2>&1
echo "--- desk.log";        grep -m 1 "opening lines" desk.log || true; tail -n 3 desk.log
echo "--- impressions.log"; grep -m 1 "surge" impressions.log || true; tail -n 2 impressions.log
for f in commentary-1 commentary-2 commentary-2-restart alerts alerts-restart; do
  echo "--- $f.log"; grep -E "rebuilt|from the record" $f.log || true; tail -n 4 $f.log
done
echo "--- audit.log"; cat audit.log
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" commentary-*.log alerts*.log | cut -c1-170 || true
status=0
# Skip each run's first 10 s (warm-up, or the rebuild right after the restart); then require 95% PASS.
steady() { for f in "$@"; do grep -E "$pat" $f | tail -n +11 || true; done; }
pat='\[commentary-[12]\] +[1-9][0-9,]* lines/s'
for node in commentary-1 commentary-2; do
  if [ $node = commentary-1 ]; then s=$(steady commentary-1.log); else s=$(steady commentary-2.log commentary-2-restart.log); fi
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$node steady-state seconds with live lines: PASS $live, FAIL $fail"
  if [ "$live" -lt 40 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then status=1; fi
done
pat='\[alerts\] +[1-9][0-9]* alerts/s'
s=$(steady alerts.log alerts-restart.log)
live=$(grep -c 'PASS (' <<<"$s" || true)
fail=$(grep -c 'FAIL (' <<<"$s" || true)
echo "match alerts steady-state seconds with alerts: PASS $live, FAIL $fail"
if [ "$live" -lt 30 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then status=1; fi
for f in commentary-1.log commentary-2.log commentary-2-restart.log; do
  gaps=$(grep -Eo 'gaps [0-9]+' $f | sort -t' ' -k2 -n | tail -n 1 | cut -d' ' -f2)
  old=$(grep -Eo 'old lines held [0-9]+' $f | sort -t' ' -k4 -n | tail -n 1 | cut -d' ' -f4)
  echo "$f worst gaps: ${gaps:-none}  worst old lines held: ${old:-none}"
  if ! ( [ "${gaps:-1}" = "0" ] && [ "${old:-1}" = "0" ] ); then status=1; fi
done
[ "$status" -eq 0 ] || { echo "commentary lane check failed"; exit 1; }
grep -Eq "rebuilt from the log in [0-9.]+s: commentary for $M of $M live matches, [0-9,]+ lines and corrections replayed" \
  commentary-2-restart.log || { echo "the restarted node did not rebuild every live match"; exit 1; }
grep -Eq "corrections live [1-9]" commentary-1.log || { echo "no corrections reached the node"; exit 1; }
grep -Eq "taken back after review [1-9]" alerts.log alerts-restart.log || { echo "no overturned wicket taken back"; exit 1; }
grep -Eq "alerts already sent today, from the record: [1-9][0-9,]* \(recorded twice 0\)" alerts-restart.log \
  || { echo "the restarted alerts service did not read its record"; exit 1; }
grep -Eq "recorded twice 0$" audit.log || { echo "an alert was recorded twice"; exit 1; }
python - <<'PY'
import re, statistics as st
def num(rows, pat):
    out = []
    for l in rows:
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
live = r"\[commentary-[12]\] +([0-9,]+) lines/s"
for name in ("commentary-1", "commentary-2-restart"):
    t = [l for l in open(f"{name}.log") if re.search(live, l)][10:]
    p99 = num(t, r" p99 +([0-9.]+)ms")
    if p99:
        print("%s steady-state medians over %d s: lines/s %.0f  line-to-node p50 %.2f ms  p99 %.2f ms  p99.9 %.2f ms  "
              "worst-second p99 %.2f ms  corrections live %d in all, slowest %.1f ms" % (
              name, len(t), st.median(num(t, live)), st.median(num(t, r"p50 +([0-9.]+)ms")), st.median(p99),
              st.median(num(t, r"p99\.9 +([0-9.]+)ms")), max(p99), sum(num(t, r"corrections live ([0-9]+)")),
              max(num(t, r"slowest \+([0-9.]+)ms") or [0])))
rows = num([l for l in open("commentary-1.log") if re.search(live, l)], r" p99 +([0-9.]+)ms")
if len(rows) > 56:   # the surge runs from its second 40 to 55, about commentary-1's seconds 40 to 55
    print("commentary-1 p99, median per second: before the innings-break surge %.2f ms (s 20-38), during it %.2f ms (s 42-54)" % (
        st.median(rows[20:39]), st.median(rows[42:55])))
al = r"\[alerts\] +([0-9]+) alerts/s"
t = [l for f in ("alerts", "alerts-restart") for l in open(f"{f}.log") if re.search(al, l)]
p99 = num(t, r" p99 +([0-9.]+)ms")
if p99:
    print("match alerts over %d s with alerts: ball-to-alert p50 %.2f ms  p99 %.2f ms (median per second), worst second %.2f ms; "
          "wickets %d, milestones %d, taken back after review %d" % (
          len(p99), st.median(num(t, r"p50 +([0-9.]+)ms")), st.median(p99), max(p99), sum(num(t, r"wickets ([0-9]+)")),
          sum(num(t, r"milestones ([0-9]+)")), sum(num(t, r"after review ([0-9]+)"))))
for f in ("commentary-1", "commentary-2", "commentary-2-restart", "alerts", "alerts-restart", "audit"):
    for l in open(f"{f}.log"):
        if "rebuilt from the log" in l or "from the record" in l or "[audit]" in l:
            print(l.strip())
PY
echo "Kit OK on Apache Iggy 0.9.0"
