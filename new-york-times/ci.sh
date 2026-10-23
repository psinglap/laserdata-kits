#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the publish-to-site lane at small scale for about a minute:
# 400 assets on the live desk (newsroom time 60x real time, about 100 editor versions a second, ~4 KB each),
# two serving nodes, a tag change that republishes 6,000 archive articles on the same lane over 15 s, and one
# node restarted mid-run.
# Pass only if each node holds the 50 ms p99 budget in 95% of its steady-state seconds, with no version gaps,
# every editor version inside the one-second SLA, the restarted node rebuilt from the log, the republish burst
# seen by the nodes, and the page-views metric printed.
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
python -m py_compile common.py newsroom.py serving_node.py tag_republish.py kafka_bridge.py
A=400
timeout 88 python -u newsroom.py --assets $A --speed 60 > newsroom.log 2>&1 &
sleep 4
timeout 76 python -u tag_republish.py --articles 6000 --at 32 --for 15 > tag-republish.log 2>&1 &
timeout 76 python -u serving_node.py --name serving-1 --assets $A > serving-1.log 2>&1 &
# serving-2 runs, stops at 34 s and starts again: on restart it must rebuild from the log before it serves.
timeout 34 python -u serving_node.py --name serving-2 --assets $A > serving-2.log 2>&1 || true
sleep 1
timeout 40 python -u serving_node.py --name serving-2 --assets $A > serving-2-restart.log 2>&1 || true
wait || true
echo "--- newsroom.log";      grep -m 1 "version 1 of" newsroom.log || true; tail -n 3 newsroom.log
echo "--- tag-republish.log"; grep -E "version 1 of|updated:" tag-republish.log || true; tail -n 2 tag-republish.log
for f in serving-1 serving-2 serving-2-restart; do
  echo "--- $f.log"; grep -E "rebuilt" $f.log || true; tail -n 3 $f.log
done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" serving-*.log | cut -c1-170 || true
status=0
# Skip each run's first 10 s (warm-up, or the rebuild right after the restart); then require 95% PASS.
steady() { for f in "$@"; do grep -E '\[serving-[12]\] +[1-9][0-9,]* editor versions/s' $f | tail -n +11 || true; done; }
for node in serving-1 serving-2; do
  if [ $node = serving-1 ]; then s=$(steady serving-1.log); else s=$(steady serving-2.log serving-2-restart.log); fi
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$node steady-state seconds with live versions: PASS $live, FAIL $fail"
  if [ "$live" -lt 40 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then status=1; fi
done
for f in serving-1.log serving-2.log serving-2-restart.log; do
  gaps=$(grep -Eo 'gaps [0-9]+' $f | sort -t' ' -k2 -n | tail -n 1 | cut -d' ' -f2)
  echo "$f worst version gaps: ${gaps:-none}"
  [ "${gaps:-1}" = "0" ] || status=1
done
# Every editor version must land inside the one-second SLA on every node, every timed second.
late=$(grep -hEo 'inside the 1 s SLA [0-9,]+ of [0-9,]+' serving-*.log | awk '{gsub(",","",$6); gsub(",","",$8); if ($6 != $8) n++} END {print n+0}')
echo "seconds with an editor version outside the one-second SLA: $late"
[ "$late" -eq 0 ] || status=1
[ "$status" -eq 0 ] || { echo "publish-to-site lane check failed"; exit 1; }
grep -Eq "rebuilt from the log in [0-9.]+s: latest version of $A of $A live assets and [0-9,]+ other assets, [0-9,]+ versions replayed" serving-2-restart.log \
  || { echo "the restarted node did not rebuild every live asset"; exit 1; }
grep -Eq "page views served the previous version [0-9.,]+ of [0-9,]+/s" serving-1.log || { echo "no business metric"; exit 1; }
grep -Eq "republishes [1-9][0-9,]*/s" serving-1.log || { echo "the republish burst never reached serving-1"; exit 1; }
python - <<'PY'
import re, statistics as st
live = r"\[serving-[12]\] +([0-9,]+) editor versions/s"
def num(rows, pat):
    out = []
    for l in rows:
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
for name in ("serving-1", "serving-2-restart"):
    t = [l for l in open(f"{name}.log") if re.search(live, l)][10:]
    p99 = num(t, r"p99 +([0-9.]+)ms")
    if p99:
        print("%s steady-state medians over %d s: editor versions/s %.0f  publish-to-node p50 %.2f ms  p99 %.2f ms  "
              "p99.9 %.2f ms  worst-second p99 %.2f ms  page views served the previous version %.1f/s (%.4f%%)" % (
              name, len(t), st.median(num(t, live)), st.median(num(t, r"p50 +([0-9.]+)ms")), st.median(p99),
              st.median(num(t, r"p99\.9 +([0-9.]+)ms")), max(p99), st.median(num(t, r"previous version ([0-9.,]+) of")),
              st.median(num(t, r"/s \(([0-9.]+)%\)"))))
rows = [l for l in open("serving-1.log") if re.search(live, l)]
burst = [l for l in rows if re.search(r"republishes [1-9]", l)]
calm = [l for l in rows[10:] if not re.search(r"republishes [1-9]", l)]
if burst and calm:
    print("serving-1 editor p99, median per second: without republishes %.2f ms (%d s), during the tag republish burst "
          "%.2f ms (%d s, %.0f republishes/s, republish p99 %.1f ms)" % (
          st.median(num(calm, r"p99 +([0-9.]+)ms")), len(calm), st.median(num(burst, r"p99 +([0-9.]+)ms")), len(burst),
          st.median(num(burst, r"republishes ([0-9,]+)/s")), st.median(num(burst, r"republishes [0-9,]+/s p99 ([0-9.]+)ms"))))
for f in ("serving-1", "serving-2", "serving-2-restart"):
    for l in open(f"{f}.log"):
        if "rebuilt from the log" in l:
            print(l.strip())
PY
echo "Kit OK on Apache Iggy 0.9.0"
