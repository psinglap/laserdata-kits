#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the episode publish status lane at small scale for about a
# minute: new episode uploads at 200/s and updates to older episodes at 80/s into a transcoding pool that
# starts 330 a second, then a bulk delivery of 3.5x new uploads for 12 s that the pool cannot keep up with, so a
# backlog builds and drains; creators re-upload 20% of episodes not live after 5 s; 0.5% of stage changes are
# delivered twice; the reprocessing job's segment log next door from second 15; and a second status replica
# started in the middle of the backlog. The backlog line is 3 s (300 s in production defaults).
# Pass only if the status service holds the 50 ms p99 budget in 95% of its steady-state seconds, every receipt
# is in the view within a second, re-uploads are answered from the view with no wrong answer, no sequence gap,
# repeats are dropped, every real backlog crossing is alerted within 1 s and no alert is raised without one,
# and the late replica rebuilds its view from the log and alerts on the backlog it finds.
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
python -m py_compile common.py pipeline.py status.py reprocess_flood.py pubsub_bridge.py
timeout 82 python -u status.py --name status-1 --threshold-s 3 > status-1.log 2>&1 &
sleep 2
timeout 76 python -u pipeline.py --new-per-s 200 --update-per-s 80 --capacity 330 --transcode-s 1.5 --analysis-s 0.4 \
  --spike-at 20 --spike-for 12 --spike-x 3.5 --patience-s 5 --reupload-share 0.2 --threshold-s 3 --duration 72 \
  > pipeline.log 2>&1 &
timeout 72 python -u reprocess_flood.py --mbps 2 --at 15 --duration 40 > reprocess.log 2>&1 &
# A second status replica comes up 40 s in, while the backlog is over the line: it must rebuild from the log first.
sleep 38
timeout 38 python -u status.py --name status-2 --threshold-s 3 > status-2.log 2>&1 || true
wait || true
echo "--- pipeline.log"; head -n 2 pipeline.log; sed -n '19,24p;34,36p' pipeline.log; grep '^\[truth\]' pipeline.log || true; tail -n 3 pipeline.log
for f in status-1 status-2; do
  echo "--- $f.log"; grep -E "rebuilt|^\[watch\]" $f.log || true; sed -n '24,26p' $f.log; tail -n 2 $f.log
done
echo "--- reprocess.log"; head -n 2 reprocess.log; tail -n 2 reprocess.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" status-1.log | cut -c1-200 || true
# Skip the first 10 s (warm-up); a shared CI runner may blip, so require 95% of the remaining seconds with live
# stage changes to pass.
steady=$(grep -E '^\[status-1\] stage changes +[1-9][0-9,]*/s' status-1.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live stage changes: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
for f in status-1 status-2; do
  gaps=$(grep -Eo 'gaps [0-9]+' $f.log | tail -n 1 | cut -d' ' -f2 || true)
  echo "$f.log sequence gaps: ${gaps:-none}"
  [ "${gaps:-1}" = "0" ] || { echo "sequence gap"; exit 1; }
done
grep -Eq 'rebuilt the status view from the log in [0-9.]+s: [1-9][0-9,]* episodes' status-2.log \
  || { echo "the late status replica did not rebuild its view from the log"; exit 1; }
grep -q "\[reprocess\] done" reprocess.log || { echo "the reprocessing neighbour did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
s1 = [l for l in open("status-1.log") if re.search(r"^\[status-1\] stage changes +[1-9][0-9,]*/s", l)]
t = s1[10:]
p99 = nums(r"  p99 +([0-9.]+)ms", t)
print("status-1 steady-state medians over %d s: stage changes/s %.0f  stage-change-to-view p50 %.1f ms  p99 %.1f ms  "
      "p99.9 %.1f ms  worst-second p99 %.1f ms  peak stage changes/s %.0f" % (
      len(t), st.median(nums(r"changes +([0-9,]+)/s", t)), st.median(nums(r"p50 +([0-9.]+)ms", t)), st.median(p99),
      st.median(nums(r"p99.9 +([0-9.]+)ms", t)), max(p99), max(nums(r"changes +([0-9,]+)/s", t))))
rec = nums(r"receipts [0-9,]+/s \(p99 ([0-9.]+)ms", t)
within = nums(r"within 1 s ([0-9.]+)%", s1)
print("receipts: median per-second p99 %.1f ms, worst %.1f ms; lowest share within 1 s in any second %.1f%%" % (
      st.median(rec), max(rec), min(within)))
if len(p99) > 52:   # the reprocessing log runs from about second 15 to 55 of the pipeline
    print("p99, median per second: before the reprocessing log %.1f ms (s 10-14), with 2 MB/s next door %.1f ms (s 20-50)" % (
        st.median(p99[0:4]), st.median(p99[8:38])))
ans = sum(nums(r"re-uploads answered ([0-9,]+)", s1))
wrong = sum(nums(r"wrong ([0-9]+)", s1))
rep = nums(r"repeats dropped ([0-9,]+)", s1)
print("re-uploads answered 'already received' from the view: %d, wrong answers %d; repeats dropped by event number %d" % (
      ans, wrong, rep[-1] if rep else 0))
pl = open("pipeline.log").read()
truth_x = [float(x) for x in re.findall(r"backlog crossed [0-9.]+s at ([0-9.]+)", pl)]
truth_c = [float(x) for x in re.findall(r"backlog back under [0-9.]+s at ([0-9.]+)", pl)]
w1 = open("status-1.log").read()
alerts = [(float(c), float(r)) for c, r in re.findall(r"crossed at ([0-9.]+), raised at ([0-9.]+)", w1)]
clears = [float(x) for x in re.findall(r"\[watch\] clear: .* at ([0-9.]+), alert open", w1)]
waits = nums(r"waiting new [0-9,]+ \(oldest ([0-9.]+)s\)", open("pipeline.log").readlines())
print("truth: oldest new episode waited up to %.1f s; backlog crossed the line %d time(s), back under %d time(s)" % (
      max(waits or [0]), len(truth_x), len(truth_c)))
for (c, r), x in zip(alerts, truth_x):
    print("watch: alert raised %.0f ms after the real crossing (crossing read off the view %+.0f ms from the truth)" % (
          (r - x) * 1000, (c - x) * 1000))
for c, x in zip(clears, truth_c):
    print("watch: cleared %+.0f ms from the real fall under half the line" % ((c - x) * 1000))
w2 = open("status-2.log").read()
m = re.search(r"rebuilt the status view from the log in ([0-9.]+)s: ([0-9,]+) episodes, ([0-9,]+) stage changes; waiting new ([0-9,]+)", w2)
if m:
    print("late replica rebuilt its view in %s s: %s episodes from %s stage changes, %s new episodes waiting" % m.groups())
print("late replica: " + ("alerted on the backlog it found" if "already over when this replica came up" in w2 else "no alert"))
bad = []
if wrong:
    bad.append("a re-upload was answered wrongly")
if ans == 0:
    bad.append("no re-upload was answered")
if not rep or rep[-1] == 0:
    bad.append("no repeated stage change was dropped: de-duplication did not run")
if min(within or [0]) < 99.9:
    bad.append("a receipt took longer than 1 s to reach the view")
if not truth_x:
    bad.append("the backlog never crossed the line: the test did not exercise the watch")
if len(alerts) != len(truth_x) or any(abs(r - x) > 1.0 or r < x - 0.05 for (c, r), x in zip(alerts, truth_x)):
    bad.append("watch alerts did not match the real crossings (missed, extra, or later than 1 s)")
if truth_c and len(clears) < len(truth_c):
    bad.append("the watch did not clear after the backlog drained")
if "already over when this replica came up" not in w2:
    bad.append("the late replica did not alert on the backlog it found")
if bad:
    raise SystemExit("; ".join(bad))
PY
echo "Kit OK on Apache Iggy 0.9.0"
