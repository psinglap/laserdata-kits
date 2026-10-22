#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the quartile tracker lane at small scale for about a minute: FAST
# channels taking staggered ad breaks, every viewer on a channel reaching the break together, 8% of ads left
# partway, 0.5% of segment events sent twice, two trackers each owning half the lane, session records next door
# from second 15, and tracker-2 restarted mid-run (a rolling deploy).
# Pass only if tracker-1 holds the 50 ms p99 budget in 95% of its steady-state seconds, repeats are dropped, the
# restarted tracker rebuilds its fired beacons from the log, and the audit of every beacon finds none missing,
# none fired twice and none fired for a quartile the viewer never reached.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst, about 30 s after a
# heavy neighbour starts, stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py tracker.py stitcher.py session_records.py kinesis_bridge.py
# 5,000 viewers on 12 channels; per channel a break of 3 x 8 s ads (1 s segments, compressed) every 32 s, staggered:
# about 3,700 segment events/s, in bursts as each channel's break begins; 2 MB/s of session records from second 15.
timeout 84 python -u tracker.py --name tracker-1 --index 0 --trackers 2 > tracker-1.log 2>&1 &
# tracker-2 runs, gets SIGTERM at 36 s, and starts again: it must rebuild what it fired and replay what it missed.
( timeout 36 python -u tracker.py --name tracker-2 --index 1 --trackers 2 > tracker-2.log 2>&1 || true
  timeout 46 python -u tracker.py --name tracker-2 --index 1 --trackers 2 > tracker-2-restart.log 2>&1 || true ) &
sleep 3
timeout 80 python -u stitcher.py --viewers 5000 --channels 12 --cycle-s 32 --slots 3 --ad-s 8 --seg-s 1 \
  --ramp-ms 1000 --duration 62 --drain-s 8 > stitcher.log 2>&1 &
timeout 70 python -u session_records.py --mbps 2 --at 15 --duration 40 > sessions.log 2>&1 &
wait || true
for f in tracker-1 tracker-2 tracker-2-restart; do
  echo "--- $f.log"; grep -E "rebuilt|caught up|stopped" $f.log || true; tail -n 2 $f.log
done
echo "--- stitcher.log"; head -n 1 stitcher.log; tail -n 3 stitcher.log
echo "--- sessions.log"; head -n 2 sessions.log; tail -n 2 sessions.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" tracker-1.log | cut -c1-260 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '^\[tracker-1\] +[1-9][0-9,]* segments/s' tracker-1.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "tracker-1 steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
grep -Eq 'rebuilt in [0-9.]+s: [1-9][0-9,]* beacons already fired' tracker-2-restart.log \
  || { echo "the restarted tracker did not rebuild its fired beacons from the log"; exit 1; }
grep -q "\[sessions\] done" sessions.log || { echo "the session-record neighbour did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
t = [l for l in open("tracker-1.log") if re.search(r"^\[tracker-1\] +[1-9][0-9,]* segments/s", l)]
s = t[10:]
print("tracker-1 steady-state medians over %d s: segments/s %.0f  segment-to-tracker p50 %.1f ms  p99 %.1f ms  "
      "p99.9 %.1f ms  worst-second p99 %.1f ms" % (len(s), st.median(nums(r"\] +([0-9,]+) segments/s", s)),
      st.median(nums(r"p50 +([0-9.]+)ms", s)), st.median(nums(r"p99 +([0-9.]+)ms", s)),
      st.median(nums(r"p99.9 +([0-9.]+)ms", s)), max(nums(r"p99 +([0-9.]+)ms", s))))
p99 = nums(r"p99 +([0-9.]+)ms", t)
if len(p99) > 50:   # session records run from about second 15 to 55 of the run, ~12 to 52 of tracker-1's lines
    print("tracker-1 p99, median per second: before the session records %.1f ms (lines 3-10), with 2 MB/s next door %.1f ms (lines 16-48)" % (
        st.median(p99[3:11]), st.median(p99[16:49])))
rep = nums(r"repeats dropped ([0-9,]+)", t)
late = nums(r"fired after the ad ended ([0-9,]+)", t)
print("tracker-1 repeats dropped: %d  beacons fired after the ad ended: %d" % (rep[-1] if rep else 0, late[-1] if late else 0))
if not rep or rep[-1] == 0:
    raise SystemExit("no repeated segment event was dropped: de-duplication did not run")
r = open("tracker-2-restart.log").read()
m = re.search(r"rebuilt in ([0-9.]+)s: ([0-9,]+) beacons", r)
c = re.search(r"caught up in ([0-9.]+)s: ([0-9,]+) segments replayed from the log, ([0-9,]+) beacons fired late", r)
if m and c:
    print("tracker-2 restart: rebuilt %s beacons in %s s, caught up in %s s, %s segments replayed, %s beacons fired late but once" % (
        m.group(2), m.group(1), c.group(1), c.group(2), c.group(3)))
d = [l for l in open("stitcher.log") if re.search(r"^\[stitcher\] +[1-9][0-9,]* segments/s", l)][10:]
if d:
    print("segment-to-beacon-record (both hops, with the audit reader): p50 %.1f ms  p99 %.1f ms (medians over %d s)" % (
        st.median(nums(r"record p50 +([0-9.]+)ms", d)), st.median(nums(r"record p50 +[0-9.]+ms  p99 +([0-9.]+)ms", d)), len(d)))
a = re.search(r"\[audit\] beacons due ([0-9,]+) \(impressions ([0-9,]+)\)  recorded ([0-9,]+)  missing ([0-9,]+)  "
              r"fired twice ([0-9,]+)  not due ([0-9,]+)  segment events sent twice ([0-9,]+)  ads left partway ([0-9,]+)",
              open("stitcher.log").read())
if not a:
    raise SystemExit("the audit did not finish")
due, imps, rec, miss, twice, notdue, dups, left = (int(x.replace(",", "")) for x in a.groups())
print("audit: beacons due %d (impressions %d)  recorded %d  missing %d  fired twice %d  not due %d  "
      "segment events sent twice %d  ads left partway %d" % (due, imps, rec, miss, twice, notdue, dups, left))
if due < 20000:
    raise SystemExit("too few beacons to judge the lane")
if miss or twice or notdue:
    raise SystemExit("a beacon was missed, fired twice, or fired for a quartile never reached")
e = [l for l in open("sessions.log") if re.search(r"\] +[1-9][0-9,]* records/s", l)]
if e:
    print("session-record neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
echo "Kit OK on Apache Iggy 0.9.0"
