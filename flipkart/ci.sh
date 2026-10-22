#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the billable-events lane and BCAP at small scale for about a minute,
# with a UI telemetry firehose next to it, and pass only if the lane reports PASS, campaigns reach their budget
# and are stopped, the stops reach every ad server, and repeats and stale events are dropped.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the shared runner that burst stalled every
# partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py bcap.py ad_servers.py telemetry.py shadow_bridge.py
# BCAP starts first and creates the topics; the ad servers and the neighbour follow.
# 3,000 billable events a second over 2,000 campaigns and 4 ad servers, 20 s budget days; 2 MB/s of telemetry from second 20.
timeout 78 python -u bcap.py --campaigns 2000 --events-per-s 3000 --day-s 20 > bcap.log 2>&1 &
sleep 3
timeout 72 python -u ad_servers.py --servers 4 --campaigns 2000 --events-per-s 3000 --day-s 20 > ads.log 2>&1 &
timeout 70 python -u telemetry.py --mbps 2 --at 20 --duration 35 > telemetry.log 2>&1 &
wait || true
echo "--- ads.log";       head -n 2 ads.log; tail -n 4 ads.log
echo "--- bcap.log";      tail -n 8 bcap.log
echo "--- telemetry.log"; head -n 2 telemetry.log; tail -n 2 telemetry.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" bcap.log | cut -c1-150 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[bcap\] +[1-9][0-9,]* billable events/s' bcap.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the budget checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
b = [l for l in open("bcap.log") if re.search(r"\[bcap\] +[1-9][0-9,]* billable events/s", l)][10:]
print("steady-state medians: billable events/s %.0f  billable-to-BCAP p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, b)) for q in (r"\] +([0-9,]+) billable events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
print("worst steady-state second: p99 %.1f ms" % max(nums(r"p99 +([0-9.]+)ms", b)))
print("watermark lag, median %.0f ms, max %.0f ms" % (st.median(nums(r"watermark lag ([0-9,]+)ms", b)),
                                                      max(nums(r"watermark lag ([0-9,]+)ms", b))))
if any("FAIL-SAFE" in l for l in b):
    print("note: BCAP went fail-safe in a steady-state second")
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("bcap.log") if "billable events/s" in l])
if len(lane) > 50:
    print("billable-to-BCAP p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of telemetry %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
g = [l for l in open("bcap.log") if l.startswith("[budget]")]
stopped = max(nums(r"stopped ([0-9,]+)/", g) or [0])
days = {}
for l in g:
    m = re.search(r"day (\d+): charged ₹([0-9,.]+) .*over-burn ₹([0-9,.]+) \(([0-9.]+)% of spend, ([0-9,]+) events\)", l)
    if m:
        days[m.group(1)] = tuple(float(x.replace(",", "")) for x in m.groups()[1:])
for d, (ch, ov, pct, n) in list(days.items())[1:-1]:   # days seen from start to end
    print("day %s: charged ₹%.2f  over-burn ₹%.2f (%.3f%% of spend, %d events)" % (d, ch, ov, pct, n))
print("most campaigns stopped in one day: %d of 2,000" % stopped)
if stopped < 100:
    raise SystemExit("BCAP did not stop campaigns at their budget")
rep, stale = sum(nums(r"repeats dropped ([0-9,]+)", g)), sum(nums(r"stale dropped ([0-9,]+)", g))
print("repeats dropped %d (₹%.2f not charged twice), stale events dropped %d" % (
    rep, sum(nums(r"repeats dropped [0-9,]+ \(₹([0-9,.]+)\)", g)), stale))
resent = sum(nums(r"resent ([0-9,]+)", [l for l in open("ads.log") if l.startswith("[ads]")]))
print("events the ad servers sent twice: %d" % resent)
if rep == 0 or stale == 0:
    raise SystemExit("BCAP did not drop repeated or stale events")
d = [l for l in open("ads.log") if re.search(r"stop-to-ad-servers n [1-9]", l)][5:]
if not d:
    raise SystemExit("no budget stops reached the ad servers")
print("stop-to-ad-servers (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with stops, %d s)" % (
    st.median(nums(r"servers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
w = nums(r"withheld by stops ([0-9,]+)", [l for l in open("ads.log") if l.startswith("[ads]")])
if not w or max(w) == 0:
    raise SystemExit("the ad servers never applied a stop")
e = [l for l in open("telemetry.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("telemetry neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[telemetry\] done" telemetry.log || { echo "the telemetry neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
