#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the destination delivery alert lane at small scale for about a
# minute, with planted destination incidents and an event-archive flood next to it, and pass only if the outcomes
# lane reports PASS, the alert check raises and resolves an alert for each failing workspace destination within
# seconds of its first failure, never alerts on a healthy one, and the alerts reach the alert service.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the shared runner that burst, about 30 s
# after a heavy neighbour starts, stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py alerts.py delivery.py archive_flood.py kafka_bridge.py
# The alert check starts first and creates the topics; the delivery fleet and the neighbour follow.
# 3,000 attempts a second over 3,000 workspace destinations and 4 delivery workers; an incident every 10 s from
# second 15; 2 MB/s of 5 KB event payloads from second 20.
timeout 78 python -u alerts.py > alerts.log 2>&1 &
sleep 3
timeout 72 python -u delivery.py --workers 4 --keys 3000 --rate 3000 > delivery.log 2>&1 &
timeout 70 python -u archive_flood.py --mbps 2 --at 20 --duration 35 > archive.log 2>&1 &
wait || true
echo "--- delivery.log"; head -n 3 delivery.log; grep -E "\] (incident|alert for) " delivery.log || true; tail -n 3 delivery.log
echo "--- alerts.log";   grep -- "->" alerts.log || true; tail -n 4 alerts.log
echo "--- archive.log";  head -n 2 archive.log; tail -n 2 archive.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" alerts.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[alerts\] +[1-9][0-9,]* outcomes/s' alerts.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the alert checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
h = [l for l in open("alerts.log") if re.search(r"\[alerts\] +[1-9][0-9,]* outcomes/s", l)][10:]
print("steady-state medians: outcomes/s %.0f  outcome-to-check p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, h)) for q in (r"\] +([0-9,]+) outcomes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("alerts.log") if "outcome-to-check" in l])
if len(lane) > 50:
    print("outcome-to-check p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of event payloads %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
al, rs = sum(nums(r"\(alert ([0-9,]+),", h)), sum(nums(r"resolved ([0-9,]+)\)", h))
print("alerts in steady state: alert %d, resolved %d" % (al, rs))
if al < 3 or rs < 2:
    raise SystemExit("the alert check did not raise and resolve alerts for the planted incidents")
inc = [l for l in open("delivery.log") if "first failure to alert" in l]
if len(inc) < 3:
    raise SystemExit("fewer than 3 incidents reached the alert service")
det = nums(r"first failure to alert ([0-9,.]+) ms", inc)
print("incidents alerted: %d  first failure to alert median %.0f ms, max %.0f ms  alert to service median %.1f ms  "
      "failed attempts before the alert median %.0f" % (len(inc), st.median(det), max(det),
      st.median(nums(r"alert service ([0-9.]+) ms", inc)), st.median(nums(r"before the alert ([0-9,]+)", inc))))
if st.median(det) > 5000:
    raise SystemExit("alerts took more than 5 s from the first failure (median)")
d = [l for l in open("delivery.log") if re.search(r"\[delivery\] +[1-9][0-9,]* attempts/s", l)][10:]
unplanned = sum(nums(r"no incident ([0-9,]+)\)", d))
print("alerts for a workspace destination with no incident: %d" % unplanned)
if unplanned:
    raise SystemExit("the alert check fired on a healthy workspace destination")
s = [l for l in d if re.search(r"alert-to-service n [1-9]", l)]
if not s:
    raise SystemExit("no alerts reached the alert service")
print("alert-to-service: p50 %.1f ms  p99 %.1f ms (medians of seconds with alerts, %d s)" % (
    st.median(nums(r"service n [0-9,]+ p50 ([0-9.]+)ms", s)), st.median(nums(r"p99 ([0-9.]+)ms", s)), len(s)))
e = [l for l in open("archive.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("event-archive neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[archive\] done" archive.log || { echo "the event-archive neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
