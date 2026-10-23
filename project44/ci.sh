#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the position share lane at small scale for about a minute: 12,000
# connected trucks, a position every 8 s each (about 1,500 positions/s), shipments assigned, opened at the pick-up
# geofence, completed on leaving the delivery geofence or stopped mid-route, and 2 MB/s of a 72-hour replay on a
# neighbouring topic from second 20. Pass only if the share gate holds the 100 ms p99 budget in 95% of its
# steady-state seconds, no position reaches a customer who was not entitled to it, and (almost) none is missed.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On a shared runner that burst stalls every
# partition for a fraction of a second at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py share_gate.py fleet.py replay_flood.py kafka_bridge.py
# The share gate starts first and creates the topics; the fleet and the neighbour follow. Driving is sped up
# (20 km between positions, 1-3 positions to load or unload, 2-8 s parked, windows opening within 4 s) so shipments
# open and close inside the minute; stops are raised to 10% of shipments so there are enough to judge.
timeout 78 python -u share_gate.py > gate.log 2>&1 &
sleep 3
timeout 72 python -u fleet.py --vehicles 12000 --ping-s 8 --km-per-ping 20 --load-pings 1 3 --unload-pings 1 3 \
  --idle-s 2 8 --window-in 0 4 --stop-p 0.1 --notifiers 3 > fleet.log 2>&1 &
timeout 70 python -u replay_flood.py --mbps 2 --at 20 --duration 35 > replay.log 2>&1 &
wait || true
echo "--- fleet.log";  head -n 3 fleet.log; tail -n 4 fleet.log
echo "--- gate.log";   tail -n 6 gate.log
echo "--- replay.log"; head -n 2 replay.log; tail -n 2 replay.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" gate.log | cut -c1-400 || true
echo "--- fleet stalls over 20 ms (line: output)"; grep -nE "stall gc ([2-9][0-9]|[0-9]{3,})ms|loop ([2-9][0-9]|[0-9]{3,})ms" fleet.log | sed -E "s/^([0-9]+):.*(stall.*)/\1: \2/" || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[share-gate\] +[1-9][0-9,]* events/s' gate.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the sharing checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
g = [l for l in open("gate.log") if re.search(r"\[share-gate\] +[1-9][0-9,]* events/s", l)][10:]
print("steady-state medians: events/s %.0f  position-to-gate p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, g)) for q in (r"\] +([0-9,]+) events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("gate.log") if "position-to-gate" in l])
if len(lane) > 50:
    print("position-to-gate p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of replay %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
f = [l for l in open("fleet.log") if re.search(r"\[fleet\] +[1-9][0-9,]* positions/s", l)][10:]
tot = lambda pat: sum(nums(pat, f))
pos, ent, ok = tot(r"\] +([0-9,]+) positions/s"), tot(r"entitled to ([0-9,]+):"), tot(r"right customer ([0-9,]+),")
leaks, missed, late, held = tot(r"leaks ([0-9,]+),"), tot(r"missed ([0-9,]+),"), tot(r"late ([0-9,]+) "), tot(r"withheld ([0-9,]+) ")
started, done, stops, stale = tot(r"tracking started ([0-9,]+)"), tot(r"completed ([0-9,]+)"), tot(r"stopped ([0-9,]+)"), tot(r"on stops: ([0-9,]+) sent")
print("in steady state: positions %d  tracking started %d  completed %d  stopped mid-route %d  positions customers were "
      "entitled to %d: shared with the right customer %d (%.3f%%), leaks %d, missed %d, late %d  withheld %d  "
      "a gate 5 s late on stops would have sent %d positions after the shipment left the truck" % (
    pos, started, done, stops, ent, ok, 100 * ok / max(ent, 1), leaks, missed, late, held, stale))
s = [l for l in f if re.search(r"position-to-customer n [1-9]", l)]
if s:
    print("position-to-customer (position to 3 notifiers, both hops): p50 %.1f ms  p99 %.1f ms (medians of %d s)" % (
        st.median(nums(r"customer n [0-9,]+ p50 ([0-9.]+)ms", s)), st.median(nums(r"ms p99 ([0-9.]+)ms", s)), len(s)))
if started < 100 or done < 100 or stops < 20:
    raise SystemExit("too few shipments opened, completed or stopped to judge the gate")
if leaks > 0:
    raise SystemExit("a position reached a customer who was not entitled to it")
if (missed + late) * 1000 > ent:
    raise SystemExit("more than 0.1% of entitled positions were missed or late")
e = [l for l in open("replay.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("replay neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[replay\] done" replay.log || { echo "the replay neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
