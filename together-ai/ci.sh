#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the GPU node health loop at small scale for about a minute, with
# planted node faults and a GPU telemetry flood next to it, and pass only if the node-signals lane reports PASS,
# the checker puts the faulted nodes on hold and clears them after repair, and the holds reach every placement
# scheduler before their next placements.
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
python -m py_compile common.py health_checker.py fleet.py telemetry_flood.py k8s_bridge.py
# The checker starts first and creates the topics; the fleet and the neighbour follow.
# 2,000 GPU nodes sampling once a second (2,000 samples/s), 4 placement schedulers placing 100 replicas and jobs
# a second on 64 nodes with free GPUs; a node fault every 10 s from second 15, each lasting 20 s; 2 MB/s of GPU
# telemetry from second 20.
timeout 78 python -u health_checker.py > checker.log 2>&1 &
sleep 3
timeout 72 python -u fleet.py --nodes 2000 --schedulers 4 --placements 100 > fleet.log 2>&1 &
timeout 70 python -u telemetry_flood.py --mbps 2 --nodes 2000 --at 20 --duration 35 > telemetry.log 2>&1 &
wait || true
echo "--- fleet.log";     head -n 3 fleet.log; grep "\] incident " fleet.log || true; tail -n 3 fleet.log
echo "--- checker.log";   grep -- "->" checker.log || true; tail -n 4 checker.log
echo "--- telemetry.log"; head -n 2 telemetry.log; tail -n 2 telemetry.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" checker.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[checker\] +[1-9][0-9,]* samples/s' checker.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the hold checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
h = [l for l in open("checker.log") if re.search(r"\[checker\] +[1-9][0-9,]* samples/s", l)][10:]
print("steady-state medians: samples/s %.0f  sample-to-checker p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, h)) for q in (r"\] +([0-9,]+) samples/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("checker.log") if "sample-to-checker" in l])
if len(lane) > 50:
    print("sample-to-checker p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of telemetry %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
hold, ok = sum(nums(r"\(hold ([0-9,]+),", h)), sum(nums(r", ok ([0-9,]+)\)", h))
print("decisions in steady state: hold %d, ok %d" % (hold, ok))
if hold < 3 or ok < 2:
    raise SystemExit("the checker did not put the planted faults on hold and clear them after repair")
inc = [l for l in open("fleet.log") if "onset to hold" in l]
if len(inc) < 3:
    raise SystemExit("fewer than 3 holds reached every placement scheduler")
print("incidents seen by every scheduler: %d  onset to hold median %.0f ms  hold to all schedulers median %.1f ms, max %.1f ms" % (
    len(inc), st.median(nums(r"onset to hold ([0-9.]+) ms", inc)), st.median(nums(r"schedulers ([0-9.]+) ms,", inc)),
    max(nums(r"schedulers ([0-9.]+) ms,", inc))))
c = [l for l in open("fleet.log") if re.search(r"\[fleet\] +[1-9][0-9,]* samples/s", l)][10:]
hit = sum(nums(r"on a faulted node ([0-9,]+),", c))
blind = sum(nums(r"hold was out ([0-9,]+)", c))
slow = sum(nums(r"it would be ([0-9,]+)\)", c))
print("placements on a faulted node: %d  after its hold was out: %d  at a 15 s path: %d (%.1f%% of that)" % (
    hit, blind, slow, 100 * blind / max(slow, 1)))
if slow < 100:
    raise SystemExit("too few placements after a hold to judge the loop")
d = [l for l in c if re.search(r"hold-to-schedulers n [1-9]", l)]
if not d:
    raise SystemExit("no holds reached the placement schedulers")
print("hold-to-schedulers (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with decisions, %d s)" % (
    st.median(nums(r"schedulers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
if blind * 100 > slow * 2:
    raise SystemExit("more than 2% of the placements a 15 s path lets through still landed after the hold")
e = [l for l in open("telemetry.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("telemetry neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[telemetry\] done" telemetry.log || { echo "the telemetry neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
