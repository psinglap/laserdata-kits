#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the provider health loop at small scale for about a minute, with
# planted provider incidents and a call-log flood next to it, and pass only if the outcomes lane reports PASS,
# the evaluator marks the failing providers degraded and recovered, and the verdicts reach every call worker
# before the next turns.
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
python -m py_compile common.py health.py call_workers.py call_log_flood.py msk_bridge.py
# The evaluator starts first and creates the topics; the call workers and the neighbour follow.
# 6,000 live calls (about 1,000 turns and 3,000 provider outcomes a second) over 4 call workers; a provider
# incident every 10 s from second 15; 2 MB/s of call logs from second 20.
timeout 78 python -u health.py > health.log 2>&1 &
sleep 3
timeout 72 python -u call_workers.py --workers 4 --calls 6000 > calls.log 2>&1 &
timeout 70 python -u call_log_flood.py --mbps 2 --at 20 --duration 35 > logs.log 2>&1 &
wait || true
echo "--- calls.log";  head -n 3 calls.log; grep "\] incident " calls.log || true; tail -n 3 calls.log
echo "--- health.log"; grep -- "->" health.log || true; tail -n 4 health.log
echo "--- logs.log";   head -n 2 logs.log; tail -n 2 logs.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" health.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[health\] +[1-9][0-9,]* outcomes/s' health.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the verdict checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
h = [l for l in open("health.log") if re.search(r"\[health\] +[1-9][0-9,]* outcomes/s", l)][10:]
print("steady-state medians: outcomes/s %.0f  outcome-to-evaluator p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, h)) for q in (r"\] +([0-9,]+) outcomes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("health.log") if "outcome-to-evaluator" in l])
if len(lane) > 50:
    print("outcome-to-evaluator p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of call logs %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
deg, ok = sum(nums(r"degraded ([0-9,]+),", h)), sum(nums(r", ok ([0-9,]+)\)", h))
print("verdicts in steady state: degraded %d, ok %d" % (deg, ok))
if deg < 3 or ok < 2:
    raise SystemExit("the evaluator did not mark the planted incidents degraded and recovered")
inc = [l for l in open("calls.log") if "onset to verdict" in l]
if len(inc) < 3:
    raise SystemExit("fewer than 3 incidents reached every call worker")
print("incidents seen by every worker: %d  onset to verdict median %.0f ms  verdict to all workers median %.1f ms, max %.1f ms" % (
    len(inc), st.median(nums(r"onset to verdict ([0-9.]+) ms", inc)), st.median(nums(r"workers ([0-9.]+) ms,", inc)),
    max(nums(r"workers ([0-9.]+) ms,", inc))))
c = [l for l in open("calls.log") if re.search(r"\[calls\] +[1-9][0-9,]* turns/s", l)][10:]
hit = sum(nums(r"failing provider ([0-9,]+),", c))
blind = sum(nums(r"verdict was out ([0-9,]+)", c))
slow = sum(nums(r"it would be ([0-9,]+)\)", c))
print("turns on a failing provider: %d  after its verdict was out: %d  at a 1,000 ms lane: %d (%.1f%% of that)" % (
    hit, blind, slow, 100 * blind / max(slow, 1)))
if slow < 100:
    raise SystemExit("too few turns after a degraded verdict to judge the loop")
d = [l for l in c if re.search(r"verdict-to-workers n [1-9]", l)]
if not d:
    raise SystemExit("no verdicts reached the call workers")
print("verdict-to-workers (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with verdicts, %d s)" % (
    st.median(nums(r"workers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
if blind * 100 > slow * 2:
    raise SystemExit("more than 2% of the turns a 1 s lane lets through still went out after the verdict")
e = [l for l in open("logs.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("call-log neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[logs\] done" logs.log || { echo "the call-log neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
