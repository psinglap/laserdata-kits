#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the key shutoff loop at small scale for about a minute, with a
# request-log stream next to it, and pass only if the request lane reports PASS, stolen and leaked keys are
# disabled, and the disables reach every request router before the keys' next requests.
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
python -m py_compile common.py enforcement.py routers.py log_flood.py kafka_bridge.py
# The enforcement service starts first and creates the topics; the routers and the neighbour follow.
# 3,000 requests a second over 20,000 keys and 4 routers; a hijack every 2 s, a leak every 3 s, 200 abusive
# requests a second each; 2 MB/s of request logs from second 20.
timeout 78 python -u enforcement.py > enforcement.log 2>&1 &
sleep 3
timeout 72 python -u routers.py --routers 4 --keys 20000 --requests-per-s 3000 > routers.log 2>&1 &
timeout 70 python -u log_flood.py --mbps 2 --at 20 --duration 35 > logs.log 2>&1 &
wait || true
echo "--- routers.log";     head -n 3 routers.log; tail -n 4 routers.log
echo "--- enforcement.log"; tail -n 6 enforcement.log
echo "--- logs.log";        head -n 2 logs.log; tail -n 2 logs.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" enforcement.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[enforcement\] +[1-9][0-9,]* requests/s' enforcement.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the shutoff checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
e = [l for l in open("enforcement.log") if re.search(r"\[enforcement\] +[1-9][0-9,]* requests/s", l)][10:]
print("steady-state medians: requests/s %.0f  request-to-enforcement p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, e)) for q in (r"\] +([0-9,]+) requests/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("enforcement.log") if "request-to-enforcement" in l])
if len(lane) > 50:
    print("request-to-enforcement p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of logs %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
hij, leak = sum(nums(r"hijack ([0-9,]+),", e)), sum(nums(r"leak ([0-9,]+)\)", e))
print("disables sent in steady state: hijack %d  leak %d" % (hij, leak))
if hij < 10 or leak < 5:
    raise SystemExit("too few keys disabled to judge the loop")
lk = nums(r"report-to-disable max ([0-9.]+)ms", e)
if lk:
    print("leak report to disable: median of per-second max %.1f ms (%d s with reports)" % (st.median(lk), len(lk)))
r = [l for l in open("routers.log") if re.search(r"\[routers\] +[1-9][0-9,]* requests/s", l)][10:]
after, after_usd = sum(nums(r"served after the disable ([0-9,]+)", r)), sum(nums(r"served after the disable [0-9,]+ \(\$([0-9.]+)", r))
would, would_usd = sum(nums(r"it would be ([0-9,]+),", r)), sum(nums(r"it would be [0-9,]+, \$([0-9.]+)\)", r))
false = sum(nums(r"false disables ([0-9]+)", r))
print("requests on a disabled key: served after the disable %d ($%.2f)  at a 1,000 ms lane: %d ($%.2f)  (%.2f%%)" % (
    after, after_usd, would, would_usd, 100 * after / max(would, 1)))
win = nums(r"hijack detected in ([0-9.]+)ms", r)
if win:
    print("hijack: first abusive request to disable, median %.0f ms" % win[-1])
d = [l for l in r if re.search(r"disable-to-routers n [1-9]", l)]
if not d:
    raise SystemExit("no disables reached the routers")
print("disable-to-routers (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with disables, %d s)" % (
    st.median(nums(r"routers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
if would < 1000:
    raise SystemExit("too few attempts after a disable to judge the loop")
if after * 100 > would:
    raise SystemExit("routers served more than 1% of what a 1 s lane would have served on disabled keys")
if false:
    raise SystemExit("a key nobody abused was disabled")
g = [l for l in open("logs.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if g:
    print("request-log neighbour: %d s, median %.1f MB/s on its own topic" % (len(g), st.median(nums(r"~([0-9.]+) MB/s", g))))
PY
grep -q "\[logs\] done" logs.log || { echo "the request-log neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
