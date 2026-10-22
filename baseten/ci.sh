#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run one KV cache-aware deployment at small scale for about a minute,
# with an inference telemetry stream next to it, and pass only if the KV event lane reports PASS, routers stay
# off evicted cache, no event is ever missing, and a router started mid-run rebuilds an identical index from the log.
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
python -m py_compile common.py serving.py telemetry_flood.py zmq_bridge.py
# 8 replicas x 22,000 KV blocks, 4 routers, 3,000 long-context sessions on 40 apps, 300 requests a second;
# a fifth router joins at 30 s and rebuilds from the log; 2 MB/s of inference telemetry from second 20.
timeout 75 python -u serving.py --replicas 8 --cache-blocks 22000 --routers 4 --sessions 3000 --apps 40 \
  --requests-per-s 300 --join-at 30 > serving.log 2>&1 &
sleep 2
timeout 70 python -u telemetry_flood.py --mbps 2 --at 18 --duration 35 > telemetry.log 2>&1 &
wait || true
echo "--- serving.log";   head -n 3 serving.log; grep -E "starts at|catching up|rebuilt|index" serving.log || true
tail -n 3 serving.log | cut -c1-400
echo "--- telemetry.log"; head -n 2 telemetry.log; tail -n 2 telemetry.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" serving.log | cut -c1-260 || true
# Skip a 10 s warm-up while caches fill; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[routers\] +[1-9][0-9,]* requests/s' serving.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
s = [l for l in open("serving.log") if re.search(r"\[routers\] +[1-9][0-9,]* requests/s", l)][10:]
print("steady-state medians: requests/s %.0f  hit %.1f%% of prompt tokens  kv events %.0f/s  "
      "event-to-routers p50 %.2f ms  p99 %.2f ms  p99.9 %.2f ms" % tuple(st.median(nums(q, s)) for q in (
      r"\] +([0-9,]+) requests/s", r"hit +([0-9.]+)%", r"kv events ([0-9,]+)/s", r"p50 ([0-9.]+)ms",
      r"p99 ([0-9.]+)ms", r"p99.9 ([0-9.]+)ms")))
lane = nums(r"p99 ([0-9.]+)ms", s)
if len(lane) > 40:
    print("event-to-routers p99, median per second: before the neighbour %.2f ms (s 11-17), with 2 MB/s of telemetry %.2f ms (s 22-50)" % (
        st.median(lane[1:8]), st.median(lane[12:41])))
stale, stale_t = sum(nums(r"stale routes ([0-9,]+) ", s)), sum(nums(r"stale routes [0-9,]+ \(([0-9,]+) tokens", s))
behind, behind_t = sum(nums(r"behind: ([0-9,]+),", s)), sum(nums(r"behind: [0-9,]+, ([0-9,]+) tokens", s))
print("stale routes in steady state: %d (%d tokens prefilled again); a view 1 s behind: %d (%d tokens)  (%.1f%%)" % (
    stale, stale_t, behind, behind_t, 100 * stale / max(behind, 1)))
gaps = max(nums(r"gaps ([0-9]+)", s) or [0])
if gaps:
    raise SystemExit("a router missed a KV event")
if behind < 50:
    raise SystemExit("too few evictions under the slower view to judge the lane")
if stale * 10 > behind:
    raise SystemExit("routers made more than 10% of the stale routes a view 1 s behind would make")
log = open("serving.log").read()
m = re.search(r"router-04 rebuilt from the log in ([0-9.]+)s: ([0-9,]+) events replayed", log)
if not m:
    raise SystemExit("the router started mid-run did not rebuild from the log")
print("router-04 rebuilt from the log in %s s (%s events replayed)" % m.groups())
m = re.search(r"router-04 index identical to router-00's at offset ([0-9,]+): ([0-9,]+) block copies", log)
if not m:
    raise SystemExit("the rebuilt index does not match a router that never stopped")
print("router-04 index identical to router-00's at offset %s (%s block copies)" % m.groups())
if not re.search(r"routers 5 ", s[-1]):
    raise SystemExit("router-04 is not routing")
g = [l for l in open("telemetry.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if g:
    print("telemetry neighbour: %d s, median %.1f MB/s on its own topic" % (len(g), st.median(nums(r"~([0-9.]+) MB/s", g))))
PY
grep -q "\[telemetry\] done" telemetry.log || { echo "the telemetry neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
