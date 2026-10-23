#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the cross-shard order group loop at small scale for about a
# minute: four matching engine shards with a 2x burst, the order group tracker, and a trade export next door.
# Pass only if the fills lane holds its 20 ms p99 in 95% of steady-state seconds, groups are triggered across
# shards and reset, and every shard applies the triggers.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which can stall a shared runner's disk and every
# reader with it; that would be the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py engine.py tracker.py trade_export.py kafka_bridge.py
# The tracker starts first and creates the topics; the shards and the neighbour follow.
# 2,000 order groups, 1,200 fills a second over 4 shards with a 2x burst (2,400/s) from second 30 to 45,
# groups reset 3 s after a trigger, 2 MB/s of trade export from second 20 for 35 s.
G=2000; F=1200
timeout 80 python -u tracker.py --groups $G --fills-per-s $F --reset-after-s 3 > tracker.log 2>&1 &
sleep 3
timeout 74 python -u engine.py --groups $G --fills-per-s $F --burst 2 --burst-at 30 --burst-for 15 > engine.log 2>&1 &
timeout 70 python -u trade_export.py --mbps 2 --at 20 --duration 35 > export.log 2>&1 &
wait || true
echo "--- engine.log";  head -n 2 engine.log; grep burst engine.log | sed -n 5p || true; tail -n 3 engine.log
echo "--- tracker.log"; head -n 2 tracker.log; tail -n 6 tracker.log
echo "--- export.log";  head -n 2 export.log; tail -n 2 export.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" tracker.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[tracker\] +[1-9][0-9,]* fills/s' tracker.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the order group checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
t = [l for l in open("tracker.log") if re.search(r"\[tracker\] +[1-9][0-9,]* fills/s", l)][10:]
print("steady-state medians over %d s: fills/s %.0f  fill-to-tracker p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  worst-second p99 %.1f ms" % (
    (len(t),) + tuple(st.median(nums(q, t)) for q in (r"\] +([0-9,]+) fills/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                                      r"p99.9 +([0-9.]+)ms")) + (max(nums(r"p99 +([0-9.]+)ms", t)),)))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("tracker.log") if "fills/s" in l and "catching up" not in l])
if len(lane) > 50:
    print("fill-to-tracker p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of trade export %.1f ms (s 26-50), in the 2x burst %.1f ms (s 32-44)" % (
        st.median(lane[12:23]), st.median(lane[26:51]), st.median(lane[32:45])))
g = [l for l in open("tracker.log") if l.startswith("[groups]")][10:]
trig, cross, reset = (sum(nums(p, g)) for p in (r"triggered ([0-9,]+) \(", r"over the limit ([0-9,]+)\)", r"reset ([0-9,]+)"))
con, past = sum(nums(r"contracts ([0-9,]+)  past", g)), sum(nums(r"past the trigger ([0-9,]+)", g))
inflight, late = sum(nums(r"to the tracker ([0-9,]+)", g)), sum(nums(r"to a shard ([0-9,]+);", g))
print("groups triggered %d (no single shard over the limit: %d, %.0f%%), reset %d" % (trig, cross, 100 * cross / max(trig, 1), reset))
print("contracts %d, past the trigger %d (%.3f%%): in flight to the tracker %d, trigger in flight to a shard %d" % (
    con, past, 100 * past / max(con, 1), inflight, late))
if trig < 50 or cross < 10 or reset < 20:
    raise SystemExit("the tracker did not trigger and reset order groups across shards")
e = [l for l in open("engine.log") if re.search(r"state-to-shards n [1-9]", l)][5:]
if not e:
    raise SystemExit("no order group states reached the shards")
print("state-to-shards (4 shard readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with states, %d s)" % (
    st.median(nums(r"shards n [0-9,]+ p50 ([0-9.]+)ms", e)), st.median(nums(r"p99 ([0-9.]+)ms", e)), len(e)))
r = nums(r"refused, group triggered ([0-9,]+)", [l for l in open("engine.log") if l.startswith("[engine]")])
if not r or max(r) == 0:
    raise SystemExit("the shards never applied a trigger")
print("fills refused on shards because their group was triggered: %d" % sum(r))
x = [l for l in open("export.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if x:
    print("trade-export neighbour: %d s, median %.1f MB/s on its own topic" % (len(x), st.median(nums(r"~([0-9.]+) MB/s", x))))
PY
grep -q "\[trade-export\] done" export.log || { echo "the trade export neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
