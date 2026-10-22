#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the task progress lane at small scale for about a minute, with a
# conversation-log flood next to it, and pass only if the lane reports PASS, the voice hosts speak updates and
# results, and almost none of the updates a 1 s lane would get wrong are out of date on this one.
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
python -m py_compile common.py reasoning_layer.py voice_layer.py conversation_log_flood.py kafka_bridge.py
# The voice hosts start first and create the topic; the reasoning layer and the neighbour follow.
# 6,000 live calls (about 1,800 task steps a second) over 4 voice hosts; 2 MB/s of conversation logs from second 20.
timeout 78 python -u voice_layer.py --hosts 4 > voice.log 2>&1 &
sleep 3
timeout 72 python -u reasoning_layer.py --calls 6000 > reasoning.log 2>&1 &
timeout 70 python -u conversation_log_flood.py --mbps 2 --at 20 --duration 35 > log.log 2>&1 &
wait || true
echo "--- reasoning.log"; head -n 2 reasoning.log; tail -n 2 reasoning.log
echo "--- voice.log";     head -n 3 voice.log; tail -n 3 voice.log
echo "--- log.log";       head -n 2 log.log; tail -n 2 log.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" voice.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[voice\] +[1-9][0-9,]* task events/s' voice.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the spoken-update checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
v = [l for l in open("voice.log") if re.search(r"\[voice\] +[1-9][0-9,]* task events/s", l)][10:]
print("steady-state medians: task events/s %.0f  event-to-voice p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, v)) for q in (r"\] +([0-9,]+) task events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("voice.log") if "event-to-voice" in l])
if len(lane) > 50:
    print("event-to-voice p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of conversation logs %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
spoken = sum(nums(r"updates spoken ([0-9,]+),", v))
stale = sum(nums(r"out of date ([0-9,]+) ", v))
stale_done = sum(nums(r"after the result ([0-9,]+)\)", v))
slow = sum(nums(r"it would be ([0-9,]+)\)", v))
results = sum(nums(r"results spoken ([0-9,]+)", v))
print("updates spoken: %d  out of date on this lane: %d (still working after the result: %d)  at a 1,000 ms lane: %d (%.1f%% of that)  results spoken: %d" % (
    spoken, stale, stale_done, slow, 100 * stale / max(slow, 1), results))
if spoken < 10000 or results < 1000:
    raise SystemExit("too few spoken updates or results to judge the lane")
if slow < 1000:
    raise SystemExit("too few updates a 1 s lane would get wrong to judge the lane")
if stale * 100 > slow * 3:
    raise SystemExit("more than 3% of the updates a 1 s lane gets wrong were out of date on this lane")
r = [l for l in open("reasoning.log") if re.search(r"\] +[1-9][0-9,]* task events/s", l)]
print("reasoning layer: %d s, median %.0f task events/s, %.0f results ready/s" % (
    len(r), st.median(nums(r"\] +([0-9,]+) task events/s", r)), st.median(nums(r"results ready ([0-9,]+)", r))))
e = [l for l in open("log.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("conversation-log neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[log\] done" log.log || { echo "the conversation-log neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
