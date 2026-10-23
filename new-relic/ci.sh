#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the time-wave watermark lane at small scale for about a minute,
# with a quarter of the Kafka partitions in two cells stalling for 20 s, repeated tallies, and a cell's backlog
# draining next door. Pass only if the lane reports PASS, no watermark ever runs ahead of the data, every stalled
# cell pipeline is flagged behind (and nothing else is), and event flow alone is shown closing windows early.
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
python -m py_compile common.py waves.py watermark.py backlog_drain.py kafka_bridge.py
# The watermark service starts first and creates the topics; the waves and the neighbour follow.
# 12 cells x 4 source pipelines x 32 Kafka partitions, a generation a second (1,536 synthetic points/s, tallied
# every 20 ms); 2% of tallies written twice; a quarter of the Kafka partitions in 2 cells stop delivering from
# second 27 to 47; a generation 3 s overdue flags its cell pipeline; event flow's delay is 2 windows;
# 3 MB/s of backlog drain next door from second 15.
timeout 80 python -u watermark.py --overdue-s 3 > watermark.log 2>&1 &
sleep 3
timeout 74 python -u waves.py --cells 12 --pipelines 4 --kafka-partitions 32 --wave-s 1 --ef-delay-s 2 \
  --overdue-s 3 --dup-rate 0.02 --stall-cells 2 --stall-share 0.25 --stall-at 27 --stall-for 20 --drain-s 2 \
  > waves.log 2>&1 &
timeout 72 python -u backlog_drain.py --mbps 3 --at 15 --duration 40 > drain.log 2>&1 &
wait || true
echo "--- waves.log";     head -n 3 waves.log; sed -n '26,34p;46,52p' waves.log; tail -n 3 waves.log
echo "--- watermark.log"; head -n 3 watermark.log; sed -n '30,36p;46,52p' watermark.log; tail -n 4 watermark.log
echo "--- drain.log";     head -n 2 drain.log; tail -n 2 drain.log
echo "--- flags and early watermarks"; grep -E "flagged behind|wrong behind|early watermark|never flagged" waves.log | head -n 20 || true
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" watermark.log | cut -c1-300 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[watermark\] +[1-9][0-9,]* msgs/s' watermark.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the watermark checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
s = [l for l in open("watermark.log") if re.search(r"\[watermark\] +[1-9][0-9,]* msgs/s", l)][10:]
print("steady-state medians: msgs/s %.0f  wave-to-watermark p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, s)) for q in (r"\] +([0-9,]+) msgs/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("watermark.log") if "wave-to-watermark" in l])
if len(lane) > 50:
    print("wave-to-watermark p99, median per second: before the drain %.1f ms (s 3-12), with 3 MB/s of drain "
          "next door %.1f ms (s 18-50)" % (st.median(lane[3:13]), st.median(lane[18:51])))
g = [l for l in open("waves.log") if l.startswith("[waves]") and "msgs/s" in l]
tot = lambda pat: int(sum(nums(pat, g)))
ef_closed, ef_partial = tot(r"windows closed ([0-9,]+),"), tot(r"on partial data ([0-9,]+)")
ef_missing = tot(r"points missing ([0-9,]+)\)")
gated, early = tot(r"gated by watermark: closed ([0-9,]+)"), tot(r"early ([0-9,]+),")
right, wrong, cleared = tot(r"behind flags right ([0-9,]+)"), tot(r"wrong ([0-9,]+)  cleared"), tot(r"cleared ([0-9,]+)")
repeats = tot(r"tallies written twice ([0-9,]+)")
missed = int(max(nums(r"missed ([0-9]+) ", g) or [0]))
detect = max(nums(r"detected max ([0-9.]+)s", g) or [0])
stalled = int(max(nums(r"\(stalled ([0-9]+)\)", g) or [0]))
waits = nums(r"after data complete p50 ([0-9]+)ms", g)
wmax = max(nums(r"after data complete p50 [0-9]+ms max ([0-9]+)ms", g[3:]) or [0])
print("event flow alone: %d windows closed, %d on partial data (%d points still in Kafka)" % (ef_closed, ef_partial, ef_missing))
print("gated by the watermark: %d windows closed, %d early; closed after data complete: median %d ms, max %d ms" % (
    gated, early, st.median(waits or [0]), wmax))
print("behind flags: %d of %d stalled cell pipelines flagged, latest %.2f s after the stall began; wrong %d; "
      "missed %d; cleared %d; tallies written twice %d, all ignored" % (right, stalled, detect, wrong, missed, cleared, repeats))
if stalled < 4 or ef_partial < 20 or gated < 1000 or repeats < 20:
    raise SystemExit("too little stall, traffic or repetition to judge the lane")
if early or wrong or missed or right < stalled:
    raise SystemExit("a watermark or behind flag did not match the truth")
if detect > 3 + 1.5:
    raise SystemExit("a stall was flagged later than --overdue-s plus 1.5 s")
# Seconds 60-70 of the watermark log: the stall and its drain are over and the waves are still running.
after = nums(r"behind now ([0-9,]+)", [l for l in open("watermark.log") if "behind now" in l][58:68])
if not after or max(after) != 0:
    raise SystemExit("a cell pipeline was still flagged behind after the stall had drained")
e = [l for l in open("drain.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("backlog drain neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[drain\] done" drain.log || { echo "the backlog drain neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
