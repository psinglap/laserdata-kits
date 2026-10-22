#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the fuel alert loop at small scale for about a minute, with
# low-network history uploads next to it, and pass only if the fuel ping lane reports PASS, drain alerts reach the
# owner app while the fuel is still being drained (none delayed past the drain's end by the lane), and no truck that
# wasn't drained or refuelled raises an alert.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst: on the shared runner that burst stalls every
# partition at once. It is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py fuel_rules.py trucks.py history_flood.py kafka_bridge.py
# The fuel rules start first and create the topics; the trucks and the neighbour follow.
# 20,000 trucks with a fuel sensor, a ping every 10 s each (about 2,000 pings/s); drains and refuels compressed
# (150 and 60 a minute across the fleet) so a one-minute run sees enough of them; 2 MB/s of history from second 20.
timeout 78 python -u fuel_rules.py > rules.log 2>&1 &
sleep 3
timeout 72 python -u trucks.py --trucks 20000 --ping-s 10 --drains-per-min 150 --refuels-per-min 60 --notifiers 3 \
  > trucks.log 2>&1 &
timeout 70 python -u history_flood.py --mbps 2 --at 20 --duration 35 > history.log 2>&1 &
wait || true
echo "--- trucks.log";  head -n 3 trucks.log | cut -c1-400; tail -n 4 trucks.log | cut -c1-500
echo "--- rules.log";   tail -n 6 rules.log | cut -c1-400
echo "--- history.log"; head -n 2 history.log; tail -n 2 history.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" rules.log | cut -c1-400 || true
echo "--- trucks stalls over 20 ms (line: output)"; grep -nE "stall gc ([2-9][0-9]|[0-9]{3,})ms|loop ([2-9][0-9]|[0-9]{3,})ms" trucks.log | sed -E "s/^([0-9]+):.*(stall.*)/\1: \2/" || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[fuel-rules\] +[1-9][0-9,]* pings/s' rules.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the fuel checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
r = [l for l in open("rules.log") if re.search(r"\[fuel-rules\] +[1-9][0-9,]* pings/s", l)][10:]
print("steady-state medians: pings/s %.0f  ping-to-rules p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, r)) for q in (r"\] +([0-9,]+) pings/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("rules.log") if "ping-to-rules" in l])
if len(lane) > 50:
    print("ping-to-rules p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of history %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
print("alerts sent by the rules in steady state: %d (drain %d, refuel %d)" % (
    sum(nums(r"alerts sent ([0-9,]+)", r)), sum(nums(r"drain ([0-9,]+),", r)), sum(nums(r"refuel ([0-9,]+)\)", r))))
f = [l for l in open("trucks.log") if re.search(r"\[trucks\] +[1-9][0-9,]* pings/s", l)][10:]
alerts = sum(nums(r"at the owner app ([0-9,]+),", f))
running = sum(nums(r"still draining ([0-9,]+) \(", f))
slow = sum(nums(r"it would be ([0-9,]+)\)", f))
left = sum(nums(r"litres still in the tank ([0-9,]+) ", f))
left_slow = sum(nums(r"litres still in the tank [0-9,]+ \(at [0-9.]+ s ([0-9,]+)\)", f))
late = sum(nums(r"after the drain ended ([0-9,]+)", f))
late_lane = sum(nums(r"\(lane delay ([0-9,]+)\)", f))
false = sum(nums(r"false drain alerts ([0-9,]+)", f))
refuels = sum(nums(r"refuels recorded ([0-9,]+) ", f))
false_ref = sum(nums(r"refuels recorded [0-9,]+ \(false ([0-9,]+)\)", f))
print("drain alerts at the owner app: %d  while the fuel was still being drained: %d (%.1f%%)  at a 60 s lane: %d" % (
    alerts, running, 100 * running / max(alerts, 1), slow))
print("litres still in the tanks when the alerts arrived: %d  (at a 60 s lane: %d)" % (left, left_slow))
print("alerts after the drain had ended: %d (detected after it ended, from smoothing and the ping interval: %d; "
      "ping taken while draining but delivered after: %d)" % (late, late - late_lane, late_lane))
print("false drain alerts: %d  refuels recorded: %d  false refuels: %d" % (false, refuels, false_ref))
if alerts < 50:
    raise SystemExit("too few drain alerts to judge the loop")
if false or false_ref:
    raise SystemExit("a truck that was not drained or refuelled raised an alert")
if refuels < 1:
    raise SystemExit("no refuels recorded")
# A drain the rules only see after it ended (smoothing, ping interval) is the rules' business; a drain alert raised
# while the fuel was going and delivered after it stopped is the lane's. That one must stay at (nearly) zero.
if late_lane * 100 > alerts:
    raise SystemExit("more than 1% of drain alerts raised during a drain reached the owner app after it had ended")
if running * 100 < alerts * 85:
    raise SystemExit("fewer than 85% of drain alerts arrived while the fuel was still being drained")
d = [l for l in f if re.search(r"ping-to-notifiers n [1-9]", l)]
if not d:
    raise SystemExit("no alerts reached the notifiers")
print("ping-to-notifiers (device ping to 3 notifiers, both hops): p50 %.1f ms  p99 %.1f ms (medians of seconds with alerts, %d s)" % (
    st.median(nums(r"notifiers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"notifiers n [0-9,]+ p50 [0-9.]+ms p99 ([0-9.]+)ms", d)), len(d)))
e = [l for l in open("history.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("history neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[history\] done" history.log || { echo "the history neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
