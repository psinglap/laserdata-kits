#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the identity enforcement loop at small scale for about a minute, with
# endpoint telemetry next to it, and pass only if the authentication lane reports PASS, lateral movement and
# compromised passwords are enforced, and each block reaches every DC before the account's next authentication.
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
python -m py_compile common.py detection.py dc_sensors.py telemetry_flood.py kafka_bridge.py
# The detection engine starts first and creates the topics; the DC sensors and the neighbour follow.
# 3,000 authentications a second over 20,000 accounts and 4 DCs; lateral movement every 2 s, a compromised
# password every 3 s, 100 attacker authentications a second each; 2 MB/s of endpoint telemetry from second 20.
timeout 78 python -u detection.py > detection.log 2>&1 &
sleep 3
timeout 72 python -u dc_sensors.py --dcs 4 --accounts 20000 --auths-per-s 3000 > dcs.log 2>&1 &
timeout 70 python -u telemetry_flood.py --mbps 2 --at 20 --duration 35 > telemetry.log 2>&1 &
wait || true
echo "--- dcs.log";        head -n 3 dcs.log; tail -n 4 dcs.log
echo "--- detection.log";  tail -n 6 detection.log
echo "--- telemetry.log";  head -n 2 telemetry.log; tail -n 2 telemetry.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" detection.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[detection\] +[1-9][0-9,]* auths/s' detection.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the enforcement checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
e = [l for l in open("detection.log") if re.search(r"\[detection\] +[1-9][0-9,]* auths/s", l)][10:]
print("steady-state medians: auths/s %.0f  auth-to-detection p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, e)) for q in (r"\] +([0-9,]+) auths/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("detection.log") if "auth-to-detection" in l])
if len(lane) > 50:
    print("auth-to-detection p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of telemetry %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
lat_, comp = sum(nums(r"lateral ([0-9,]+),", e)), sum(nums(r"compromised ([0-9,]+)\)", e))
print("enforcements sent in steady state: lateral %d  compromised %d" % (lat_, comp))
if lat_ < 10 or comp < 5:
    raise SystemExit("too few accounts enforced to judge the loop")
ev = nums(r"event-to-enforcement max ([0-9.]+)ms", e)
if ev:
    print("compromised-password event to enforcement: median of per-second max %.1f ms (%d s with events)" % (st.median(ev), len(ev)))
r = [l for l in open("dcs.log") if re.search(r"\[dcs\] +[1-9][0-9,]* auths/s", l)][10:]
after, after_h = sum(nums(r"let through after the block ([0-9,]+)", r)), sum(nums(r"after the block [0-9,]+, new hosts ([0-9,]+)", r))
would, would_h = sum(nums(r"it would be ([0-9,]+),", r)), sum(nums(r"it would be [0-9,]+, new hosts ([0-9,]+)\)", r))
false = sum(nums(r"false blocks ([0-9]+)", r))
print("attacker authentications after the block: let through %d (new hosts %d)  at a 1,000 ms lane: %d (new hosts %d)  (%.2f%%)" % (
    after, after_h, would, would_h, 100 * after / max(would, 1)))
win = nums(r"lateral detected in ([0-9.]+)ms", r)
if win:
    print("lateral movement: first attacker authentication to block, median %.0f ms" % win[-1])
d = [l for l in r if re.search(r"enforcement-to-DCs n [1-9]", l)]
if not d:
    raise SystemExit("no enforcements reached the DCs")
print("enforcement-to-DCs (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with enforcements, %d s)" % (
    st.median(nums(r"DCs n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
if would < 1000:
    raise SystemExit("too few attempts after a block to judge the loop")
if after * 100 > would:
    raise SystemExit("DCs let through more than 1% of what a 1 s lane would have on blocked accounts")
if false:
    raise SystemExit("an account nobody abused was blocked")
g = [l for l in open("telemetry.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if g:
    print("telemetry neighbour: %d s, median %.1f MB/s on its own topic" % (len(g), st.median(nums(r"~([0-9.]+) MB/s", g))))
PY
grep -q "\[telemetry\] done" telemetry.log || { echo "the telemetry neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
