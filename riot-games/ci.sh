#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the match termination loop at small scale for about a minute, with
# analytics telemetry next to it, and pass only if the combat-action lane reports PASS, aimbots are caught, and
# each termination reaches its game server host before the cheater's next shot lands.
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
python -m py_compile common.py anticheat.py game_servers.py analytics_flood.py msk_bridge.py
# The scorer starts first and creates the topics; the game servers and the neighbour follow.
# 3,000 shots a second over 4 hosts x 75 matches; 40 s matches; an aimbot every 2 s at 8 shots a second;
# 2 MB/s of analytics telemetry from second 20.
timeout 78 python -u anticheat.py > anticheat.log 2>&1 &
sleep 3
timeout 72 python -u game_servers.py --hosts 4 --matches-per-host 75 --actions-per-s 3000 --match-s 40 > servers.log 2>&1 &
timeout 70 python -u analytics_flood.py --mbps 2 --at 20 --duration 35 > analytics.log 2>&1 &
wait || true
echo "--- servers.log";    head -n 3 servers.log; tail -n 4 servers.log
echo "--- anticheat.log";  tail -n 6 anticheat.log
echo "--- analytics.log";  head -n 2 analytics.log; tail -n 2 analytics.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" anticheat.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[anticheat\] +[1-9][0-9,]* shots/s' anticheat.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the termination checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
e = [l for l in open("anticheat.log") if re.search(r"\[anticheat\] +[1-9][0-9,]* shots/s", l)][10:]
print("steady-state medians: shots/s %.0f  action-to-scorer p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, e)) for q in (r"\] +([0-9,]+) shots/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("anticheat.log") if "action-to-scorer" in l])
if len(lane) > 50:
    print("action-to-scorer p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of analytics %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
sent = sum(nums(r"terminations sent ([0-9,]+)", e))
print("terminations sent in steady state: %d" % sent)
if sent < 10:
    raise SystemExit("too few matches terminated to judge the loop")
r = [l for l in open("servers.log") if re.search(r"\[servers\] +[1-9][0-9,]* shots/s", l)][10:]
after, would = sum(nums(r"after the termination ([0-9,]+)", r)), sum(nums(r"it would be ([0-9,]+)\)", r))
with_result, false = sum(nums(r"with a result ([0-9]+)", r)), sum(nums(r"false terminations ([0-9]+)", r))
print("aimbot shots landed after the termination: %d  at a 1,000 ms lane: %d  (%.2f%%)" % (
    after, would, 100 * after / max(would, 1)))
print("cheater matches that still ended with a result: %d" % with_result)
win = nums(r"aimbot caught in ([0-9.]+)ms", r)
if win:
    print("aimbot: first shot to termination published, median %.0f ms" % win[-1])
d = [l for l in r if re.search(r"termination-to-host n [1-9]", l)]
if not d:
    raise SystemExit("no terminations reached the game servers")
print("termination-to-host: p50 %.1f ms  p99 %.1f ms (medians of seconds with terminations, %d s)" % (
    st.median(nums(r"host n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
if would < 100:
    raise SystemExit("too few aimbot shots after a termination to judge the loop")
if after * 100 > would:
    raise SystemExit("hosts let land more than 1% of what a 1 s lane would have")
if with_result:
    raise SystemExit("a cheater's match ended with a result")
if false:
    raise SystemExit("a match nobody cheated in was terminated")
g = [l for l in open("analytics.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if g:
    print("analytics neighbour: %d s, median %.1f MB/s on its own topic" % (len(g), st.median(nums(r"~([0-9.]+) MB/s", g))))
PY
grep -q "\[analytics\] done" analytics.log || { echo "the analytics neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
