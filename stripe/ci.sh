#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the card testing control loop at small scale for about a minute,
# with an API request-log stream next to it, and pass only if the attempt lane reports PASS, attacks are put
# under controls and eased, flash sales are left alone, and the controls reach every payments API node before
# the testers' next attempts.
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
python -m py_compile common.py detector.py api_nodes.py log_flood.py kafka_bridge.py
# The detector starts first and creates the topics; the API nodes and the neighbour follow.
# 2,500 attempts a second over 20,000 businesses and 4 nodes; a card testing attack every 3 s at 150 attempts
# a second for 8 s, a flash sale every 5 s at 200 attempts a second for 4 s; 2 MB/s of request logs from second 20.
timeout 78 python -u detector.py > detector.log 2>&1 &
sleep 3
timeout 72 python -u api_nodes.py --nodes 4 --businesses 20000 --attempts-per-s 2500 > nodes.log 2>&1 &
timeout 70 python -u log_flood.py --mbps 2 --at 20 --duration 35 > logs.log 2>&1 &
wait || true
echo "--- nodes.log";    head -n 3 nodes.log; tail -n 4 nodes.log
echo "--- detector.log"; tail -n 6 detector.log
echo "--- logs.log";     head -n 2 logs.log; tail -n 2 logs.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" detector.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[detector\] +[1-9][0-9,]* attempts/s' detector.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the card testing checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
e = [l for l in open("detector.log") if re.search(r"\[detector\] +[1-9][0-9,]* attempts/s", l)][10:]
print("steady-state medians: attempts/s %.0f  attempt-to-detector p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, e)) for q in (r"\] +([0-9,]+) attempts/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("detector.log") if "attempt-to-detector" in l])
if len(lane) > 50:
    print("attempt-to-detector p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of logs %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
ctl, eased = sum(nums(r"\(controls ([0-9,]+),", e)), sum(nums(r"eased ([0-9,]+)\)", e))
print("changes sent in steady state: controls %d  eased %d" % (ctl, eased))
if ctl < 10 or eased < 5:
    raise SystemExit("too few attacks put under controls and eased to judge the loop")
r = [l for l in open("nodes.log") if re.search(r"\[nodes\] +[1-9][0-9,]* attempts/s", l)][10:]
after, after_cards = sum(nums(r"after controls ([0-9,]+)", r)), sum(nums(r"after controls [0-9,]+ \(([0-9,]+) cards", r))
would, would_cards = sum(nums(r"it would be ([0-9,]+),", r)), sum(nums(r"it would be [0-9,]+, ([0-9,]+) cards", r))
blocked, challenged = sum(nums(r"blocked ([0-9,]+)", r)), sum(nums(r"buyers challenged ([0-9,]+)", r))
false, sales, flagged = sum(nums(r"false controls ([0-9]+)", r)), sum(nums(r"flash sales ([0-9]+),", r)), sum(nums(r"flagged ([0-9]+)", r))
print("tester attempts blocked by a challenge %d  real buyers challenged %d" % (blocked, challenged))
print("tester attempts that reached an issuer after controls: %d (%d stolen cards confirmed)  at a 1,000 ms lane: %d (%d cards)  (%.2f%%)" % (
    after, after_cards, would, would_cards, 100 * after / max(would, 1)))
win = nums(r"attack detected in ([0-9.]+)ms", r)
if win:
    print("attack: first tester attempt to controls published, median %.0f ms" % win[-1])
d = [l for l in r if re.search(r"control-to-nodes n [1-9]", l)]
if not d:
    raise SystemExit("no controls reached the API nodes")
print("control-to-nodes (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with changes, %d s)" % (
    st.median(nums(r"nodes n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
print("flash sales: %d, put under controls %d; false controls %d" % (sales, flagged, false))
if would < 1000:
    raise SystemExit("too few tester attempts after controls to judge the loop")
if after * 100 > would:
    raise SystemExit("nodes let through more than 1% of what a 1 s lane would have let through")
if sales < 5:
    raise SystemExit("too few flash sales to judge the detector")
if false:
    raise SystemExit("a business nobody attacked was put under controls")
g = [l for l in open("logs.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if g:
    print("request-log neighbour: %d s, median %.1f MB/s on its own topic" % (len(g), st.median(nums(r"~([0-9.]+) MB/s", g))))
PY
grep -q "\[logs\] done" logs.log || { echo "the request-log neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
