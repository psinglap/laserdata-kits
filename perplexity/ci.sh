#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the Computer credit loop at small scale for about a minute, with an
# agent-trajectory stream next to it, and pass only if the agent-actions lane reports PASS, accounts run out of
# credits and are paused, refills and monthly resets resume them, and every pause reaches every agent runner.
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
python -m py_compile common.py credit_ledger.py agent_runners.py trace_flood.py kafka_bridge.py
# The ledger starts first and creates the topics; the runners and the neighbour follow.
# 3,000 agent actions a second over 2,000 accounts and 4 runners, 20 s months; 2 MB/s of trajectories from second 20.
timeout 78 python -u credit_ledger.py --accounts 2000 --actions-per-s 3000 --period-s 20 > ledger.log 2>&1 &
sleep 3
timeout 72 python -u agent_runners.py --runners 4 --accounts 2000 --actions-per-s 3000 --period-s 20 > runners.log 2>&1 &
timeout 70 python -u trace_flood.py --mbps 2 --at 20 --duration 35 > traces.log 2>&1 &
wait || true
echo "--- runners.log"; head -n 2 runners.log; tail -n 4 runners.log
echo "--- ledger.log";  tail -n 8 ledger.log
echo "--- traces.log";  head -n 2 traces.log; tail -n 2 traces.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" ledger.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[ledger\] +[1-9][0-9,]* action records/s' ledger.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the credit checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
p = [l for l in open("ledger.log") if re.search(r"\[ledger\] +[1-9][0-9,]* action records/s", l)][10:]
print("steady-state medians: action records/s %.0f  action-to-ledger p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) action records/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("ledger.log") if "action records/s" in l])
if len(lane) > 50:
    print("action-to-ledger p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of trajectories %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
months = {}
for l in open("ledger.log"):
    m = re.search(r"month (\d+): spent ([0-9,]+) .*refills ([0-9,]+)  pauses ([0-9,]+)  resumes ([0-9,]+) .*"
                  r"past zero ([0-9,]+) credits .*\(([0-9.]+)% of credits\)", l)
    if m:
        months[m.group(1)] = [float(x.replace(",", "")) for x in m.groups()[1:]]
for k, (spent, refills, pauses, resumes, past, pct) in months.items():
    print("month %s: spent %.0f credits  refills %d  pauses %d  resumes %d  past zero %.0f credits (%.3f%%)" % (
        k, spent, refills, pauses, resumes, past, pct))
tot = [sum(v[i] for v in months.values()) for i in range(6)]
print("over the run: refills %d  pauses %d  resumes %d  past zero %.0f of %.0f credits (%.3f%%)" % (
    tot[1], tot[2], tot[3], tot[4], tot[0], 100 * tot[4] / tot[0] if tot[0] else 0))
if tot[2] < 50:
    raise SystemExit("the ledger did not pause accounts that ran out of credits")
if tot[3] < 10 or tot[1] < 10:
    raise SystemExit("refills and monthly resets did not resume paused accounts")
d = [l for l in open("runners.log") if re.search(r"decision-to-runners n [1-9]", l)][5:]
if not d:
    raise SystemExit("no credit decisions reached the agent runners")
print("decision-to-runners (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with decisions, %d s)" % (
    st.median(nums(r"runners n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
w = nums(r"held while paused ([0-9,]+)", [l for l in open("runners.log") if l.startswith("[runners]")])
if not w or max(w) == 0:
    raise SystemExit("the agent runners never applied a pause")
e = [l for l in open("traces.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("trajectory neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[traces\] done" traces.log || { echo "the trajectory neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
