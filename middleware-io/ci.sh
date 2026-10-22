#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the OpsAI command lane at small scale for about a minute, with a
# customer log burst next to it and one partition's agents disconnected for 30 s, and pass only if the lane
# reports PASS, no command finishes after its deadline, the disconnected agents' stale requests are refused,
# and every request is answered.
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
python -m py_compile common.py agents.py opsai.py log_burst.py pulsar_bridge.py
# The agents start first and create the topics; OpsAI and the neighbour follow.
# 80 investigations over 400 clusters; partition 15's agents drop off at 25 s for 30 s; 2 MB/s of logs from 20 s.
timeout 82 python -u agents.py --offline-partition 15 --offline-at 25 --offline-for 30 > agents.log 2>&1 &
sleep 3
timeout 74 python -u opsai.py --clusters 400 --investigations 80 --write-timeout 20 > opsai.log 2>&1 &
timeout 70 python -u log_burst.py --mbps 2 --at 20 --duration 30 > logs.log 2>&1 &
wait || true
echo "--- opsai.log";  head -n 1 opsai.log; tail -n 6 opsai.log
echo "--- agents.log"; head -n 1 agents.log; grep "partition 15" agents.log || true; tail -n 4 agents.log
echo "--- logs.log";   head -n 2 logs.log; tail -n 2 logs.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" opsai.log | cut -c1-160 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[opsai\] +[1-9][0-9,]* replies/s' opsai.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the deadline checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
o = [l for l in open("opsai.log") if re.search(r"\[opsai\] +[1-9][0-9,]* replies/s", l)][10:]
print("steady-state medians: replies/s %.0f  request-to-agent p50 %.1f ms p99 %.1f ms  reply-to-opsai p50 %.1f ms p99 %.1f ms" % tuple(
    st.median(nums(q, o)) for q in (r"\] +([0-9,]+) replies/s", r"agent p50 +([0-9.]+)ms", r"agent p50 +[0-9.]+ms p99 +([0-9.]+)ms",
                                    r"opsai p50 +([0-9.]+)ms", r"opsai p50 +[0-9.]+ms p99 +([0-9.]+)ms")))
print("lane round trip (both hops): p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, o)) for q in (r"trip p50 +([0-9.]+)ms", r"trip p50 +[0-9.]+ms p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
lane = nums(r"trip p50 +[0-9.]+ms p99 +([0-9.]+)ms", [l for l in open("opsai.log") if "replies/s" in l])
if len(lane) > 45:
    print("lane round trip p99, median per second: before the log burst %.1f ms (s 8-16), with 2 MB/s of logs %.1f ms (s 20-44)" % (
        st.median(lane[8:17]), st.median(lane[20:45])))
tot = [l for l in open("opsai.log") if "[opsai] totals:" in l][-1]
f = {k: int(v.replace(",", "")) for k, v in re.findall(r"(sent|in time|expired, not run|stopped at deadline|gave up|dropped|finished|on-call) ([0-9,]+)", tot)}
print("commands sent %d  answered in time %d  stopped at deadline %d  expired, not run %d  answered after OpsAI gave up %d  duplicates dropped %d  investigations finished %d, to on-call %d" % (
    f["sent"], f["in time"], f["stopped at deadline"], f["expired, not run"], f["gave up"], f["dropped"], f["finished"], f["on-call"]))
missing = max(nums(r"never answered ([0-9,]+)", open("opsai.log")) or [0])
ag = [l for l in open("agents.log") if "[agents] totals:" in l][-1]
blind = int(re.search(r"would have run ([0-9,]+) stale", ag).group(1).replace(",", ""))
blind_w = int(re.search(r"([0-9,]+) of them writes", ag).group(1).replace(",", ""))
stale = int(re.search(r"finished after deadline ([0-9,]+)", ag).group(1).replace(",", ""))
print("agents: finished after deadline %d  stale commands refused %d (a deadline-blind agent would have run them, %d writes)" % (stale, blind, blind_w))
if f["finished"] < 50:
    raise SystemExit("too few investigations finished to judge the lane")
if stale:
    raise SystemExit("a command finished after its deadline")
if blind < 1:
    raise SystemExit("the disconnected agents found no expired requests: the deadline check was not exercised")
if missing:
    raise SystemExit("%d requests were never answered" % missing)
if f["gave up"] > f["expired, not run"] + 5:
    raise SystemExit("replies that ran and arrived after OpsAI gave up: %d late vs %d expired" % (f["gave up"], f["expired, not run"]))
e = [l for l in open("logs.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("log burst neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[logs\] done" logs.log || { echo "the log burst neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
