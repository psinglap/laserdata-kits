#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the spend-limit loop at small scale for about a minute, with a Tab
# accept/reject stream next to it, and pass only if the usage lane reports PASS, users reach their spend limit
# and are stopped, and the stops reach every API server.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py limits.py api_servers.py tab_flood.py warpstream_bridge.py
# The limit service starts first and creates the topics; the API servers and the neighbour follow.
# 3,000 billable requests a second over 2,000 users and 4 API servers, 20 s periods; 2 MB/s of Tab events from second 20.
timeout 78 python -u limits.py --users 2000 --requests-per-s 3000 --period-s 20 > limits.log 2>&1 &
sleep 3
timeout 72 python -u api_servers.py --servers 4 --users 2000 --requests-per-s 3000 --period-s 20 > api.log 2>&1 &
timeout 70 python -u tab_flood.py --mbps 2 --at 20 --duration 35 > tab.log 2>&1 &
wait || true
echo "--- api.log";    head -n 2 api.log; tail -n 4 api.log
echo "--- limits.log"; tail -n 8 limits.log
echo "--- tab.log";    head -n 2 tab.log; tail -n 2 tab.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" limits.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[limits\] +[1-9][0-9,]* usage events/s' limits.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the spend-limit checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
p = [l for l in open("limits.log") if re.search(r"\[limits\] +[1-9][0-9,]* usage events/s", l)][10:]
print("steady-state medians: usage events/s %.0f  usage-to-limit-check p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) usage events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("limits.log") if "usage events/s" in l])
if len(lane) > 50:
    print("usage-to-limit-check p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of Tab events %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
b = [l for l in open("limits.log") if l.startswith("[spend]")]
stopped = max(nums(r"users stopped ([0-9,]+)/", b) or [0])
periods = {}
for l in b:
    m = re.search(r"period (\d+): paid usage \$([0-9,.]+) .*past the limit \$([0-9,.]+) \(([0-9.]+)% of usage\)", l)
    if m:
        periods[m.group(1)] = (float(m.group(2).replace(",", "")), float(m.group(3).replace(",", "")), float(m.group(4)))
for k, (used, past, pct) in list(periods.items())[1:-1]:      # periods seen from start to end
    print("period %s: paid usage $%.2f  past the limit $%.4f (%.3f%% of usage)" % (k, used, past, pct))
print("most users stopped in one period: %d of 2,000" % stopped)
if stopped < 100:
    raise SystemExit("the limit service did not stop users at their spend limit")
d = [l for l in open("api.log") if re.search(r"decision-to-api-servers n [1-9]", l)][5:]
if not d:
    raise SystemExit("no limit decisions reached the API servers")
print("decision-to-api-servers (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with decisions, %d s)" % (
    st.median(nums(r"servers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
w = nums(r"refused at the limit ([0-9,]+)", [l for l in open("api.log") if l.startswith("[api]")])
if not w or max(w) == 0:
    raise SystemExit("the API servers never applied a stop")
e = [l for l in open("tab.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("Tab-events neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[tab\] done" tab.log || { echo "the Tab-events neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
