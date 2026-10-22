#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run a staggered release at small scale for about a minute, with
# an API-log stream next to it and a defect injected into the canary, and pass only if the outcome lane
# reports PASS, the guard scales the healthy canary up, and it aborts the defective one within seconds.
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
python -m py_compile common.py router.py guard.py api_log_flood.py kafka_bridge.py
# The guard starts first and creates the topics; the router and the neighbour follow.
# Short stages (10 s, 40 canary payments) so the ladder 1% -> 5% -> 10% -> 25% fits in a minute.
timeout 78 python -u guard.py --stage-n 40 --stage-min-s 10 > guard.log 2>&1 &
sleep 3
# 500 payments a second; the canary's card 3DS breaks at second 35; 1 MB/s of API logs from second 20.
timeout 72 python -u router.py --payments-per-s 500 --merchants 500 --bad-at 35 > router.log 2>&1 &
timeout 70 python -u api_log_flood.py --mbps 1 --at 20 --duration 35 > api_logs.log 2>&1 &
wait || true
echo "--- router.log";   head -n 2 router.log; grep -E "applied|defect starts" router.log || true; tail -n 3 router.log
echo "--- guard.log";    grep "\[verdict\]" guard.log || true; tail -n 6 guard.log
echo "--- api_logs.log"; head -n 2 api_logs.log; tail -n 2 api_logs.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" guard.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[guard\] +[1-9][0-9,]* outcomes/s' guard.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary and the release checks: medians of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
h = [l for l in open("guard.log") if re.search(r"\[guard\] +[1-9][0-9,]* outcomes/s", l)][10:]
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
print("steady-state medians: outcomes/s %.0f  outcome-to-guard p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(num(q, h)) for q in (r"\] +([0-9,]+) outcomes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
g, r = open("guard.log").read(), open("router.log").read()
scales = re.findall(r"\[verdict\] SCALE \S+ -> (\d+)%", g)
aborts = re.findall(r"\[verdict\] ABORT .*", g)
print("verdicts: SCALE to %s; ABORT %d" % (", ".join(s + "%" for s in scales) or "none", len(aborts)))
if len(scales) < 2:
    raise SystemExit("the guard did not scale the healthy canary up the ladder")
if len(aborts) != 1 or "PROMOTE" in g:
    raise SystemExit("expected exactly one ABORT and no PROMOTE")
d = re.search(r"decided ([0-9.]+) s after the defect's first failed payment", aborts[0])
a = re.search(r"ABORT applied ([0-9.]+) s after the defect \(verdict reached the rollout in ([0-9.]+) ms\).*"
              r"after the defect ([0-9,]+), failed by the defect ([0-9,]+)", r)
if not d or not a:
    raise SystemExit("the ABORT came before the defect, or never reached the rollout")
print("defect: guard decided %s s after its first failed payment; rollout applied ABORT %s s after the defect "
      "(verdict delivered in %s ms); %s payments on the defective release, %s failed by the defect" % (
      d.group(1), a.group(1), a.group(2), a.group(3), a.group(4)))
if float(a.group(1)) > 10:
    raise SystemExit("the defect served payments for more than 10 s")
e = [l for l in open("api_logs.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("API-log neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(num(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[api-logs\] done" api_logs.log || { echo "the API-log neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
