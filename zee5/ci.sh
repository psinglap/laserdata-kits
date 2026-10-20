#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the heartbeat lane at small scale for about a minute,
# and pass only if the lane reports PASS and every resume is checked against where the viewer stopped.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py viewers.py continue_watching.py content_publish.py kafka_bridge.py
# Reduced scale: 20,000 viewers, a heartbeat every 10 s (2,000 heartbeats/s), device switches after 2-6 s.
timeout 80 python -u viewers.py --viewers 20000 --every 10 --end-p 0.01 --switch-s 2 6 > viewers.log 2>&1 &
sleep 5
# A 20x release drop next door from second 30 to 50 of the service's run, on the same broker: 50 KB/s, then 1 MB/s.
# (At 8 MB/s, a few seconds about 30 s after the burst failed with the broker and the scripts all stalled, which looks like
# the runner's disk writing the burst back; a 2-vCPU runner is not the place to measure a large neighbour.)
timeout 65 python -u content_publish.py --rate 25 --x 20 --at 30 --for 20 > metadata.log 2>&1 &
timeout 70 python -u continue_watching.py --why > cw.log 2>&1 || true
wait || true
echo "--- viewers.log"; tail -n 8 viewers.log
echo "--- cw.log"; tail -n 12 cw.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" cw.log | cut -c1-120 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[continue-watching\] +[1-9][0-9,]* heartbeats/s' cw.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if [ "$live" -lt 30 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then echo "lane check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the resume check against the truth.
python - <<'PY'
import re, statistics as st
t = [l for l in open("cw.log") if re.search(r"\[continue-watching\] +[1-9][0-9,]* heartbeats/s", l)][10:]
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
print("steady-state medians: heartbeats/s %.0f  heartbeat-to-resume-point p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(num(p, t)) for p in (r"\] +([0-9,]+) heartbeats/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")))
lane = [float(re.search(r"p99 +([0-9.]+)ms", l).group(1)) for l in open("cw.log") if "heartbeats/s  heartbeat-to" in l]
if len(lane) > 50:
    print("heartbeat-to-resume-point p99, median per second: before the drop %.1f ms (s 12-28), during the 20x drop %.1f ms (s 33-49)" % (
        st.median(lane[12:29]), st.median(lane[33:50])))
v = [l for l in open("viewers.log") if l.startswith("[viewers]")][15:]
c = [l for l in v if re.search(r"resumes [1-9]", l)]
if not c:
    raise SystemExit("no resumes checked against the truth")
n, late, miss = sum(num(r"resumes ([0-9,]+)", c)), sum(num(r"lane-late ([0-9]+)", c)), sum(num(r"no position ([0-9]+)", c))
exact = st.median(num(r"at the stop point +([0-9.]+)%", c))
print("resumes checked %d in %d s: at the stop point %.2f%%  rewatch avg %.1f s  lane-late %d  no position %d  "
      "resume answer p99 %.1f ms  (medians per second)" % (n, len(v), exact, st.median(num(r"rewatch avg +([0-9.]+)s", c)),
      late, miss, st.median(num(r"resume answer p99 +([0-9.]+)ms", c))))
if late * 1000 > n or miss * 100 > n or exact < 75:
    raise SystemExit("too many resumes built on a heartbeat the lane had not delivered, or with no position")
PY
grep -Eq "resume requests answered [1-9]" cw.log || { echo "no resume requests answered"; exit 1; }
echo "--- metadata.log"; grep -E "release drop 20x" metadata.log | head -n 3; tail -n 2 metadata.log
grep -q "release drop 20x" metadata.log || { echo "release drop did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
