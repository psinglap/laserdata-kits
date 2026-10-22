#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the captain location lane at small scale for about a minute,
# and pass only if the lane reports PASS, ride offers flow end to end, and offers get checked against the truth.
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
python -m py_compile common.py captains.py match.py comms_burst.py kafka_bridge.py
timeout 80 python -u captains.py --captains 10000 --every 5 --rpm 600 --wait-s 10 40 > captains.log 2>&1 &
sleep 5
# A 20x campaign burst of comms events next door from second 30 to 50 of the matcher's run, on the same broker.
timeout 65 python -u comms_burst.py --mbps 0.05 --x 20 --at 30 --for 20 > comms.log 2>&1 &
timeout 70 python -u match.py > match.log 2>&1 || true
wait || true
echo "--- captains.log"; grep -v "offers checked 0 " captains.log | tail -n 8
echo "--- match.log"; tail -n 12 match.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" match.log | cut -c1-120 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[match\] +[1-9][0-9,]* pings/s' match.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if [ "$live" -lt 30 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then echo "lane check failed"; exit 1; fi
# Steady-state summary: median of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
t = [l for l in open("match.log") if re.search(r"\[match\] +[1-9][0-9,]* pings/s", l)][10:]
f = [l for l in open("captains.log") if re.search(r"offers checked [1-9]", l)][10:]
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
if t:
    print("steady-state medians: pings/s %.0f  ping-to-index p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  request-to-offer p99 %.1f ms" % tuple(
        st.median(num(p, t)) for p in (r"\] +([0-9,]+) pings/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms",
                                       r"request-to-offer p99 +([0-9.]+)ms")))
lane = [float(re.search(r"p99 +([0-9.]+)ms", l).group(1)) for l in open("match.log") if l.startswith("[match]") and "pings/s" in l]
if len(lane) > 50:
    print("ping-to-index p99, median per second: before the burst %.1f ms (s 12-28), during the 20x burst %.1f ms (s 33-49)" % (
        st.median(lane[12:29]), st.median(lane[33:50])))
if f:
    behind2, off = st.median(num(r"two or more behind +([0-9.]+)%", f)), st.median(num(r"off >250 m +([0-9.]+)%", f))
    print("offers on the latest ping %.2f%%  one behind %.2f%%  two or more behind %.3f%%  pickup distance off >250 m %.3f%%  (median per second, %d s)" % (
        st.median(num(r"latest ping +([0-9.]+)%", f)), st.median(num(r"one ping behind +([0-9.]+)%", f)), behind2, off, len(f)))
    if behind2 > 1.0 or off > 1.0:
        raise SystemExit("too many offers computed on an old captain position")
PY
grep -Eq "offers [1-9]" match.log || { echo "no offers published"; exit 1; }
grep -Eq "offers checked [1-9]" captains.log || { echo "no offers checked against the truth"; exit 1; }
grep -Eq "pickup-track [1-9]" match.log || { echo "no pickup tracking published"; exit 1; }
echo "--- comms.log"; grep -E "burst 20x" comms.log | head -n 3; tail -n 2 comms.log
grep -q "burst 20x" comms.log || { echo "campaign burst did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
