#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the driver ping lane at small scale for about a minute,
# and pass only if the lane reports PASS and arrivals flow end to end and get checked against the truth.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst (seen ~33 s in, with
# a busy neighbour writing) stalls every reader for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py drivers.py trips.py log_flood.py pubsub_bridge.py
# Reduced scale: 10,000 drivers, road time 3x so trips reach their fences inside the run; pings stay real-time.
timeout 80 python -u drivers.py --drivers 10000 --every 5 --assign-p 0.05 --pickup-km 0.3 0.8 --drop-km 0.4 1.2 \
  --wait-scale 0.05 --pace 3 > drivers.log 2>&1 &
sleep 5
# A 20x log burst next door from second 30 to 50 of the trip service's run, on the same broker.
timeout 65 python -u log_flood.py --mbps 0.05 --x 20 --at 30 --for 20 > logs.log 2>&1 &
timeout 70 python -u trips.py --why > trips.log 2>&1 || true
wait || true
echo "--- drivers.log"; grep -v "arrivals checked 0 " drivers.log | tail -n 8
echo "--- trips.log"; tail -n 12 trips.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" trips.log | cut -c1-120 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[trips\] +[1-9][0-9,]* pings/s' trips.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if [ "$live" -lt 30 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then echo "lane check failed"; exit 1; fi
# Steady-state summary: median of the per-second figures, for the README.
python - <<'PY'
import re, statistics as st
t = [l for l in open("trips.log") if re.search(r"\[trips\] +[1-9][0-9,]* pings/s", l)][10:]
f = [l for l in open("drivers.log") if l.startswith("[drivers]")][15:]
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
if t:
    print("steady-state medians: pings/s %.0f  ping-to-tracker p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  tracking-link position age p99 %.1f s" % tuple(
        st.median(num(p, t)) for p in (r"\] +([0-9,]+) pings/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms",
                                       r"position age p99 +([0-9.]+)s")))
lane = [float(re.search(r"p99 +([0-9.]+)ms", l).group(1)) for l in open("trips.log") if l.startswith("[trips]") and "pings/s" in l]
if len(lane) > 50:
    print("ping-to-tracker p99, median per second: before the burst %.1f ms (s 12-28), during the 20x burst %.1f ms (s 33-49)" % (
        st.median(lane[12:29]), st.median(lane[33:50])))
checked = num(r"arrivals checked ([0-9,]+)", f)
c = [l for l in f if re.search(r"arrivals checked [1-9]", l)]
if c:
    late2, missed = st.median(num(r"two or more late +([0-9.]+)%", c)), sum(num(r"missed ([0-9,]+)", f))
    print("arrivals checked %d in %d s: on the first ping inside the fence %.2f%%  one ping late %.2f%%  two or more late %.3f%%  "
          "missed %d  ping-to-notice p99 %.1f ms  (medians per second)" % (
        sum(checked), len(f), st.median(num(r"inside the fence +([0-9.]+)%", c)), st.median(num(r"one ping late +([0-9.]+)%", c)),
        late2, missed, st.median(num(r"ping-to-notice p99 +([0-9.]+)ms", c))))
    if late2 > 1.0 or missed * 100 > sum(checked):
        raise SystemExit("too many arrivals fired late or never")
PY
grep -Eq "reached_pickup [1-9]" trips.log || { echo "no reached_pickup arrivals published"; exit 1; }
grep -Eq "reached_drop [1-9]" trips.log || { echo "no reached_drop arrivals published"; exit 1; }
grep -Eq "arrivals checked [1-9]" drivers.log || { echo "no arrivals checked against the truth"; exit 1; }
echo "--- logs.log"; grep -E "burst 20x" logs.log | head -n 3; tail -n 2 logs.log
grep -q "burst 20x" logs.log || { echo "log burst did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
