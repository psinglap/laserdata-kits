#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the Roku Exchange live floor lane at small scale for about a minute,
# with an impression log next to it, and pass only if the outcome lane reports PASS, floors move and reach every
# exchange node, the yield comparison prints, and a node added mid-run rebuilds its floor table from the log.
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
python -m py_compile common.py floors.py exchange.py impression_log.py kafka_bridge.py
# The floor service starts first and creates the topics; the exchange nodes and the neighbour follow.
# 3,000 auctions a second over 400 segments and 4 exchange nodes; a live ad break every 40 s for 12 s; floors
# compared with a 30 s refresh; a fifth node joins at 45 s; 2 MB/s of impression log from second 20.
timeout 78 python -u floors.py > floors.log 2>&1 &
sleep 3
timeout 72 python -u exchange.py --nodes 4 --auctions-per-s 3000 --surge-every 40 --surge-for 12 --refresh-s 30 \
  --join-at 45 > exchange.log 2>&1 &
timeout 70 python -u impression_log.py --mbps 2 --at 20 --duration 35 > impressions.log 2>&1 &
wait || true
echo "--- exchange.log";    head -n 4 exchange.log; tail -n 6 exchange.log
echo "--- floors.log";      tail -n 6 floors.log
echo "--- impressions.log"; head -n 2 impressions.log; tail -n 2 impressions.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" floors.log | cut -c1-160 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[floors\] +[1-9][0-9,]* outcomes/s' floors.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the floor checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
f = [l for l in open("floors.log") if re.search(r"\[floors\] +[1-9][0-9,]* outcomes/s", l)][10:]
print("steady-state medians: outcomes/s %.0f  outcome-to-floor-service p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, f)) for q in (r"\] +([0-9,]+) outcomes/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("floors.log") if "outcomes/s" in l])
if len(lane) > 50:
    print("outcome-to-floor-service p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of impression log %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
moved = sum(nums(r"floors moved ([0-9,]+)", f))
priced = max(nums(r"segments priced ([0-9,]+)", f) or [0])
print("floors moved in steady state: %d  segments priced: %d of 400" % (moved, priced))
if moved < 100 or priced < 200:
    raise SystemExit("the floor service did not price the segments")
d = [l for l in open("exchange.log") if re.search(r"floor-to-exchange-nodes n [1-9]", l)][5:]
if not d:
    raise SystemExit("no floors reached the exchange nodes")
print("floor-to-exchange-nodes (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with floor updates, %d s)" % (
    st.median(nums(r"exchange-nodes n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
y = [l for l in open("exchange.log") if l.startswith("[yield]")]
if len(y) < 30:
    raise SystemExit("the yield comparison did not print")
c = [l for l in y if "live floors: RPM $0.00 fill 0.0%" not in l]     # seconds compared (after the first refresh)
if len(c) < 15:
    raise SystemExit("too few seconds compared live floors with refreshed floors")
for name, q in (("live floors", r"live floors: RPM \$([0-9.]+)"), ("floors refreshed every 30 s", r"30 s: RPM \$([0-9.]+)")):
    print("%s, median per second over %d s: RPM $%.2f  fill %.1f%%" % (name, len(c), st.median(nums(q, c)),
          st.median(nums(q.replace("RPM \\$([0-9.]+)", "RPM \\$[0-9.]+ fill ([0-9.]+)%"), c))))
print("yield at the end of the run:", re.search(r"run total .*", y[-1]).group(0))
j = [l for l in open("exchange.log") if l.startswith("[join]")]
if not j:
    raise SystemExit("the late node did not rebuild its floor table from the log")
print(j[0].strip())
e = [l for l in open("impressions.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("impression-log neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[impressions\] done" impressions.log || { echo "the impression-log neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
