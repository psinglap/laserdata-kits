#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run Tubi's creative readiness lane at small scale for about a minute,
# with an ad event log next to it, and pass only if the creative-seen lane reports PASS, the registry starts one
# job per new creative and absorbs repeat sightings, verdicts reach every ad server node, the yield comparison
# prints, and a node added mid-run rebuilds its verdict table from the log.
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
python -m py_compile common.py creative_registry.py ad_servers.py ad_event_log.py kafka_bridge.py
# The registry starts first, creates the topics and publishes its snapshot of 3,000 approved creatives; the ad
# servers and the neighbour follow. 2,000 requests a second over 4 nodes, 20 new creatives a second and a wave of
# 200 every 40 s, a 3 s job per creative, verdicts compared with a 20 s refresh, a fifth node joins at 45 s,
# 2 MB/s of ad event log from second 20.
timeout 78 python -u creative_registry.py --catalog 3000 --process-s 3 > registry.log 2>&1 &
sleep 3
timeout 72 python -u ad_servers.py --nodes 4 --requests-per-s 2000 --catalog 3000 --new-per-s 20 --flight-every 40 \
  --flight-n 200 --process-s 3 --refresh-s 20 --join-at 45 > ad_servers.log 2>&1 &
timeout 70 python -u ad_event_log.py --mbps 2 --at 20 --duration 35 > ad_events.log 2>&1 &
wait || true
echo "--- ad_servers.log"; head -n 4 ad_servers.log; tail -n 6 ad_servers.log
echo "--- registry.log";   head -n 2 registry.log; tail -n 6 registry.log
echo "--- ad_events.log";  head -n 2 ad_events.log; tail -n 2 ad_events.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" registry.log | cut -c1-160 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[registry\] +[1-9][0-9,]* sightings/s' registry.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the business checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
f = [l for l in open("registry.log") if re.search(r"\[registry\] +[1-9][0-9,]* sightings/s", l)][10:]
print("steady-state medians: sightings/s %.0f  seen-to-registry p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, f)) for q in (r"\] +([0-9,]+) sightings/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("registry.log") if "sightings/s" in l])
if len(lane) > 50:
    print("seen-to-registry p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of ad event log %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
jobs, absorbed = sum(nums(r"jobs started ([0-9,]+)", f)), sum(nums(r"absorbed ([0-9,]+)", f))
ready, held = sum(nums(r"verdicts ready ([0-9,]+)", f)), sum(nums(r"held ([0-9,]+)", f))
print("steady state: jobs started %d, repeat sightings absorbed %d (%.0f%% of sightings), verdicts ready %d, held %d" % (
    jobs, absorbed, 100 * absorbed / max(1, jobs + absorbed), ready, held))
if jobs < 300 or absorbed < 1 or ready < 200:
    raise SystemExit("the registry did not process new creatives")
d = [l for l in open("ad_servers.log") if re.search(r"verdict-to-ad-servers n [1-9]", l)][5:]
if len(d) < 30:
    raise SystemExit("verdicts did not reach the ad server nodes")
p50, p99 = st.median(nums(r"ad-servers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d))
print("verdict-to-ad-servers (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with verdicts, %d s)" % (p50, p99, len(d)))
if p99 >= 50:
    raise SystemExit("verdict-to-ad-servers p99 over 50 ms")
y = [l for l in open("ad_servers.log") if l.startswith("[yield]")]
if len(y) < 30:
    raise SystemExit("the yield comparison did not print")
for name, q in (("live verdicts", r"live verdicts: bids held ([0-9.]+)%"), ("refreshed every 20 s", r"20 s: bids held ([0-9.]+)%")):
    print("%s, median per second over %d s: bids held %.1f%%  pod fill %.1f%%  RPM $%.2f" % (name, len(y),
          st.median(nums(q, y)), st.median(nums(q.replace("bids held ([0-9.]+)%", r"bids held [0-9.]+%  pod fill ([0-9.]+)%"), y)),
          st.median(nums(q.replace("bids held ([0-9.]+)%", r"bids held [0-9.]+%  pod fill [0-9.]+%  RPM \$([0-9.]+)"), y))))
print("yield at the end of the run:", re.search(r"run total .*", y[-1]).group(0))
n = [l for l in open("ad_servers.log") if l.startswith("[new]")]
if len(n) < 20:
    raise SystemExit("new creatives did not become servable on every node")
print("new creatives, first bid to servable on all 4 nodes, median of per-second p50s over %d s: live %.1f s | 20 s refresh %.1f s" % (
    len(n), st.median(nums(r"everywhere p50 ([0-9.]+)s", n)), st.median(nums(r"refresh p50 ([0-9.]+)s", n))))
j = [l for l in open("ad_servers.log") if l.startswith("[join]")]
if not j:
    raise SystemExit("the late node did not rebuild its verdict table from the log")
print(j[0].strip())
e = [l for l in open("ad_events.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("ad-event-log neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[ad-events\] done" ad_events.log || { echo "the ad-event-log neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
