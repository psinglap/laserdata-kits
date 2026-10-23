#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the surge window lane at small scale for about a minute, with two
# regions' surge workers reading the same lane, a stadium demand spike, an analytics stream next to it, and the
# second region's worker stopped mid-window and restarted. Pass only if the lane reports PASS, every closed
# window matches the marketplace's own count exactly, no event arrives after its window closed, both regions
# agree on every window, the restarted worker rebuilds its windows from the log, and multipliers flow.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On shared runners that burst stalled every
# partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py marketplace.py surge_window.py analytics_flood.py kafka_bridge.py
# The workers start first and create the topics; the marketplace and the neighbour follow.
# 2 cities x 400 hexagons, 45 demand events per hexagon per minute (600/s) and 800 drivers per city reporting
# every 4 s (about 240 available/s): about 840 events/s; 10 s windows with 0.5 s grace. A stadium spike
# multiplies demand 30x in 19 hexagons from second 30 for 15 s; 2 MB/s of app analytics from second 20.
# region-b is stopped at second 38 and started again at second 45: it rebuilds its windows from the log.
W="--window-s 10 --grace-s 0.5"
timeout 84 python -u surge_window.py --region region-a --primary $W > region-a.log 2>&1 &
timeout 38 python -u surge_window.py --region region-b $W > region-b.log 2>&1 &
sleep 3
timeout 75 python -u marketplace.py --cities 2 --hexes 400 --drivers 800 $W \
  --spike 30 --spike-at 30 --spike-for 15 > marketplace.log 2>&1 &
timeout 70 python -u analytics_flood.py --mbps 2 --at 20 --duration 35 > analytics.log 2>&1 &
sleep 42
timeout 36 python -u surge_window.py --region region-b $W > region-b2.log 2>&1 &
wait || true
echo "--- marketplace.log"; head -n 2 marketplace.log; tail -n 3 marketplace.log
echo "--- region-a.log";    head -n 1 region-a.log; tail -n 6 region-a.log
echo "--- region-b.log (stopped at ~38 s)"; tail -n 3 region-b.log
echo "--- region-b2.log (restarted)"; head -n 4 region-b2.log; tail -n 4 region-b2.log
echo "--- analytics.log";   head -n 2 analytics.log; tail -n 2 analytics.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" region-a.log region-b.log region-b2.log | cut -c1-150 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[region-a\] +[1-9][0-9,]* events/s' region-a.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "region-a steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
def lane(f, region):
    return [l for l in open(f) if re.search(r"\[%s\] +[1-9][0-9,]* events/s" % region, l)]
p = lane("region-a.log", "region-a")[10:]
print("region-a steady-state medians: events/s %.0f  event-to-window p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
a = [l for l in open("region-a.log") if re.search(r"\[region-a\] +[0-9,]+ events/s", l)]
p99, rate = nums(r"p99 +([0-9.]+)ms", a), nums(r"\] +([0-9,]+) events/s", a)
if len(p99) > 45:
    print("region-a p99, median per second: before the spike and the neighbour %.1f ms (s 6-16), with 2 MB/s of "
          "analytics %.1f ms (s 19-26), stadium spike plus analytics %.1f ms at %.0f events/s (s 29-41)" % (
        st.median(p99[6:17]), st.median(p99[19:27]), st.median(p99[29:42]), st.median(rate[29:42])))
pat = (r"exact ([0-9]+)/([0-9]+)  late ([0-9]+)  rebuilt ([0-9]+)  regions agree ([0-9]+)/([0-9]+)")
def last_window(f):
    u = [l for l in open(f) if l.startswith("[window]")]
    if not u:
        raise SystemExit("no window lines in " + f)
    m = re.search(pat, u[-1])
    if not m:
        raise SystemExit("could not read the last window line of " + f)
    return u, [int(x) for x in m.groups()]
u, (exact, checked, late, rebuilt, agree, compared) = last_window("region-a.log")
print(u[-1].strip())
# the last event of each full window (the first and last windows of the run are partial), once per window
seen = {}
for l in u:
    m = re.search(r"closed ([0-9]+) .*last event ([+-][0-9,]+) ms after its end", l)
    if m:
        seen[int(m.group(1))] = int(m.group(2).replace(",", ""))
tails = [seen[w] for w in sorted(seen)[1:-1]] or [0]
mx = nums(r"\(max ([0-9.]+)x\)", u)
print("region-a: windows exact %d of %d; late events %d; regions agree on %d of %d windows; last event of a "
      "window arrived %+d to %+d ms after its end (grace 500 ms); max multiplier %.1fx" % (
        exact, checked, late, agree, compared, min(tails), max(tails), max(mx)))
if checked < 4 or exact != checked:
    raise SystemExit("windows did not match the marketplace's own count")
if late:
    raise SystemExit("events arrived after their window closed")
if compared < 4 or agree != compared:
    raise SystemExit("the two regions did not agree on every window")
if max(mx) < 2.0:
    raise SystemExit("the stadium spike did not raise a multiplier")
s = nums(r"multipliers sent ([0-9,]+)", a)
if not s or sum(s) == 0:
    raise SystemExit("no multipliers were published")
print("multipliers published: %d over the run" % sum(s))
b2 = open("region-b2.log").read()
if "catching up: replayed" not in b2 or not re.search(r"catching up: replayed [1-9]", b2):
    raise SystemExit("the restarted worker did not replay the log")
u2, (exact2, checked2, late2, rebuilt2, agree2, compared2) = last_window("region-b2.log")
print(u2[-1].strip())
p2 = lane("region-b2.log", "region-b")
f2 = sum("FAIL (" in l for l in p2)
print("region-b restarted: windows rebuilt from the log %d; exact %d of %d; late %d; regions agree %d of %d; "
      "%d live seconds after catching up, %d FAIL" % (rebuilt2, exact2, checked2, late2, agree2, compared2, len(p2), f2))
if rebuilt2 < 3 or checked2 < 4 or exact2 != checked2 or late2 or agree2 != compared2:
    raise SystemExit("the restarted worker did not rebuild its windows exactly")
if len(p2) < 15 or f2 * 100 > len(p2) * 5:
    raise SystemExit("the restarted worker did not keep up after catching up")
e = [l for l in open("analytics.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("analytics neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[analytics\] done" analytics.log || { echo "the analytics neighbour did not run"; exit 1; }
grep -q "(spike)" marketplace.log || { echo "the stadium spike did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
