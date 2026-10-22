#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the agent state lane at small scale for about a minute: about
# 1,500 agents in 150 groups, 100 new tickets and chats a second, Freshcaller calls, breaks and tickets assigned by
# hand, a shift change in the busiest group with a 4x spike of its tickets and chats, 2 MB/s of product events next
# door, two routing nodes (each owning half the groups' inquiries), and one of them restarted before the spike.
# Pass only if each routing node holds the 50 ms change-to-view p99 in 95% of its steady-state seconds, the
# restarted one rebuilds its view from the log, and assignments are checked against the agents, with fewer
# misrouted inquiries than a view refreshed every 3 s would have caused.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst stalled every
# partition for 0.2-0.8 s at once while a heavy neighbour ran; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py agents.py router.py product_events_flood.py kafka_bridge.py
timeout 82 python -u agents.py --agents 1500 --groups 150 --inquiries 100 --calls 6 --call-s 20 --breaks 3 \
  --break-s 15 --manual 5 --heartbeat-s 5 --surge 4 --surge-at 40 --surge-for 15 --drain-s 8 > agents.log 2>&1 &
sleep 4
timeout 70 python -u product_events_flood.py --mbps 2 --at 20 --duration 35 > product.log 2>&1 &
timeout 76 python -u router.py --name route-1 --shard 0 --shards 2 > route-1.log 2>&1 &
# route-2 runs, stops at 24 s and starts again: it must rebuild its view from the log before it routes anything.
timeout 24 python -u router.py --name route-2 --shard 1 --shards 2 > route-2.log 2>&1 || true
sleep 1
timeout 51 python -u router.py --name route-2 --shard 1 --shards 2 > route-2-restart.log 2>&1 || true
wait || true
echo "--- agents.log"; head -n 1 agents.log; grep -m 1 "shift change" agents.log || true
grep -E "\(surge\)" agents.log | head -n 3 | cut -c1-420 || true; tail -n 2 agents.log | cut -c1-420
echo "--- agents seconds with a tick started or sent over 50 ms late"; grep -nE "(late|send) max ([5-9][0-9]|[0-9]{3,})ms" agents.log | sed -E 's/inquiries .*tick/tick/' || true
echo "--- product.log"; head -n 2 product.log; tail -n 2 product.log
for f in route-1 route-2 route-2-restart; do echo "--- $f.log"; head -n 2 $f.log; tail -n 2 $f.log; done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" route-*.log | sed -E 's/  routed .*//' || true
steady() { for f in "$@"; do grep -E '\[route-[12]\] +[1-9][0-9,]* changes/s' $f | tail -n +11 || true; done; }
for r in route-1 route-2; do
  if [ $r = route-1 ]; then s=$(steady route-1.log); else s=$(steady route-2.log route-2-restart.log); fi
  # Skip each run's first 10 s (warm-up, or the rebuild after the restart); a shared runner may blip, so 95%.
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$r steady-state seconds with live changes: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
done
grep -Eq "rebuilt the view from the log in [0-9.]+s: [1-9][0-9,]* agents" route-2-restart.log \
  || { echo "the restarted routing node did not rebuild its view from the log"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
live = r"\] +([1-9][0-9,]*) changes/s"
for n in ("route-1", "route-2", "route-2-restart"):
    p = [l for l in open(f"{n}.log") if re.search(live, l)][10:]
    if p:
        print("%s steady-state medians over %d s: changes/s %.0f  change-to-view p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  "
              "worst p99 %.1f ms" % ((n, len(p)) + tuple(st.median(nums(q, p)) for q in (
                  live, r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")) + (max(nums(r"p99 +([0-9.]+)ms", p)),)))
for l in open("route-2-restart.log"):
    if "rebuilt" in l:
        print(l.strip())
# skip the warm-up, and the last seconds after the routing nodes have stopped (nothing routed)
f = [l for l in open("agents.log") if "inquiries" in l and "routed" in l and not re.search(r"\brouted 0 \(", l)][12:]
surge = [l for l in f if "(surge)" in l]
def tot(lines):
    return [sum(nums(q, lines)) for q in (r"routed ([0-9,]+)", r"\) +misrouted ([0-9,]+)",
            r"\) +misrouted [0-9,]+ \(away ([0-9,]+)", r"\) +misrouted [0-9,]+ \(away [0-9,]+ on a call ([0-9,]+)",
            r"\) +misrouted [0-9,]+ \(away [0-9,]+ on a call [0-9,]+ at capacity ([0-9,]+)",
            r"\) no agent ([0-9,]+)  a view", r"would misroute ([0-9,]+)",
            r"would misroute [0-9,]+ \(away ([0-9,]+)", r"would misroute [0-9,]+ \(away [0-9,]+ on a call ([0-9,]+)",
            r"would misroute [0-9,]+ \(away [0-9,]+ on a call [0-9,]+ at capacity ([0-9,]+)",
            r"would misroute .*?\) no agent ([0-9,]+)", r"unrouted ([0-9,]+)")]
for name, lines in (("after warm-up", f), ("during the shift change and spike", surge)):
    c, m, ma, mc, mf, mn, s, sa, sc, sf, sn, un = tot(lines)
    print("%s (%d s): assignments checked %d  misrouted %d (away %d, on a call %d, at capacity %d, %.2f%%) no agent %d  "
          "unrouted %d | a view refreshed every 3 s would misroute %d (away %d, on a call %d, at capacity %d, %.2f%%) "
          "and find no agent for %d (%.2f%%)" % (name, len(lines), c, m, ma, mc, mf, 100 * m / max(c, 1), mn, un,
                                                  s, sa, sc, sf, 100 * s / max(c, 1), sn, 100 * sn / max(c, 1)))
c, m, *_ , s, _, _, _, sn, un = tot(f)
pp = [l for l in f if "inquiry-to-assignment" in l]
print("inquiry-to-assignment, median per second: p50 %.1f ms  p99 %.1f ms" % (
    st.median(nums(r"assignment p50 ([0-9.]+)ms", pp)), st.median(nums(r"assignment p50 [0-9.]+ms p99 ([0-9.]+)ms", pp))))
if c < 2000 or not surge:
    raise SystemExit("assignments or the spike did not flow")
if un * 50 > c:
    raise SystemExit("more than 2% of inquiries were never routed")
if m >= s:
    raise SystemExit("the live view misrouted as many inquiries as the 3 s comparison view")
PY
grep -q "\[product-events\] done" product.log || { echo "the product events neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
