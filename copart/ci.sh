#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the VB3 live lot lane at small scale for about a minute: 60 yards x
# 2 virtual sale lanes (120 lots on the block at once, a new lot opening a second after each sale), members on two
# bidder-facing edge nodes bidding up to 6 live bids a second on each lot, a 3 s countdown after every accepted bid,
# 2 MB/s of member activity next door, and one edge node restarted.
# Pass only if each edge holds the 50 ms state-to-edge p99 in 95% of its steady-state seconds, the restarted edge
# rebuilds every lane from the log, lots sell, and live bids placed on a stale screen are fewer than a screen
# refreshed every second would have caused.
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
python -m py_compile common.py auction_block.py bid_edge.py member_activity_flood.py kafka_bridge.py
timeout 82 python -u auction_block.py --yards 60 --lanes-per-yard 2 > auction.log 2>&1 &
sleep 4
timeout 70 python -u member_activity_flood.py --mbps 2 --at 20 --duration 35 > member-activity.log 2>&1 &
timeout 76 python -u bid_edge.py --name edge-1 --edges 2 --bid-rate 6 > edge-1.log 2>&1 &
# edge-2 runs, stops at 24 s and starts again: it must rebuild every lane from the log before it takes a bid.
timeout 24 python -u bid_edge.py --name edge-2 --edges 2 --bid-rate 6 > edge-2.log 2>&1 || true
sleep 1
timeout 51 python -u bid_edge.py --name edge-2 --edges 2 --bid-rate 6 > edge-2-restart.log 2>&1 || true
wait || true
echo "--- auction.log"; head -n 1 auction.log; sed -n '15,17p' auction.log | cut -c1-400; tail -n 2 auction.log | cut -c1-400
echo "--- auction seconds with a tick started or sent over 50 ms late"; grep -nE "(late|send) max ([5-9][0-9]|[0-9]{3,})ms" auction.log | sed -E 's/live bids.*tick/tick/' || true
echo "--- member-activity.log"; head -n 2 member-activity.log; tail -n 2 member-activity.log
for f in edge-1 edge-2 edge-2-restart; do echo "--- $f.log"; head -n 2 $f.log; tail -n 2 $f.log; done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" edge-*.log | sed -E 's/  bids sent.*//' || true
steady() { for f in "$@"; do grep -E '\[edge-[12]\] +[1-9][0-9,]* state changes/s' $f | tail -n +11 || true; done; }
for e in edge-1 edge-2; do
  if [ $e = edge-1 ]; then s=$(steady edge-1.log); else s=$(steady edge-2.log edge-2-restart.log); fi
  # Skip each run's first 10 s (warm-up, or the rebuild after the restart); a shared runner may blip, so 95%.
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$e steady-state seconds with live changes: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
done
grep -Eq "rebuilt every lane from the log in [0-9.]+s: [1-9][0-9,]* lanes" edge-2-restart.log \
  || { echo "the restarted edge did not rebuild every lane from the log"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
live = r"\] +([1-9][0-9,]*) state changes/s"
for n in ("edge-1", "edge-2", "edge-2-restart"):
    p = [l for l in open(f"{n}.log") if re.search(live, l)][10:]
    if p:
        print("%s steady-state medians over %d s: changes/s %.0f  state-to-edge p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  "
              "worst p99 %.1f ms  bid-to-flag p50 %.1f ms p99 %.1f ms" % ((n, len(p)) + tuple(st.median(nums(q, p)) for q in (
                  live, r"edge p50 +([0-9.]+)ms", r"edge p50 +[0-9.]+ms +p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")) +
              (max(nums(r"edge p50 +[0-9.]+ms +p99 +([0-9.]+)ms", p)),) +
              tuple(st.median(nums(q, p) or [0]) for q in (r"flag p50 ([0-9.]+)ms", r"flag p50 [0-9.]+ms p99 ([0-9.]+)ms"))))
for l in open("edge-2-restart.log"):
    if "rebuilt" in l:
        print(l.strip())
# skip the warm-up, and the last seconds after the edges have stopped (no bids)
f = [l for l in open("auction.log") if "live bids" in l and not re.search(r"live bids 0 ", l)][12:]
b, ok, bad, alt, sold = (sum(nums(q, f)) for q in (r"live bids ([0-9,]+) ", r"accepted ([0-9,]+) ", r"stale-screen bids ([0-9,]+) ",
                                                   r"would lose ([0-9,]+) ", r"lots sold ([0-9,]+) "))
ho, hh = (sum(nums(q, f)) for q in (r"\(outbid ([0-9,]+) after", r"\(outbid [0-9,]+ after the hammer ([0-9,]+)\)  a screen"))
ao, ah, am = (sum(nums(q, f)) for q in (r"would lose [0-9,]+ \(outbid ([0-9,]+)", r"would lose [0-9,]+ \(outbid [0-9,]+ after the hammer ([0-9,]+)",
                                        r"missed lot ([0-9,]+)"))
print("after warm-up (%d s): live bids judged %d  accepted %d  lots sold %d | placed on a stale screen %d (%.2f%%: outbid %d, after the hammer %d) | "
      "a screen refreshed every 1 s would lose %d (%.2f%%: outbid %d, after the hammer %d, missed lot %d)" % (
      len(f), b, ok, sold, bad, 100 * bad / max(b, 1), ho, hh, alt, 100 * alt / max(b, 1), ao, ah, am))
bc = [l for l in f if "bid-to-core" in l]
print("bid-to-core, median per second: p50 %.1f ms  p99 %.1f ms" % (
    st.median(nums(r"core p50 ([0-9.]+)ms", bc)), st.median(nums(r"core p50 [0-9.]+ms p99 ([0-9.]+)ms", bc))))
if b < 2000 or ok < 500 or sold < 20:
    raise SystemExit("live bids or sales did not flow")
if bad >= alt:
    raise SystemExit("live bids on a stale screen were as many as a 1 s screen would have caused")
PY
grep -q "\[member-activity\] done" member-activity.log || { echo "the member activity neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
