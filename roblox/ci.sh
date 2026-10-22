#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the server state lane at small scale for about a minute: about
# 1,500 game servers of 60 places in 6 edge data centers, 100 players pressing Play a second plus friend joins and
# leaves, a creator update with a 10x thundering herd of joins into the most popular place, 2 MB/s of analytics
# events next door, two matchmakers (each owning half the places' joins), and one of them restarted before the herd.
# Pass only if each matchmaker holds the 50 ms change-to-index p99 in 95% of its steady-state seconds, the
# restarted one rebuilds its index from the log, and placements are checked against the servers, with fewer
# bounced joins than an index refreshed every 3 s would have caused.
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
python -m py_compile common.py fleet.py matchmaker.py analytics_flood.py qaas_bridge.py
timeout 82 python -u fleet.py --places 60 --servers 1500 --regions 6 --joins 100 --direct 40 --leaves 140 \
  --heartbeat-s 5 --herd 10 --herd-at 40 --herd-for 15 > fleet.log 2>&1 &
sleep 4
timeout 70 python -u analytics_flood.py --mbps 2 --at 20 --duration 35 > analytics.log 2>&1 &
timeout 76 python -u matchmaker.py --name mm-1 --shard 0 --shards 2 > mm-1.log 2>&1 &
# mm-2 runs, stops at 24 s and starts again: it must rebuild its index from the log before it takes a join.
timeout 24 python -u matchmaker.py --name mm-2 --shard 1 --shards 2 > mm-2.log 2>&1 || true
sleep 1
timeout 51 python -u matchmaker.py --name mm-2 --shard 1 --shards 2 > mm-2-restart.log 2>&1 || true
wait || true
echo "--- fleet.log"; head -n 1 fleet.log; grep -m 1 "creator update" fleet.log || true
grep -E "\(herd\)" fleet.log | head -n 3 | cut -c1-400 || true; tail -n 2 fleet.log | cut -c1-400
echo "--- fleet seconds with a tick started or sent over 50 ms late"; grep -nE "(late|send) max ([5-9][0-9]|[0-9]{3,})ms" fleet.log | sed -E 's/joins requested.*tick/tick/' || true
echo "--- analytics.log"; head -n 2 analytics.log; tail -n 2 analytics.log
for f in mm-1 mm-2 mm-2-restart; do echo "--- $f.log"; head -n 2 $f.log; tail -n 2 $f.log; done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" mm-*.log | sed -E 's/  joins matched.*//' || true
steady() { for f in "$@"; do grep -E '\[mm-[12]\] +[1-9][0-9,]* changes/s' $f | tail -n +11 || true; done; }
for mm in mm-1 mm-2; do
  if [ $mm = mm-1 ]; then s=$(steady mm-1.log); else s=$(steady mm-2.log mm-2-restart.log); fi
  # Skip each run's first 10 s (warm-up, or the rebuild after the restart); a shared runner may blip, so 95%.
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$mm steady-state seconds with live changes: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
done
grep -Eq "rebuilt the index from the log in [0-9.]+s: [1-9][0-9,]* servers" mm-2-restart.log \
  || { echo "the restarted matchmaker did not rebuild its index from the log"; exit 1; }
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
for n in ("mm-1", "mm-2", "mm-2-restart"):
    p = [l for l in open(f"{n}.log") if re.search(live, l)][10:]
    if p:
        print("%s steady-state medians over %d s: changes/s %.0f  change-to-index p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  "
              "worst p99 %.1f ms" % ((n, len(p)) + tuple(st.median(nums(q, p)) for q in (
                  live, r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")) + (max(nums(r"p99 +([0-9.]+)ms", p)),)))
for l in open("mm-2-restart.log"):
    if "rebuilt" in l:
        print(l.strip())
# skip the warm-up, and the last seconds after the matchmakers have stopped (nothing placed)
f = [l for l in open("fleet.log") if "joins requested" in l and not re.search(r"placed 0 ", l)][12:]
herd = [l for l in f if "(herd)" in l]
def tot(lines):
    return [sum(nums(q, lines)) for q in (r"placed ([0-9,]+)", r"\) +bounced ([0-9,]+)", r"\) +bounced [0-9,]+ \(full ([0-9,]+)",
            r"\) +bounced [0-9,]+ \(full [0-9,]+ shutting down ([0-9,]+)", r"\) no server ([0-9,]+)  an index",
            r"would bounce ([0-9,]+)", r"would bounce [0-9,]+ \(full ([0-9,]+)", r"would bounce [0-9,]+ \(full [0-9,]+ shutting down ([0-9,]+)",
            r"would bounce .*?\) no server ([0-9,]+)", r"unplaced ([0-9,]+)")]
for name, lines in (("after warm-up", f), ("during the herd", herd)):
    c, b, bf, bc, bn, s, sf, sc, sn, un = tot(lines)
    print("%s (%d s): placements checked %d  bounced %d (full %d, shutting down %d, %.2f%%) no server %d  unplaced %d | "
          "an index refreshed every 3 s would bounce %d (full %d, shutting down %d, %.2f%%) and find no server for %d (%.2f%%)" % (
          name, len(lines), c, b, bf, bc, 100 * b / max(c, 1), bn, un, s, sf, sc, 100 * s / max(c, 1), sn, 100 * sn / max(c, 1)))
c, b, _, _, bn, s, _, _, sn, un = tot(f)
pp = [l for l in f if "press-to-placement" in l]
print("press-to-placement, median per second: p50 %.1f ms  p99 %.1f ms" % (
    st.median(nums(r"placement p50 ([0-9.]+)ms", pp)), st.median(nums(r"placement p50 [0-9.]+ms p99 ([0-9.]+)ms", pp))))
if c < 2000 or not herd:
    raise SystemExit("placements or the herd did not flow")
if un * 50 > c:
    raise SystemExit("more than 2% of joins were never placed")
if b + bn >= s + sn:
    raise SystemExit("the live index bounced as many joins as the 3 s comparison index")
PY
grep -q "\[analytics\] done" analytics.log || { echo "the analytics neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
