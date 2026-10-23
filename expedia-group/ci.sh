#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the bandit decision lane at small scale for about a minute: 6,000
# properties with a hero-image bandit in 60 destinations, 150 page views a second from travelers who search,
# re-sort and open properties, a 3x spike in new sessions, 2 MB/s of clickstream next door, two Sampler pods behind
# a load balancer with no stickiness, and one of them restarted before the spike.
# Pass only if each Sampler holds the 50 ms decision-to-cache p99 in 95% of its steady-state seconds, the
# restarted one rebuilds its cache from the log, almost no traveler sees an image flip on a revisit, and fewer
# flip than with a cache synced every second.
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
python -m py_compile common.py travelers.py sampler.py clickstream_flood.py kafka_bridge.py
timeout 76 python -u sampler.py --name sampler-1 --shard 0 --shards 2 > sampler-1.log 2>&1 &
# sampler-2 runs, stops at 24 s and starts again: it must rebuild its cache from the log before it takes a page.
( timeout 24 python -u sampler.py --name sampler-2 --shard 1 --shards 2 > sampler-2.log 2>&1 || true
  sleep 1
  timeout 51 python -u sampler.py --name sampler-2 --shard 1 --shards 2 > sampler-2-restart.log 2>&1 || true ) &
sleep 3
timeout 74 python -u travelers.py --properties 6000 --destinations 60 --pages-per-s 150 \
  --surge 3 --surge-at 37 --surge-for 15 > travelers.log 2>&1 &
timeout 66 python -u clickstream_flood.py --mbps 2 --at 17 --duration 35 > clickstream.log 2>&1 &
wait || true
echo "--- travelers.log"; head -n 2 travelers.log | cut -c1-400
grep -E "\(surge\)" travelers.log | head -n 3 | cut -c1-400 || true; tail -n 2 travelers.log | cut -c1-400
echo "--- traveler seconds with a tick started or sent over 50 ms late"; grep -nE "(late|send) max ([5-9][0-9]|[0-9]{3,})ms" travelers.log | sed -E 's/pages requested.*tick/tick/' || true
echo "--- clickstream.log"; head -n 2 clickstream.log; tail -n 2 clickstream.log
for f in sampler-1 sampler-2 sampler-2-restart; do echo "--- $f.log"; head -n 2 $f.log; tail -n 2 $f.log; done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" sampler-*.log | sed -E 's/  pages .*//' || true
steady() { for f in "$@"; do grep -E '\[sampler-[12]\] +[1-9][0-9,]* decisions/s' $f | tail -n +11 || true; done; }
for s in sampler-1 sampler-2; do
  if [ $s = sampler-1 ]; then x=$(steady sampler-1.log); else x=$(steady sampler-2.log sampler-2-restart.log); fi
  # Skip each run's first 10 s (warm-up, or the rebuild after the restart); a shared runner may blip, so 95%.
  live=$(grep -c 'PASS (' <<<"$x" || true)
  fail=$(grep -c 'FAIL (' <<<"$x" || true)
  echo "$s steady-state seconds with live decisions: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
done
grep -Eq "rebuilt the cache from the log in [0-9.]+s: [1-9][0-9,]* sessions" sampler-2-restart.log \
  || { echo "the restarted Sampler did not rebuild its cache from the log"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
live = r"\] +([1-9][0-9,]*) decisions/s"
for n in ("sampler-1", "sampler-2", "sampler-2-restart"):
    p = [l for l in open(f"{n}.log") if re.search(live, l)][10:]
    if p:
        print("%s steady-state medians over %d s: decisions/s %.0f  decision-to-cache p50 %.1f ms  p99 %.1f ms  "
              "p99.9 %.1f ms  worst p99 %.1f ms" % ((n, len(p)) + tuple(st.median(nums(q, p)) for q in (
                  live, r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")) + (max(nums(r"p99 +([0-9.]+)ms", p)),)))
for l in open("sampler-2-restart.log"):
    if "rebuilt" in l:
        print(l.strip())
# skip the warm-up, and the last seconds after the Samplers have stopped (nothing answered)
f = [l for l in open("travelers.log") if "pages requested" in l and not re.search(r"\banswered 0 \(", l)][10:]
surge = [l for l in f if "(surge)" in l]
def tot(lines):
    return [sum(nums(q, lines)) for q in (r"answered ([0-9,]+)", r"revisits ([0-9,]+)", r"flipped ([0-9,]+) \(",
            r"would flip ([0-9,]+)", r"unanswered ([0-9,]+)", r"pages requested ([0-9,]+)")]
for name, lines in (("after warm-up", f), ("during the spike", surge)):
    a, rv, fl, alt, un, rq = tot(lines)
    print("%s (%d s): pages answered %d  revisits %d  flipped %d (%.3f%%) | a cache synced every 1 s would flip %d "
          "(%.2f%%)  unanswered %d" % (name, len(lines), a, rv, fl, 100 * fl / max(rv, 1), alt, 100 * alt / max(rv, 1), un))
pp = [l for l in f if "page-to-picks" in l]
print("page-to-picks, median per second: p50 %.1f ms  p99 %.1f ms" % (
    st.median(nums(r"picks p50 ([0-9.]+)ms", pp)), st.median(nums(r"picks p50 [0-9.]+ms p99 ([0-9.]+)ms", pp))))
a, rv, fl, alt, un, rq = tot(f)
if a < 3000 or rv < 1000 or not surge:
    raise SystemExit("pages, revisits or the spike did not flow")
if un * 50 > rq:
    raise SystemExit("more than 2% of pages were never answered")
if fl * 200 > rv:
    raise SystemExit("more than 0.5% of revisits showed a flipped image")
if fl >= alt:
    raise SystemExit("the live lane flipped as many images as a cache synced every second")
PY
grep -q "\[clickstream\] done" clickstream.log || { echo "the clickstream neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
