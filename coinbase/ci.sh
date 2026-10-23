#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the chain event lane at small scale for about a minute:
# ChainStorage's events for 12 chains at their configured block times (about 12 blocks/s) with reorgs at 20x the
# illustrative rate, 30 minutes of history in the log, two indexer processes with two indexers each (the second
# starting 20 s in and rebuilding every chain from the log), one indexer restarting from its checkpoint, and a
# backfill replaying the whole log at 5,000 events/s next door.
# Pass only if each process holds the 50 ms ChainStorage-to-indexer p99 in 95% of its steady-state seconds, no
# indexer ever sees a sequence gap, a duplicate or a broken chain, the restart resumes with events held in the
# log, reorgs flow, and the backfill rebuilds every chain without a gap.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which can stall a shared runner's disk and every
# reader with it; that would be the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py chainstorage.py indexer.py backfill.py chainstorage_bridge.py
timeout 86 python -u chainstorage.py --history-s 1800 --reorgs 20 > chainstorage.log 2>&1 &
sleep 4
timeout 76 python -u indexer.py --name idx-1 --indexers 2 --restart-at 25 --restart-for 3 > idx-1.log 2>&1 &
timeout 66 python -u backfill.py --rate 5000 --at 16 --duration 40 > backfill.log 2>&1 &
sleep 16
# idx-2 starts 20 s into the run: it rebuilds every chain from the whole log before it serves.
timeout 60 python -u indexer.py --name idx-2 --indexers 2 > idx-2.log 2>&1 &
wait || true
echo "--- chainstorage.log"; head -n 2 chainstorage.log; tail -n 2 chainstorage.log
echo "--- backfill.log"; head -n 1 backfill.log; grep "pass" backfill.log | tail -n 2 || true; tail -n 1 backfill.log
for g in idx-1 idx-2; do
  echo "--- $g.log"; head -n 1 $g.log; grep -E "rebuilt|catching up|down for|resumed" $g.log | tail -n 3 || true; tail -n 3 $g.log
done
echo "--- chainstorage seconds with a tick started over 50 ms late"; grep -nE "tick late max ([5-9][0-9]|[0-9]{3,})ms" chainstorage.log || true
status=0
for g in idx-1 idx-2; do
  echo "--- $g FAIL seconds (line: output)"; grep -n "FAIL (" $g.log | sed -E 's/  new blocks.*//' || true
  # Skip a 10 s warm-up; a shared CI runner may blip, so require 95% of steady-state seconds to pass.
  steady=$(grep -E "\[$g\] +[1-9][0-9]* events/s" $g.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$g steady-state seconds with live traffic: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "$g latency check failed"; status=1; fi
  if grep -qE "gaps [1-9]|breaks [1-9]|dups [1-9]" $g.log; then echo "$g: an indexer saw a gap, a duplicate or a broken chain"; status=1; fi
  grep -q "rebuilt every chain from the log" $g.log || { echo "$g never caught up"; status=1; }
done
grep -q "indexer 0 resumed from its checkpoint" idx-1.log || { echo "the indexer restart did not run"; status=1; }
if ! grep -qE "pass [0-9]+: rebuilt 12 chains .* gaps 0  breaks 0" backfill.log; then echo "the backfill did not rebuild every chain cleanly"; status=1; fi
if grep -qE "pass [0-9]+: .*(gaps [1-9]|breaks [1-9])" backfill.log; then echo "the backfill saw a gap or a broken chain"; status=1; fi
[ "$status" -eq 0 ] || { echo "chain event lane check failed"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
tot = {"rem": 0, "lane": 0.0, "poll": 0.0, "held": 0}
for g in ("idx-1", "idx-2"):
    p = [l for l in open(f"{g}.log") if re.search(r"\] +[1-9][0-9]* events/s", l)][10:]
    print("%s steady-state medians over %d s: events/s %.0f  chain-to-indexer p50 %.2f ms  p99 %.2f ms  p99.9 %.2f ms  "
          "worst-second p99 %.2f ms;  new blocks: lane p99 %.0f ms, published loop p50 %.0f ms p99 %.0f ms" % (
        (g, len(p)) + tuple(st.median(nums(q, p) or [0]) for q in (
            r"\] +([0-9]+) events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")) +
        (max(nums(r"p99 +([0-9.]+)ms", p) or [0]),
         st.median(nums(r"lane p99 ([0-9]+)ms", p) or [0]), st.median(nums(r"loop p50 ([0-9]+)ms", p) or [0]),
         st.median(nums(r"loop p50 [0-9]+ms p99 ([0-9]+)ms", p) or [0]))))
    for l in open(f"{g}.log"):
        if "rebuilt every chain" in l:
            print("  " + l.strip())
    p = [l for l in open(f"{g}.log") if re.search(r"\] +[1-9][0-9]* events/s", l)]
    tot["rem"] += sum(nums(r"removals ([0-9]+)", p))
    tot["lane"] += sum(nums(r"removed block: lane ([0-9.]+)", p))
    tot["poll"] += sum(nums(r"published loop ([0-9]+)  held", p))
    tot["held"] += sum(nums(r"held in the log ([0-9]+)", p))
print("both processes: removals %d; tip reads from a removed block per indexer, at 200 reads/s per chain: lane %.1f, "
      "published loop (model) %.0f; events held in the log through the restart %d; gaps 0, duplicates 0, broken chains 0" % (
      tot["rem"], tot["lane"], tot["poll"], tot["held"]))
if tot["rem"] < 5 or tot["held"] < 1 or not tot["lane"] < tot["poll"]:
    raise SystemExit("the business metric did not flow: too few removals, nothing held, or the lane was not ahead")
PY
grep -q "\[backfill\] done" backfill.log || { echo "the backfill neighbour did not finish"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
