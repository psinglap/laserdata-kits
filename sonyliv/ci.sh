#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and play part of a KBC episode with Play Along at small scale for about
# a minute: 30,000 players, 80% locking an answer on each 16 s question (most inside the 10 s bonus window), 1%
# of locks arriving after the close, 1% sent twice, and an engagement stream next door from second 20. A second
# scorer starts from the log at second 40. Pass only if the scorer holds the 100 ms p99 budget in 95% of its
# steady-state seconds, no valid lock is scored after its reveal, late locks are rejected and repeats dropped,
# and the rebuilt scorer prints the same per-question score digests as the live one.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which can stall a shared runner's disk.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py episode.py scorer.py engagement_flood.py kinesis_bridge.py
# The scorer starts first and creates the topics; the episode and the neighbour follow.
timeout 82 python -u scorer.py > scorer.log 2>&1 &
sleep 3
timeout 75 python -u episode.py --players 30000 --answer-share 0.8 --timer-s 16 --reveal-gap-s 2 --next-gap-s 2 \
  --cut-share 0.25 > episode.log 2>&1 &
timeout 72 python -u engagement_flood.py --mbps 1 --at 20 --duration 40 > engagement.log 2>&1 &
# A second scorer joins mid-episode and rebuilds every score from the log.
( sleep 37; timeout 40 python -u scorer.py --name rebuild > rebuild.log 2>&1 ) &
wait || true
echo "--- episode.log";    grep "revealed" episode.log || true; tail -n 2 episode.log
echo "--- scorer.log";     grep -E "^\[(reveal|digest)\]" scorer.log || true; tail -n 4 scorer.log
echo "--- rebuild.log";    head -n 2 rebuild.log; grep -E "^\[digest\]" rebuild.log || true
echo "--- engagement.log"; head -n 2 engagement.log; tail -n 2 engagement.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" scorer.log | cut -c1-160 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '^\[scorer\] +[1-9][0-9,]* locks/s' scorer.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
grep -q "\[engagement\] done" engagement.log || { echo "the engagement neighbour did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
log = open("scorer.log").read().splitlines()
h = [l for l in log if re.search(r"^\[scorer\] +[1-9][0-9,]* locks/s", l)][10:]
print("steady-state medians over %d s: locks/s %.0f  lock-to-score p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  worst p99 %.1f ms" % (
    len(h), *(st.median(nums(q, h)) for q in (r"\] +([0-9,]+) locks/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                               r"p99.9 +([0-9.]+)ms")), max(nums(r"p99 +([0-9.]+)ms", h))))
burst = [l for l in h if nums(r"\] +([0-9,]+) locks/s", [l])[0] > 1500]
if burst:
    print("bonus-window bursts: %d s above 1,500 locks/s, median p99 %.1f ms, worst p99 %.1f ms" % (
        len(burst), st.median(nums(r"p99 +([0-9.]+)ms", burst)), max(nums(r"p99 +([0-9.]+)ms", burst))))
pa = [l for l in log if l.startswith("[play-along]")]
tot = {k: sum(nums(k + r" ([0-9,]+)", pa)) for k in ("scored", "repeats dropped", "second locks dropped",
                                                     "scored after the reveal")}
rv = [l for l in log if l.startswith("[reveal]")]
late = sum(nums(r"late rejected ([0-9,]+)", rv))
print("Play Along: %d reveals, %.0f locks scored, %.0f late locks rejected, %.0f repeats dropped, "
      "valid locks scored after their reveal %.0f" % (len(rv), tot["scored"], late, tot["repeats dropped"],
                                                       tot["scored after the reveal"]))
def digests(path):
    return {m.group(1): m.group(2) for m in (re.search(r"^\[digest\] (\S+) .*digest ([0-9a-f]{8})", l)
                                             for l in open(path)) if m}
live_d, rebuilt = digests("scorer.log"), digests("rebuild.log")
common = sorted(set(live_d) & set(rebuilt))
same = [q for q in common if live_d[q] == rebuilt[q]]
print("rebuild from the log: %d questions compared, %d identical (%s)" % (len(common), len(same), ", ".join(common)))
e = [l for l in open("engagement.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("engagement neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
if len(rv) < 2 or tot["scored"] < 20000:
    raise SystemExit("too few reveals or scored locks")
if tot["scored after the reveal"] > 0:
    raise SystemExit("a valid lock was scored after its answer was revealed")
if late == 0 or tot["repeats dropped"] == 0:
    raise SystemExit("late locks or repeats did not reach the scorer")
if len(common) < 2 or len(same) != len(common):
    raise SystemExit("the rebuilt scorer did not match the live scores")
PY
echo "Kit OK on Apache Iggy 0.9.0"
