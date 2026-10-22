#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the player form lane at small scale for about a minute: 5,000
# players in 16 lobbies playing 3-6 s games and tapping Play again 1-4 s after each result, a live-event surge that
# brings 2.5x the players online, 2 MB/s of app events next door, two matchmakers (each owning half the lobbies),
# and one of them restarted before the surge.
# Pass only if each matchmaker holds the 50 ms result-to-matchmaker p99 in 95% of its steady-state seconds, the
# restarted one rebuilds player form from the log, and decisions are checked against the players' true form, with
# fewer stale or wrong decisions than a view 10 s behind would have made.
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
python -m py_compile common.py players.py matchmaker.py app_events.py confluent_bridge.py
timeout 82 python -u players.py --players 5000 --game-min 3 --game-max 6 --think-min 1 --think-max 4 \
  --surge 2.5 --surge-at 40 --surge-for 15 > players.log 2>&1 &
sleep 4
timeout 70 python -u app_events.py --mbps 2 --at 20 --duration 35 > app-events.log 2>&1 &
timeout 76 python -u matchmaker.py --name mm-1 --shard 0 --shards 2 > mm-1.log 2>&1 &
# mm-2 runs, stops at 24 s and starts again: it must rebuild player form from the log before it takes a request.
timeout 24 python -u matchmaker.py --name mm-2 --shard 1 --shards 2 > mm-2.log 2>&1 || true
sleep 1
timeout 51 python -u matchmaker.py --name mm-2 --shard 1 --shards 2 > mm-2-restart.log 2>&1 || true
wait || true
echo "--- players.log"; head -n 1 players.log; grep -m 1 "live event" players.log || true
grep -E "\(surge\)" players.log | head -n 2 | cut -c1-420 || true; tail -n 2 players.log | cut -c1-420
echo "--- players seconds with a tick started or sent over 50 ms late"; grep -nE "(late|send) max ([5-9][0-9]|[0-9]{3,})ms" players.log | sed -E 's/taps .*tick/tick/' || true
echo "--- app-events.log"; head -n 2 app-events.log; tail -n 2 app-events.log
for f in mm-1 mm-2 mm-2-restart; do echo "--- $f.log"; head -n 2 $f.log; tail -n 2 $f.log; done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" mm-*.log | sed -E 's/  decided.*//' || true
steady() { for f in "$@"; do grep -E '\[mm-[12]\] +[1-9][0-9,]* results/s' $f | tail -n +11 || true; done; }
for mm in mm-1 mm-2; do
  if [ $mm = mm-1 ]; then s=$(steady mm-1.log); else s=$(steady mm-2.log mm-2-restart.log); fi
  # Skip each run's first 10 s (warm-up, or the rebuild after the restart); a shared runner may blip, so 95%.
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$mm steady-state seconds with live results: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
done
grep -Eq "rebuilt player form from the log in [0-9.]+s: [1-9][0-9,]* players" mm-2-restart.log \
  || { echo "the restarted matchmaker did not rebuild player form from the log"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
live = r"\] +([1-9][0-9,]*) results/s"
for n in ("mm-1", "mm-2", "mm-2-restart"):
    p = [l for l in open(f"{n}.log") if re.search(live, l)][10:]
    if p:
        print("%s steady-state medians over %d s: results/s %.0f  result-to-matchmaker p50 %.1f ms  p99 %.1f ms  "
              "p99.9 %.1f ms  worst p99 %.1f ms" % ((n, len(p)) + tuple(st.median(nums(q, p)) for q in (
                  live, r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")) + (max(nums(r"p99 +([0-9.]+)ms", p)),)))
for l in open("mm-2-restart.log"):
    if "rebuilt" in l:
        print(l.strip())
# skip the warm-up, and the last seconds after the matchmakers have stopped (nothing matched)
f = [l for l in open("players.log") if "results/s" in l and not re.search(r"\bmatched 0 \(", l)][12:]
surge = [l for l in f if "(surge)" in l]
def tot(lines):
    return [sum(nums(q, lines)) for q in (r"taps ([0-9,]+)", r"matched ([0-9,]+)", r"losing streak ([0-9,]+)",
            r"live view: stale form ([0-9,]+)", r"live view: .*? streak missed ([0-9,]+)", r"live view: .*? after a win ([0-9,]+)",
            r"behind: stale form ([0-9,]+)", r"behind: .*? streak missed ([0-9,]+)", r"behind: .*? after a win ([0-9,]+)",
            r"unmatched ([0-9,]+)")]
for name, lines in (("after warm-up", f), ("during the surge", surge)):
    t, c, lo, ls, lm, lw, ss, sm, sw, un = tot(lines)
    print("%s (%d s): taps %d  matches checked %d  on a losing streak %d | live view: stale form %d (%.2f%%), streak missed "
          "%d, protected after a win %d | a view 10 s behind: stale form %d (%.2f%%), streak missed %d (%.1f%% of losing "
          "streaks), protected after a win %d | unmatched %d" % (
          name, len(lines), t, c, lo, ls, 100 * ls / max(c, 1), lm, lw, ss, 100 * ss / max(c, 1), sm, 100 * sm / max(lo, 1), sw, un))
t, c, lo, ls, lm, lw, ss, sm, sw, un = tot(f)
pp = [l for l in f if "tap-to-match" in l]
print("tap-to-match, median per second: p50 %.1f ms  p99 %.1f ms" % (
    st.median(nums(r"tap-to-match p50 ([0-9.]+)ms", pp)), st.median(nums(r"tap-to-match p50 [0-9.]+ms p99 ([0-9.]+)ms", pp))))
if c < 2000 or not surge or lo < 100:
    raise SystemExit("matches, losing streaks or the surge did not flow")
if un * 50 > t:
    raise SystemExit("more than 2% of taps were never matched")
if ls >= ss or lm + lw >= sm + sw:
    raise SystemExit("the live view made as many stale or wrong decisions as the view 10 s behind")
PY
grep -q "\[app-events\] done" app-events.log || { echo "the app events neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
