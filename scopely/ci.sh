#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the MONOPOLY GO! race lane at small scale for about a minute:
# 6,700 players in 420 races of 4 teams of 4 arriving over 15 s (zero to 3,000 rolls/s, the way a race mini game
# starts), rolls keyed by race on 16 partitions with 0.5% sent twice as client retries, two race owners holding
# 8 partitions each, 4 game servers reading every lap, finish and race end, and 2 MB/s of client analytics next
# door from second 30. Races are compressed to 600 points so several finish in a run. Pass only if each owner holds
# the 50 ms p99 budget in 95% of its steady-state seconds, races finish, retried rolls count once, every place the
# owners announce matches the race rules run on the rolls in send order, and almost no player rolls into a race
# their game server should already have shown as decided.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst (30-35 s after a heavy
# neighbour starts) stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py race_owner.py game_servers.py analytics_flood.py dynamodb_bridge.py
# The race owners start first and create the topics; the game servers and the neighbour follow.
for w in 0 1; do
  timeout 85 python -u race_owner.py --worker $w --workers 2 --track 600 --lap 150 > owner-$w.log 2>&1 &
done
sleep 3
timeout 80 python -u game_servers.py --servers 4 --rate 3000 --ramp-s 15 --track 600 --lap 150 > game.log 2>&1 &
timeout 75 python -u analytics_flood.py --mbps 2 --at 30 --duration 35 > analytics.log 2>&1 &
wait || true
echo "--- game.log";      head -n 3 game.log; tail -n 3 game.log
echo "--- analytics.log"; head -n 2 analytics.log; tail -n 2 analytics.log
for w in 0 1; do echo "--- owner-$w.log"; head -n 2 owner-$w.log; tail -n 3 owner-$w.log; done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" owner-*.log | cut -c1-150 || true
status=0
# Skip a 10 s warm-up while the topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
for w in 0 1; do
  steady=$(grep -E "\[owner-$w\] +[1-9][0-9,]* rolls/s" owner-$w.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "owner-$w steady-state seconds with live rolls: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
done
[ "$status" -eq 0 ] || { echo "race lane latency check failed"; exit 1; }
grep -q "places checked wrong 0 " <<<"$(grep -E '\[game\] +[0-9,]+ rolls/s' game.log | tail -n 1)" \
  || { echo "a place the race owners announced differs from the race rules"; exit 1; }
grep -q "\[analytics\] done" analytics.log || { echo "the analytics neighbour did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
def num(pat, ls):
    out = []
    for l in ls:                                  # skip lines the pattern doesn't match instead of failing
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
fin = retry = 0
for w in (0, 1):
    t = [l for l in open(f"owner-{w}.log") if re.search(r"\] +[1-9][0-9,]* rolls/s", l)][10:]
    p99 = num(r"p99 +([0-9.]+)ms", t)
    fin += sum(num(r"finishes ([0-9,]+)", t))
    retry += sum(num(r"retries counted once ([0-9,]+)", t))
    if not p99:
        continue
    print("owner-%d steady-state medians over %d s: rolls/s %.0f  roll-to-race-owner p50 %.1f ms  p99 %.1f ms  "
          "p99.9 %.1f ms  worst-second p99 %.1f ms" % (
          w, len(t), st.median(num(r"\] +([0-9,]+) rolls/s", t)), st.median(num(r"p50 +([0-9.]+)ms", t)),
          st.median(p99), st.median(num(r"p99.9 +([0-9.]+)ms", t)), max(p99)))
    allp = num(r"p99 +([0-9.]+)ms", [l for l in open(f"owner-{w}.log") if " rolls/s " in l])
    before, during = allp[10:26], allp[28:]
    if before and during:
        print("owner-%d p99 median before the analytics neighbour %.1f ms (%d s), with 2 MB/s next door %.1f ms (%d s)" % (
              w, st.median(before), len(before), st.median(during), len(during)))
print("steady state, both owners: finishes %d  retried rolls counted once %d" % (fin, retry))
if fin < 100:
    raise SystemExit("too few races finished to judge the lane")
if retry < 10:
    raise SystemExit("no retried rolls reached the owners")
g = [l for l in open("game.log") if re.search(r"\[game\] +[1-9][0-9,]* rolls/s", l)][10:]
after = sum(num(r"race end ([0-9,]+),", g))
early = sum(num(r"before the server had it ([0-9,]+)", g))
slow = sum(num(r"it would be ([0-9,]+)\)", g))
print("first rolls after a finish or race end: %d  rolled before the game server had it: %d (%.2f%%)  "
      "with a 2 s race view: %d (%.2f%%)" % (after, early, 100 * early / max(after, 1), slow, 100 * slow / max(after, 1)))
if after < 100:
    raise SystemExit("too few rolls after a finish to judge the loop")
d = [l for l in g if re.search(r"game-server n [1-9]", l)]
if not d:
    raise SystemExit("no race updates reached the game servers")
print("roll-to-every-game-server (4 servers): p50 %.1f ms  p99 %.1f ms (medians of seconds with updates, %d s)" % (
    st.median(num(r"game-server n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(num(r"p99 ([0-9.]+)ms", d)), len(d)))
if early * 100 > after * 3:
    raise SystemExit("more than 3% of players rolled into a race their game server should have shown as decided")
e = [l for l in open("analytics.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("analytics neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(num(r"~([0-9.]+) MB/s", e))))
PY
echo "Kit OK on Apache Iggy 0.9.0"
