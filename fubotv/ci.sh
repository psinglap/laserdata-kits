#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the key-play alerts lane at small scale for about a minute:
# 80 live games (game time 60x real time, about 250 moments a second, clips ready 1-4 s after each key play, some
# home runs reviewed and overturned), Game Alerts reading the lane, player viewing events next door with a 4x
# first-pitch surge, and Game Alerts restarted mid-game.
# Pass only if Game Alerts holds its 50 ms clip-to-alert p99 budget in 95% of its steady-state seconds with alerts,
# sees no gaps, holds back alerts whose call was overturned before the clip was ready, corrects ones overturned
# after the push, rebuilds every live game from the log after its restart, and the alerts-sent record holds no alert
# twice across the restart.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst; on shared runners that burst stalled every
# reader for 0.2-0.8 s at once. It is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py moments.py game_alerts.py viewing_events.py kafka_bridge.py
G=80
timeout 88 python -u moments.py --games $G --speed 60 --clip-s 1,4 --review-share 0.3 > moments.log 2>&1 &
sleep 4
timeout 76 python -u viewing_events.py --rate 250 --x 4 --at 30 --for 15 > viewing.log 2>&1 &
# Game Alerts stops at 46 s and starts again: it must rebuild every live game from the log and push nothing twice.
# The restart runs at nice 10: on a real deployment a restarted service starts on its own machine, while here its
# interpreter start-up and replay share 2 vCPUs with the broker and the other processes.
timeout 46 python -u game_alerts.py > alerts.log 2>&1 || true
sleep 1
timeout 34 nice -n 10 python -u game_alerts.py > alerts-restart.log 2>&1 || true
wait || true
python -u game_alerts.py --audit > audit.log 2>&1
echo "--- moments.log";  grep -m 1 "opening lines" moments.log || true; tail -n 3 moments.log
echo "--- viewing.log";  grep -m 1 "surge" viewing.log || true; tail -n 2 viewing.log
for f in alerts alerts-restart; do
  echo "--- $f.log"; grep -E "rebuilt|from the record" $f.log || true; tail -n 4 $f.log
done
echo "--- audit.log"; cat audit.log
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" alerts*.log | cut -c1-200 || true
status=0
# Skip each run's first 10 s (warm-up, or the rebuild right after the restart); then require 95% PASS.
steady() { for f in "$@"; do grep -E "$pat" $f | tail -n +11 || true; done; }
pat='\[alerts\] +[1-9][0-9]* alerts/s'
s=$(steady alerts.log alerts-restart.log)
live=$(grep -c 'PASS (' <<<"$s" || true)
fail=$(grep -c 'FAIL (' <<<"$s" || true)
echo "Game Alerts steady-state seconds with alerts: PASS $live, FAIL $fail"
if [ "$live" -lt 30 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then status=1; fi
for f in alerts.log alerts-restart.log; do
  gaps=$(grep -Eo 'gaps [0-9]+' $f | sort -t' ' -k2 -n | tail -n 1 | cut -d' ' -f2)
  echo "$f worst gaps: ${gaps:-none}"
  if [ "${gaps:-1}" != "0" ]; then status=1; fi
done
[ "$status" -eq 0 ] || { echo "key-play alerts check failed"; exit 1; }
grep -Eq "pushed [1-9]" alerts.log || { echo "no key-play alert was pushed"; exit 1; }
grep -Eq "held back by review [1-9]" alerts.log alerts-restart.log || { echo "no overturned call was held back"; exit 1; }
grep -Eq "corrected after push [1-9]" alerts.log alerts-restart.log || { echo "no correction followed an overturned call"; exit 1; }
grep -Eq "rebuilt from the log in [0-9.]+s: $G live games" alerts-restart.log \
  || { echo "the restarted Game Alerts did not rebuild every live game"; exit 1; }
grep -Eq "alerts already pushed today, from the record: [1-9][0-9,]* \(recorded twice 0\)" alerts-restart.log \
  || { echo "the restarted Game Alerts did not read its record"; exit 1; }
grep -Eq "recorded twice 0$" audit.log || { echo "an alert was recorded twice"; exit 1; }
python - <<'PY'
import re, statistics as st
def num(rows, pat):
    out = []
    for l in rows:
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
al = r"\[alerts\] +([0-9]+) alerts/s"
both = []
for f in ("alerts", "alerts-restart"):
    t = [l for l in open(f"{f}.log") if re.search(al, l)]
    both += t[10:]
p99 = num(both, r" p99 +([0-9.]+)ms")
if p99:
    print("Game Alerts over %d steady s with alerts: clip-to-alert p50 %.2f ms  p99 %.2f ms (median per second), "
          "worst second %.2f ms; pushed %d, held back by review %d, corrected after push %d, play-to-push %.1f s (median)" % (
          len(p99), st.median(num(both, r"p50 +([0-9.]+)ms")), st.median(p99), max(p99), sum(num(both, r"pushed ([0-9]+)")),
          sum(num(both, r"held back by review ([0-9]+)")), sum(num(both, r"corrected after push ([0-9]+)")),
          st.median(num(both, r"play-to-push ([0-9.]+)s") or [0])))
rows = [l for l in open("alerts.log") if re.search(r"\[alerts\] +[0-9]+ alerts/s", l)]   # one row per second
sec = [float(m.group(1)) if (m := re.search(r" p99 +([0-9.]+)ms", l)) else None for l in rows]
pick = lambda a, b: [x for x in sec[a:b] if x is not None]
if len(sec) > 40 and pick(11, 26) and pick(30, 42):   # the surge runs from its second 30 to 45, the first run's s 29-44
    print("Game Alerts p99, median per second: before the first-pitch surge %.2f ms (s 11-25), during it %.2f ms (s 30-41)" % (
        st.median(pick(11, 26)), st.median(pick(30, 42))))
for f in ("alerts", "alerts-restart", "audit"):
    for l in open(f"{f}.log"):
        if "rebuilt from the log" in l or "from the record" in l or "[audit]" in l:
            print(l.strip())
PY
echo "Kit OK on Apache Iggy 0.9.0"
