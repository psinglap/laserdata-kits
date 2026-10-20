#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the tick lane at small scale for about a minute:
# two ticker nodes, an order-postback burst next door, and one node restarted mid-session.
# The 10x tick burst is left to your own machines: on a shared 2-vCPU runner the Python feed and
# readers, not the broker, run out of CPU first.
# Pass only if each node holds the 10 ms p99 budget in 90% of its steady-state seconds with no sequence gaps,
# and the restarted node rebuilds a quote for every instrument from the log.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py feed.py ticker.py postbacks.py nats_bridge.py
N=8000
timeout 88 python -u feed.py --instruments $N --rate 1000 --snapshot-every 15 > feed.log 2>&1 &
sleep 4
timeout 76 python -u postbacks.py --rate 100 --x 10 --at 40 --for 15 > postbacks.log 2>&1 &
timeout 76 python -u ticker.py --name ticker-1 --instruments $N --users 5000 --replay-s 0 > ticker-1.log 2>&1 &
# ticker-2 runs, stops at 34 s and starts again: on restart it must rebuild every quote from the
# last 20 s of the log (one 15 s rolling snapshot plus the ticks since).
timeout 34 python -u ticker.py --name ticker-2 --instruments $N --users 5000 --replay-s 0 > ticker-2.log 2>&1 || true
sleep 1
timeout 40 python -u ticker.py --name ticker-2 --instruments $N --users 5000 --replay-s 20 > ticker-2-restart.log 2>&1 || true
wait || true
echo "--- feed.log";      grep -m 2 -E "snapshot|burst" feed.log || true; tail -n 3 feed.log
echo "--- postbacks.log"; grep -m 1 burst postbacks.log || true; tail -n 2 postbacks.log
for f in ticker-1 ticker-2 ticker-2-restart; do
  echo "--- $f.log"; grep -E "rebuilt|without the log" $f.log || true; tail -n 4 $f.log
done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" ticker-*.log | cut -c1-150 || true
status=0
# Skip each run's first 10 s (warm-up, or the replay right after the restart); a shared CI runner may blip,
# so require 90% of each node's remaining seconds to pass (a 10 ms budget on a shared 2-vCPU runner sees
# a few 11-15 ms seconds from neighbours on the host while p50 stays near 4 ms), and no sequence gap anywhere.
steady() { for f in "$@"; do grep -E '\[ticker-[12]\] +[1-9][0-9,]* ticks/s' $f | tail -n +11 || true; done; }
for node in ticker-1 ticker-2; do
  if [ $node = ticker-1 ]; then s=$(steady ticker-1.log); else s=$(steady ticker-2.log ticker-2-restart.log); fi
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$node steady-state seconds with live ticks: PASS $live, FAIL $fail"
  if [ "$live" -lt 40 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 10 )) ]; then status=1; fi
done
for f in ticker-1.log ticker-2.log ticker-2-restart.log; do
  gaps=$(grep -Eo 'gaps [0-9]+' $f | tail -n 1 | cut -d' ' -f2)
  echo "$f sequence gaps: ${gaps:-none}"
  [ "${gaps:-1}" = "0" ] || status=1
done
[ "$status" -eq 0 ] || { echo "tick lane check failed"; exit 1; }
grep -Eq 'rebuilt from the last 20 s of the log in [0-9.]+s: quotes for ([0-9,]+) of \1 instruments' ticker-2-restart.log \
  || { echo "the restarted node did not rebuild a quote for every instrument"; exit 1; }
grep -q "shown after a newer tick existed" ticker-1.log || { echo "no business metric"; exit 1; }
python - <<'PY'
import re, statistics as st
live = r"\[ticker-[12]\] +([0-9,]+) ticks/s"
for name in ("ticker-1", "ticker-2-restart"):
    t = [l for l in open(f"{name}.log") if re.search(live, l) and not re.search(r"\] +0 ticks/s", l)][10:]
    num = lambda pat: [float(re.search(pat, l).group(1).replace(",", "")) for l in t]
    if t:
        p99 = num(r"p99 +([0-9.]+)ms")
        print("%s steady-state medians over %d s: ticks/s %.0f  feed-to-ticker p50 %.2f ms  p99 %.2f ms  p99.9 %.2f ms  "
              "worst-second p99 %.2f ms  shown after a newer tick existed %.3f%%  packets to users/s %.0f" % (
              name, len(t), st.median(num(live)), st.median(num(r"p50 +([0-9.]+)ms")), st.median(p99),
              st.median(num(r"p99.9 +([0-9.]+)ms")), max(p99), st.median(num(r"newer tick existed ([0-9.]+)%")),
              st.median(num(r"packets to users ([0-9,]+)/s"))))
rows = [float(re.search(r"p99 +([0-9.]+)ms", l).group(1)) for l in open("ticker-1.log") if re.search(live, l)]
if len(rows) > 60:   # postbacks burst 10x from its second 40 to 55, about ticker-1's seconds 40 to 55
    print("ticker-1 p99, median per second: before the postback burst %.2f ms (s 20-38), during it %.2f ms (s 42-54)" % (
        st.median(rows[20:39]), st.median(rows[42:55])))
PY
echo "Kit OK on Apache Iggy 0.9.0"
