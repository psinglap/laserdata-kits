#!/usr/bin/env bash
# CI smoke test: start two Apache Iggy 0.9.0 brokers (sites nj-1 and nj-2 of one state) and run the in-state
# wallet lane at small scale for about 70 s: 50,000 accounts, 300 account and wallet events a second at nj-1 with a
# 5x kickoff surge for 15 s, the mirror copying every event to nj-2, two wallet replicas at nj-2, the mirror killed
# and restarted mid-run, one replica restarted mid-run, then nj-1 stops and the failover check runs.
# Pass only if each replica holds the 50 ms p99 budget in 95% of its steady-state seconds with no gaps or repeats,
# the mirror resumes without offset drift or cross-state copies, the restarted replica rebuilds from nj-2's log,
# events that waited for the mirror restart are counted apart, the at-risk metric prints, and after nj-1 stops nj-2
# holds every event nj-1 accepted, byte-identical at the same offsets, with every balance matching.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 30 || true' ERR
# Flush the brokers' writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst; on shared runners that burst stalled every
# reader for 0.2-0.8 s at once. It is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for port in 8090 8091; do
  for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/$port) 2>/dev/null && break; sleep 1; done
done
pip install -q -r requirements.txt
python -m py_compile common.py sportsbook.py site_mirror.py standby.py failover_check.py kafka_bridge.py
# The mirror and the replicas start first and create the topics; the sportsbook writes into them.
timeout 40 python -u site_mirror.py > mirror-1.log 2>&1 &            # killed at 40 s, restarted below
timeout 84 python -u standby.py --name replica-1 > replica-1.log 2>&1 &
# replica-2 runs, stops at 34 s and starts again: on restart it must rebuild from nj-2's log before it serves.
( timeout 34 python -u standby.py --name replica-2 > replica-2.log 2>&1 || true
  sleep 1
  timeout 48 python -u standby.py --name replica-2 > replica-2-restart.log 2>&1 || true ) &
sleep 3
timeout 80 python -u sportsbook.py --accounts 50000 --rate 300 --surge 5 --surge-at 15 --surge-s 15 --duration 70 \
  > sportsbook.log 2>&1 &
book=$!
sleep 37.5
timeout 46 python -u site_mirror.py > mirror-2.log 2>&1 &
wait $book || true
fo=0
python -u failover_check.py > failover.log 2>&1 || fo=1
wait || true
echo "--- sportsbook.log"; head -n 1 sportsbook.log; grep -m 2 "kickoff surge" sportsbook.log || true; tail -n 2 sportsbook.log
for f in mirror-1 mirror-2; do echo "--- $f.log"; head -n 1 $f.log; tail -n 2 $f.log; done
for f in replica-1 replica-2 replica-2-restart; do
  echo "--- $f.log"; grep -E "rebuilt" $f.log || true; grep -m 1 "for a mirror start [1-9]" $f.log | cut -c1-260 || true; tail -n 2 $f.log
done
echo "--- failover.log"; cat failover.log
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" replica-*.log | cut -c1-180 || true
status=0
# Skip each run's first 10 timed seconds (warm-up, or the rebuild right after the restart); then require 95% PASS.
steady() { for f in "$@"; do grep -E '\[replica-[12]\] +[1-9][0-9,]* events/s' $f | tail -n +11 || true; done; }
for r in replica-1 replica-2; do
  if [ $r = replica-1 ]; then s=$(steady replica-1.log); else s=$(steady replica-2.log replica-2-restart.log); fi
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$r steady-state seconds with live events: PASS $live, FAIL $fail"
  if [ "$live" -lt 30 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then status=1; fi
done
for f in replica-1.log replica-2.log replica-2-restart.log; do
  g=$(grep -Eo 'gaps [0-9]+' $f | sort -t' ' -k2 -n | tail -n 1 | cut -d' ' -f2)
  r=$(grep -Eo 'repeats [0-9]+' $f | sort -t' ' -k2 -n | tail -n 1 | cut -d' ' -f2)
  echo "$f worst gaps: ${g:-none}, repeats: ${r:-none}"
  if ! ( [ "${g:-1}" = "0" ] && [ "${r:-1}" = "0" ] ); then status=1; fi
done
bad=$(grep -hE 'offset drift [1-9]|cross-state refused [1-9]' mirror-*.log | wc -l)
echo "mirror seconds with offset drift or a cross-state refusal: $bad"
[ "$bad" -eq 0 ] || status=1
[ "$fo" -eq 0 ] || { echo "failover check failed"; status=1; }
[ "$status" -eq 0 ] || { echo "in-state wallet lane check failed"; exit 1; }
grep -Eq "resuming at nj-2's offsets .* \([1-9][0-9,]* events waited at nj-1" mirror-2.log \
  || { echo "the restarted mirror did not resume from nj-2's offsets"; exit 1; }
grep -Eq "held at nj-1 for a mirror start [1-9]" replica-1.log || { echo "events held for the mirror restart were not seen"; exit 1; }
grep -Eq "rebuilt from nj-2's log in [0-9.]+s: balance and open bets of [0-9,]+ accounts, [1-9][0-9,]* events replayed" \
  replica-2-restart.log || { echo "the restarted replica did not rebuild from the log"; exit 1; }
grep -Eq "at risk if nj-1 went dark now [0-9,]+ events" replica-1.log || { echo "no business metric"; exit 1; }
grep -q "OK$" failover.log || { echo "failover check did not pass"; exit 1; }
python - <<'PY'
import re, statistics as st
live = r"\[replica-[12]\] +([1-9][0-9,]*) events/s"
def num(rows, pat):
    out = []
    for l in rows:
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
for name in ("replica-1", "replica-2-restart"):
    t = [l for l in open(f"{name}.log") if re.search(live, l)][10:]
    p99 = num(t, r"p99 +([0-9.]+)ms")
    if p99:
        risk = num(t, r"went dark now ([0-9,]+) events")
        print("%s steady-state medians over %d s: events/s %.0f  accepted-to-replica p50 %.2f ms  p99 %.2f ms  "
              "p99.9 %.2f ms  worst-second p99 %.2f ms  at risk if nj-1 went dark: median %.0f, max %.0f events" % (
              name, len(t), st.median(num(t, live)), st.median(num(t, r"p50 +([0-9.]+)ms")), st.median(p99),
              st.median(num(t, r"p99\.9 +([0-9.]+)ms")), max(p99), st.median(risk) if risk else -1,
              max(risk) if risk else -1))
rows = [l for l in open("replica-1.log") if re.search(live, l)][10:]
if rows:
    rates = num(rows, live)
    surge = [l for l, r in zip(rows, rates) if r >= 900]
    calm = [l for l, r in zip(rows, rates) if r < 900]
    if surge and calm:
        print("replica-1 p99, median per second: normal evening %.2f ms (%d s, %.0f events/s), kickoff surge %.2f ms "
              "(%d s, %.0f events/s)" % (st.median(num(calm, r"p99 +([0-9.]+)ms")), len(calm), st.median(num(calm, live)),
              st.median(num(surge, r"p99 +([0-9.]+)ms")), len(surge), st.median(num(surge, live))))
held = [l for l in open("replica-1.log") if re.search(r"for a mirror start [1-9]", l)]
for l in held:
    m = re.search(r"for a mirror start ([0-9,]+) \(max ([0-9]+)ms\)", l)
    if m:
        print("replica-1: %s events waited at nj-1 for the mirror restart, longest %s ms; counted apart" % m.groups())
for f in ("replica-2-restart",):
    for l in open(f"{f}.log"):
        if "rebuilt from" in l:
            print(l.strip())
print(open("failover.log").read().strip())
PY
echo "Kit OK on Apache Iggy 0.9.0"
