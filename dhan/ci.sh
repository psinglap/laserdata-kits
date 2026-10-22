#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the live order update lane at small scale for about a minute:
# DEXT's order updates with a 2x market-open burst, two order update gateways with sockets dropping and reconnecting,
# the second gateway starting late and rebuilding its index from the log, and a compliance export next door.
# Pass only if each gateway holds the 10 ms DEXT-to-gateway p99 in 95% of its steady-state seconds, reconnecting
# sockets get their missed updates replayed from the log, and no socket sees a gap or an unrecoverable resend.
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
python -m py_compile common.py dext.py order_gateway.py compliance_export.py ws_bridge.py
# 20,000 clients, 1,200 order updates/s with a 2x burst (2,400/s) from second 30 to 45, sockets split over two
# gateways, 20 sockets dropped per second on each, 2 MB/s of compliance records from second 20 for 35 s.
C=20000
timeout 86 python -u dext.py --clients $C --rate 1200 --burst 2 --burst-at 30 --burst-for 15 > dext.log 2>&1 &
sleep 4
timeout 76 python -u order_gateway.py --name ws-1 --index 0 --gateways 2 --clients $C > ws-1.log 2>&1 &
timeout 66 python -u compliance_export.py --mbps 2 --at 16 --duration 35 > export.log 2>&1 &
sleep 16
# ws-2 starts 20 s into the run: it rebuilds its index from the whole log before accepting its sockets.
timeout 60 python -u order_gateway.py --name ws-2 --index 1 --gateways 2 --clients $C > ws-2.log 2>&1 &
wait || true
echo "--- dext.log"; head -n 2 dext.log; grep burst dext.log | sed -n 5p || true; tail -n 2 dext.log
echo "--- export.log"; head -n 2 export.log; tail -n 2 export.log
for g in ws-1 ws-2; do
  echo "--- $g.log"; head -n 1 $g.log; grep -E "rebuilt|catching up" $g.log | tail -n 2 || true; tail -n 3 $g.log
done
echo "--- dext seconds with a tick started over 50 ms late"; grep -nE "tick late max ([5-9][0-9]|[0-9]{3,})ms" dext.log || true
status=0
for g in ws-1 ws-2; do
  echo "--- $g FAIL seconds (line: output)"; grep -n "FAIL (" $g.log | sed -E 's/  to sockets.*//' || true
  # Skip a 10 s warm-up; a shared CI runner may blip, so require 95% of steady-state seconds to pass.
  steady=$(grep -E "\[$g\] +[1-9][0-9,]* updates/s" $g.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$g steady-state seconds with live traffic: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "$g latency check failed"; status=1; fi
  if grep -qE "gaps [1-9]|dups [1-9]|unrecoverable [1-9]" $g.log; then echo "$g: a socket saw a gap or duplicate, or a resend failed"; status=1; fi
  grep -q "rebuilt the index from the log" $g.log || { echo "$g never caught up"; status=1; }
done
[ "$status" -eq 0 ] || { echo "order update lane check failed"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
tot = {}
for g in ("ws-1", "ws-2"):
    p = [l for l in open(f"{g}.log") if re.search(r"\] +[1-9][0-9,]* updates/s", l)][10:]
    print("%s steady-state medians over %d s: updates/s %.0f  DEXT-to-gateway p50 %.2f ms  p99 %.2f ms  p99.9 %.2f ms  "
          "worst-second p99 %.2f ms  resend p99 %.0f ms" % (
        (g, len(p)) + tuple(st.median(nums(q, p) or [0]) for q in (
            r"\] +([0-9,]+) updates/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")) +
        (max(nums(r"p99 +([0-9.]+)ms", p) or [0]), st.median(nums(r"resend p99 ([0-9]+)ms", p) or [0]))))
    rb = [l for l in open(f"{g}.log") if "rebuilt the index" in l]
    if rb:
        print("  " + rb[0].strip())
    allp = [l for l in open(f"{g}.log") if "reconnects" in l]
    for key, pat in (("reconnects", r"reconnects ([0-9]+)"), ("recovered", r"recovered ([0-9]+)"),
                     ("replayed", r"from the log ([0-9,]+)"), ("fills", r"\(fills ([0-9,]+)"),
                     ("closed", r"closed while away ([0-9,]+)"), ("legs", r"exit legs ([0-9,]+)")):
        tot[key] = tot.get(key, 0) + sum(nums(pat, allp))
print("both gateways: reconnects %d, recovered %d; updates replayed from the log %d (fills %d, orders that closed "
      "while the socket was away %d, Super Order exit legs %d); gaps 0, duplicates 0, unrecoverable resends 0" % (
      tot["reconnects"], tot["recovered"], tot["replayed"], tot["fills"], tot["closed"], tot["legs"]))
if tot["replayed"] < 50 or tot["closed"] < 1:
    raise SystemExit("no updates were replayed from the log")
PY
grep -q "\[compliance-export\] done" export.log || { echo "the compliance export neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
