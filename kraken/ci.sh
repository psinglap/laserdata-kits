#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the execution report lane at small scale for about a minute:
# the engine with a 2x volatile-market burst, two gateways with sessions dropping and reconnecting, the second
# gateway starting late and rebuilding its index from the log, and a ledger export next door.
# Pass only if each gateway holds the 10 ms engine-to-gateway p99 in 95% of its steady-state seconds, reconnecting
# sessions get their missed reports replayed from the log, and no session sees a gap or an unrecoverable resend.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py engine.py gateway.py ledger_export.py kafka_bridge.py
# 20,000 accounts, 1,500 execution reports/s with a 2x burst (3,000/s) from second 30 to 45, sessions split over
# two gateways, 20 sessions dropped per second on each, 2 MB/s of ledger entries from second 20 for 35 s.
A=20000
timeout 86 python -u engine.py --accounts $A --rate 1500 --burst 2 --burst-at 30 --burst-for 15 > engine.log 2>&1 &
sleep 4
timeout 76 python -u gateway.py --name gw-1 --index 0 --gateways 2 --accounts $A > gw-1.log 2>&1 &
timeout 66 python -u ledger_export.py --mbps 2 --at 16 --duration 35 > export.log 2>&1 &
sleep 16
# gw-2 starts 20 s into the run: it rebuilds its index from the whole log before logging its sessions on.
timeout 60 python -u gateway.py --name gw-2 --index 1 --gateways 2 --accounts $A > gw-2.log 2>&1 &
wait || true
echo "--- engine.log"; head -n 2 engine.log; grep -m 1 burst engine.log || true; tail -n 2 engine.log
echo "--- export.log"; head -n 2 export.log; tail -n 2 export.log
for g in gw-1 gw-2; do
  echo "--- $g.log"; head -n 1 $g.log; grep -E "rebuilt|catching up" $g.log | tail -n 2 || true; tail -n 3 $g.log
done
echo "--- engine seconds with a tick started over 50 ms late"; grep -nE "tick late max ([5-9][0-9]|[0-9]{3,})ms" engine.log || true
status=0
for g in gw-1 gw-2; do
  echo "--- $g FAIL seconds (line: output)"; grep -n "FAIL (" $g.log | sed -E 's/  to sessions.*//' || true
  # Skip a 10 s warm-up; a shared CI runner may blip, so require 95% of steady-state seconds to pass.
  steady=$(grep -E "\[$g\] +[1-9][0-9,]* reports/s" $g.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$g steady-state seconds with live traffic: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "$g latency check failed"; status=1; fi
  if grep -qE "gaps [1-9]|dups [1-9]|unrecoverable [1-9]" $g.log; then echo "$g: a session saw a gap or duplicate, or a resend failed"; status=1; fi
  grep -q "rebuilt the index from the log" $g.log || { echo "$g never caught up"; status=1; }
done
[ "$status" -eq 0 ] || { echo "execution report lane check failed"; exit 1; }
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
for g in ("gw-1", "gw-2"):
    p = [l for l in open(f"{g}.log") if re.search(r"\] +[1-9][0-9,]* reports/s", l)][10:]
    print("%s steady-state medians over %d s: reports/s %.0f  engine-to-gateway p50 %.2f ms  p99 %.2f ms  p99.9 %.2f ms  "
          "worst-second p99 %.2f ms  resend p99 %.0f ms" % (
        (g, len(p)) + tuple(st.median(nums(q, p) or [0]) for q in (
            r"\] +([0-9,]+) reports/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")) +
        (max(nums(r"p99 +([0-9.]+)ms", p) or [0]), st.median(nums(r"resend p99 ([0-9]+)ms", p) or [0]))))
    rb = [l for l in open(f"{g}.log") if "rebuilt the index" in l]
    if rb:
        print("  " + rb[0].strip())
    allp = [l for l in open(f"{g}.log") if "reconnects" in l]
    for key, pat in (("reconnects", r"reconnects ([0-9]+)"), ("recovered", r"recovered ([0-9]+)"),
                     ("replayed", r"from the log ([0-9,]+)"), ("fills", r"\(fills ([0-9,]+)"),
                     ("closed", r"closed while away ([0-9,]+)")):
        tot[key] = tot.get(key, 0) + sum(nums(pat, allp))
print("both gateways: reconnects %d, recovered %d; reports replayed from the log %d (fills %d, orders that closed "
      "while the session was away %d); gaps 0, duplicates 0, unrecoverable resends 0" % (
      tot["reconnects"], tot["recovered"], tot["replayed"], tot["fills"], tot["closed"]))
if tot["replayed"] < 50 or tot["closed"] < 1:
    raise SystemExit("no reports were replayed from the log")
PY
grep -q "\[ledger-export\] done" export.log || { echo "the ledger export neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
