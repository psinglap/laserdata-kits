#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the partner trade event lane at small scale for about a minute:
# the OMS's trade events into one log per broker partner, with a robo-advisor rebalance burst (3x on its partner),
# two event stream gateways, partners disconnecting and resuming from their last event_id, and one partner reading
# slowly for 15 s. Pass only if each gateway holds the 10 ms OMS-to-gateway p99 for live partners in 95% of its
# steady-state seconds, every resume is exact (no gaps, no duplicates), and the slow partner falls behind in its
# log and catches up without losing an event.
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
python -m py_compile common.py oms.py event_gateway.py redpanda_bridge.py
# 50,000 accounts across 6 partners, 1,000 trade events/s, the robo-advisor's rebalance at 3x from second 30 to 45;
# gw-1 streams to corr-01..03, gw-2 to corr-04..06, and corr-06 can process only 25 events/s for 15 s.
timeout 84 python -u oms.py --accounts 50000 --rate 1000 --rebalance 3 --rebalance-at 30 --rebalance-for 15 > oms.log 2>&1 &
sleep 4
timeout 76 python -u event_gateway.py --name gw-1 --partners corr-01 corr-02 corr-03 > gw-1.log 2>&1 &
timeout 76 python -u event_gateway.py --name gw-2 --partners corr-04 corr-05 corr-06 --slow corr-06 \
  --slow-at 20 --slow-for 15 --slow-rate 25 > gw-2.log 2>&1 &
wait || true
echo "--- oms.log"; head -n 2 oms.log; grep rebalance oms.log | sed -n 5p || true; tail -n 2 oms.log
for g in gw-1 gw-2; do
  echo "--- $g.log"; head -n 1 $g.log; grep -E "caught up with|catching up" $g.log | tail -n 2 || true; tail -n 2 $g.log
done
echo "--- gw-2 slow partner"; grep -E "slow partner corr-06: (slow|draining)" gw-2.log | sed -E 's/.*(slow partner)/\1/' | sed -n '1p;8p;$p' || true
grep -E "caught up [0-9.]+s after" gw-2.log | sed -E 's/.*(slow partner)/\1/' || true
echo "--- oms seconds with a tick started over 50 ms late"; grep -nE "tick late max ([5-9][0-9]|[0-9]{3,})ms" oms.log || true
status=0
for g in gw-1 gw-2; do
  echo "--- $g FAIL seconds (line: output)"; grep -n "FAIL (" $g.log | sed -E 's/  to partners.*//' || true
  # Skip a 10 s warm-up; a shared CI runner may blip, so require 95% of steady-state seconds to pass.
  steady=$(grep -E "\[$g\] +[1-9][0-9,]* events/s" $g.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$g steady-state seconds with live traffic: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "$g latency check failed"; status=1; fi
  if grep -qE "gaps [1-9]|dups [1-9]|unrecoverable [1-9]" $g.log; then echo "$g: a partner saw a gap or duplicate, or a resume failed"; status=1; fi
  grep -q "caught up with the logs" $g.log || { echo "$g never caught up"; status=1; }
done
if ! grep -qE "caught up [0-9.]+s after it sped up" gw-2.log; then echo "the slow partner never caught up"; status=1; fi
[ "$status" -eq 0 ] || { echo "partner trade event lane check failed"; exit 1; }
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
    p = [l for l in open(f"{g}.log") if re.search(r"\] +[1-9][0-9,]* events/s", l)][10:]
    print("%s steady-state medians over %d s: events/s %.0f  OMS-to-gateway p50 %.2f ms  p99 %.2f ms  p99.9 %.2f ms  "
          "worst-second p99 %.2f ms" % ((g, len(p)) + tuple(st.median(nums(q, p) or [0]) for q in (
            r"\] +([0-9,]+) events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms")) +
          (max(nums(r"p99 +([0-9.]+)ms", p) or [0]),)))
    allp = [l for l in open(f"{g}.log") if "disconnects" in l]
    rs = nums(r"resume-to-live p99 ([0-9]+)ms", [l for l in allp if not re.search(r"resumes 0 ", l)])
    print("  resume-to-live p99 per second with a resume: median %.0f ms, worst %.0f ms" % (
        st.median(rs or [0]), max(rs or [0])))
    for key, pat in (("disconnects", r"disconnects ([0-9]+)"), ("resumes", r"resumes ([0-9]+)"),
                     ("lost", r"lost in flight ([0-9]+)"), ("held", r"held in the log ([0-9,]+)"),
                     ("slow", r"read from the log ([0-9,]+)")):
        tot[key] = tot.get(key, 0) + sum(nums(pat, allp))
lag = nums(r"lag ([0-9,]+) events", open("gw-2.log").readlines())
cu = nums(r"caught up ([0-9.]+)s after it sped up", open("gw-2.log").readlines())
print("both gateways: disconnects %d, resumes %d from since_id, %d events lost in flight and redelivered, %d events "
      "held in the log while partners were away or behind; gaps 0, duplicates 0, unrecoverable 0" % (
      tot["disconnects"], tot["resumes"], tot["lost"], tot["held"]))
print("slow partner corr-06: peak lag %d events, %d events read back from its log, caught up %.1f s after it sped up, "
      "none lost" % (max(lag or [0]), tot["slow"], cu[0] if cu else float("nan")))
if tot["resumes"] < 5 or tot["lost"] < 5 or max(lag or [0]) < 100:
    raise SystemExit("too few resumes, or the slow partner never fell behind")
PY
echo "Kit OK on Apache Iggy 0.9.0"
