#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the Airtel domain verdict lane at small scale for about a
# minute and a half: two resolver nodes, a smishing campaign (5x the usual flips to block, all from the SMS link
# scan) with a 4x burst of call and SMS records next door, and a third resolver added as the campaign starts.
# Pass only if each resolver holds the 25 ms p99 budget in 95% of its steady-state seconds with no sequence gaps,
# blocks reach the resolvers and are counted, and the added resolver rebuilds every domain's verdict from the log.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst (30-35 s after a
# heavy neighbour starts) stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py verdicts.py resolver.py records.py verdict_bridge.py
# 20,000 domains, 1,000 verdicts a second, 20 of them flips (far more flips than a real day, so every second has
# some to count); a smishing campaign from second 40 to 55 (5x the flips, all to block);
# 1 MB/s of call and SMS records next door, 4x over the same 15 s.
D=20000; R="--lookups-per-s 50000 --clicks-per-s 2"
timeout 92 python -u verdicts.py --domains $D --rate 1000 --flips 20 \
  --campaign-at 40 --campaign-for 15 --campaign-x 5 --snapshot-every 15 > verdicts.log 2>&1 &
sleep 4
timeout 80 python -u records.py --mbps 1 --x 4 --at 36 --for 15 --duration 74 > records.log 2>&1 &
timeout 82 python -u resolver.py --name resolver-haryana --domains $D $R --replay-s 0 > resolver-haryana.log 2>&1 &
timeout 34 python -u resolver.py --name resolver-delhi --domains $D $R --replay-s 0 > resolver-delhi.log 2>&1 || true
sleep 1
# resolver-mumbai is added as the campaign starts: it must rebuild every verdict from the last 20 s of the log
# (one 15 s rolling snapshot plus the verdicts since) while the campaign is running.
timeout 46 python -u resolver.py --name resolver-mumbai --domains $D $R --replay-s 20 > resolver-mumbai.log 2>&1 || true
wait || true
echo "--- verdicts.log"; grep -m 1 snapshot verdicts.log || true; grep -m 1 "smishing campaign" verdicts.log || true; tail -n 3 verdicts.log
echo "--- records.log";  head -n 1 records.log; grep -m 1 -E 'records/s .*burst' records.log || true; tail -n 2 records.log
for f in resolver-haryana resolver-delhi resolver-mumbai; do
  echo "--- $f.log"; grep -E "rebuilt" $f.log || true; tail -n 4 $f.log | cut -c1-330
done
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" resolver-*.log | cut -c1-170 || true
status=0
# Skip each node's first 10 live seconds (warm-up, or the replay right after it was added); a shared CI runner
# may blip, so require 95% of each node's remaining seconds to pass, and no sequence gap anywhere.
for node in resolver-haryana:40 resolver-delhi:18 resolver-mumbai:28; do
  name=${node%%:*}; need=${node##*:}
  s=$(grep -E "\[$name\] +[1-9][0-9,]* verdicts/s" $name.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$s" || true)
  fail=$(grep -c 'FAIL (' <<<"$s" || true)
  echo "$name steady-state seconds with live verdicts: PASS $live, FAIL $fail (need $need live, 95% PASS)"
  if [ "$live" -lt "$need" ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then status=1; fi
  gaps=$(grep -Eo 'gaps [0-9]+' $name.log | tail -n 1 | cut -d' ' -f2 || true)
  echo "$name sequence gaps: ${gaps:-none}"
  [ "${gaps:-1}" = "0" ] || status=1
done
[ "$status" -eq 0 ] || { echo "verdict lane check failed"; exit 1; }
grep -Eq 'rebuilt from the last 20 s of the log in [0-9.]+s: verdicts for ([0-9,]+) of \1 domains' resolver-mumbai.log \
  || { echo "the added resolver did not rebuild every domain's verdict"; exit 1; }
if ! grep -Eq 'to block [1-9][0-9,]*  clicks let through' resolver-haryana.log; then echo "no blocks reached the resolver"; exit 1; fi
if ! grep -Eq 'lifted [1-9][0-9,]*  legit lookups' resolver-haryana.log; then echo "no lifted blocks reached the resolver"; exit 1; fi
grep -q "smishing campaign" verdicts.log || { echo "the smishing campaign did not run"; exit 1; }
grep -q "\[records\] done" records.log || { echo "the call and SMS record neighbour did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
live = r"\[resolver-[a-z]+\] +([0-9,]+) verdicts/s"
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
for name in ("resolver-haryana", "resolver-mumbai"):
    t = [l for l in open(f"{name}.log") if re.search(live, l) and not re.search(r"\] +0 verdicts/s", l)][10:]
    if not t:
        continue
    p99 = nums(r"p99 +([0-9.]+)ms", t)
    med = lambda pat: st.median(nums(pat, t) or [float("nan")])
    tot = lambda pat: sum(nums(pat, t))
    print("%s steady-state over %d s: medians verdicts/s %.0f  verdict-to-resolver p50 %.2f ms  p99 %.2f ms  "
          "p99.9 %.2f ms  worst-second p99 %.2f ms; totals: blocks %.0f, clicks let through %.3f "
          "(%.0f at the daily refresh), blocks lifted %.0f, legit lookups still blocked %.3f" % (
          name, len(t), med(live), med(r"p50 +([0-9.]+)ms"), med(r"p99 +([0-9.]+)ms"), med(r"p99.9 +([0-9.]+)ms"),
          max(p99 or [float("nan")]), tot(r"to block ([0-9,]+)"), tot(r"clicks let through ([0-9,.]+)/s"),
          tot(r"at the daily refresh ([0-9,]+)"), tot(r"lifted ([0-9,]+)"), tot(r"still blocked ([0-9,.]+)/s")))
rows = nums(r"p99 +([0-9.]+)ms", [l for l in open("resolver-haryana.log") if re.search(live, l)])
if len(rows) > 56:   # the campaign and the record burst run from verdicts.py's second 40 to 55, about this node's 36 to 51
    print("resolver-haryana p99, median per second: before the campaign %.2f ms (s 15-33), during it %.2f ms (s 38-50)" % (
        st.median(rows[15:34]), st.median(rows[38:51])))
for l in open("resolver-mumbai.log"):
    if "rebuilt" in l:
        print(l.strip())
vl = [l for l in open("verdicts.log") if "verdicts/s" in l]
camp, calm = nums(r"to block ([0-9,]+)", [l for l in vl if "smishing" in l]), \
             nums(r"to block ([0-9,]+)", [l for l in vl if "smishing" not in l][2:])
if camp and calm:
    print("verdicts.py: median %.0f blocks/s, %.0f during the smishing campaign (%d s)" % (st.median(calm), st.median(camp), len(camp)))
e = nums(r"~([0-9.]+) MB/s", [l for l in open("records.log") if re.search(r"\] +[1-9][0-9,]* records/s", l)])
if e:
    print("call and SMS record neighbour: %d s, median %.1f MB/s, peak %.1f MB/s on its own topic" % (len(e), st.median(e), max(e)))
PY
echo "Kit OK on Apache Iggy 0.9.0"
