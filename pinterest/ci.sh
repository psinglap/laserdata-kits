#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the in-flight spend lane at small scale for about a minute:
# three ad servers running auctions over the same ad groups, each inserting only ad groups with budget left,
# 0.5% of insertions delivered twice, ad insertion logs next door from second 20, and one ad server
# restarted mid-run. Then a 50 s what-if: the same three servers applying each other's spend 2 s late, as a
# slower spend pipeline would.
# Pass only if each long-running ad server holds the 50 ms p99 budget in 90% of its steady-state seconds,
# no sequence gap anywhere, repeats are dropped, ad groups reach their budgets, overdelivery stays under 0.1%
# of spend in every full budget day, and the restarted server rebuilds its budget view from the log.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst (150-180 MB, 30-35 s
# after the heavy neighbour starts) stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py ad_server.py insertion_logs.py kafka_bridge.py
# Per server 400 auctions a second over 2,000 ad groups; budget days of 20 s (a compressed day).
ARGS="--rate 400 --adgroups 2000 --day-s 20"
timeout 78 python -u ad_server.py --name ads-1 $ARGS > ads-1.log 2>&1 &
sleep 1
timeout 77 python -u ad_server.py --name ads-2 $ARGS > ads-2.log 2>&1 &
timeout 74 python -u insertion_logs.py --mbps 2 --at 20 --duration 35 > logs.log 2>&1 &
# ads-3 runs, stops at 34 s and starts again: on restart it must rebuild its budget view from the log first.
timeout 34 python -u ad_server.py --name ads-3 $ARGS > ads-3.log 2>&1 || true
timeout 40 python -u ad_server.py --name ads-3 $ARGS > ads-3-restart.log 2>&1 || true
wait || true
# What-if: every server applies the others' spend only once it is 2 s old.
for n in 1 2 3; do
  timeout 50 python -u ad_server.py --name hold-$n $ARGS --hold-ms 2000 > hold-$n.log 2>&1 &
done
wait || true
for f in ads-1 ads-2 ads-3 ads-3-restart hold-1; do
  echo "--- $f.log"; grep -E "rebuilt" $f.log || true; grep "^\[day\]" $f.log | tail -n 3 || true; tail -n 2 $f.log
done
echo "--- logs.log"; head -n 2 logs.log; tail -n 2 logs.log
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" ads-1.log ads-2.log | cut -c1-200 || true
status=0
# Skip each server's first 10 s (warm-up); a shared CI runner may blip, so require 90% of the remaining seconds
# with spend from other servers to pass.
for s in ads-1 ads-2; do
  steady=$(grep -E "^\[$s\] .*from other servers +[1-9][0-9,]*/s" $s.log | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$s steady-state seconds with live spend: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 10 )) ]; }; then status=1; fi
done
for f in ads-1 ads-2 ads-3 ads-3-restart; do
  gaps=$(grep -Eo 'gaps [0-9]+' $f.log | tail -n 1 | cut -d' ' -f2 || true)
  echo "$f.log sequence gaps: ${gaps:-none}"
  [ "${gaps:-1}" = "0" ] || status=1
done
[ "$status" -eq 0 ] || { echo "in-flight spend lane check failed"; exit 1; }
grep -Eq 'rebuilt the budget view from the log in [0-9.]+s: [1-9][0-9,]* ad groups' ads-3-restart.log \
  || { echo "the restarted ad server did not rebuild its budget view from the log"; exit 1; }
grep -q "\[logs\] done" logs.log || { echo "the insertion-log neighbour did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
live = r"from other servers +[1-9][0-9,]*/s"
for s in ("ads-1", "ads-2"):
    t = [l for l in open(f"{s}.log") if l.startswith(f"[{s}]") and re.search(live, l)][10:]
    if t:
        p99 = nums(r"p99 +([0-9.]+)ms", t)
        print("%s steady-state medians over %d s: insertions/s %.0f  from other servers/s %.0f  spend-to-budget-view "
              "p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  worst-second p99 %.1f ms" % (
              s, len(t), st.median(nums(r"\] +([0-9,]+) insertions", t)), st.median(nums(r"servers +([0-9,]+)/s", t)),
              st.median(nums(r"p50 +([0-9.]+)ms", t)), st.median(p99), st.median(nums(r"p99.9 +([0-9.]+)ms", t)), max(p99)))
rows = nums(r"p99 +([0-9.]+)ms", [l for l in open("ads-1.log") if l.startswith("[ads-1]") and re.search(live, l)])
if len(rows) > 55:   # the insertion logs run from about second 20 to 55
    print("ads-1 p99, median per second: before the insertion logs %.1f ms (s 10-19), with 2 MB/s next door %.1f ms (s 24-52)" % (
        st.median(rows[10:20]), st.median(rows[24:53])))
rep = nums(r"repeats dropped ([0-9,]+)", open("ads-1.log").readlines())
print("ads-1 repeats dropped by event ID: %d" % (rep[-1] if rep else 0))
if not rep or rep[-1] == 0:
    raise SystemExit("no repeated insertion was dropped: de-duplication did not run")
PAT = (r"spend \$([0-9,.]+)  ad groups at budget ([0-9,]+)  overdelivery \$([0-9,.]+) \(([0-9.]+)% of spend, "
       r"([0-9,]+) insertions? past budget\)")
def days(f):
    out = []
    for l in open(f):
        m = re.search(PAT, l)
        if m:
            out.append(tuple(float(g.replace(",", "")) for g in m.groups()))
    return out
d = days("ads-1.log")[1:]                   # the first day was partly before the servers started
for s, at, o, pct, fr in d:
    print("day: spend $%.2f  ad groups at budget %d  overdelivery $%.2f (%.3f%% of spend, %d insertions past budget)" % (
          s, at, o, pct, fr))
if len(d) < 2 or any(at == 0 for _, at, *_ in d):
    raise SystemExit("ad groups did not reach their budgets in enough full days")
if any(pct >= 0.1 for _, _, _, pct, _ in d):
    raise SystemExit("overdelivery reached 0.1% of spend in a day")
r = re.search(r"rebuilt the budget view from the log in ([0-9.]+)s: ([0-9,]+) ad groups, ([0-9,]+) insertions", open("ads-3-restart.log").read())
if r:
    print("restarted ad server rebuilt its budget view in %s s: %s ad groups, %s insertions" % r.groups())
for s, at, o, pct, fr in days("hold-1.log")[1:]:
    print("what-if, spend applied 2 s late: spend $%.2f  ad groups at budget %d  overdelivery $%.2f (%.3f%% of spend, "
          "%d insertions past budget)" % (s, at, o, pct, fr))
PY
echo "Kit OK on Apache Iggy 0.9.0"
