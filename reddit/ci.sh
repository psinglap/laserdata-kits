#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the REV2 safety signal lane at small scale for about a minute:
# 600 posts and comments a second (Reddit's published normal rate for its enrichment service; 2x for the first
# 15 s) with their text, spam and media signals on 4 partitions per topic, two signals joiners owning 2 partition
# numbers each, REV2's reader on the enriched topic, and a Time-Travel replay of the content stream next door
# from second 30. Pass only if each joiner holds the 50 ms p99 budget (last signal written to joined) and REV2
# the 100 ms budget (last signal written to REV2) in 95% of their steady-state seconds, the median join-rate is
# at least 97%, content past the wait limit still reaches REV2, REV2 acts on ML signals, and the replay ran.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst that stalls every partition at once.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py content_events.py signals_joiner.py rev2_reader.py time_travel.py kafka_bridge.py
vmstat 1 88 > vmstat.log 2>&1 &   # CPU, steal and I/O wait per second, printed if the latency check fails
# The joiners and REV2 start first and create the topics; the content and signals are written into them.
for j in 0 1; do
  timeout 85 python -u signals_joiner.py --joiner $j --joiners 2 --partitions 4 > joiner-$j.log 2>&1 &
done
timeout 85 python -u rev2_reader.py --partitions 4 > rev2.log 2>&1 &
sleep 3
timeout 80 python -u content_events.py --rate 600 --peak 2 --peak-s 15 --partitions 4 > content.log 2>&1 &
timeout 80 python -u time_travel.py --rate 20000 --at 30 --partitions 4 > timetravel.log 2>&1 &
wait || true
echo "--- content.log";    head -n 2 content.log; tail -n 2 content.log
echo "--- timetravel.log"; head -n 2 timetravel.log; tail -n 1 timetravel.log
for j in 0 1; do echo "--- joiner-$j.log"; head -n 3 joiner-$j.log; tail -n 3 joiner-$j.log; done
echo "--- rev2.log"; head -n 3 rev2.log; tail -n 3 rev2.log
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" joiner-*.log rev2.log | cut -c1-190 || true
status=0
# Skip a 10 s warm-up while the topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
check() {  # name, file, pattern of a live line
  local steady live fail
  steady=$(grep -E "$3" "$2" | tail -n +11 || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$1 steady-state seconds with live traffic: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
}
for j in 0 1; do check "joiner-$j" joiner-$j.log "\[joiner-$j\] +[1-9][0-9,]* joins/s"; done
check rev2 rev2.log "\[rev2\] +[1-9][0-9,]* enriched/s"
if [ "$status" -ne 0 ]; then
  echo "signal lane latency check failed"; echo "--- vmstat.log"; cat vmstat.log || true
  docker compose logs --tail 40 iggy || true; exit 1
fi
last=$(grep -E "\[rev2\] .* enriched/s" rev2.log | tail -n 1)
grep -Eq "actioned on ML signals +[0-9,]+/s \([1-9][0-9,]* total\)" <<<"$last" || { echo "REV2 took no action on ML signals"; exit 1; }
grep -Eq "without a signal +[0-9,]+/s \([1-9][0-9,]* total\)" <<<"$last" || { echo "no content went out past the wait limit"; exit 1; }
grep -Eq "\[time-travel\] +[1-9]" timetravel.log || { echo "the Time-Travel replay did not run"; exit 1; }
python - <<'PY'
import re, statistics as st, sys
def num(pat, ls):
    out = []
    for l in ls:                                  # skip lines the pattern doesn't match instead of failing
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
bad = False
for j in (0, 1):
    t = [l for l in open(f"joiner-{j}.log") if re.search(r"\] +[1-9][0-9,]* joins/s", l)][10:]
    p99 = num(r"p99 +([0-9.]+)ms", t)
    jr = num(r"\] .*? join-rate +([0-9.]+)%", t)
    if not p99 or not jr:
        continue
    print("joiner-%d steady-state medians over %d s: joins/s %.0f  last-signal-to-joined p50 %.1f ms  p99 %.1f ms  "
          "p99.9 %.1f ms  worst-second p99 %.1f ms  join-rate %.1f%% (5 s tumbling window on the same events %.1f%%)  "
          "content to enriched p50 %.0f ms  p99 %.0f ms (window writes out at p50 %.0f ms)" % (
          j, len(t), st.median(num(r"\] +([0-9,]+) joins/s", t)), st.median(num(r"p50 +([0-9.]+)ms", t)),
          st.median(p99), st.median(num(r"p99.9 +([0-9.]+)ms", t)), max(p99), st.median(jr),
          st.median(num(r"window join-rate +([0-9.]+)%", t)), st.median(num(r"enriched p50 +([0-9.]+)ms", t)),
          st.median(num(r"enriched p50 +[0-9.]+ms +p99 +([0-9.]+)ms", t)), st.median(num(r"window p50 +([0-9.]+)ms", t))))
    allp = num(r"p99 +([0-9.]+)ms", [l for l in open(f"joiner-{j}.log") if "joins/s" in l])
    before, during = allp[10:28], allp[30:]
    if before and during:
        print("joiner-%d p99 median before the replay %.1f ms (%d s), during it %.1f ms (%d s)" % (
              j, st.median(before), len(before), st.median(during), len(during)))
    if st.median(jr) < 97:
        print("joiner-%d median join-rate under 97%%" % j); bad = True
r = [l for l in open("rev2.log") if re.search(r"\] +[1-9][0-9,]* enriched/s.* (PASS|FAIL) \(", l)][10:]
if r:
    print("REV2 steady-state medians over %d s: enriched/s %.0f  last-signal-to-REV2 p50 %.1f ms  p99 %.1f ms  "
          "worst-second p99 %.1f ms  joiner-to-REV2 p99 %.1f ms  actions/s %.0f  time to action p50 %.0f ms  p99 %.0f ms" % (
          len(r), st.median(num(r"\] +([0-9,]+) enriched/s", r)), st.median(num(r"REV2 p50 +([0-9.]+)ms", r)),
          st.median(num(r"REV2 p50 +[0-9.]+ms +p99 +([0-9.]+)ms", r)), max(num(r"REV2 p50 +[0-9.]+ms +p99 +([0-9.]+)ms", r)),
          st.median(num(r"joiner-to-REV2 p99 +([0-9.]+)ms", r)), st.median(num(r"signals +([0-9,]+)/s", r)),
          st.median(num(r"action p50 +([0-9.]+)ms", r)), st.median(num(r"action p50 +[0-9.]+ms +p99 +([0-9.]+)ms", r))))
    print("REV2 at the end:", re.split(r"(?:PASS|FAIL) \(<[0-9]+ms\)", r[-1], 1)[-1].strip())
f = [l for l in open("content.log") if "posts and comments/s" in l]
print("content: median %.0f posts and comments/s, %.0f signals/s" % (
      st.median(num(r"\] +([0-9,]+) posts", f)), st.median(num(r"([0-9,]+) signals/s", f))))
tt = [l for l in open("timetravel.log") if "replayed" in l]
if tt:
    print("time-travel: median %.0f msgs/s replayed over %d s" % (st.median(num(r"\] +([0-9,]+) msgs/s", tt)), len(tt)))
sys.exit(1 if bad else 0)
PY
echo "Kit OK on Apache Iggy 0.9.0"
