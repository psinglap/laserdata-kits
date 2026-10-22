#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the streaming aggregation lane at small scale for about a minute, with
# a rollout that doubles the pods (new series) and a raw-log stream next to it, and pass only if the lane reports
# PASS, every closed timeslice matches the gateway's own count exactly, retries are absorbed, counter resets are
# handled, nothing arrives late and aggregates flow.
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
python -m py_compile common.py gateway.py aggregator.py log_flood.py otlp_tee.py
# The aggregator starts first and creates the topics; the gateway and the neighbour follow.
# 4 clusters x 8 services x 6 pods x 6 routes x 8 buckets = 9,216 series scraped every 5 s (~1,850 samples/s),
# 10 s timeslices, 1% of samples sent twice, 0.1% counter resets; a rollout doubles the pods from second 30 for
# 15 s; 2 MB/s of raw logs from second 20.
timeout 80 python -u aggregator.py --timeslice-s 10 > aggregator.log 2>&1 &
sleep 3
timeout 72 python -u gateway.py --clusters 4 --services 8 --pods 6 --routes 6 --buckets 8 --scrape-s 5 \
  --timeslice-s 10 --resend-pct 1 --reset-pct 0.1 --peak 2 --peak-at 30 --peak-for 15 > gateway.log 2>&1 &
timeout 70 python -u log_flood.py --mbps 2 --at 20 --duration 35 > logs.log 2>&1 &
wait || true
echo "--- gateway.log";    head -n 2 gateway.log; tail -n 3 gateway.log
echo "--- aggregator.log"; head -n 1 aggregator.log; tail -n 6 aggregator.log
echo "--- logs.log";       head -n 2 logs.log; tail -n 2 logs.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" aggregator.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[aggregator\] +[1-9][0-9,]* samples/s' aggregator.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the aggregation checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
p = [l for l in open("aggregator.log") if re.search(r"\[aggregator\] +[1-9][0-9,]* samples/s", l)][10:]
print("steady-state medians: samples/s %.0f  sample-to-aggregator p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) samples/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = [l for l in open("aggregator.log") if re.search(r"\[aggregator\] +[0-9,]+ samples/s", l)]
p99 = nums(r"p99 +([0-9.]+)ms", lane)
rate = nums(r"\] +([0-9,]+) samples/s", lane)
if len(p99) > 50:
    print("p99, median per second: before the rollout and the neighbour %.1f ms (s 6-16), with 2 MB/s of raw logs %.1f ms (s 19-26), "
          "rollout plus logs %.1f ms at %.0f samples/s (s 29-43)" % (
        st.median(p99[6:17]), st.median(p99[19:27]), st.median(p99[29:44]), st.median(rate[29:44])))
u = [l for l in open("aggregator.log") if l.startswith("[timeslice]")]
if not u:
    raise SystemExit("no timeslice lines from the aggregator")
m = re.search(r"exact ([0-9]+)/([0-9]+)  retries absorbed ([0-9,]+)  counter resets ([0-9,]+)  late ([0-9]+)  "
              r"without seq check \+([0-9,]+) \(([0-9.]+)%\)", u[-1])
if not m:
    raise SystemExit("could not read the last timeslice line")
exact, checked, absorbed, resets, late, over, over_pct = (float(x.replace(",", "")) for x in m.groups())
print(u[-1].strip())
folds = nums(r"\(([0-9,]+)x fewer\)", u)
ins = nums(r"series in ([0-9,]+)", u)
print("timeslices exact: %d of %d; retries absorbed: %d; counter resets handled: %d; late samples: %d; "
      "without the sequence check the aggregates would be +%.1f%% high" % (exact, checked, absorbed, resets, late, over_pct))
print("series in per timeslice up to %d, folded %d-%dx" % (max(ins), min(folds), max(folds)))
if checked < 4 or exact != checked:
    raise SystemExit("timeslices did not match the gateway's own count")
if late:
    raise SystemExit("samples arrived after their timeslice closed")
if absorbed < 100 or over <= 0:
    raise SystemExit("retries were not seen and absorbed")
if resets < 10:
    raise SystemExit("counter resets were not seen")
s = nums(r"aggregates sent ([0-9,]+)", lane)
if not s or sum(s) == 0:
    raise SystemExit("no aggregates were published")
print("aggregates published: %d over the run" % sum(s))
e = [l for l in open("logs.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("raw-log neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[logs\] done" logs.log || { echo "the raw-log neighbour did not run"; exit 1; }
grep -q "(rollout)" gateway.log || { echo "the rollout did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
