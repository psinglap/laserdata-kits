#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the crash spike lane at small scale for about 80 s, with full ingest
# batches next to it, and pass only if the lane reports PASS, both spikes reach Slack within a second of the batch
# that tipped them (the exact batch the ground truth names, although 1% of records arrive twice), nothing else
# alerts, and the hourly job over the same batches is later for spike A and never fires for spike B.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst, about 30 s after a
# heavy neighbour starts, stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py spike_judge.py ingest_worker.py ingest_flood.py clickhouse_shadow.py
# The spike check starts first and creates the topics; the apps and the neighbour follow.
# 50 apps, 1,500 ingest batches/s from 20,000 concurrent sessions, 1% of records sent twice. The hour is cut to
# 30 s so the run fits in a minute and a half: a 30 s window and an "hourly" job every 30 s, at +18, +48 and +78 s.
# Spike A: 12 crashes/s of one issue from +20 s for 14 s. Spike B: 128 ANRs of one issue from +40 s to +56 s,
# half before and half after the run at +48 s. 2 MB/s of full ingest batches from +15 s.
W="--window-s 30"
timeout 90 python -u spike_judge.py $W > judge.log 2>&1 &
sleep 3
timeout 84 python -u ingest_worker.py $W --apps 50 --batches 1500 --sessions 20000 --session-s 20 --issues 100 \
  --crash-p 0.002 --anr-p 0.001 --check-every-s 30 --check-phase-s 18 --spike-a-at 20 --spike-a-for 14 \
  --spike-a-rate 12 --spike-b-after 40 --spike-b-for 16 --spike-b-count 128 > worker.log 2>&1 &
timeout 75 python -u ingest_flood.py --mbps 2 --at 15 --duration 45 > ingest_batch.log 2>&1 &
wait || true
echo "--- worker.log";       head -n 1 worker.log; grep -E "TRUTH|ALERT|\[hourly\]" worker.log || true; tail -n 2 worker.log
echo "--- judge.log";        head -n 1 judge.log; tail -n 3 judge.log
echo "--- ingest_batch.log"; head -n 2 ingest_batch.log; tail -n 2 ingest_batch.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" judge.log | cut -c1-400 || true
echo "--- worker stalls over 20 ms (line: output)"; grep -nE "stall gc ([2-9][0-9]|[0-9]{3,})ms|loop ([2-9][0-9]|[0-9]{3,})ms" worker.log | sed -E "s/^([0-9]+):.*(stall.*)/\1: \2/" || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[spike-check\] +[1-9][0-9,]* batches/s' judge.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the spike checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
allj = [l for l in open("judge.log") if re.search(r"\[spike-check\] +[1-9][0-9,]* batches/s", l)]
r = allj[10:]
print("steady-state medians: batches/s %.0f  batch-to-check p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, r)) for q in (r"\] +([0-9,]+) batches/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", allj)
if len(lane) > 50:
    print("batch-to-check p99, median per second: before the neighbour %.1f ms (s 3-11), with 2 MB/s of ingest batches %.1f ms (s 20-50)" % (
        st.median(lane[3:12]), st.median(lane[20:51])))
w = open("worker.log").read()
wl = [l for l in w.splitlines() if re.search(r"\[ingest-worker\] +[0-9,]+ batches/s", l)]
last = wl[-1]
dupes = nums(r"\(([0-9,]+) total\)", allj[-1:])
print("records sent twice: %d  skipped by the spike check as repeats: %d" % (nums(r"repeats sent ([0-9,]+)", [last])[0], dupes[0] if dupes else 0))
if not dupes or dupes[0] < 100:
    raise SystemExit("too few repeated records to test counting once")
for s in ("A", "B"):
    t = re.search(r"TRUTH spike %s tips at batch (\S+), ([0-9.]+) s after it began: ([0-9]+) occurrences, ([0-9,]+) sessions, ([0-9.]+)%%" % s, w)
    a = re.search(r"ALERT [^:]+: spike %s \([^)]*\): at Slack ([0-9.]+) ms after the tipping batch; ([0-9.]+) s after the spike began; tip batch matches the truth: (\w+)" % s, w)
    if not t or not a:
        raise SystemExit("spike %s: no %s" % (s, "ground truth" if not t else "alert at Slack"))
    print("spike %s tipped %s s after it began (%s occurrences, %s sessions, %s%%); Slack had it %s ms after the tipping batch; tip batch matches: %s" % (
        s, t.group(2), t.group(3), t.group(4), t.group(5), a.group(1), a.group(3)))
    if a.group(3) != "yes":
        raise SystemExit("spike %s alerted on a different batch than the ground truth" % s)
    if float(a.group(1)) > 1000:
        raise SystemExit("spike %s reached Slack more than a second after its tipping batch" % s)
ha = re.search(r"spike A: Slack ([0-9.]+) s, hourly job \+([0-9.]+) s, after Slack before the hourly job ([0-9]+)", last)
hb = re.search(r"spike B: Slack ([0-9.]+) s, hourly job (not yet|\+[0-9.]+ s)", last)
runs = re.findall(r"\[hourly\] run at \+([0-9.]+) s", w)
print("hourly runs at +%s s" % ", +".join(runs))
if not ha:
    raise SystemExit("the hourly job never alerted spike A in this run")
print("spike A: Slack %s s after it began, the hourly job %s s after it began; %s of its crashes came after Slack had it and before the hourly job would" % ha.groups())
if not hb or hb.group(2) != "not yet" or len(runs) < 3:
    raise SystemExit("spike B: the hourly job should never alert it (half of it falls in each run's window)")
print("spike B: Slack %s s after it began; the hourly job never alerts it (runs at +%s s)" % (hb.group(1), ", +".join(runs)))
for k in ("unexpected", "tip mismatches", "hourly-job alerts for other issues"):
    v = nums(k + r" ([0-9,]+)", [last])
    if not v or v[0]:
        raise SystemExit("%s: %s" % (k, v))
print("alerts for other issues: 0 live, 0 hourly; tip mismatches 0")
e = [l for l in open("ingest_batch.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("ingest-batch neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[ingest-batch\] done" ingest_batch.log || { echo "the ingest-batch neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
