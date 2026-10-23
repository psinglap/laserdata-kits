#!/usr/bin/env bash
# CI smoke test: Apache Iggy 0.9.0 and ClickHouse 25.8. Collectors write OpenTelemetry log events into Iggy for
# about 80 s (with a 3x spike), and the pipe inserts them into otel_logs. ClickHouse is stopped for 12 s mid-run and
# the pipe is killed with SIGKILL and restarted later. Pass only if event-to-ClickHouse freshness stays inside 1 s in
# 95% of steady-state seconds, the outage and the restart both happened and were recovered, ClickHouse dropped the
# deliberately repeated blocks, and every event is in ClickHouse exactly once (no missing row, duplicate or gap).
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 30 iggy clickhouse || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which stalled every reader on the runner for
# 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
for _ in $(seq 1 60); do curl -sf http://127.0.0.1:8123/ping >/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py collectors.py iggy_pipe.py verify.py kafka_shadow.py

# The pipe re-sends every 20th block after it landed, as if the acknowledgement had been lost.
python -u iggy_pipe.py --repeat-every 20 > pipe.log 2>&1 &
PIPE=$!
sleep 3
# 2,000 events/s from 2,000 sources for 80 s, 3x for 8 s from 15 s.
timeout 95 python -u collectors.py --rate 2000 --sources 2000 --duration 80 --spike-x 3 --spike-at 15 --spike-for 8 \
  > collectors.log 2>&1 &
COLL=$!
sleep 30
echo "--- $(date +%T) stopping ClickHouse for 12 s"
docker compose stop -t 5 clickhouse
sleep 12
docker compose start clickhouse
echo "--- $(date +%T) ClickHouse started"
sleep 18
echo "--- $(date +%T) killing the pipe with SIGKILL and restarting it"
kill -9 "$PIPE" || true
sleep 1
python -u iggy_pipe.py --repeat-every 20 >> pipe.log 2>&1 &
PIPE=$!
wait "$COLL" || true
python -u verify.py --wait-s 60 | tee verify.log || true
kill "$PIPE" || true

echo "--- collectors.log"; head -n 2 collectors.log; tail -n 2 collectors.log
echo "--- pipe.log (outage, restart and the last lines)"
grep -E "insert failed|is back|unavailable for 1?[05] s|restored|no pipe-state|catching up" pipe.log || true
grep -v totals pipe.log | tail -n 4; grep totals pipe.log | tail -n 1
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" pipe.log | cut -c1-170 || true
# Skip a 10 s warm-up; a shared CI runner may blip, so require 95% of seconds to pass. Seconds while ClickHouse is
# down or the pipe is catching up are reported separately and not counted.
steady=$(grep -E '\[pipe\] +[1-9][0-9,]* rows/s' pipe.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
log = open("pipe.log").read().splitlines()
s = [l for l in log if re.search(r"\[pipe\] +[1-9][0-9,]* rows/s", l)][10:]
print("steady-state medians: rows/s %.0f  inserts/s %.0f  event-to-ClickHouse p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, s)) for q in (r"\] +([0-9,]+) rows/s", r"rows/s in ([0-9]+) inserts", r"p50 +([0-9.]+)ms",
                                    r"p99 +([0-9.]+)ms", r"p99\.9 +([0-9.]+)ms")))
print("steady-state p99, worst second %.1f ms over %d seconds" % (max(nums(r"p99 +([0-9.]+)ms", s)), len(s)))
held = nums(r"unavailable for [0-9]+ s: .*; ([0-9,]+) events held", log)
print("ClickHouse outage: up to %d events held in Iggy" % max(held or [0]))
catch = [l for l in log if "catching up" in l]
print("catching-up seconds %d (start, outage, restart); oldest held event %.1f s" % (
    len(catch), max(nums(r"oldest ([0-9.]+) s", catch) or [0])))
back = next((i for i, l in enumerate(log) if "is back after" in l), None)
if back is not None:
    drain = []
    for l in log[back + 1:]:
        if "catching up" in l:
            drain.append(l)
        elif re.search(r"rows/s in", l):
            break
    print("after the outage: %d catching-up seconds, %d held events inserted, then live again" % (
        len(drain), sum(nums(r"inserted ([0-9,]+) held", drain))))
# The pipe is killed and restarted mid-run, so add up the last totals line of each process.
runs, cur = [], []
for l in log:
    if "restored from" in l or "no pipe-state" in l:
        runs.append(cur)
        cur = []
    cur.append(l)
runs.append(cur)
f = {}
for run in runs:
    t = [l for l in run if "totals:" in l]
    if t:
        for k, v in re.findall(r"(inserted|repeats|twice|retries|restart|outage) ([0-9.,]+)", t[-1]):
            f[k] = f.get(k, 0) + float(v.replace(",", ""))
print("totals: inserted %d  dropped by ClickHouse as repeats %d  written twice %d  insert retries %d  "
      "replayed on restart %d  outage %.1f s" % (f["inserted"], f["repeats"], f["twice"], f["retries"], f["restart"], f["outage"]))
r = [l for l in log if "re-inserted the block in flight" in l]
if r:
    print(r[-1])
if not any("is back after" in l for l in log):
    raise SystemExit("the ClickHouse outage was not seen or not recovered")
if not r:
    raise SystemExit("the restarted pipe did not restore from pipe-state")
if f["repeats"] < 1:
    raise SystemExit("no repeated block was dropped by ClickHouse: deduplication was not exercised")
if f["twice"]:
    raise SystemExit("a repeated block was written twice")
PY
grep -q "\[verify\] OK" verify.log || { echo "exactly-once check failed"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0 and ClickHouse 25.8"
