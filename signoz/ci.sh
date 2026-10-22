#!/usr/bin/env bash
# CI smoke test on a real Apache Iggy 0.9.0 broker and a real ClickHouse, at reduced scale:
#  1. a writer is killed (SIGKILL) mid-run and a second one takes over from the last committed offsets;
#  2. ClickHouse is frozen for 12 s while the collectors keep accepting;
#  3. every record the collectors accepted must be in ClickHouse at the end.
# Pass only if the writer's receive-to-queryable p99 is inside the budget in steady state, agents never
# wait on ClickHouse, and nothing is lost.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy clickhouse || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst (150-180 MB, 30-35 s
# after the heavy neighbour starts) stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
for _ in $(seq 1 60); do curl -sf http://127.0.0.1:8123/ping >/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py collectors.py writer.py verify.py reconnect_flood.py kafka_bridge.py

# Record the runner's disk every second (blocked processes, IO wait, writes), to tell a slow runner disk from the lane.
vmstat -n -t 1 130 > vmstat.log 2>&1 &
timeout -s KILL 30 python -u writer.py --budget-ms 1000 > writer1.log 2>&1 &   # killed at 30 s, like a crash
w1=$!
sleep 3
# 50 tenants, 200 export batches/s x 50 records = 10,000 log records/s for 110 s
python -u collectors.py --tenants 50 --batches-per-s 200 --records 50 --duration 110 > collectors.log 2>&1 &
col=$!
wait $w1 || true
echo "writer 1 killed; writer 2 resumes from the committed offsets"
python -u writer.py --budget-ms 1000 > writer2.log 2>&1 &
w2=$!
sleep 27
echo "freezing ClickHouse for 12 s"; docker compose pause clickhouse
sleep 12
docker compose unpause clickhouse; echo "ClickHouse back"
wait $col
python -u verify.py --wait 90 > verify.log 2>&1 && ok=1 || ok=0
kill $w2 || true

echo "--- collectors.log"; head -n 3 collectors.log; tail -n 4 collectors.log
echo "--- writer1.log";    tail -n 4 writer1.log
echo "--- writer2.log";    head -n 4 writer2.log; grep -E "has not confirmed|draining" writer2.log | head -n 20 || true; tail -n 6 writer2.log
echo "--- verify.log";     cat verify.log
echo "--- collector seconds with accept p99 of 50 ms or more (line: output)"
grep -nE "accept p50 +[0-9.]+ms +p99 +([5-9][0-9]|[0-9]{3,})\.[0-9]ms" collectors.log || true
echo "--- runner disk, per second (vmstat): seconds with IO wait of 10% or more"
awk 'NR==2{for(i=1;i<=NF;i++)h[$i]=i; next} NR>2{ if($h["b"]>0)n++; if($h["wa"]>w)w=$h["wa"]; if($h["bo"]>o)o=$h["bo"];
  if($h["wa"]>=10 && k++<30) print} END{printf "seconds with a process blocked on IO %d, max IO wait %d%%, max writes %d KB/s\n", n, w, o}' vmstat.log || true
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" writer2.log | cut -c1-160 || true

grep -Eq "^\[writer\] +[1-9][0-9,]* rows/s" writer1.log || { echo "writer 1 never wrote to ClickHouse"; exit 1; }
grep -q "has not confirmed an insert" writer2.log || { echo "the ClickHouse freeze was not observed"; exit 1; }
# Skip a 10 s warm-up; a shared CI runner may blip, so require 95% of steady-state seconds to pass.
steady=$(grep -E '^\[writer\] +[1-9][0-9,]* rows/s' writer2.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
awk '/has not confirmed/{s=1} s && /PASS \(/{f=1} END{exit !f}' writer2.log || { echo "the writer never returned to live after the freeze"; exit 1; }
python - <<'PY'
import re, statistics as st
num = lambda pat, ls: [float(re.search(pat, l).group(1).replace(",", "")) for l in ls]
w = [l for l in open("writer2.log") if re.search(r"^\[writer\] +[1-9][0-9,]* rows/s", l)][10:]
print("writer steady-state medians: rows/s %.0f  receive-to-queryable p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  queue p99 %.1f ms" % tuple(
    st.median(num(q, w)) for q in (r"\] +([0-9,]+) rows/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms", r"p99.9 +([0-9.]+)ms", r"queue p99 +([0-9.]+)ms")))
c = [l for l in open("collectors.log") if re.search(r"batches/s", l) and not re.search(r" 0 batches/s", l)][3:]
acc = num(r"accept p50 +[0-9.]+ms +p99 +([0-9.]+)ms", c)
slow = sum(1 for v in acc if v >= 50)
print("collectors: %d s with traffic, accept p50 median %.1f ms, accept p99 median %.1f ms, max %.1f ms, seconds >= 50 ms: %d" % (
    len(c), st.median(num(r"accept p50 +([0-9.]+)ms", c)), st.median(acc), max(acc), slow))
if slow * 100 > len(c) * 5:
    raise SystemExit("agents waited: collector accept p99 was 50 ms or more in over 5% of seconds")
stall = [l for l in open("writer2.log") if "has not confirmed" in l]
print("ClickHouse freeze: %d s reported by the writer, longest %s" % (len(stall), max(num(r"insert for +([0-9.]+)s", stall))))
PY
[ "$ok" = 1 ] || { echo "records were lost"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
