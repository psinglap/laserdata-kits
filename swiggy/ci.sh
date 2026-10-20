#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the ping lane at small scale for about a minute,
# and pass only if the lane reports PASS and ETAs flow end to end and get checked against the truth.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py fleet.py track.py retry_storm.py confluent_bridge.py
KIT_DEBUG=1 timeout 80 python -u fleet.py --keep-hours 0 --partners 10000 --every 5 --assign-p 0.3 --wait-s 10 40 > fleet.log 2>&1 &
sleep 5
KIT_DEBUG=1 timeout 70 python -u track.py --refresh 5 > track.log 2>&1 || true
wait || true
echo "--- fleet.log"; grep -v debug fleet.log | tail -n 8; grep debug fleet.log | tail -n 30
echo "--- track.log"; grep -v debug track.log | tail -n 8; grep debug track.log | head -n 60
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" track.log | cut -c1-120 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[track\] +[1-9][0-9,]* pings/s' track.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if [ "$live" -lt 30 ] || [ $(( fail * 100 )) -gt $(( (live + fail) * 5 )) ]; then echo "lane check failed"; exit 1; fi
grep -Eq "ETAs published [1-9]" track.log || { echo "no ETAs published"; exit 1; }
grep -Eq "ETAs checked [1-9]" fleet.log || { echo "no ETAs checked against the truth"; exit 1; }
# The retry-storm neighbour must at least run against the same broker.
timeout 10 python -u retry_storm.py --mbps 0.2 --x 5 --at 3 --for 4 > storm.log 2>&1 || true
echo "--- storm.log"; cat storm.log
grep -q "storm 5x" storm.log || { echo "retry storm did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
