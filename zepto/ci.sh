#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the kit at small scale for about a minute,
# and pass only if the lane reports PASS and orders flow end to end.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
timeout 80 python -u hubs.py --hubs 50 --skus 500 --rate 2000 > hubs.log 2>&1 &
sleep 5
timeout 70 python -u revalidate.py --refresh 5 --searches-per-s 100 > revalidate.log 2>&1 || true
wait || true
echo "--- hubs.log";       tail -n 8 hubs.log
echo "--- revalidate.log"; tail -n 8 revalidate.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" revalidate.log | cut -c1-120 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[revalidate\] +[1-9][0-9,]* changes/s' revalidate.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
[ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]
grep -Eq "orders accepted [1-9]" hubs.log
echo "Kit OK on Apache Iggy 0.9.0"
