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
live=$(grep -Ec '\[revalidate\] +[1-9][0-9,]* changes/s.*PASS' revalidate.log || true)
fail=$(grep -Ec '\[revalidate\] +[1-9][0-9,]* changes/s.*FAIL' revalidate.log || true)
echo "seconds with live traffic: PASS $live, FAIL $fail"
[ "$live" -ge 20 ] && [ "$fail" -eq 0 ]
grep -Eq "orders accepted [1-9]" hubs.log
echo "Kit OK on Apache Iggy 0.9.0"
