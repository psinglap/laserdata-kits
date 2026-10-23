#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run the SIEM outage lane at small scale for about 85 s: one Worker Node
# writes a security Route's events once (800/s from 600 hosts, 8 partitions); Cribl Lake and the SIEM read them from
# their own offsets; the SIEM is down from its second 25 to 45, then gets fresh events first while its held range
# drains behind them at 1,500/s; a new SIEM joins at second ~50 and backfills the whole log behind live. Also checks
# that the Webhook receiver writes a gzip NDJSON request from Cribl Stream into the log.
# Pass only if each Destination holds the 100 ms p99 budget for live events in 95% of its steady-state seconds
# (the SIEM's included while it drains), the outage happened, both drains finish, and no event is lost.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which stalled every reader 0.2-0.8 s at once
# on the shared runner; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py workers.py destination.py cribl_webhook.py

# 1. The Webhook receiver: one gzip NDJSON request of 500 events, as Cribl Stream's Webhook Destination sends it.
timeout 25 python -u cribl_webhook.py --topic webhook_smoke --partitions 4 > webhook.log 2>&1 &
for _ in $(seq 1 20); do (echo > /dev/tcp/127.0.0.1/10080) 2>/dev/null && break; sleep 1; done
python - <<'PY'
import asyncio, gzip, json, urllib.request
from common import STREAM, connect
body = "\n".join(json.dumps({"_time": 1761000000 + i / 10, "host": f"fw-smoke{i % 7}", "sourcetype": "firewall",
                             "_raw": f"action=allow n={i}"}) for i in range(500)).encode()
req = urllib.request.Request("http://127.0.0.1:10080/", data=gzip.compress(body), method="POST",
                             headers={"Content-Type": "application/x-ndjson", "Content-Encoding": "gzip"})
print("webhook answered", urllib.request.urlopen(req, timeout=10).status)
async def count():
    c = await connect("iggy+tcp://iggy:iggy@127.0.0.1:8090?nodelay=true")
    return (await c.get_topic(STREAM, "webhook_smoke")).messages_count
n = asyncio.run(count())
print("webhook_smoke messages:", n)
assert n == 500, n
PY

# 2. The lane. Destinations start first; workers.py creates the topic.
timeout 88 python -u destination.py --dest cribl_lake > lake.log 2>&1 &
timeout 88 python -u destination.py --dest splunk_lb --outage-at 25 --outage-for 20 --drain-rate 1500 > siem.log 2>&1 &
sleep 2
timeout 84 python -u workers.py --rate 800 --processes 2 --hosts 600 --partitions 8 > workers.log 2>&1 &
sleep 48
timeout 36 python -u destination.py --dest new_siem --backfill --drain-rate 1500 > new.log 2>&1 &
wait || true
echo "--- webhook.log"; cat webhook.log
echo "--- workers.log"; head -n 2 workers.log; tail -n 2 workers.log
for f in lake siem new; do echo "--- $f.log"; head -n 3 $f.log; tail -n 4 $f.log; done
echo "--- SIEM outage (first, last), back, drain (first, last) and drain end"
(grep "destination down: delivered" siem.log | sed -n '1p;$p'; grep -E "destination back|backfill from" siem.log new.log
 grep "| drained" siem.log | sed -n '1p;$p'; grep "backlog drained:" siem.log new.log) || true
echo "--- FAIL seconds (file:line: output)"; grep -n "FAIL (" lake.log siem.log new.log | cut -c1-170 || true
status=0
# Skip a 10 s warm-up (3 s for the late joiner); a shared CI runner may blip, so require 95% of seconds to pass.
check() {  # file, label, warm-up lines, minimum live seconds
  steady=$(grep -E "\] +[1-9][0-9,]* events/s live" "$1" | tail -n +$(( $3 + 1 )) || true)
  live=$(grep -c 'PASS (' <<<"$steady" || true)
  fail=$(grep -c 'FAIL (' <<<"$steady" || true)
  echo "$2 steady-state seconds with live events: PASS $live, FAIL $fail"
  if ! { [ "$live" -ge "$4" ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then status=1; fi
}
check lake.log cribl_lake 10 30
check siem.log splunk_lb 10 30
check new.log new_siem 3 15
[ "$status" -eq 0 ] || { echo "latency check failed"; exit 1; }
grep -q "destination down: delivered 0/s" siem.log || { echo "the SIEM outage never happened"; exit 1; }
grep -q "backlog drained: .*, 0 events lost" siem.log || { echo "the SIEM did not drain its held range cleanly"; exit 1; }
grep -q "backlog drained: .*, 0 events lost" new.log || { echo "the new SIEM did not finish its backfill cleanly"; exit 1; }
if grep -hE "events/s live|destination down: delivered" lake.log siem.log new.log | grep -vEq " lost 0( |$)"; then
  echo "events were lost"; exit 1; fi
grep -Eq "\[worker-00\] +[1-9]" workers.log || { echo "the workers did not run"; exit 1; }
python - <<'PY'
import re, statistics as st
def num(pat, ls):
    out = []
    for l in ls:                                  # skip lines the pattern doesn't match instead of failing
        m = re.search(pat, l)
        if m:
            out.append(float(m.group(1).replace(",", "")))
    return out
live = r"\] +([1-9][0-9,]*) events/s live"
for f, label, warm in (("lake.log", "cribl_lake", 10), ("siem.log", "splunk_lb", 10), ("new.log", "new_siem", 3)):
    t = [l for l in open(f) if re.search(live, l)][warm:]
    p99 = num(r"p99 +([0-9.]+)ms", t)
    if not p99:
        continue
    print("%s steady-state medians over %d s: events/s %.0f  p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms  "
          "worst-second p99 %.1f ms" % (label, len(t), st.median(num(live, t)), st.median(num(r"p50 +([0-9.]+)ms", t)),
          st.median(p99), st.median(num(r"p99.9 +([0-9.]+)ms", t)), max(p99)))
for f, label in (("siem.log", "splunk_lb"), ("new.log", "new_siem")):
    t = [l for l in open(f) if re.search(live, l)][3:]
    draining = [l for l in t if "| drained" in l]
    calm = [l for l in t if "| drained" not in l]
    if draining:
        print("%s p99 for fresh events, median per second: while draining %.1f ms (%d s), otherwise %.1f ms (%d s); "
              "drain rate median %.0f/s" % (label, st.median(num(r"p99 +([0-9.]+)ms", draining)), len(draining),
              st.median(num(r"p99 +([0-9.]+)ms", calm) or [0]), len(calm), st.median(num(r"drained ([0-9,]+)/s", draining))))
down = [l for l in open("siem.log") if "destination down: delivered" in l]
waits = num(r"waiting ([0-9,]+) in the log", down)
mb = num(r"in the log \(([0-9.]+) MB\)", down)
print("splunk_lb outage: down %d s, largest wait %s events (%s MB) in the log; %s" % (
    len(down), f"{max(waits):,.0f}" if waits else "n/a", f"{max(mb):.1f}" if mb else "n/a",
    next((l.split("] ", 1)[1].strip() for l in open("siem.log") if "backlog drained:" in l), "no drain line")))
PY
echo "Kit OK on Apache Iggy 0.9.0"
