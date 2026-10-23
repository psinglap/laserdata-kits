#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0 and run one building's carton lane for about a minute: 120 new cartons/s
# (2x for ten seconds), six events each (about 720 carton events/s), 1% sent twice as handheld retries, 3 MB/s of
# control-system telemetry on a neighbouring topic from second 20, and the building's link to AWS down for 15 s
# mid-run. The building's broker and the AWS broker are one broker here (two streams).
# Pass only if the floor view holds its 50 ms p99 budget in 95% of steady-state seconds (outage included), with no
# gaps and retries absorbed; the uplink holds events through the outage and drains them; and at the end AWS has
# every carton the building has, each history complete and in order, with the same ship-confirms.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst, which on shared runners stalled every reader
# for 0.2-0.8 s at once. It is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py fc_floor.py site_state.py uplink.py cloud_ledger.py wcs_telemetry.py kafka_bridge.py
# The readers and the uplink start first and create the topics; the floor and the neighbour follow.
timeout 80 python -u site_state.py > site.log 2>&1 &
timeout 80 python -u cloud_ledger.py > cloud.log 2>&1 &
timeout 80 python -u uplink.py --wan-down-at 30 --wan-down-for 15 > uplink.log 2>&1 &
sleep 3
timeout 66 python -u fc_floor.py --cartons-per-s 120 --resend-pct 1 --peak 2 --peak-at 12 --peak-for 10 > floor.log 2>&1 &
timeout 60 python -u wcs_telemetry.py --mbps 3 --at 20 --duration 35 > wcs.log 2>&1 &
wait || true
echo "--- floor.log";  head -n 2 floor.log; tail -n 2 floor.log
echo "--- site.log";   tail -n 4 site.log
echo "--- uplink.log"; grep -E "down for|link back" uplink.log | sed -n '1p;$p' || true; tail -n 2 uplink.log
echo "--- cloud.log";  grep "held events" cloud.log | head -n 3 || true; tail -n 2 cloud.log
echo "--- wcs.log";    sed -n '2p;$p' wcs.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" site.log | cut -c1-160 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[site\] +[1-9][0-9,]* carton events/s' site.log | tail -n +11 || true)
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
alls = [l for l in open("site.log") if re.search(r"\[site\] +[1-9][0-9,]* carton events/s", l)]
p = alls[10:]
print("steady-state medians: carton events/s %.0f  scan-to-floor-view p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) carton events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", alls)
if len(lane) > 45:                          # the floor view while the link was down (about s 27-41 of its lines)
    print("floor view p99, median per second: before the outage %.1f ms (s 12-22), while the link was down %.1f ms (s 28-40)" % (
        st.median(lane[12:23]), st.median(lane[28:41])))
f = [l for l in open("site.log") if l.startswith("[floor view]")]
if not f:
    raise SystemExit("no floor view lines")
m = re.search(r"cartons ([0-9,]+) .*ship-confirmed ([0-9,]+)  retries absorbed ([0-9,]+)  gaps ([0-9,]+)", f[-1])
if not m:
    raise SystemExit("could not read the last floor view line")
s_cart, s_conf, s_abs, s_gaps = (int(x.replace(",", "")) for x in m.groups())
print(f[-1].strip())
if s_cart < 3000 or s_conf < 1000 or s_abs < 100 or s_gaps:
    raise SystemExit("floor view: too few cartons, retries not absorbed, or gaps")
u = open("uplink.log").read().splitlines()
down = [l for l in u if "link to AWS down for" in l]
back = [l for l in u if "link back" in l]
if not down or not back or "live:" not in u[-1]:
    raise SystemExit("the link outage was not seen, drained, and back to live")
print("link down for %d s of log lines; %s" % (len(down), back[-1].split("] ", 1)[1]))
lv = nums(r"floor-to-cloud p50 +[0-9.]+ms  p99 +([0-9.]+)ms", [l for l in u if "live:" in l])
if lv:
    print("uplink floor-to-cloud p99 when live, median per second: %.1f ms" % st.median(lv))
c = open("cloud.log").read().splitlines()
held = nums(r"held events arrived ([0-9,]+)", c)
if sum(held) < 1000:
    raise SystemExit("AWS did not receive the events held through the outage")
print("held events that reached AWS after the outage: %d" % sum(held))
lg = [l for l in c if l.startswith("[ledger]")][-1]
m = re.search(r"cartons ([0-9,]+)  ship-confirmed in AWS ([0-9,]+)  history complete and in order ([0-9,]+)/([0-9,]+)  "
              r"retries absorbed ([0-9,]+)  gaps ([0-9,]+)", lg)
c_cart, c_conf, c_ok, c_all, c_abs, c_gaps = (int(x.replace(",", "")) for x in m.groups())
print(lg.strip())
print("AWS vs the building: cartons %d / %d, ship-confirmed %d / %d, histories complete %d / %d, gaps %d" % (
    c_cart, s_cart, c_conf, s_conf, c_ok, c_all, c_gaps))
if c_cart != s_cart or c_conf != s_conf or c_ok != c_all or c_gaps:
    raise SystemExit("AWS does not match the building")
cl = nums(r"floor-to-AWS p50 +[0-9.]+ms  p99 +([0-9.]+)ms", [l for l in c if not l.startswith("[ledger]")
                                                              and not re.search(r"\] +0 live", l)])
if cl:
    print("floor-to-AWS p99 for live events, median per second: %.1f ms" % st.median(cl))
PY
echo "Kit OK on Apache Iggy 0.9.0"
