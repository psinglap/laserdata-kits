#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the app wallet loop at small scale for about a minute, with an
# inbound-messages stream next to it, and pass only if the charges lane reports PASS, apps reach their overdraft
# limit and are blocked, top-ups unblock them, records sent twice are charged once, and every state change
# reaches every send API node.
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
python -m py_compile common.py wallet.py send_api.py inbound_flood.py kafka_bridge.py
# The wallet service starts first and creates the topics; the send nodes and the neighbour follow.
# 2,000 outbound messages a second over 1,000 apps and 4 send nodes, 15 s median runway, top-ups 4 s after a
# block; 2 MB/s of inbound messages from second 20.
timeout 78 python -u wallet.py --apps 1000 --msgs-per-s 2000 --runway-s 15 > wallet.log 2>&1 &
sleep 3
timeout 72 python -u send_api.py --nodes 4 --apps 1000 --msgs-per-s 2000 --runway-s 15 --recharge-s 4 > send.log 2>&1 &
timeout 70 python -u inbound_flood.py --mbps 2 --at 20 --duration 35 > inbound.log 2>&1 &
wait || true
echo "--- send.log";    head -n 2 send.log; tail -n 4 send.log
echo "--- wallet.log";  tail -n 8 wallet.log
echo "--- inbound.log"; head -n 2 inbound.log; tail -n 2 inbound.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" wallet.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[wallet\] +[1-9][0-9,]* charges/s' wallet.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the wallet checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
p = [l for l in open("wallet.log") if re.search(r"\[wallet\] +[1-9][0-9,]* charges/s", l)][10:]
print("steady-state medians: charges/s %.0f  charge-to-wallet p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) charges/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
print("worst steady-state second: p99 %.1f ms" % max(nums(r"p99 +([0-9.]+)ms", p)))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("wallet.log") if "charges/s" in l])
if len(lane) > 50:
    print("charge-to-wallet p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of inbound messages %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
b = [l for l in open("wallet.log") if l.startswith("[ledger]")]
if not b:
    raise SystemExit("the wallet service printed no ledger lines")
last = b[-1]
print("ledger at the end:", last.strip())
blocks = nums(r"blocks ([0-9,]+)", [last])[0]
recharges = nums(r"recharges ([0-9,]+)", [last])[0]
pct = nums(r"msgs \(([0-9.]+)%\)", [last])[0]
repeats = sum(nums(r"repeats ignored ([0-9,]+)", b))
print("blocks %d  recharges %d  records sent twice and charged once %d  accepted past the overdraft %.4f%% of messages" % (
    blocks, recharges, repeats, pct))
if blocks < 100:
    raise SystemExit("the wallet service did not block apps at their overdraft limit")
if recharges < 20:
    raise SystemExit("no top-ups reached the wallet")
if repeats < 100:
    raise SystemExit("records sent twice were not seen")
if pct > 1.0:
    raise SystemExit("more than 1% of messages were accepted past the overdraft")
d = [l for l in open("send.log") if re.search(r"state-to-send-nodes n [1-9]", l)][5:]
if not d:
    raise SystemExit("no wallet states reached the send nodes")
print("state-to-send-nodes (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with states, %d s)" % (
    st.median(nums(r"nodes n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
w = nums(r"rejected 1003 ([0-9,]+)", [l for l in open("send.log") if l.startswith("[send]")])
if not w or max(w) == 0:
    raise SystemExit("the send nodes never rejected with 1003")
print("1003 rejections at the send nodes: %d in total" % sum(w))
e = [l for l in open("inbound.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("inbound-messages neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[inbound\] done" inbound.log || { echo "the inbound-messages neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
