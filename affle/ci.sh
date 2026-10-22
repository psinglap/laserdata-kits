#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the click-to-install loop at small scale for about a minute, with
# planted click injection and a bid-log flood next to it, and pass only if the click-install lane reports PASS,
# every install is scored with its click, the scorer blocks the injecting publishers and no genuine one, and the
# blocks reach every attribution node before their next conversions are billed.
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
python -m py_compile common.py ctit_scorer.py attribution_nodes.py bid_log_flood.py kafka_bridge.py
# The scorer starts first and creates the topics; the attribution nodes and the neighbour follow.
# 2,500 clicks/s over 4 attribution nodes, 3% followed by an install 11-30 s later (about 75 installs/s); a new
# publisher injecting clicks on 80% of beginning installs every 10 s from second 25; 2 MB/s of bid logs from second 20.
timeout 78 python -u ctit_scorer.py > ctit.log 2>&1 &
sleep 3
timeout 72 python -u attribution_nodes.py --nodes 4 --clicks 2500 --install-rate 0.03 --publishers 300 \
  --incident-at 25 --hijack 0.8 > nodes.log 2>&1 &
timeout 70 python -u bid_log_flood.py --mbps 2 --at 20 --duration 35 > bidlogs.log 2>&1 &
wait || true
echo "--- nodes.log";   head -n 3 nodes.log; grep "\] incident \|FALSE BLOCK" nodes.log || true; tail -n 3 nodes.log
echo "--- ctit.log";    grep -- "-> BLOCKED" ctit.log || true; tail -n 4 ctit.log
echo "--- bidlogs.log"; head -n 2 bidlogs.log; tail -n 2 bidlogs.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" ctit.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[ctit\] +[1-9][0-9,]* events/s' ctit.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the block checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
h = [l for l in open("ctit.log") if re.search(r"\[ctit\] +[1-9][0-9,]* events/s", l)][10:]
print("steady-state medians: events/s %.0f  event-to-scorer p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, h)) for q in (r"\] +([0-9,]+) events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("ctit.log") if "event-to-scorer" in l])
if len(lane) > 50:
    print("event-to-scorer p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of bid logs %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
inst, withc = sum(nums(r"installs ([0-9,]+) \(", h)), sum(nums(r"with their click ([0-9,]+),", h))
print("installs scored in steady state: %d, with their click already on the lane: %d" % (inst, withc))
if inst < 1000 or withc != inst:
    raise SystemExit("installs were scored without their click, or too few installs to judge")
blocks = sum(nums(r"blocks sent ([0-9,]+)", h))
print("blocks sent in steady state: %d" % blocks)
if blocks < 3:
    raise SystemExit("the scorer did not block the planted injecting publishers")
n = open("nodes.log").read().splitlines()
false = nums(r"false blocks ([0-9]+)", n)
if not false or false[-1] or any("FALSE BLOCK" in l for l in n):
    raise SystemExit("a genuine publisher was blocked")
inc = [l for l in n if "onset to block" in l]
if len(inc) < 3:
    raise SystemExit("fewer than 3 blocks reached every attribution node")
print("injecting publishers blocked on every node: %d  onset to block median %.0f ms  block to all nodes median %.1f ms, max %.1f ms" % (
    len(inc), st.median(nums(r"onset to block ([0-9.]+) ms", inc)), st.median(nums(r"nodes ([0-9.]+) ms,", inc)),
    max(nums(r"nodes ([0-9.]+) ms,", inc))))
print("hijacked conversions billed before each block (detection, set by the rule): median %.0f" % st.median(
    nums(r"before the block ([0-9,]+)", inc)))
c = [l for l in n if re.search(r"\[nodes\] +[1-9][0-9,]* clicks/s", l)][10:]
blind = sum(nums(r"block was out ([0-9,]+)", c))
slow = sum(nums(r"it would be ([0-9,]+),", c))
print("conversions billed to a publisher after its block was out: %d  at a 1,000 ms lane: %d (%.1f%% of that)" % (
    blind, slow, 100 * blind / max(slow, 1)))
if slow < 100:
    raise SystemExit("too few conversions after a block to judge the loop")
d = [l for l in c if re.search(r"block-to-nodes n [1-9]", l)]
if not d:
    raise SystemExit("no blocks reached the attribution nodes")
print("block-to-nodes (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with blocks, %d s)" % (
    st.median(nums(r"nodes n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
if blind * 100 > slow * 2:
    raise SystemExit("more than 2% of the conversions a 1 s lane bills were still billed after the block")
e = [l for l in open("bidlogs.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("bid-log neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[bidlogs\] done" bidlogs.log || { echo "the bid-log neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
