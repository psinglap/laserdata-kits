#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the concurrency loop at small scale for about a minute, with a
# post-call payload stream next to it, and pass only if the slot-event lane reports PASS, organizations reach their
# ceilings and outbound is held back, and headroom changes reach every worker.
set -euo pipefail
cd "$(dirname "$0")"
trap 'docker compose logs --tail 40 iggy || true' ERR
# Flush the broker's writes to disk continuously, as a streaming host is tuned, instead of Linux's default of
# holding up to 30 s of dirty pages and writing them in one burst. On the runner that burst (150-180 MB, 30-35 s
# after a heavy neighbour starts) stalled every partition for 0.2-0.8 s at once; it is the runner's disk, not the lane.
sudo sysctl -q -w vm.dirty_background_bytes=33554432 vm.dirty_expire_centisecs=200 vm.dirty_writeback_centisecs=100 || true
docker compose up -d
for _ in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/8090) 2>/dev/null && break; sleep 1; done
pip install -q -r requirements.txt
python -m py_compile common.py ledger.py call_workers.py transcript_flood.py call_events_bridge.py
# The ledger starts first and creates the topics; the workers and the neighbour follow.
# 400 conversations offered a second over 200 organizations and 4 workers, 6 s average talk time (compressed from
# minutes, so ceilings fill and free while you watch); 2 MB/s of post-call payloads from second 20.
timeout 78 python -u ledger.py --orgs 200 --conv-per-s 400 --talk-s 6 > ledger.log 2>&1 &
sleep 3
timeout 72 python -u call_workers.py --workers 4 --orgs 200 --conv-per-s 400 --talk-s 6 > workers.log 2>&1 &
timeout 70 python -u transcript_flood.py --mbps 2 --at 20 --duration 35 > transcripts.log 2>&1 &
wait || true
echo "--- workers.log";     head -n 2 workers.log; tail -n 4 workers.log
echo "--- ledger.log";      tail -n 6 ledger.log
echo "--- transcripts.log"; head -n 2 transcripts.log; tail -n 2 transcripts.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" ledger.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[ledger\] +[1-9][0-9,]* slot events/s' ledger.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the ceiling checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
p = [l for l in open("ledger.log") if re.search(r"\[ledger\] +[1-9][0-9,]* slot events/s", l)][10:]
print("steady-state medians: slot events/s %.0f  slot-event-to-ledger p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, p)) for q in (r"\] +([0-9,]+) slot events/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("ledger.log") if "slot events/s" in l])
if len(lane) > 50:
    print("slot-event-to-ledger p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of post-call payloads %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
c = [l for l in open("ledger.log") if l.startswith("[ceiling]")][10:]
full = nums(r"orgs outbound-full ([0-9,]+)", c)
fullc = nums(r"  full ([0-9,]+)/", c)
at_ceiling = max(a + b for a, b in zip(full, fullc)) if full else 0
starts = sum(nums(r"  starts ([0-9,]+)", c))
past = sum(nums(r"outbound past its share ([0-9,]+)", c))
pastc = sum(nums(r"past the plan ceiling ([0-9,]+)", c))
outs = [l for l in open("workers.log") if l.startswith("[workers]")][10:]
out_starts = sum(nums(r"outbound ([0-9,]+) inbound", outs))
print("most organizations at a ceiling in one second: %d of 200" % at_ceiling)
print("steady state: %d conversations started, %d outbound; outbound started past its share %d (%.2f%%), past the plan ceiling %d" % (
    starts, out_starts, past, 100 * past / out_starts if out_starts else 0, pastc))
if at_ceiling < 20:
    raise SystemExit("organizations did not reach their ceilings")
held = sum(nums(r"held back at the ceiling ([0-9,]+)", outs))
away = sum(nums(r"inbound turned away ([0-9,]+)", outs))
print("workers: outbound attempts held back at the ceiling %d, inbound calls turned away %d" % (held, away))
if held == 0:
    raise SystemExit("the workers never held outbound back")
late = [l for l in open("workers.log") if l.startswith("[1s-behind]")][10:]
lo, lt, li = (sum(nums(q, late)) for q in (r"past the share ([0-9,]+)", r"inbound turned away ([0-9,]+)", r"room free ([0-9,]+)"))
print("same attempts with every decision 1 s late: outbound past the share %d (%.2f%% of outbound starts), inbound turned away %d, outbound held with room free %d" % (
    lo, 100 * lo / out_starts if out_starts else 0, lt, li))
d = [l for l in open("workers.log") if re.search(r"decision-to-workers n [1-9]", l)][5:]
if not d:
    raise SystemExit("no headroom decisions reached the workers")
print("decision-to-workers (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with decisions, %d s)" % (
    st.median(nums(r"workers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
e = [l for l in open("transcripts.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("post-call payload neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[transcripts\] done" transcripts.log || { echo "the post-call payload neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
