#!/usr/bin/env bash
# CI smoke test: start Apache Iggy 0.9.0, run the safety verdict loop at small scale for about a minute, with a
# full-prompt log next to it, and pass only if the turns lane reports PASS, the classifiers send verdicts, and
# the verdicts reach every chat server before the conversations' next turns.
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
python -m py_compile common.py classifiers.py chat_servers.py prompt_firehose.py warpstream_bridge.py
# The classifiers start first and create the topics; the chat servers and the neighbour follow.
# 3,000 turns a second over 4 chat servers; 2 MB/s of full prompts from second 20.
timeout 78 python -u classifiers.py > classifiers.log 2>&1 &
sleep 3
timeout 72 python -u chat_servers.py --servers 4 --turns-per-s 3000 > chat.log 2>&1 &
timeout 70 python -u prompt_firehose.py --mbps 2 --at 20 --duration 35 > prompts.log 2>&1 &
wait || true
echo "--- chat.log";        head -n 3 chat.log; tail -n 4 chat.log
echo "--- classifiers.log"; tail -n 6 classifiers.log
echo "--- prompts.log";     head -n 2 prompts.log; tail -n 2 prompts.log
echo "--- FAIL seconds (line: output)"; grep -n "FAIL (" classifiers.log | cut -c1-140 || true
# Skip a 10 s warm-up while topics come online; a shared CI runner may blip, so require 95% of seconds to pass.
steady=$(grep -E '\[classifiers\] +[1-9][0-9,]* turns/s' classifiers.log | tail -n +11 || true)
live=$(grep -c 'PASS (' <<<"$steady" || true)
fail=$(grep -c 'FAIL (' <<<"$steady" || true)
echo "steady-state seconds with live traffic: PASS $live, FAIL $fail"
if ! { [ "$live" -ge 30 ] && [ $(( fail * 100 )) -le $(( (live + fail) * 5 )) ]; }; then echo "latency check failed"; exit 1; fi
# Steady-state summary (medians of the per-second figures) and the verdict checks, for the README.
python - <<'PY'
import re, statistics as st
def nums(pat, lines):
    out = []
    for l in lines:
        m = re.search(pat, l)
        if m:                               # skip lines without a value instead of failing the summary
            out.append(float(m.group(1).replace(",", "")))
    return out
c = [l for l in open("classifiers.log") if re.search(r"\[classifiers\] +[1-9][0-9,]* turns/s", l)][10:]
print("steady-state medians: turns/s %.0f  turn-to-classifier p50 %.1f ms  p99 %.1f ms  p99.9 %.1f ms" % tuple(
    st.median(nums(q, c)) for q in (r"\] +([0-9,]+) turns/s", r"p50 +([0-9.]+)ms", r"p99 +([0-9.]+)ms",
                                    r"p99.9 +([0-9.]+)ms")))
lane = nums(r"p99 +([0-9.]+)ms", [l for l in open("classifiers.log") if "turn-to-classifier" in l])
if len(lane) > 50:
    print("turn-to-classifier p99, median per second: before the neighbour %.1f ms (s 12-22), with 2 MB/s of full prompts %.1f ms (s 26-50)" % (
        st.median(lane[12:23]), st.median(lane[26:51])))
sent = sum(nums(r"verdicts sent ([0-9,]+)", c))
print("verdicts sent in steady state: %d" % sent)
if sent < 100:
    raise SystemExit("the classifiers did not send verdicts")
ch = [l for l in open("chat.log") if re.search(r"\[chat\] +[1-9][0-9,]* turns/s", l)][10:]
after = sum(nums(r"after a flag ([0-9,]+)", ch))
early = sum(nums(r"served before the verdict ([0-9,]+)", ch))
slow = sum(nums(r"it would be ([0-9,]+)\)", ch))
print("next turns after a flagged turn: %d  served before the verdict: %d (%.2f%%)  at a 900 ms lane: %d (%.2f%%)" % (
    after, early, 100 * early / max(after, 1), slow, 100 * slow / max(after, 1)))
if after < 100:
    raise SystemExit("too few turns after a flagged turn to judge the loop")
d = [l for l in ch if re.search(r"verdict-to-chat-servers n [1-9]", l)]
if not d:
    raise SystemExit("no verdicts reached the chat servers")
print("verdict-to-chat-servers (4 readers): p50 %.1f ms  p99 %.1f ms (medians of seconds with verdicts, %d s)" % (
    st.median(nums(r"servers n [0-9,]+ p50 ([0-9.]+)ms", d)), st.median(nums(r"p99 ([0-9.]+)ms", d)), len(d)))
if early * 100 > after:
    raise SystemExit("more than 1% of next turns were built before their verdict arrived")
e = [l for l in open("prompts.log") if re.search(r"\] +[1-9][0-9,]* msgs/s", l)]
if e:
    print("full-prompt neighbour: %d s, median %.1f MB/s on its own topic" % (len(e), st.median(nums(r"~([0-9.]+) MB/s", e))))
PY
grep -q "\[prompts\] done" prompts.log || { echo "the full-prompt neighbour did not run"; exit 1; }
echo "Kit OK on Apache Iggy 0.9.0"
