# Benchmark plan: the provider health lane

**Question:** at campaign-surge call volumes, with a heavy call-log stream on the same brokers, does every
provider outcome reach the health evaluator, and every degraded or recovered verdict reach every call worker,
within 10 ms at p99 per hop, on Apache Iggy and on the same lane in MSK?

Rules are fixed before any result exists. If MSK wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~160 B outcome record · ~90 B verdict | Stage:provider:region, worker, sequence, call, ok, time to first byte, timestamp. |
| Rate | 10K → 30K → 100K outcomes/s | Three provider calls per turn; 20,000 live calls at a turn every ~6 s is about 10,000 outcomes/s. Vapi describes campaigns going from 10 to 10,000 concurrent calls in minutes, so the upper steps hold several such surges at once. |
| Partitions | 16 on outcomes, keyed by stage:provider:region · 1 on verdicts | One evaluator decision per key, in order; every call worker reads every verdict. |
| Readers | Evaluator on outcomes; 50 → 500 call workers on verdicts | The fan-out back to the call fleet is the second hop and has to hold as the fleet grows. |
| Durability | Iggy `replicated` × 3 · MSK `acks=all`, RF 3, `min.insync.replicas=2` | Verdicts change where live calls go and cannot be lost. |
| MSK tuning | `linger.ms=0`, consumer `fetch.min.bytes=1`, `fetch.max.wait.ms` 5, same region and instance class as Vapi's cell | MSK on its best latency settings. |
| Incidents | One stage:provider:region failing 60% of requests for 5 s, every 10 s | Detection time, verdict fan-out time and turns sent to the failing provider, per incident. |
| Peak | 5× for 10 min | A campaign surge. |
| Neighbour | 200 MB/s of turn logs and end-of-call reports on a separate topic, same brokers | The call-log pipeline must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the evaluator and the call workers receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, MSK by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows turns sent to a provider after its verdict next to p99.

**Pass line:** outcome-to-evaluator p99 and verdict-to-worker p99 each under 10 ms at 30K outcomes/s with
500 call workers, replicated, through the 5× surge and with the neighbour running; under 1% of the turns a
1 s lane would let through sent to a provider after it was marked degraded.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), per-incident detection and fan-out
times, the surge trace, failover recovery, node CPU, node count and monthly cost for the lane, and every config
and raw output.
