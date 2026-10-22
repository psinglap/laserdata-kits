# Benchmark plan: the quartile tracker lane

**Question:** at THUNDERSTORM's ad break rates, with every viewer on a channel reaching the break together and the
session records on the same brokers, does every ad segment served reach the tracker that owns its session within
10 ms at p99, with every quartile beacon fired once, on Apache Iggy and on the same lane in Kinesis or Kafka?

Rules are fixed before any result exists. If the incumbent wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~230 B | Session, channel, impression, creative, segment index, segment count, ad and segment length, ad start, timestamp. |
| Viewers | 100K steady, then a 2M concurrent ramp on 20 channels | Your 2021 post: about 100K concurrent viewers per linear FAST channel normally, up to 2 million planned for the US election. |
| Break shape | Every viewer on a channel enters the break within 2 s; 4 × 30 s ads, 2 s segments | On a linear channel the break is one moment for everyone watching. |
| Partitions | 64, keyed by session | A session's segments reach one tracker, in order. |
| Trackers | 4 → 16 → 32, each owning a share of partitions, reading directly | The tracker fleet scales with concurrency; no shard-count changes during the event. |
| Repeats | 0.5% of segment events delivered twice | At-least-once delivery; the tracker fires each (impression, quartile) once. |
| Restart | Rolling restart of every tracker during a break | Beacons fired late but once; time to rebuild and catch up. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, min ISR 2 · Kinesis default | Beacons are revenue and cannot be lost. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0` | Kafka on its best latency settings, on the same hardware. |
| Neighbour | 100 MB/s of session records on a separate topic, same brokers | Bulk analytics data must not move the lane's tail. |
| Failover | Kill the leader during a break | Time until every tracker receives again, and beacons missed (target 0). |
| Duration | 30 min per step + 24 h soak | A full day of breaks; drift shows over hours. |
| Hardware | 3 brokers each side, same instance type, same AWS region | Like for like; nodes and cost per billion segment events reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark, Kinesis by its own producer library, with
the same workload shape. The kit runs alongside at each rate, so the report shows the beacon audit (missing, fired
twice, not due) next to p99.

**Pass line:** segment-to-tracker p99 under 10 ms at the 2M-viewer ramp with 32 trackers, replicated, with the
neighbour running; zero beacons missed or fired twice across rolling restarts.

**Published:** p99 and p99.9 vs viewers and vs tracker count (one line per system), the break trace, restart and
failover recovery, broker CPU, node count and cost per billion segment events, and every config and raw output.
