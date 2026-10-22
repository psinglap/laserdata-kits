# Benchmark plan: the destination delivery alert lane

**Question:** at delivery volume, with a heavy event-payload stream on the same brokers, does every delivery
attempt's outcome reach the alert check, and every ALERT and RESOLVED reach the alert service, within 10 ms at p99
per hop, on Apache Iggy and on the same lane in Kafka?

Rules are fixed before any result exists. If Kafka wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B outcome record · ~120 B alert | Workspace:destination, worker, sequence, HTTP status, delivered or failed, attempt, timestamp. |
| Rate | 20K → 100K → 400K outcomes/s | Segment's Centrifuge post describes 400,000 outbound HTTP requests a second (2018); the top step holds that whole rate on one lane. |
| Keys | 100K workspace destinations, power-law volume | The Centrifuge post counts ~88,000 source-destination pairs; a few carry most of the traffic. |
| Partitions | 16 → 64 on outcomes, keyed by workspace:destination · 1 on alerts | One alert decision per key, in order; one alert service reads every alert. |
| Durability | Iggy `replicated` × 3 · Kafka `acks=all`, RF 3, `min.insync.replicas=2` | Alerts reach customers and cannot be lost. |
| Kafka tuning | `linger.ms=0`, consumer `fetch.min.bytes=1`, `fetch.max.wait.ms` 5, same region and instance class | Kafka on its best latency settings. |
| Incidents | A 401, 429 or 503 incident on one workspace destination every 10 s; once per step, one destination type failing 80% for every workspace that uses it | First failure to alert, alerts per workspace, and alerts on keys with no incident. |
| Peak | 5× for 10 min | A large customer's launch or a sale day. |
| Neighbour | 200 MB/s of 5 KB event payloads on a separate topic, same brokers | The event stream must not move the lane's tail. |
| Failover | Kill a broker at minute 15 | Time until the alert check and the alert service receive again, and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Drift shows over hours. |
| Hardware | 3 nodes each side, same instance family and region | Like for like; nodes and $ per month reported. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Kafka by the OpenMessaging Benchmark Kafka driver, with the same workload shape.
The kit runs alongside at each rate, so the report shows each incident's first failure to alert next to p99.

**Pass line:** outcome-to-check p99 and alert-to-service p99 each under 10 ms at 100K outcomes/s, replicated,
through the 5× peak and with the neighbour running; every planted incident alerted within 2 s of its first
failure; no alert on a workspace destination without an incident.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), per-incident first failure to alert,
the peak trace, failover recovery, node CPU, node count and monthly cost for the lane, and every config and raw
output.
