# Benchmark plan: the crash spike lane

**Question:** at Measure's ingest volume and above, with one record per ingest batch keyed by app and full ingest
batches on the same broker, does every record reach the spike check within 10 ms at p99, and every spike alert
reach the Slack and email senders within 10 ms more, on Apache Iggy and on the same lane in Google Pub/Sub?

Rules are fixed before any result exists. If Pub/Sub wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message size | ~150 B record (batch ID, app, session, 0–3 fingerprints) · ~400 B alert | One record per ingest batch after step 7. |
| Rate | 1K → 50K records/s | Measure's Powered By entry cites "billions of mobile data points every week" (data points, not batches); 50K batches/s is well above that. |
| Partitions | 9 on issue-occurrences, keyed by app ID · 1 on alerts | Every record of an app reaches one spike check in order; 9 matches `ingest-batch` in self-host/compose.yml. |
| Readers | 1 → 4 spike checks on issue-occurrences; 2 → 10 senders on alerts | Slack and email today, webhooks next. |
| Durability | Iggy `replicated` × 3 · Pub/Sub default | Alerts page people; records cannot be lost. |
| Delivery | 1% of records delivered twice | Both buses deliver at least once; every spike must tip on the same batch. |
| Peak | 10× ingest for 10 minutes | The Powered By entry cites spikes of up to 10×; a bad release causes exactly that. |
| Neighbour | 200 MB/s of full ingest batches on `ingest-batch`, same broker | Uploads must not move the lane's tail. |
| Failover | Kill a node at minute 15 | Time until the spike check reads again; spikes missed during it (target 0). |
| Duration | 30 min per step + 24 h soak | The window is an hour; drift shows over hours. |
| Hardware | 3 nodes, GCP, same region as the clients; Pub/Sub in the same region | Measure Cloud runs on GCP. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench`, Pub/Sub by its load generator with the same message shape and ordering keys. The kit
runs alongside at each rate, so the report shows time-to-Slack and tip-batch matches next to p99.

**Pass line:** record-to-check p99 and alert-to-sender p99 each under 10 ms at 50K records/s, replicated, through
the peak and with the neighbour running; every spike tips on the ground-truth batch; no alert for any other issue.

**Published:** p99 and p99.9 vs rate for both hops (one line per system), the peak trace, failover recovery, node
CPU and memory, monthly cost for the lane, and every config and raw output.
