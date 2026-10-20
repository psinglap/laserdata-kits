# LaserData account kits

One folder per target account. Each has a runnable kit on Apache Iggy, built by LaserData,
a README written for that company, and its one-slide pitch. CI runs every kit on a real
Apache Iggy 0.9.0 broker on each push.

| Account | Use case | Folder |
|---|---|---|
| Zepto | Real-time hub stock and prices for search re-validation, beside MSK | [`zepto/`](zepto/) |
| Swiggy | Delivery partner location pings for the tracking screen and last-mile ETA, beside Confluent | [`swiggy/`](swiggy/) |
| Meesho | Order-update notifications into Meecom, kept ahead of sale-day campaigns, beside Confluent | [`meesho/`](meesho/) |
| Zomato (Eternal) | Restaurant kitchen state for food preparation time predictions, beside Confluent Cloud | [`eternal/`](eternal/) |
| Rapido | On-duty captain location for ride matching and pickup tracking, beside Kafka | [`rapido/`](rapido/) |
| Porter | Driver-partner location pings for the live tracking link and pickup and drop arrivals, beside Cloud Pub/Sub | [`porter/`](porter/) |
| Groww | Position updates from OMS fills into the Holdings service, inside the app's post-trade refresh, beside Strimzi Kafka | [`groww/`](groww/) |
| PhonePe | Payer velocity counts for the risk check on the payer's next payment, self-hosted beside Kafka | [`phonepe/`](phonepe/) |
| Razorpay | Payment downtime signals: payment status into the downtime detector, and its signals out to Checkout and merchant webhooks, beside MSK | [`razorpay/`](razorpay/) |
| Zerodha | Exchange ticks to every Kite ticker node, with rebuild and replay from the log, beside NATS | [`zerodha/`](zerodha/) |
| Juspay | Release guard: payment outcomes tagged with the serving release into the A/B comparison, and SCALE or ABORT back to the staggered rollout, beside Kafka and ClickHouse | [`juspay/`](juspay/) |
| JioHotstar | Playback health signals: minimized heartbeats by cohort and CDN into the health check, and CDN health signals out to routing and alerting, beside Kafka | [`jiohotstar/`](jiohotstar/) |
| ShareChat | Viewed posts into the feed dedup service before the next feed request, beside Redpanda and WarpStream | [`sharechat/`](sharechat/) |
| ZEE5 | Continue Watching heartbeats: playback position into the resume point a viewer gets on their next device, beside Confluent Cloud and ScyllaDB | [`zee5/`](zee5/) |
| MoEngage | Smart Trigger events, one topic per workspace: trigger events into the campaign evaluator at each brand's own rate, beside self-run Kafka | [`moengage/`](moengage/) |
| SigNoz | OTLP ingest buffer between gateway collectors and ClickHouse writers: bursts and ClickHouse stalls absorbed in the log, offsets committed only after ClickHouse confirms | [`signoz/`](signoz/) |
| Dream11 | Live fantasy points: every ball of every live match to every points node, with rebuild from the log, beside Kafka | [`dream11/`](dream11/) |
| The Trade Desk | Spend-to-pacing loop: every win's spend into the pacing controllers, and their pause and throttle decisions back to every bid server, beside Kafka | [`the-trade-desk/`](the-trade-desk/) |
| Magnite | DSP appetite lane for traffic shaping: which DSPs are bidding on which supply, to every shaping node, with rebuild from the log, beside Kafka | [`magnite/`](magnite/) |
| Braze | Currents connector lane: engagement events on one topic per connector, each read at its partner's pace from its own offset, so one partner's outage or rate limit delays only its own export, beside Kafka | [`braze/`](braze/) |
| PubMatic | CTV household frequency lane: every impression to every ad server before that household's next ad decision, so caps hold across screens and servers, with rebuild from the log, beside Kafka | [`pubmatic/`](pubmatic/) |
| Klaviyo | Failed-writes lane for the Event Publisher: every event whose MSK publish fails captured in milliseconds and re-published inside its priority lane's SLO, with the DLQ beside it, in place of the self-hosted failed-writes Kafka cluster | [`klaviyo/`](klaviyo/) |
