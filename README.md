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
