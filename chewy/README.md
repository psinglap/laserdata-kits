# Fulfillment center events at the edge for Chewy, on LaserData

Every carton event in a fulfillment center (picked, packed, labeled, sorted to a dock door, loaded,
ship-confirmed) lands in an event log inside the building in milliseconds, keeps the floor moving when the
building's link to AWS drops, and reaches the cloud afterwards in order, each event once. It runs beside the
WMS and your cloud stack, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.chewy-kit
- **One slide:** [`LaserData_Chewy_FCEdge.pdf`](LaserData_Chewy_FCEdge.pdf)

## Why this use case

From Chewy's Principal Software Engineer, Fulfillment Technology posting (Fulfillment Technology & Science
team, October 2026): the role sets "the technical direction for the next evolution of our warehouse management
system (WMS)" and will "Architect highly available, low-latency systems across cloud and edge environments,
partnering with infrastructure and networking teams to maintain warehouse operations through failures and
disruptions". It asks for "a robust workflow framework for fulfillment operations" with "explicit state
management, validation, exception handling, retries, recovery, observability", for "patterns that capture the
history of operational events, decisions, workflow transitions, and system changes so critical processes can be
traced, explained, reconciled, and improved", and for deep experience with "event-driven distributed systems
using Kafka or comparable streaming technologies, including concurrency, idempotency, asynchronous processing,
retry and replay patterns". It names AWS across cloud and edge, and integration with "robotics, warehouse
control systems, and other packaged platforms".

From Chewy's Senior Controls Systems Engineer posting (2026): the fulfillment centers run control systems for
"conveyors, sortation, scanners, cameras, print-and-apply, and robotics", integrated with WMS, WCS and WES.
From Chewy's fiscal Q2 2026 call (September 2026): "north of 50%" of volume now flows through automated sites,
with a stated goal of more than 70%.

This kit runs one lane from that brief on Apache Iggy: every carton event in one building, keyed by carton, into
(1) a floor view in the same building that the WMS workflow, handhelds and control system read, in milliseconds,
applied once, and (2) AWS, through an uplink that reads at its own pace, holds the events in the building's log
while the link is down, and drains them afterwards, so AWS ends with every carton's history complete and in
order. The WMS, the control system and your cloud services stay as they are.

## Run it

Needs Docker and Python 3.10+. Defaults: one building (site AVP2), 20 new cartons a second, six events each,
0.5% of events sent twice as handheld retries. The carton rate is an assumption; set `--cartons-per-s` to your
building's.

```
docker compose up -d                                  # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt                       # apache-iggy 0.9.0
python site_state.py &                                # the floor view inside the building
python cloud_ledger.py &                              # the ledger in AWS
python uplink.py --wan-down-at 60 --wan-down-for 30 & # building to AWS; the link drops for 30 s
python fc_floor.py                                    # the floor: picks, packs, labels, sorts, loads, ship-confirms
python wcs_telemetry.py                               # optional: conveyor and sorter telemetry next door
```

They print a line a second:

```
[site]    120 carton events/s  scan-to-floor-view p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)
[floor view] cartons …  on the floor: picked …  packed …  labeled …  sorted …  loaded …  ship-confirmed …
             retries absorbed …  gaps 0
[uplink] link to AWS down for 12.0s: the floor keeps going, … events read and waiting, the rest held in the site log
[uplink] link back: … carton events (…s of the floor) held in the building and drained to AWS in …s, in order
[cloud]    120 live events/s  floor-to-AWS p50 …ms  p99 …ms  held events arrived …
[ledger] cartons …  ship-confirmed in AWS …  history complete and in order …/…  retries absorbed …  gaps 0
```

To run the building and AWS on two brokers, start a second Apache Iggy and pass its address to `uplink.py` and
`cloud_ledger.py` with `--cloud-url`.

CI_RESULTS_PLACEHOLDER

## Shadow your own traffic

`kafka_bridge.py` reads one building's existing fulfillment events from a Kafka topic as its own consumer group
(`iggy-shadow`), read-only, maps your event types onto the six steps (`--steps PICK=PICKED,...`) and writes them
into the site lane, so the floor view, the uplink and the ledger run on real cartons beside what you run today.

## Files

| File | What it is |
|---|---|
| `fc_floor.py` | The floor: WMS, control system and handhelds sending every carton's six events, with retries. |
| `site_state.py` | The floor view inside the building: each carton's newest step, applied once; prints p99 and the floor. |
| `uplink.py` | Building to AWS: forwards in order, holds events in the site log through a link outage, then drains. |
| `cloud_ledger.py` | The ledger in AWS: applies each event once and checks every carton's history for gaps. |
| `wcs_telemetry.py` | A heavy neighbour: conveyor, sorter and scanner telemetry on its own topic. |
| `kafka_bridge.py` | Read-only shadow of one building's existing event topic into the lane. |
| `common.py` | Topics, keys, the six steps, and the latency window. |
| `bench/PLAN.md` | The head-to-head benchmark plan, fixed before results. |
| `docker-compose.yml` | Apache Iggy 0.9.0. |

## What Apache Iggy 0.9 has today, and what it doesn't

- A single small binary that runs on an edge host, with ARM builds; durable, retained, replayable topics.
- At-least-once delivery. Applying each event once comes from the per-carton sequence the lane carries.
- No built-in cross-site mirroring yet: the uplink in this kit is the forwarder, about 150 lines you own.
- A Kafka-compatible API is in progress, so Kafka clients don't connect to Iggy directly today; the bridge
  reads from Kafka with a Kafka client.
- Clustering with replication is new (September 2026). One broker per building is the design here; the
  building's durability is the edge host's disk until clustering is proven at your sites.

Talk to us: kranti@laserdata.com · https://laserdata.com
