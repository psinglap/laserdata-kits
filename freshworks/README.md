# Real-time agent availability for Freshworks Omniroute, on LaserData

Every status change, call, chat and ticket from Freshdesk, Freshchat and Freshcaller reaches every routing node in
milliseconds, so tickets and chats go only to agents who are free, even through a shift change. It runs beside
your self-hosted Kafka, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the open-source engine
LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.freshworks-kit
- **One slide:** [`LaserData_Freshworks_AgentState.pdf`](LaserData_Freshworks_AgentState.pdf)

## Why this use case

From Freshworks' own posts and docs:

- "Kafka is central to all inter-product communications at Freshworks", run by a team known internally as
  Central: 5 clusters across regions, the largest 21 nodes, "over 12TB of data per day with peak egress of
  ~250MB/s". Freshworks moved from a hosted provider to self-hosted clusters (Freshworks platform blog, "Kafka
  migration from hosted service to in-house cluster", January 2023).
- Omniroute now assigns tickets "based on each agent's real-time workload, availability, and assignment
  preferences", and "status changes are directly integrated with routing, ensuring tickets are only assigned to
  available agents" (Freshdesk docs, upcoming changes December 2025 to June 2026).
- Only Available agents are eligible; an agent at their ticket limit gets nothing new until they resolve one;
  load-based routing assigns by how many more tickets each agent can take, and the last assignment time breaks
  ties (Freshdesk docs, "Understanding automatic ticket assignment"). Availability spans "Freshdesk, Freshchat,
  and Freshcaller" (Freshworks support, "Omnichannel availability dashboard").

The fields that decide eligibility change fastest, and in three products: a call starts in Freshcaller, a chat
closes in Freshchat, a ticket is resolved or assigned by hand in Freshdesk, an agent sets a break. This kit runs
that one feed as a lane on Apache Iggy: every agent's resulting state, keyed by agent, read by every routing node
directly, kept for an hour so a new or restarted node rebuilds its view from the log. It prints the lane's p99
every second, tickets and chats sent to an agent who is away, on a call or at capacity, and what a view refreshed
every 3 seconds would have done with the same work.

## Run it

Needs Docker and Python 3.10+. Defaults: 20,000 agents in 2,000 groups at 75% of capacity, 300 new tickets and
chats a second, 40 calls starting a second, 20 breaks and 20 tickets assigned by hand a second, and every agent's
state re-sent every 10 s.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python agents.py &                   # agents in three products, customers writing in, and the check
python router.py --name route-1 --shard 0 --shards 2 &   # a routing node: half the groups' inquiries
python router.py --name route-2 --shard 1 --shards 2     # the other half; each holds every agent
```

They print a line a second:

```
[route-1]  3,000 changes/s  change-to-view p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  routed …  no agent …
[agents]   3,000 changes/s  inquiries 300 routed 300 (steady)  misrouted … (away … on a call … at capacity …)
           no agent …  a view refreshed every 3s would misroute … (away … on a call … at capacity …) no agent …
```

**What the numbers mean.** The kit times each change from the moment `agents.py` creates it (including its wait
for the next write) to the moment a `router.py` has applied it to its view, through Python clients, on whatever
machine you run it on. On a shared 2-vCPU GitHub runner with the broker and all four scripts on one box (about
1,580 agents in 150 groups, 100 new tickets and chats a second, a shift change in the busiest group with a 4×
spike for 15 s, 2 MB/s of product events next door, two routing nodes, one restarted), the medians of the
steady-state seconds over three green runs on the final code were p50 5.5–9.4 ms, p99 13.9–21.1 ms and p99.9
16.9–24.0 ms on each routing node, at about 500–550 agent changes a second. Every steady-state second passed the
50 ms budget (65 of 65 and 53 of 53 per node in all three runs); the worst second reached 37.5–47.5 ms. The
restarted node rebuilt its view from the log (1,578 agents, about 15,200–15,600 changes) in 0.12–0.18 s before it
routed anything. Of about 6,800 assignments checked against the agents, none went to an agent who was away, on a
call or at capacity. The same nodes' view copies refreshed every 3 s would have sent 142–177 (2.1–2.6%) to such
agents, mostly to agents who had just started a call or gone on a break, and 48–80 (2.5–4.1%) during the shift
change; they also found no free agent for 5.1–5.7% of inquiries, against 3.8–4.6% for the live view, because
agents logging on were not in the copy yet. Inquiry-to-assignment was p50 7.3–10.1 ms and p99 12.7–17.4 ms.
About 1.2% of inquiries went unrouted, all in the second while the restarted node was down (it takes inquiries
from its restart on). That is the end-to-end lane through this demo code at reduced scale, not the broker alone,
and the comparison is the kit's model, not a Freshworks figure. One Python reader tops out near 4,000 changes/s on
that runner; past it, the reader falls behind, not the broker. The broker's own tail latency is measured with
`iggy-bench` in `bench/PLAN.md`, on dedicated machines.

| File | What it is |
|---|---|
| `agents.py` | The agents and the customers. Publishes each agent's state to `agent-state` with a per-agent sequence and the resulting state (status, tickets and chats against capacity, on a call, assignments received, the product that changed it); sends new tickets and chats keyed by group; checks every assignment against the agent at that moment. `--surge` runs a shift change with a spike. |
| `router.py` | A routing node: reads every partition of the lane directly into its view, rebuilds from the log on start, takes the inquiries of its share of groups (`--shard` of `--shards`), assigns like load-based Omniroute (available, not on a call, under capacity, most room first, then the longest since an assignment), and keeps a copy of its view refreshed every 3 s for comparison. |
| `product_events_flood.py` | A heavy, latency-tolerant stream (product events bound for the data platform) on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies Kafka topics that carry agent status, call, chat and ticket changes into Iggy as a read-only consumer group, for a shadow run on real traffic. |
| `ci.sh` | The CI run: about 1,580 agents, a shift change with a 4× spike, a product-events neighbour, two routing nodes and a restart. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

The agents are a model, not Freshworks data: group sizes, capacities, loads, call and break rates, the shift
change and the 3-second comparison view are the kit's assumptions. Freshworks does not publish how agent state
reaches Omniroute; set `--refresh-s` on `router.py` to match what you run, or point the bridge at the real feed.

## Three tests

1. **A shift change in a ticket spike:** `python agents.py --surge 4 --surge-at 60 --surge-for 30`.
   Pass: change-to-view p99 under 50 ms on every routing node, and work sent to an agent who is away, on a call or
   at capacity near zero through the spike.
2. **Product events at full volume next door:** `python product_events_flood.py --mbps 50` while it runs.
   Pass: the lane's p99 moves by less than 10%.
3. **A routing node restarts:** stop one `router.py` and start it again with the same `--shard`; it rebuilds its
   view from the log before it routes anything. Pass: every agent in its view within seconds.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <brokers> --topic <agent status> --topic <call events> -X security.protocol=SASL_SSL …
```

The bridge reads as its own consumer group, `iggy-shadow`, and never commits offsets; the products, Omniroute and
every other consumer are untouched. Map your record shape in `to_state()`: the kit needs `k` (agent), `seq`, `g`,
`st`, `t`, `ct`, `c`, `cc` and `call`. If agent state reaches Omniroute another way than Kafka (the public sources
don't say), the evaluation starts with a tap on that path.

## What Apache Iggy 0.9 does not have yet

A Kafka-compatible API (a gateway is in progress), so Kafka clients can't read the lane; routing nodes use the
Apache Iggy SDK. Delivery is at-least-once; the per-agent sequence makes replays and retries safe. Replicated
clusters shipped in September 2026, which is why the evaluation kills a leader on purpose.

## Next step

If the numbers are interesting: a 2–4 week evaluation on one mirrored agent state feed, beside Omniroute, on
self-hosted nodes like Central's own clusters. No advantage, no migration.

kranti@laserdata.com · laserdata.com
