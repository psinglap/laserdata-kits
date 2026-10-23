# Benchmark plan: the in-state wallet lane

**Question:** at one FanDuel state's shape, does every account and wallet event accepted at the site taking bets
reach the second site in the same state within 20 ms at p99, at the same offset, through a 5-10x kickoff surge,
on Apache Iggy and on the replication path FanDuel uses or is evaluating today? When the first site goes dark, how
many accepted events are missing at the second site, and how long until it takes bets from the right balances?

Rules are fixed before any result exists. If the current path wins a step, that row is published.

| Parameter | Value | Why |
|---|---|---|
| Message | One account and wallet event: account, sequence number, kind, bet, amount, resulting balance and open bets, state (about 200 bytes) | The resulting state, never a delta, so a repeat is harmless, a gap is visible and the second site can take over from the newest event alone. |
| Rate | One state's normal evening, then 5x and 10x for 15 minutes | FanDuel scales capacity 5-10x for events like the Super Bowl (AWS case study). Actual per-state rates to be taken from your wallet service. |
| Partitions | 8, 16 and 32, keyed by account | Per-account order is all the lane needs. |
| Sites | Two Iggy deployments in one state: the site taking bets and a second in-state site (Local Zone and Outpost, or two Outposts), with the network between them as deployed | "Local HA and DR in state"; "additional sites for scale and DR per state" (AWS Summit 2022, CMP207). |
| Replication | Steps: (a) the kit's mirror, asynchronous, same offsets; (b) one Iggy cluster spanning both sites, `replicated` durability (VSR quorum), where the inter-site latency allows | (a) is what the kit tests; (b) makes the second site synchronous and is new in Apache Iggy 0.9. |
| Budget | Accepted at site 1 to applied by the wallet replica at site 2: p99 under 20 ms, p99.9 under 50 ms | The lane's p99 is the most a failover inside the state can lose. |
| Retention | 72 hours on the lane | A weekend of events: replica rebuilds and DR drills replay from it. The wallet database stays the system of record. |
| Failover | Stop site 1 during the surge (network cut, then process kill) | Events accepted at site 1 and missing at site 2, time until site 2's replica is ready, balances compared account by account. |
| State boundary | Point the mirror at a site in another state; inject an event marked with another state | Both must be refused; nothing leaves the state. |
| Duration | One NFL Sunday per step | Drift shows over hours. |
| Hardware | The instance types your Outposts and Local Zones offer, the same for Iggy and for the current path | Nodes, vCPUs, memory and $ per hour reported per site. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

Iggy is driven by `iggy-bench` plus the kit; the current path by its own tooling (for Kafka, MirrorMaker 2 or
Cluster Linking timed with the same message size and rate). The kit runs alongside at each step, so the report
shows `at risk if nj-1 went dark now`, `gaps`, `repeats` and the failover check next to p99.

**Pass line:** accepted-to-replica p99 under 20 ms and p99.9 under 50 ms at 10x, offsets identical at both sites,
`gaps` and `repeats` 0, and after site 1 is cut every event it acknowledged more than the p99 earlier present at
site 2, with every balance matching.

**Published:** p99 and p99.9 per step (one line per system), the surge trace, the failover trace and the count of
missing events, broker CPU and memory per site, and every config and raw output.
