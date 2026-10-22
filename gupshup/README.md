# Real-time wallet balances for Gupshup, on LaserData

Every message's fees reach the app's wallet, and every 1003 block reaches every send node, in
milliseconds, even at campaign peak. It runs beside your billing pipeline, so nothing else has to move.

This kit lets your team measure that in 15 minutes on a laptop, with Apache Iggy 0.9.0, the
open-source engine LaserData builds and runs.

- **Write-up:** https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.gupshup-kit
- **One slide:** [`LaserData_Gupshup_AppWallets.pdf`](LaserData_Gupshup_AppWallets.pdf)

## Why this use case

From Gupshup's partner docs, Wallet Overview: "The prepaid method of billing is called Wallet in Gupshup.
You must recharge your Gupshup wallet, and based on the usage of apps, deductions are done from the
wallet." "The WhatsApp fee and Gupshup fee deduction will be made automatically from your wallet
balance", and an email goes out "when the balance touches $5".

From the Partner Portal release notes (v9.1, June 2024): "Meta fees will be deducted based on delivered
or read events instead of sent events", and "Gupshup will now calculate the overdraft limit that is
available to its users based on their past usage."

From the error codes: 1003, Wallet Balance Low, "Unable To Send Message / Check your wallet balance."
And from the billing events page, updated this month: "Meta charges for service conversation messages
sent by business - w.e.f Oct 01, 2026."

Enforcement is a loop with two hops: a message's fees have to reach the wallet that owns the app's
balance, and the wallet's block has to reach every send node before the app's next message. Whatever the
loop takes, the fleet keeps accepting messages past the overdraft. This kit runs that one loop on Apache
Iggy, prints both hops' latency every second, and prints the messages accepted past the overdraft.

## Run it

Needs Docker and Python 3.10+. Defaults: 8 send API nodes, 5,000 apps (a few large brands and a long
tail), 4,000 outbound messages a second, a Gupshup fee at accept and a Meta fee by category at delivery,
and balances compressed to about 30 seconds of each app's sending, so apps reach their overdraft while you
watch. Two apps in three top up a few seconds after they are blocked.

```
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python wallet.py &                   # the wallet service, fed by the charges lane
python send_api.py                   # 8 send nodes: fees out, 1003 in
```

They print a line a second:

```
[wallet]   7,700 charges/s  charge-to-wallet p50 …ms  p99 …ms  p99.9 …ms  PASS (<50ms)  states sent …
[ledger] apps blocked … low … of 5,000  blocks …  recharges …  repeats ignored …
         accepted past the overdraft … of … msgs (…%)  Meta fees after block $…
[send]  4,000 msgs accepted/s (steady)  delivered …  rejected 1003 …  recharges …
        in send-00's view: blocked … low …  state-to-send-nodes n … p50 …ms p99 …ms
```

**What the numbers mean.** The kit times each charge record from the moment `send_api.py` creates it to
the moment `wallet.py` has applied it, and each wallet state from `wallet.py` to every send node, through
Python clients, on whatever machine you run it on. On a shared 2-vCPU CI runner with the broker and all
scripts on one box, at reduced scale (2,000 outbound messages/s, 1,000 apps, 4 send nodes, 15-second
runway), three green runs held charge-to-wallet p50 3.7–4.1 ms and p99 6.8–7.3 ms (worst second
9.0–14.9 ms; all 62 steady-state seconds inside 50 ms in every run), and state-to-send-nodes p50
2.5 ms and p99 4.7–5.0 ms. With 2 MB/s of inbound messages on the same broker, the lane's p99 moved by
0.2 ms or less. Apps hit their overdraft 1,691–1,707 times, the send nodes refused 32,401–33,080 messages with 1003,
1,359–1,365 top-ups unblocked them, 2,079–2,126 records sent twice were charged once, and 3–5 of about
110,000 accepted messages (0.003–0.005%) were accepted past the overdraft. That is the end-to-end loop
through this demo code, not the broker alone. The broker's own tail latency is measured with `iggy-bench`
in `bench/PLAN.md`, on dedicated machines. CI runs:
https://github.com/psinglap/laserdata-kits/actions/runs/39858811179,
https://github.com/psinglap/laserdata-kits/actions/runs/39858834401,
https://github.com/psinglap/laserdata-kits/actions/runs/39858848027

| File | What it is |
|---|---|
| `send_api.py` | A fleet of send API nodes. Every accepted message publishes the node's running messages and Gupshup fee for that app, and every delivery its running Meta fee (resulting totals, with a sequence) to `charges`, keyed by app. Top-ups ride the same lane. Each node reads `wallet-state` directly and rejects with 1003 when told. |
| `wallet.py` | The wallet service. Sums the latest totals from every node per app against the opening balance and top-ups, raises low, blocks at the overdraft limit, unblocks on a top-up, and publishes each state. Prints the lane's p99 and messages accepted past the overdraft. |
| `inbound_flood.py` | A heavy inbound-messages stream on its own topic, for the isolation test. |
| `kafka_bridge.py` | Copies billing events (the documented `billing-event` shape) from Kafka into Iggy as a read-only consumer group, charged once per `gsId`, for a shadow run on real traffic. |
| `bench/PLAN.md` | The benchmark we run with you; rules fixed before results. |

## Three tests

1. **A festival campaign:** `python send_api.py --peak 5 --peak-at 60 --peak-for 120`.
   Pass: charge-to-wallet p99 under 50 ms, and messages accepted past the overdraft under 0.1%.
2. **Inbound messages next door:** `python inbound_flood.py --mbps 20` while both run.
   Pass: the charges lane's p99 moves by less than 10%.
3. **Restart the wallet:** stop `wallet.py` and start it again; it replays the charges topic and
   rebuilds every app's balance from the running totals, without charging anything twice.
   Pass: it catches up within seconds and blocked apps stay blocked.

## Design notes

- Running totals with a sequence make at-least-once delivery charge once: a repeated or replayed record
  changes nothing (`--dup` sends 1% of records twice to prove it). Apache Iggy 0.9 does not offer
  exactly-once, and this lane doesn't need it: it only decides when to accept and when to refuse.
  Invoices, daily usage and discount reports stay on your billing pipeline.
- Balances, overdrafts and the low threshold are compressed here (a quarter and a fifth of the opening
  balance). In production they are dollars: the overdraft you set per partner and the $5 alert. Apps the
  wallet doesn't know (a shadow run) start with `--default-balance`, `--default-overdraft 20` and
  `--default-low 5`.
- Fees are illustrative; set your rate card in `common.py`.

## Shadow your own traffic

```
pip install confluent-kafka
python kafka_bridge.py --bootstrap <kafka:9092> --topic <billing-events-topic> \
  --prices '{"marketing": …, "utility": …, "authentication": …, "service": …}'
```

The bridge only reads, as its own consumer group `iggy-shadow`; the services that produce billing events,
today's wallet deductions, invoices and the portal are untouched. It expects the `billing-event` JSON
from your inbound events docs (app, `deductions.category`, `billable`, `references.gsId`); if your internal
record differs or carries the deducted amount, change `to_charge()`.

## Next step

If the numbers are interesting: a 2–4 week evaluation on mirrored billing events for a set of apps,
beside your current pipeline. No advantage, no migration.

kranti@laserdata.com · laserdata.com
