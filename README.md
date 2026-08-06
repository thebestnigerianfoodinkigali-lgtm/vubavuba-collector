# Vubavuba collector

Scrapes the merchant portal (merchant.vubavuba.rw) every ~5 minutes via GitHub
Actions and POSTs normalized orders to the resto-ledger Worker, which books
them and — in auto mode — pushes today's new orders to the kitchen's Telegram
group.

**Why this exists here and not on a laptop:** the Worker cannot fetch the
portal directly (Cloudflare 1042), and the owner wanted a free, always-on
runner. Public repos get unmetered Actions minutes; nothing secret is in the
code — credentials live only in Actions secrets (`VUBAVUBA_USERNAME`,
`VUBAVUBA_PASSWORD`, `LEDGER_API_BASE`, `COLLECTOR_TOKEN`), and the collector
redacts them from its own logs.

**Honest latency:** GitHub's schedule floor is 5 minutes and delivery is
queued, so real cadence is 5–15 minutes. The Vubavuba merchant app on the
restaurant phone remains the instant alert; this is the bookkeeping path and
the kitchen-ticket backstop.

The collector itself (`vubavuba_collector.py`) is a verbatim copy of
`resto-ledger/collector/` — fix bugs there first, then copy the file here.
Every write it causes is an idempotent upsert over a rolling 3-day window, so
overlapping or delayed runs are harmless.
