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
queued, so real cadence is 5–15 minutes. That is why this is now the *backstop*
rather than the main path — see below.

**The fast path is `--watch` on the shop's phone.** `phone.py` starts the same
collector with `--watch` and keeps it running: it logs in **once**, then
re-reads the recent orders every ten seconds on that one session and books
whatever changed, so an order reaches the kitchen in about ten seconds instead
of five to fifteen minutes. The Actions run above stays exactly as it is — a
one-shot every five minutes, catching anything a phone that was off, flat or
off the wifi missed. Both write the same idempotent upserts, so they cannot
double-book each other.

A one-shot every ten seconds would have been 8,640 logins a day against a live
merchant account; ten-second polls on one session is a merchant watching their
own dashboard. That distinction is the whole reason watch mode exists.

The collector itself (`vubavuba_collector.py`) is a verbatim copy of
`resto-ledger/collector/` — fix bugs there first, then copy the file here.
Every write it causes is an idempotent upsert over a rolling 3-day window, so
overlapping or delayed runs are harmless.
