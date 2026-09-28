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
re-reads the recent orders every five seconds on that one session and books
whatever changed, so an order reaches the kitchen in seconds instead of five to
fifteen minutes. The Actions run above stays exactly as it is — a
one-shot every five minutes, catching anything a phone that was off, flat or
off the wifi missed. Both write the same idempotent upserts, so they cannot
double-book each other.

A one-shot every ten seconds would have been 8,640 logins a day against a live
merchant account; ten-second polls on one session is a merchant watching their
own dashboard. That distinction is the whole reason watch mode exists.

**All day, every day.** The restaurant trades 24 hours (since 2026-09-28), so
the collector no longer sleeps from 00:00 to 07:00 Kigali — both the phone's
watch and the Actions one-shot now work around the clock. The hours are a
setting, `COLLECTOR_ACTIVE_HOURS=H-H` (inclusive Kigali hours, e.g. `7-23` for
the old behaviour), read from the environment or the credentials file; unset or
unreadable means all day, and every start logs `active hours: …`. The reasoning
about logins is unchanged: one login per watch session, then one read every
five seconds on it.

The collector itself (`vubavuba_collector.py`) is a byte-identical copy of
`resto-ledger/collector/vubavuba_collector.py` — fix bugs there first, then copy
the file here unchanged (resto-ledger's `collector/tests/test_sync.py` compares
the two byte for byte). Every write it causes is an idempotent upsert over a
rolling 3-day window, so overlapping or delayed runs are harmless.

## Getting a new version onto the phone

`phone.py` re-downloads `vubavuba_collector.py` from this repo's raw URL on
`main` every time it starts or restarts the watch, and keeps the phone's copy if
the download fails. A running watch never re-reads its file, so a new version
takes effect at the next start:

1. Merge to `main` and push.
2. Check GitHub serves it: `curl -s https://raw.githubusercontent.com/thebestnigerianfoodinkigali-lgtm/vubavuba-collector/main/vubavuba_collector.py | shasum -a 256 | cut -c1-8`
   must equal `shasum -a 256 vubavuba_collector.py | cut -c1-8` (the raw cache can
   lag a few minutes).
3. Restart the watch on the phone — restart the phone (Termux:Boot starts it) or
   stop it and run `phone.py` again. Any automatic restart also fetches the file.
4. Check the phone's first lines: `this collector is version <those 8 characters>`
   and `active hours: all day`; the ledger's Ops page shows the same version.

The Actions backstop needs nothing: it checks out `main` on every run. A phone
with no internet gets the file through resto-ledger's `collector/phone-serve/`
LAN handover; a phone whose boot script runs the collector directly instead of
through `phone.py` does not self-update and needs that copy too.
