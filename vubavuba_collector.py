#!/usr/bin/env python3
"""
The Vubavuba collector: scrape the merchant portal on the owner's Mac, POST it in.

WHY THIS RUNS ON A LAPTOP AND NOT IN THE WORKER (ADR-004). Roughly half the
restaurant's revenue arrives through Vubavuba, whose merchant portal has no API.
The plan was a Cloudflare Worker cron: log in, scrape, upsert. A probe on
2026-08-05 settled it — a throwaway Worker fetching
`https://merchant.vubavuba.rw/views/auth/login.php` gets **Cloudflare error 1042**,
a platform-level block of the subrequest, while the same URL from the Mac answers
200 with a fresh PHPSESSID. Workers cannot reach that host at all. So the scrape
runs here, under launchd, and posts normalized JSON to the Worker's
`/api/imports/vubavuba` behind a collector token.

WHERE THE ORDERS COME FROM (portal rebuild, 2026-08-06). `views/sales-report.php`
used to server-render the table this program read. The rebuilt portal serves it as
a JavaScript shell whose rows arrive from `api/load_orders.php` — same query
parameters, `Accept: application/json`, same session cookie — as
`{success, ordersHtml, paginationHtml, pagination:{…}, isCombinedView}`. The
drift guard caught the change on its first run and refused to import anything,
which is exactly what it is for. So this program now asks for the JSON directly:
it is the same data one hop earlier, without a browser in the middle.

WHERE THE CUSTOMER COMES FROM (2026-08-08). The order list has no customer in it
— the nearest thing to one is the delivery address — so the name and phone number
the owner asked for come from a SECOND endpoint, the one behind the portal's own
**Details** button (`api/order_handler.php?id=…`). It is asked ONLY about orders
this program has never seen before, which is a handful a day against a window that
is re-read every ten seconds: `~/.local/state/resto-ledger/customer-details-seen.json`
is what remembers which those are across restarts. A details fetch that fails
costs that order its customer and nothing else — the order still imports, and both
fields are optional all the way through the ledger.

…AND, SINCE 2026-08-13, THE DISHES (`itemData`). That same response carries
`item_data`: one row per dish with its quantity, size, price, per-dish notes and
the ADD-ONS the customer chose — the protein, the sides, the spice level. The
report's free-text `Item` cell has none of that, so a four-combo order reached the
cooks as "3× Fried rice, 1× Party Jollof" and every choice in it was lost between
the customer and the pan (order 2929745, the owner's screenshots). It rides the
SAME request the customer already costs — zero extra load on the portal — and is
attached as one more optional wire field. A fetch that fails still sends the order
WITHOUT it, exactly as before, and the ledger falls back to what the list gave us.

  The split the owner asked for, enforced on the SERVER and written here because
  this is where the data enters: the ledger app shows the name AND the phone; the
  Telegram kitchen ticket and counter alert show the NAME ONLY. Nothing in this
  program decides that, and nothing in it should — it sends both fields and
  `worker/lib/kitchen.ts` is where the line is drawn.

TWO WAYS TO RUN IT, AND WHY THE SECOND ONE EXISTS (`--watch`, 2026-08-07). Bare,
this is a one-shot: log in, read the window, POST, exit — which is what the
launchd agent and the GitHub Actions backstop run, and what everything below
describes unless it says otherwise. `--watch` is the same program with the
sleeping done on the inside: it logs in ONCE, then re-reads the recent window
every five seconds on that one session and imports only when the payload actually
changed. The owner wants an order in the kitchen within seconds of it being
placed, and the arithmetic is what forces the shape. A one-shot at that cadence is
tens of thousands of logins a day against somebody's merchant account — a login
flood that looks exactly like credential stuffing, and the account it endangers is
the restaurant's. Fast polls on ONE session is a merchant watching their own
dashboard, which is what the portal is for. `WATCH_POLL_SECONDS` carries the rest
of the arithmetic, including why five and not two.

AND SINCE 2026-08-14, IT SAYS SO OUT LOUD. Watch mode adds two things that exist
for one failure — the phone that quietly stops watching. Every fourth completed
poll POSTs a liveness ping to the ledger (`ping_ledger`), which is how the Worker
can tell within seconds that this program has gone quiet and put the plain-English
"pick up the black phone" instructions on the counter's Telegram. And a deadman
thread (`Deadman`) watches the loop's own progress: a hang — the process alive,
the loop stuck in a call that never returns — becomes an exit, and an exit is the
one failure the phone's launcher already fixes by itself, in thirty seconds.
Neither can cost an import: the ping never raises into the loop and is never
retried, and the deadman is beaten by every request and every deliberate wait.

WHAT THIS PROGRAM PROMISES

  * It never guesses. Money that does not parse, a row whose column count
    changed, a payload missing the count that drives pagination — all of it stops
    the run with an exit code that says which, and dumps what surprised it. A
    scraper that carries on "best effort" writes wrong numbers into a ledger,
    which is worse than writing none.
  * It never logs a password or a token. Both are registered with a redacting
    filter on the root logger, so even a line written by a library cannot leak
    them.
  * It is polite. One request per second, minimum, and a User-Agent that says who
    it is. This is somebody's production server.
  * It is safe to run twice. Every write it causes is an idempotent upsert keyed
    on the portal's own ref number, so a double run, an overlapping window and a
    retry after a timeout all converge on the same rows.

EXIT CODES — launchd and a human read these.

    0  ok, or "not now" (outside 07:00-23:00 Kigali; nothing to do)
    1  unexpected error, including a pagination loop that would not end (a bug here)
    2  configuration problem (missing file, wrong permissions, missing key)
    3  authentication failed (the message says wrong credentials vs portal change)
       — and, in `--watch` ONLY, the deadman: the loop stopped making progress and
       killed the process so the launcher would start a fresh one. The two cannot
       be confused; `EXIT_HANG` says why, and the log line above the exit shouts.
    4  PORTAL DRIFT — the page is not the page we know. HTML dumped for a human.
    5  the ledger API refused or could not be reached after retries
    6  the portal was unreachable (network, DNS, timeout) — nothing to fix here

USAGE

    uv run --project collector vubavuba-collect            # the launchd path
    uv run --project collector vubavuba-collect --dry-run  # print, do not POST
    uv run --project collector vubavuba-collect --full     # backfill from 2026-01-01
    uv run --project collector vubavuba-collect --capture-fixtures
    uv run --project collector vubavuba-collect --watch    # one login, poll every 10s
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import signal
import stat
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterator, Sequence

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Exit codes
# ---------------------------------------------------------------------------

EXIT_OK = 0
EXIT_UNEXPECTED = 1
EXIT_CONFIG = 2
EXIT_AUTH = 3
EXIT_DRIFT = 4
EXIT_API = 5
EXIT_UNREACHABLE = 6

#: What the deadman exits with when `--watch` has hung. **THREE, the same number
#: `EXIT_AUTH` uses**, and the overlap is deliberate rather than an oversight.
#:
#: The two can never be confused, because they belong to different programs. A
#: one-shot run is the only thing that exits 3 for authentication — watch mode
#: catches every failure inside its own loop and backs off instead (see `watch`),
#: so an `AuthError` cannot reach `main` from there — and the deadman only exists
#: inside watch mode, which never exits 3 for anything else. The line logged
#: immediately before this exit says which one happened, in capitals, with a stack
#: for every thread under it.
EXIT_HANG = 3


class CollectorError(Exception):
    """Anything this program refuses to do, with the exit code it means."""

    exit_code = EXIT_UNEXPECTED


class ConfigError(CollectorError):
    exit_code = EXIT_CONFIG


class AuthError(CollectorError):
    exit_code = EXIT_AUTH


class PortalDrift(CollectorError):
    """The page is not the page we know. Never guess past this."""

    exit_code = EXIT_DRIFT


class PortalUnreachable(CollectorError):
    """
    We never got a page at all — DNS, a dropped connection, a timeout, no Wi-Fi.

    Deliberately NOT a `PortalDrift`. Drift means "somebody changed the portal and
    a human has to read the HTML and fix the parser"; this means "the laptop's
    network was down for a minute" and the next scheduled run will very likely
    just work. Reporting the second as the first sends somebody to read a drift
    dump that does not exist, and — worse — makes the exit code that means "the
    parser is broken" the one that fires most often, until nobody believes it.
    """

    exit_code = EXIT_UNREACHABLE


class PaginationError(CollectorError):
    """
    The page loop would not end. A bug here, or a portal disagreeing with itself.

    Also not drift: the columns were all exactly where they should be, which is
    the one thing a drift exit code is supposed to tell somebody about.
    """

    exit_code = EXIT_UNEXPECTED


class ApiError(CollectorError):
    exit_code = EXIT_API


# ---------------------------------------------------------------------------
# What the portal looks like (probes 2026-08-05, re-probed after the rebuild
# 2026-08-06)
# ---------------------------------------------------------------------------

PORTAL_BASE = "https://merchant.vubavuba.rw"
LOGIN_PATH = "/api/auth/auth.php"
LOGIN_PAGE_PATH = "/views/auth/login.php"

#: WHERE THE ORDERS ACTUALLY ARE. `views/sales-report.php` is a JavaScript shell
#: since the 2026-08-06 rebuild; its own script fetches this with
#: `Accept: application/json` and the session cookie, and so do we.
ORDERS_API_PATH = "/api/load_orders.php"

#: WHERE THE CUSTOMER IS (probed 2026-08-08). The order LIST has no customer in
#: it at all — the nearest thing is the delivery address — so the name and the
#: phone number the owner asked for come from the endpoint behind the portal's own
#: **Details** button: `sales-report.php`'s `openOrderModal(id)` fetches this with
#: the session cookie and renders the modal from it.
#:
#: The keys this program reads are four: `first_name`, `last_name`,
#: `contact_phone` and — since 2026-08-13 — `item_data`, the per-dish rows with
#: quantities, prices, sizes, notes and add-ons (`parse_order_items`). The response
#: carries more still (`delivery_address`, `delivery_charge`, `sub_total`,
#: `status`, `json_details`, `comment`, `ebm`); none of that is touched, because
#: every one of those facts already reaches the ledger from the order list.
ORDER_DETAILS_PATH = "/api/order_handler.php"

#: The page a human opens. Nothing is read from it — it is here so that the log
#: and the RUNBOOK can name the thing the owner sees in a browser.
REPORT_PAGE_PATH = "/views/sales-report.php"

SUMMARY_PATH = "/views/sales-summary.php"

USER_AGENT = "resto-ledger-collector/1.0"

#: The columns of a `load_orders.php` row, in order. The JSON carries no header
#: row, so — unlike the table this replaced — the names are documentation and the
#: COUNT is the contract. Everything downstream indexes into this order.
REPORT_COLUMNS = (
    "Ref#",
    "Location",
    "Items",
    "TransType",
    "Payment Type",
    "Packaging",
    "Total",
    "Status",
    "Date",
    "Time",
    "Actions",
)

(
    COL_REF,
    COL_LOCATION,
    COL_ITEMS,
    COL_TRANS_TYPE,
    COL_PAYMENT,
    COL_PACKAGING,
    COL_TOTAL,
    COL_STATUS,
    COL_DATE,
    COL_TIME,
    COL_ACTIONS,
) = range(len(REPORT_COLUMNS))

#: The keys `pagination` must carry, all of them whole numbers. `total_orders` is
#: what the page budget is derived from, and a payload without it is refused
#: rather than defaulted — see `parse_orders_json`.
PAGINATION_KEYS = ("current_page", "per_page", "total_orders", "total_pages")

#: `sales-summary.php` (Sales By Item) — still server-rendered after the rebuild.
#: These are the header cells AS THE MARKUP SPELLS THEM; the page's CSS
#: (`text-transform: uppercase`) is what makes them shout in a browser, which is
#: why the comparison is case-insensitive: the casing here is somebody's
#: stylesheet, not a fact about the data.
SUMMARY_HEADERS = (
    "Rank",
    "Item Name",
    "Size",
    "Unit Price (RWF)",
    "Quantity Sold",
    "Total Sales (RWF)",
)

#: The three the ledger's schema permits. A fourth is stored as null, not refused.
KNOWN_PAYMENT_TYPES = frozenset({"momo", "cod", "cc"})

#: Kigali is UTC+2, fixed, no DST, ever.
KIGALI = timezone(timedelta(hours=2))

#: Portal courtesy: never more than one request per second.
MIN_REQUEST_INTERVAL = 1.0

#: Rows per portal page (the portal's own maximum) and orders per API call.
PER_PAGE = 100
CHUNK_SIZE = 50

#: How many pages beyond the reported total we are willing to ask for. The table
#: is live: orders land while we page through it, so `entries of N` read on page 1
#: can legitimately be a few short by the time we reach the end.
PAGE_SLACK = 5

#: The true runaway guard, and nothing else. It is not a "how big can a backfill
#: be" limit — that is derived per run from the portal's own reported total — it
#: is the number at which a pagination loop that will never terminate is stopped
#: by force. 2000 pages is 200,000 orders, decades of this restaurant's volume.
ABSOLUTE_MAX_PAGES = 2000

#: `--full` starts here. The store's Vubavuba history does not predate it.
HISTORY_START = date(2026, 1, 1)

#: THE BUDGET FOR CUSTOMER DETAILS, and the two numbers that bound it.
#:
#: One extra GET per NEWLY-SEEN order — never per poll, never for a ref this
#: collector has already asked about. On an ordinary day that is one request per
#: order placed, which for this restaurant is a handful; the window is re-read
#: hundreds of times a day and costs the details endpoint nothing on any of them.
#:
#: The cap is not a queue and is not about ordinary days. It is about `--full`,
#: which first-sees a January-to-today backfill in one run: without a ceiling that
#: is thousands of sequential requests against somebody's production server, at one
#: per second, for hours — to fill in the customer of a meal eaten in February,
#: which nobody is going to ring. Orders past the cap are imported WITHOUT customer
#: fields and are not retried, and the run says so in its summary line. The fetches
#: are spent NEWEST FIRST, so the ones that get them are the ones a kitchen ticket
#: and a phone call are actually about.
MAX_DETAIL_FETCHES_PER_RUN = 25

#: The most dishes, and the most add-ons on one dish, this program will carry off
#: the details endpoint.
#:
#: A CEILING, NOT A TRUNCATION. Both are far beyond anything this restaurant sells
#: in one order — the biggest seen is four combos of three add-ons each — so a
#: payload past either of them is not a big order, it is a response this program
#: does not understand. It is refused WHOLE (`parse_order_items` returns None) and
#: the order imports with the item cell it has always had, because half a dish list
#: on a kitchen ticket is worse than the honest short one: a cook who can see four
#: dishes and is handed three has no way to know they were shorted.
MAX_DETAIL_ITEMS = 50
MAX_DETAIL_ADDONS = 20

#: Where "we have already asked about this ref" is remembered between runs, under
#: the state directory beside the log and the drift dumps.
#:
#: WITHOUT IT THE ONE-SHOT WOULD RE-ASK EVERY TIME. `--watch` is a long-lived
#: process and could have kept this in memory, but the launchd agent is a fresh
#: process every two hours reading the same rolling three-day window, so an
#: in-memory set would mean the whole window's details re-fetched eight times a
#: day, for ever, for data that cannot change. A small file is the difference
#: between one request per order and one request per order per run.
#:
#: BEST-EFFORT IN BOTH DIRECTIONS. Losing it costs one window's worth of re-asked
#: details (idempotent, and the ledger's COALESCE means a second answer is harmless);
#: failing to write it must never fail a run that has already imported.
KNOWN_REFS_FILE = "customer-details-seen.json"

#: How long a ref stays remembered. Long enough that a `--full` backfill and every
#: ordinary window are covered, short enough that the file stays a few hundred
#: lines rather than growing for ever. Pruned by the order's own date on every save.
KNOWN_REFS_KEEP_DAYS = 30

#: Kigali hours in which a run is worth doing at all.
ACTIVE_HOUR_FROM = 7
ACTIVE_HOUR_TO = 23

#: `--watch`: how often to re-read the window, and the floor under it. Both are
#: seconds, and the default now SITS ON the floor.
#:
#: It is the GAP between polls, not a period: the request itself and the
#: one-per-second courtesy throttle add a second or two on top, so five here is a
#: new order noticed within about seven seconds and often less.
#:
#: ── WHY IT MOVED FROM TEN TO FIVE, WITH THE ARITHMETIC ─────────────────────
#:
#: The owner's complaint after a week of live use was that the counter learns
#: about a Vubavuba order roughly ten seconds after the rider's app does, and in
#: that ten seconds a customer is standing at a till nobody has told. Halving the
#: gap costs, over the 17 active hours (07:00–23:59, `ACTIVE_HOUR_*`):
#:
#:     61,200 s ÷ 5 s  ≈  12,000 portal reads a day   (was ≈ 6,000 at ten)
#:
#: which is one request every five seconds from ONE logged-in session — about what
#: a human refreshing a dashboard with a hand on F5 produces, and the shape is what
#: matters here rather than the number. The portal is somebody else's production
#: server and this is a merchant account that can be suspended by a person who
#: thinks they are being scraped, so the honest ceiling is "still looks like a
#: person watching a screen".
#:
#: ── THE TWO-SECOND ASK IS REFUSED, AND THIS IS THE REASON ──────────────────
#:
#: Two seconds is ≈ 31,000 reads over the same hours — and ≈ 43,000 a day if
#: anybody ever removed the night skip — which stops looking like somebody
#: watching a screen and starts looking like a bot hammering a login-gated
#: endpoint. It also stops buying anything: the request and the one-per-second
#: throttle already put a second or two under every poll, so the real gain from
#: 5 s to 2 s is about three seconds of freshness in exchange for two and a half
#: times the traffic and a merchant account nobody can un-suspend from here. The
#: floor stays at five and this is the line: below it, `load_config` clamps and
#: says so.
#:
#: The floor also exists because `WATCH_POLL_SECONDS=1` in a config file is a typo
#: away. Somebody who wants the old behaviour back sets `WATCH_POLL_SECONDS=10`.
WATCH_POLL_SECONDS = 5.0
WATCH_POLL_FLOOR = 5.0

#: `--watch` backoff after a failed poll: 15s, doubling, never longer than five
#: minutes. THE CAP IS THE POINT. A portal that goes down at 19:00 must not be
#: met by a program that keeps knocking every ten seconds until morning — at the
#: cap it is twelve attempts an hour (and at most two login POSTs each, in the
#: one case where the failure IS the authentication), which is a patient client
#: rather than a login storm, and it still recovers within five minutes of the
#: portal coming back. Backing off is also strictly gentler than exiting: the
#: phone's supervisor restarts a dead collector in 30 seconds, forever.
WATCH_BACKOFF_START = 15.0
WATCH_BACKOFF_MAX = 300.0

#: `--watch`: re-read the WHOLE window this often even when the digest says
#: nothing moved. See `watch` — this is what makes the one-page poll safe on a
#: window that no longer fits in one page.
WATCH_SWEEP_SECONDS = 900.0

#: `--watch`: one line to say the loop is alive, this often. Anything more
#: frequent is 8,640 lines a day of "still nothing", which is how a log becomes
#: a thing nobody greps.
WATCH_HEARTBEAT_SECONDS = 3600.0

#: `--watch`: the longest a sleep may ignore a signal. Every wait in watch mode
#: is chopped into pieces this size, because PEP 475 makes `time.sleep` RESUME
#: after a handler returns — an eight-hour night sleep would swallow the SIGTERM
#: that was meant to stop it, and the operator would be left holding Ctrl-C.
WATCH_SLEEP_CHUNK = 5.0

#: WHERE THE LIVENESS PING GOES, and the ONE thing it is for: so the ledger can
#: tell "this phone is watching" from "this phone stopped", within seconds, and
#: ring the counter with the instructions when it stopped.
#:
#: Not to be confused with `WATCH_HEARTBEAT_SECONDS` above, which is a LINE IN THE
#: LOG for a human once an hour. This is a POST for the Worker, and the Worker's
#: Durable Object is what turns its absence into a message on somebody's phone.
HEARTBEAT_PATH = "/api/imports/vubavuba/heartbeat"

#: One ping every fourth completed poll — about one every 20-25 seconds at the
#: five-second gap, against the Worker's ">90 seconds of silence is an outage"
#: rule. Three or four pings have to go missing before anybody is told, which is
#: what keeps one dropped packet on a phone's wifi from ringing the counter.
#:
#: FOUR AND NOT ONE. A ping per poll is ~12,000 extra requests a day against the
#: Workers Free plan's 100,000 for the whole account (ADR-022 does that
#: arithmetic for the sockets) — real money's worth of budget for freshness
#: nobody can perceive. Four is ~3,000 a day, and the detection window it buys is
#: still inside the owner's "within about thirty seconds".
#:
#: AN IMPORT IS ALSO A HEARTBEAT — the Worker stamps one on every scraped chunk
#: it accepts — so a busy service is proving itself alive far more often than
#: this, and this is what covers the quiet hour between two orders.
WATCH_PING_EVERY_N_POLLS = 4

#: The ping's own timeout, and it is deliberately short. Everything else this
#: program says to the ledger is money and gets 120 seconds and three tries; this
#: is a timestamp, it is repeated in twenty seconds, and a slow answer must never
#: stand between a customer's order and the kitchen. See `ping_ledger`.
WATCH_PING_TIMEOUT = 5.0

#: THE DEADMAN: how far behind the loop may fall before this process kills itself
#: so that something else can start it again.
#:
#: `go.py`, the phone's launcher, already restarts a collector that EXITS. The
#: failure it cannot see is the one the owner actually had: the process alive, the
#: log silent, the loop stuck inside a call that never came back — a network read
#: blocking for ever despite its timeout, or Android suspending the thread. From
#: outside, a hang and a healthy quiet afternoon look identical.
#:
#: Three times the poll interval is "we have missed several polls in a row", and
#: the sixty-second floor is what keeps the default five-second gap from arming a
#: fifteen-second hair trigger — one slow page on a phone's mobile data must not
#: be a restart. At the default that is one minute of provable silence.
DEADMAN_POLL_MULTIPLE = 3
DEADMAN_FLOOR_SECONDS = 60.0

#: How often the deadman thread wakes up to look at the clock. Cheap: a sleeping
#: thread costs nothing, and this only bounds how late the diagnosis can be.
DEADMAN_CHECK_SECONDS = 5.0

DEFAULT_ENV_FILE = Path.home() / ".config" / "resto-ledger" / "collector.env"
DEFAULT_STATE_DIR = Path.home() / ".local" / "state" / "resto-ledger"

LOG = logging.getLogger("vubavuba")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Config:
    username: str
    password: str
    api_base: str
    collector_token: str
    window_days: int = 3
    portal_base: str = PORTAL_BASE
    #: `--watch` only. Ignored by every one-shot run.
    watch_poll_seconds: float = WATCH_POLL_SECONDS

    @property
    def secrets(self) -> tuple[str, ...]:
        """Everything the log must never contain."""
        return tuple(s for s in (self.password, self.collector_token) if s)


REQUIRED_KEYS = ("VUBAVUBA_USERNAME", "VUBAVUBA_PASSWORD", "LEDGER_API_BASE", "COLLECTOR_TOKEN")


def _parse_env_text(text: str) -> dict[str, str]:
    """`KEY=value` lines. `#` comments, blank lines and surrounding quotes ignored."""
    values: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def load_config(path: Path = DEFAULT_ENV_FILE) -> Config:
    """
    Read the credentials file, and REFUSE to read a readable one.

    The file holds the portal password and the ledger's collector token in clear
    text. On a laptop that is an acceptable trade only while the file is `chmod
    600`: anything looser means every process running as any other user on the
    machine — and anything that got a foothold as one — can read the credentials
    to a live merchant account. So group or world permission bits are a hard
    refusal with the fix printed, not a warning somebody scrolls past.
    """
    if not path.exists():
        raise ConfigError(
            f"no credentials file at {path}\n"
            f"  cp collector/collector.env.example {path}\n"
            f"  chmod 600 {path}   # then fill it in"
        )

    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise ConfigError(
            f"{path} is mode {mode:04o} — it holds a live portal password and the ledger's "
            f"collector token, so it must not be readable by anybody else.\n"
            f"  chmod 600 {path}"
        )

    values = _parse_env_text(path.read_text(encoding="utf-8"))
    missing = [k for k in REQUIRED_KEYS if not values.get(k)]
    if missing:
        raise ConfigError(f"{path} is missing: {', '.join(missing)}")

    api_base = values["LEDGER_API_BASE"].rstrip("/")
    if not api_base.startswith(("http://", "https://")):
        raise ConfigError(f"LEDGER_API_BASE must be an absolute URL, got {api_base!r}")
    if api_base.startswith("http://") and "localhost" not in api_base and "127.0.0.1" not in api_base:
        # The collector token travels in a header on every call. Over plain http
        # anybody on the same network reads it once and imports whatever they like.
        raise ConfigError(f"LEDGER_API_BASE must be https (except on localhost), got {api_base!r}")

    raw_days = values.get("WINDOW_DAYS", "3").strip() or "3"
    try:
        window_days = int(raw_days)
    except ValueError as err:
        raise ConfigError(f"WINDOW_DAYS must be a whole number of days, got {raw_days!r}") from err
    if not 1 <= window_days <= 365:
        raise ConfigError(f"WINDOW_DAYS must be between 1 and 365, got {window_days}")

    # CLAMPED, NOT REFUSED — the opposite of how WINDOW_DAYS is treated, on
    # purpose. A wrong WINDOW_DAYS silently changes which orders get imported, so
    # it stops the run; a wrong poll interval only changes how often we knock, so
    # the safe value is applied and said out loud. Refusing here would take a
    # working collector off the air over a number that has a right answer.
    raw_poll = values.get("WATCH_POLL_SECONDS", "").strip()
    try:
        poll_seconds = float(raw_poll) if raw_poll else WATCH_POLL_SECONDS
    except ValueError:
        LOG.warning("WATCH_POLL_SECONDS=%r is not a number — using %.0fs", raw_poll, WATCH_POLL_SECONDS)
        poll_seconds = WATCH_POLL_SECONDS
    if poll_seconds < WATCH_POLL_FLOOR:
        LOG.warning(
            "WATCH_POLL_SECONDS=%s is below the %.0fs floor — polling every %.0fs instead",
            raw_poll,
            WATCH_POLL_FLOOR,
            WATCH_POLL_FLOOR,
        )
        poll_seconds = WATCH_POLL_FLOOR

    return Config(
        username=values["VUBAVUBA_USERNAME"],
        password=values["VUBAVUBA_PASSWORD"],
        api_base=api_base,
        collector_token=values["COLLECTOR_TOKEN"],
        window_days=window_days,
        portal_base=(values.get("VUBAVUBA_BASE_URL") or PORTAL_BASE).rstrip("/"),
        watch_poll_seconds=poll_seconds,
    )


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


class RedactingFilter(logging.Filter):
    """
    Replace every known secret with `***`, everywhere, before anything is written.

    A rule like "do not log the password" is a rule somebody breaks in six months
    by adding a helpful debug line, or that a library breaks for them by echoing a
    request body. This makes the rule structural: the secrets are registered once
    and the filter sits on the root logger, so a leak has to get past it rather
    than merely past a reviewer.

    KNOWN LIMIT: this scrubs the formatted MESSAGE. A traceback rendered from
    `exc_info` is assembled by the Formatter afterwards and is not covered — so an
    exception whose own `str()` embedded a secret would still print. None of the
    exceptions raised here carry one (they carry paths, status codes and column
    lists), and Python does not put local variables in a traceback, so the gap is
    theoretical today; it is written down because that could stop being true.
    """

    def __init__(self, secrets: Sequence[str]) -> None:
        super().__init__()
        self._secrets = tuple(s for s in secrets if s and len(s) >= 4)

    def filter(self, record: logging.LogRecord) -> bool:
        if not self._secrets:
            return True
        try:
            message = record.getMessage()
        except Exception:  # pragma: no cover - a broken format string is not our business
            return True
        redacted = message
        for secret in self._secrets:
            if secret in redacted:
                redacted = redacted.replace(secret, "***")
        if redacted != message:
            record.msg = redacted
            record.args = ()
        return True


def setup_logging(state_dir: Path, secrets: Sequence[str] = (), verbose: bool = False) -> Path:
    """File (rotating, 1 MB × 5) plus stderr. Returns the log path."""
    state_dir.mkdir(parents=True, exist_ok=True)
    log_path = state_dir / "collector.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    for handler in list(root.handlers):
        root.removeHandler(handler)

    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%dT%H:%M:%S%z")

    file_handler = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(fmt)
    root.addHandler(stream)

    redactor = RedactingFilter(secrets)
    for handler in root.handlers:
        handler.addFilter(redactor)
    return log_path


# ---------------------------------------------------------------------------
# Cell parsers
# ---------------------------------------------------------------------------

_WHITESPACE = re.compile(r"\s+")
_INTEGER = re.compile(r"^-?\d+$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TIME = re.compile(r"^\d{2}:\d{2}:\d{2}$")


def collapse(text: str) -> str:
    """One space between words, nothing at the ends. HTML whitespace is noise."""
    return _WHITESPACE.sub(" ", (text or "").replace("\xa0", " ")).strip()


def parse_money(cell: str, field_name: str) -> int:
    """
    `"2,000 RWF"` → `2000`. `"0 RWF"` → `0`. `""` → `0`.

    RWF has no minor unit, so a decimal point here is not a rounding question: it
    means the portal started formatting money differently and every number on the
    page is now suspect. That is drift, and drift stops the run — a scraper that
    rounds writes a figure nobody was ever charged into an immutable ledger.
    """
    text = collapse(cell)
    if not text:
        return 0
    cleaned = text.upper()
    if cleaned.endswith("RWF"):
        cleaned = cleaned[: -len("RWF")]
    cleaned = cleaned.replace(",", "").replace(" ", "")
    if not cleaned:
        return 0
    if not _INTEGER.match(cleaned):
        raise PortalDrift(f"{field_name}: cannot read {text!r} as whole RWF (expected 'N,NNN RWF')")
    return int(cleaned)


def parse_date(cell: str, field_name: str) -> str:
    text = collapse(cell)
    if not _DATE.match(text):
        raise PortalDrift(f"{field_name}: expected YYYY-MM-DD, got {text!r}")
    return text


def parse_time(cell: str, field_name: str) -> str | None:
    text = collapse(cell)
    if not text:
        return None
    if not _TIME.match(text):
        raise PortalDrift(f"{field_name}: expected HH:MM:SS, got {text!r}")
    return text


# ---------------------------------------------------------------------------
# The report table
# ---------------------------------------------------------------------------


def _header_row(table: Any) -> list[str] | None:
    head = table.find("thead")
    row = head.find("tr") if head else table.find("tr")
    if row is None:
        return None
    cells = row.find_all(["th", "td"])
    if not cells:
        return None
    return [collapse(c.get_text(" ")) for c in cells]


def _first_table_with_headers(soup: BeautifulSoup) -> tuple[Any, list[str]] | None:
    for table in soup.find_all("table"):
        headers = _header_row(table)
        if headers:
            return table, headers
    return None


def assert_headers(soup: BeautifulSoup, expected: Sequence[str], what: str) -> Any:
    """
    HARD-ASSERT the column headers, and return the table they belong to.

    This is the line of defence for every page that still arrives as a rendered
    table. Every index used against it is a claim about markup written by somebody
    else's PHP. If a column is inserted, those indexes silently start reading the
    wrong thing and the ledger fills with plausible, wrong numbers. Refusing to
    parse an unfamiliar table is the only safe response.

    CASE IS IGNORED, and only case. The summary page's own stylesheet uppercases
    these cells (`text-transform: uppercase`), so the markup says `Rank` where a
    browser — and the probe notes taken from one — says `RANK`. Failing the run
    over which of the two a template happens to emit would be a drift alarm about
    a stylesheet. Everything else about the header, including its wording, its
    order and how many there are, still has to match exactly.
    """
    found = _first_table_with_headers(soup)
    if found is None:
        raise PortalDrift(f"{what}: no table with a header row on the page")
    table, headers = found
    if [h.casefold() for h in headers] != [h.casefold() for h in expected]:
        raise PortalDrift(
            f"{what}: PORTAL DRIFT — the columns are not the ones we know.\n"
            f"  expected ({len(expected)}): {list(expected)}\n"
            f"  found    ({len(headers)}): {headers}"
        )
    return table


def _data_rows(table: Any) -> Iterator[list[str]]:
    body = table.find("tbody") or table
    for tr in body.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue  # the header row, when there is no <thead>
        yield [collapse(td.get_text(" ")) for td in cells]


@dataclass
class PageResult:
    orders: list[dict[str, Any]]
    #: `pagination.total_orders` — how many rows the window holds in all.
    total_entries: int
    #: `pagination.total_pages` — the portal's own answer to "how many pages".
    total_pages: int = 0
    unknown_payment_types: list[str] = field(default_factory=list)
    #: `refNo` → the id `order_handler.php` wants, read out of the row's own
    #: **Details** link. See `detail_id_of`; a row whose link we cannot read is
    #: simply absent here and gets no customer.
    detail_ids: dict[str, str] = field(default_factory=dict)


def normalize_report_row(cells: Sequence[str], row_no: int) -> tuple[dict[str, Any], str | None]:
    """
    One row's eleven cells → the JSON the ledger's endpoint accepts.

    Returns the order and, when the payment column held something we do not know,
    that raw value — so the run can report it without failing. A platform that
    adds a payment rail tomorrow must not be able to stop the sync; the ledger
    stores null and a human sees the count in the summary line.
    """
    if len(cells) != len(REPORT_COLUMNS):
        raise PortalDrift(
            f"row {row_no}: expected {len(REPORT_COLUMNS)} columns, found {len(cells)} — {cells}"
        )

    payment_raw = collapse(cells[COL_PAYMENT]).lower()
    payment = payment_raw if payment_raw in KNOWN_PAYMENT_TYPES else None
    unknown = payment_raw if (payment_raw and payment is None) else None

    ref = collapse(cells[COL_REF])
    if not ref:
        raise PortalDrift(f"row {row_no}: no reference number — {cells}")

    order = {
        "refNo": ref,
        "location": collapse(cells[COL_LOCATION]) or None,
        "itemsRaw": collapse(cells[COL_ITEMS]) or None,
        "transType": collapse(cells[COL_TRANS_TYPE]) or None,
        "paymentType": payment,
        "packagingFee": parse_money(cells[COL_PACKAGING], f"row {row_no} packaging"),
        "total": parse_money(cells[COL_TOTAL], f"row {row_no} total"),
        "status": collapse(cells[COL_STATUS]).lower(),
        "orderDate": parse_date(cells[COL_DATE], f"row {row_no} date"),
        "orderTime": parse_time(cells[COL_TIME], f"row {row_no} time"),
    }
    if not order["status"]:
        raise PortalDrift(f"row {row_no}: no status — {cells}")
    return order, unknown


# ---------------------------------------------------------------------------
# `api/load_orders.php` — the rows, without the page around them
# ---------------------------------------------------------------------------

#: `<td><span>A</span>, <span>B</span></td>` rendered with a separator lands a
#: space in front of each comma. See `items_cell_text`.
_SPACE_BEFORE_COMMA = re.compile(r"\s+,")

#: The class the rebuilt portal puts on the status pill. Finding it at index 7 is
#: what pins the status column in place now that no header row arrives.
STATUS_BADGE_CLASS = "status-badge"

#: The single cell the endpoint sends instead of rows when a window is empty:
#: `<tr><td colspan='11' class='no-results'>No orders found.</td></tr>`.
_PLACEHOLDER_CELLS = 1


def items_cell_text(cell: Any) -> str:
    """
    The Items cell → `"Jollof Rice, Puff Puff"`, the way `itemsRaw` is spelled.

    The rebuilt portal wraps each dish in `<span class='title grey'>` and puts a
    literal `", "` between the spans. Pulling text out of that with a separator
    gives `"Jollof Rice , Puff Puff"` — a space in front of every comma — and that
    single space is not cosmetic. `items_raw` is stored verbatim and is an input
    to the row's content hash, and the XLSX importer (the other way orders reach
    this ledger) writes the cell without it. Leaving it in would make the two
    import paths disagree about every multi-item order in the history: one hash
    each, an "updated" row on every re-scrape, and a diff nobody can explain.

    Anything the cell carries beyond the spans — a `Comment:` note in its own
    `notes-indicator` div (the portal writes it in title case; the server's own
    marker regex is case-insensitive, which is why it lands), which the
    server splits off and stores separately — is kept, because the raw cell is the
    evidence and throwing away the part we did not expect is how a scraper loses
    the thing somebody needed.
    """
    return _SPACE_BEFORE_COMMA.sub(",", collapse(cell.get_text(" ")))


#: The id inside the row's own **Details** link: `onclick='openOrderModal(2915384)'`.
#: Quotes are tolerated because a template that starts emitting them has not changed
#: anything that matters, and refusing over one would cost every customer name.
_ORDER_MODAL_ID = re.compile(r"openOrderModal\(\s*['\"]?(\d+)['\"]?\s*\)")


def detail_id_of(actions_cell: Any) -> str | None:
    """
    The id `order_handler.php` wants for this row, or None.

    READ OUT OF THE ROW RATHER THAN ASSUMED. In every response probed so far the
    id and the Ref# are the same number, and it would be one character cheaper to
    send the ref. But they are two different fields of somebody else's database
    that happen to agree, and the failure mode if they ever stop agreeing is not a
    missing name — it is THE WRONG CUSTOMER'S name and phone number attached to
    this order, on a kitchen ticket and on the Orders screen. That is not a bug
    worth risking to save a regex.

    A row whose Actions cell we cannot read gets no detail fetch at all. Returning
    None is the whole error handling: no customer is the ordinary state of most
    orders in this ledger, and guessing an id is the one thing that must not happen.
    """
    match = _ORDER_MODAL_ID.search(str(actions_cell))
    return match.group(1) if match else None


def status_cell_text(cell: Any, row_no: int) -> str:
    """
    The Status cell → `"successful"`, read out of the badge that marks it.

    THIS IS A COLUMN-ORDER ASSERTION, not a convenience. The old table shipped a
    header row and this program refused to read a table whose headers had moved;
    the JSON ships no headers at all, so the guarantee that `cells[7]` is the
    status has to come from the markup instead. A status pill at index 7 is that
    guarantee: insert a column anywhere to its left and the span is not there any
    more, and the run stops instead of filing a date as a status.
    """
    badge = cell.find("span", class_=STATUS_BADGE_CLASS)
    if badge is None:
        raise PortalDrift(
            f"row {row_no}: no '{STATUS_BADGE_CLASS}' span in the status column (index {COL_STATUS}).\n"
            f"  found instead: {collapse(str(cell))[:200]!r}\n"
            f"  Either a column moved — in which case every index in this row is now wrong — or the "
            f"portal restyled the status pill. Both need a human to look before anything is imported."
        )
    return collapse(badge.get_text(" "))


def parse_orders_rows(orders_html: str) -> tuple[list[dict[str, Any]], list[str], dict[str, str]]:
    """
    `ordersHtml` (bare `<tr>`s) → normalized orders, the payment rails we do not
    know, and each row's details id.

    The fragment is wrapped in a table before parsing: `<tr>` outside a table is
    not valid HTML and a parser is entitled to throw the rows away, which would
    turn a busy day into a silent zero.
    """
    soup = BeautifulSoup(f"<table><tbody>{orders_html or ''}</tbody></table>", "html.parser")

    orders: list[dict[str, Any]] = []
    unknown: list[str] = []
    detail_ids: dict[str, str] = {}
    for row_no, tr in enumerate(soup.find_all("tr"), start=1):
        cells = tr.find_all("td")
        if not cells:
            continue
        if len(cells) == _PLACEHOLDER_CELLS:
            # `No orders found.` — one cell spanning the table. An empty window is
            # a quiet day, not a broken portal.
            continue
        if len(cells) != len(REPORT_COLUMNS):
            raise PortalDrift(
                f"row {row_no}: PORTAL DRIFT — expected {len(REPORT_COLUMNS)} columns "
                f"({', '.join(REPORT_COLUMNS)}), found {len(cells)}.\n"
                f"  row: {collapse(str(tr))[:400]!r}"
            )

        text = [collapse(td.get_text(" ")) for td in cells]
        text[COL_ITEMS] = items_cell_text(cells[COL_ITEMS])
        text[COL_STATUS] = status_cell_text(cells[COL_STATUS], row_no)

        order, unknown_payment = normalize_report_row(text, row_no)
        orders.append(order)
        if unknown_payment:
            unknown.append(unknown_payment)
        # Kept beside the order rather than inside it: it is a handle for a second
        # request, not a field of an order, and it must never reach the POST.
        detail_id = detail_id_of(cells[COL_ACTIONS])
        if detail_id is not None:
            detail_ids[order["refNo"]] = detail_id

    return orders, unknown, detail_ids


def _whole_number(value: Any) -> bool:
    """`True` for an int that is not a bool. JSON `true` is not a row count."""
    return isinstance(value, int) and not isinstance(value, bool)


def parse_orders_json(payload: Any) -> PageResult:
    """
    One `load_orders.php` response → the orders it carries and the counts that end
    pagination.

    THE GUARDS, AND WHY EACH ONE REFUSES RATHER THAN DEFAULTS:

      * `success` must be exactly `true`. The endpoint's own error shape is
        `{success:false, message:…}`, and the page's JavaScript renders that
        message into the table. Treating it as "no orders" would import a quiet
        day over a real one.
      * `pagination` must carry all four keys as whole numbers. `total_orders` is
        the only thing that knows when paging is finished, and a missing key
        defaulted to 0 would stop after page one and report a healthy
        `parsed=100 exit=0`. Silently importing the first hundred orders of a busy
        day is the worst failure this program could have, because nothing anywhere
        would look wrong. (That is not hypothetical: the footer-regex version this
        replaced had exactly that bug, from a pattern that matched "Page 1 of 3".)
      * `ordersHtml` must be a string. `null` is not "no rows"; it is a payload
        this program does not understand.
    """
    if not isinstance(payload, dict):
        raise PortalDrift(
            f"{ORDERS_API_PATH}: PORTAL DRIFT — expected a JSON object, got {type(payload).__name__}"
        )

    if payload.get("success") is not True:
        message = collapse(str(payload.get("message") or ""))[:300]
        raise PortalDrift(
            f"{ORDERS_API_PATH}: PORTAL DRIFT — the endpoint did not report success "
            f"(success={payload.get('success')!r}).\n"
            f"  message: {message or '(none)'}\n"
            f"  keys: {sorted(payload)}"
        )

    pagination = payload.get("pagination")
    if not isinstance(pagination, dict):
        raise PortalDrift(
            f"{ORDERS_API_PATH}: PORTAL DRIFT — no 'pagination' object, so there is no way to know "
            f"how many pages to read. Refusing to guess: an under-read looks exactly like a quiet day.\n"
            f"  keys: {sorted(payload)}"
        )
    bad = [k for k in PAGINATION_KEYS if not _whole_number(pagination.get(k))]
    if bad:
        raise PortalDrift(
            f"{ORDERS_API_PATH}: PORTAL DRIFT — pagination.{{{', '.join(bad)}}} missing or not a whole "
            f"number, and the page budget is derived from it. Refusing to guess: an under-read looks "
            f"exactly like a quiet day.\n"
            f"  pagination: {pagination}"
        )

    orders_html = payload.get("ordersHtml")
    if not isinstance(orders_html, str):
        raise PortalDrift(
            f"{ORDERS_API_PATH}: PORTAL DRIFT — 'ordersHtml' is {type(orders_html).__name__}, not a string"
        )

    orders, unknown, detail_ids = parse_orders_rows(orders_html)
    return PageResult(
        orders=orders,
        total_entries=int(pagination["total_orders"]),
        total_pages=int(pagination["total_pages"]),
        unknown_payment_types=unknown,
        detail_ids=detail_ids,
    )


#: What `sales-summary.php` renders INSTEAD OF A TABLE when a window holds no
#: sales: `<div class="no-data-message"><h3>No Sales Data Available</h3>…`.
SUMMARY_EMPTY_CLASS = "no-data-message"


def summary_says_empty(soup: BeautifulSoup) -> bool:
    """
    Did the summary page say "nothing sold", as opposed to changing shape?

    THE BUG THIS EXISTS FOR. On a day with no sales the page drops the table
    entirely — there is no header row to assert, so `assert_headers` called it
    "no table with a header row on the page" and the run exited 4, PORTAL DRIFT,
    complete with a dump for somebody to read. The collector runs from 07:00; a
    restaurant has sold nothing at 07:00 most mornings. That is a drift alarm
    fired by an ordinary quiet hour, and an alarm that cries wolf every morning is
    an alarm nobody reads on the day the columns really do move.

    It is matched on the page's own empty-state block rather than on the absence
    of a table, because "the portal said there is nothing" and "the table we knew
    is gone" have to stay different answers.
    """
    node = soup.find(class_=SUMMARY_EMPTY_CLASS)
    return node is not None and "no sales data" in collapse(node.get_text(" ")).lower()


def parse_summary_page(html: str) -> list[dict[str, Any]]:
    """`sales-summary.php` → the item snapshot rows the ledger stores per day."""
    soup = BeautifulSoup(html, "html.parser")
    if _first_table_with_headers(soup) is None and summary_says_empty(soup):
        LOG.info("sales-summary.php reports no sales in this window")
        return []
    table = assert_headers(soup, SUMMARY_HEADERS, "sales-summary.php")

    rows: list[dict[str, Any]] = []
    for index, cells in enumerate(_data_rows(table), start=1):
        if len(cells) == 1:
            continue
        if len(cells) != len(SUMMARY_HEADERS):
            raise PortalDrift(
                f"summary row {index}: expected {len(SUMMARY_HEADERS)} columns, found {len(cells)} — {cells}"
            )
        name = collapse(cells[1])
        if not name:
            continue
        quantity = collapse(cells[4]).replace(",", "")
        if not _INTEGER.match(quantity):
            raise PortalDrift(f"summary row {index}: cannot read quantity {cells[4]!r}")
        rows.append(
            {
                "itemName": name,
                # '' and not null: the ledger's UNIQUE key is
                # (business_date, item_name, size, unit_price) and SQLite treats
                # every NULL as distinct, so a null here would insert a fresh row
                # on every capture instead of updating the day's.
                "size": collapse(cells[2]),
                "unitPrice": parse_money(cells[3], f"summary row {index} unit price"),
                "qty": int(quantity),
                "total": parse_money(cells[5], f"summary row {index} total"),
            }
        )
    return rows


def looks_like_login(url: str, html: str) -> bool:
    """
    Did the portal bounce us to the login form?

    Sessions expire server-side (the probe watched it happen mid-browse), and on
    the RENDERED pages the bounce is a 302 to `views/auth/login.php` that requests
    follows into a 200 with a login form — not a 401 — so "did this work" has to
    be answered by looking at what came back. Both signals are checked because
    either can be the one present: the URL after redirects, and the form's own
    markers (`id="form_submit"`, a password input).
    """
    if "login.php" in (url or ""):
        return True
    text = html or ""
    if 'id="form_submit"' in text:
        return True
    return 'type="password"' in text and 'name="password"' in text


def api_session_expired(response: requests.Response) -> bool:
    """
    Did the JSON endpoint say "log in again", in either of the two ways it might?

    THE API DOES NOT BOUNCE LIKE THE PAGES DO. Probed 2026-08-06 with no cookie
    and with a junk `PHPSESSID`, `api/load_orders.php` answers **HTTP 401** with
    `{"session_expired":true,"message":"Session expired. Please login again."}` —
    no redirect, no HTML, and `looks_like_login` is False on every byte of it. A
    collector that only knew the old bounce would have taken that for an
    unreadable payload and reported PORTAL DRIFT every time a session aged out,
    which is a thing that happens on its own schedule and fixes itself with one
    POST.

    The HTML shape is checked too, and deliberately not because it was observed:
    the rendered pages still 302 to the login form, the two behaviours live in the
    same codebase, and a portal that starts sending the API down the same path is
    a normal afternoon's deploy. Detecting a bounce that never comes costs
    nothing; missing it costs a night of exit 4.
    """
    if response.status_code == 401:
        return True
    if looks_like_login(response.url, response.text):
        return True
    try:
        body = response.json()
    except ValueError:
        return False
    return isinstance(body, dict) and bool(body.get("session_expired"))


# ---------------------------------------------------------------------------
# The portal session
# ---------------------------------------------------------------------------


class Throttle:
    """One request per second, minimum. This is somebody's production server."""

    def __init__(self, interval: float = MIN_REQUEST_INTERVAL, sleeper=time.sleep) -> None:
        self.interval = interval
        self._sleep = sleeper
        self._last = 0.0

    def wait(self) -> None:
        # Every request this program makes to the portal passes through here, so
        # this is the cheapest honest place to say "still working" to the deadman
        # — an import that legitimately takes minutes (a busy day's details, a
        # sweep over a long window) keeps beating, and a call that hangs stops.
        beat()
        now = time.monotonic()
        gap = self.interval - (now - self._last)
        if gap > 0:
            self._sleep(gap)
        self._last = time.monotonic()


class Portal:
    """A logged-in `requests.Session`, with the re-login built in."""

    def __init__(self, cfg: Config, session: requests.Session | None = None, throttle: Throttle | None = None) -> None:
        self.cfg = cfg
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.throttle = throttle or Throttle()

    def _get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        self.throttle.wait()
        url = f"{self.cfg.portal_base}{path}"
        LOG.debug("GET %s %s", url, params or {})
        try:
            return self.session.get(url, params=params, headers=headers, timeout=60)
        except requests.RequestException as err:
            # A network failure is NOT drift. Nobody has to read any HTML, there
            # is no dump to read, and the next run in two hours will very likely
            # just work.
            raise PortalUnreachable(f"GET {path}: {type(err).__name__}: {err}") from err

    def login(self) -> None:
        """
        Form-POST `username`/`password` to `api/auth/auth.php`. No captcha, no CSRF.

        The failure message distinguishes the two causes, because they have
        completely different fixes and the person reading the log is not here.
        Wrong credentials means edit the env file; a portal that answers something
        unexpected means the login flow changed and this code needs a human.
        """
        self.throttle.wait()
        url = f"{self.cfg.portal_base}{LOGIN_PATH}"
        LOG.info("logging in as %s", self.cfg.username)
        try:
            response = self.session.post(
                url,
                data={"username": self.cfg.username, "password": self.cfg.password},
                timeout=60,
                allow_redirects=True,
            )
        except requests.RequestException as err:
            # Reaching nothing is not the same as being refused by something.
            raise PortalUnreachable(f"POST {LOGIN_PATH}: {type(err).__name__}: {err}") from err

        if response.status_code >= 400:
            raise AuthError(
                f"the login endpoint answered HTTP {response.status_code} — the portal's login flow "
                f"has changed (PORTAL CHANGE, not a credentials problem)"
            )

        body = (response.text or "")[:4000].lower()
        if any(word in body for word in ("invalid", "incorrect", "wrong password", "user not found", "failed")):
            raise AuthError(
                "the portal rejected these credentials (WRONG CREDENTIALS) — check VUBAVUBA_USERNAME "
                f"and VUBAVUBA_PASSWORD in {DEFAULT_ENV_FILE}"
            )
        # No "logged in" flag is kept. Whether the session is good is answered by
        # what the next page LOOKS like (`looks_like_login`), because the portal
        # expires sessions server-side and a boolean here could only ever record
        # what was true a moment ago.

    def get_authenticated(self, path: str, params: dict[str, Any] | None = None) -> requests.Response:
        """GET, and if that lands on the login form, log in once and GET again."""
        response = self._get(path, params)
        if not looks_like_login(response.url, response.text):
            return response

        LOG.info("session expired or absent — re-authenticating")
        self.login()
        response = self._get(path, params)
        if looks_like_login(response.url, response.text):
            raise AuthError(
                "still on the login page after a successful-looking login POST — either the "
                "credentials are wrong or the portal's session handling has changed. "
                "(WRONG CREDENTIALS is the likelier of the two.)"
            )
        return response

    def get_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        dump_label: str | None = None,
        dump: bool = True,
    ) -> Any:
        """
        GET a JSON endpoint, logging back in once if the session has aged out.

        Same contract as `get_authenticated`, against an endpoint that reports an
        expired session as a 401 with a JSON body rather than as a page. What
        comes back is DECODED HERE rather than by the caller, so that a body which
        is not JSON at all is refused with the dump that makes it diagnosable
        instead of surfacing three frames away as a `ValueError`.

        `dump_label` names the drift dump. It defaults to the endpoint's own file
        name, so a second JSON endpoint added here cannot quietly write its dumps
        under the first one's name — which is exactly the sort of thing that
        wastes twenty minutes at 07:00.

        `dump=False` puts the evidence in the LOG instead of in a file, and exists
        for the one caller whose failures are survivable. A dump is an unbounded
        file written per failure; that is exactly right for the orders endpoint,
        where a failure stops the run and there will be one of them. The details
        endpoint is asked once per new order and a failure there costs a customer
        name and nothing else, so a portal that removed it would otherwise leave a
        dump per order per day, for ever, in a directory nobody prunes. The log
        rotates at 1 MB × 5 and is where somebody looks anyway.
        """
        label = dump_label or _label_for(path)
        headers = {"Accept": "application/json"}
        response = self._get(path, params, headers)
        if api_session_expired(response):
            LOG.info("session expired or absent — re-authenticating")
            self.login()
            response = self._get(path, params, headers)
            if api_session_expired(response):
                raise AuthError(
                    f"{path} still reports an expired session (HTTP {response.status_code}) after a "
                    f"successful-looking login POST — either the credentials are wrong or the portal's "
                    f"session handling has changed. (WRONG CREDENTIALS is the likelier of the two.)"
                )

        body = response.text or ""
        if response.status_code >= 400:
            # Not a session problem — that was ruled out above — and not something
            # to retry blindly against somebody's production server. Keep the
            # evidence and stop: whatever this is, a human reading the body is the
            # fast path.
            raise PortalDrift(
                f"{path}: PORTAL DRIFT — HTTP {response.status_code} from an endpoint that should "
                f"answer 200 with JSON. This is the portal refusing or failing, not a column that "
                f"moved; {_evidence(body, label, dump)}"
            )
        try:
            return response.json()
        except ValueError as err:
            raise PortalDrift(
                f"{path}: PORTAL DRIFT — the response is not JSON ({err}). "
                f"content-type was {response.headers.get('content-type')!r}. "
                f"{_evidence(body, label, dump)}"
            ) from err


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------


@dataclass
class Scraped:
    orders: list[dict[str, Any]]
    pages: int
    unknown_payment_types: list[str]
    duplicate_refs: int
    #: The page-1 payload, verbatim, for `--capture-fixtures`.
    first_page_payload: Any = None
    #: Times the portal reported orders it then declined to hand over.
    contradictions: int = 0
    #: `refNo` → the id its Details link carries, across every page. See
    #: `detail_id_of`; a ref missing from here gets no customer fetch.
    detail_ids: dict[str, str] = field(default_factory=dict)


def orders_params(page: int, start: date, end: date) -> dict[str, Any]:
    """
    The query `load_orders.php` is asked with — ONE definition, two callers.

    The paging loop below and `--watch`'s poll both ask this endpoint for rows,
    and they have to ask the same question: a poll that filtered differently from
    the import would go quiet on exactly the orders the import would have caught.
    So the shape lives here rather than in two dict literals that agree today.
    """
    return {
        "page": page,
        "per_page": PER_PAGE,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        # Sent empty, as the portal's own script sends them. An endpoint that
        # starts requiring them must not be able to fail us quietly.
        "search": "",
        "status": "",
    }


def scrape_orders(portal: Portal, start: date, end: date) -> Scraped:
    """
    Page through `api/load_orders.php` for the window and normalize everything.

    DEDUPED BY REF NUMBER across pages, on purpose. The portal paginates a live
    table: an order created between page 1 and page 2 shifts every row along, and
    the same ref can arrive twice. The ledger's endpoint refuses a chunk that
    repeats a ref (two upserts of one row inside one transaction would fight), so
    a duplicate here would turn into a 400 for the whole batch. The later copy
    wins — it is the fresher read of the same row.

    THE PAGE BUDGET IS DERIVED, NOT GUESSED. Page 1 reports how many orders the
    window holds and how many pages it means to serve them in, so the number of
    pages a correct run needs is arithmetic rather than a constant. A fixed cap
    cannot be both generous enough for a January-to-today backfill and tight
    enough to catch a loop, and the fixed 200 an earlier version used was actually
    SMALLER than a backfill needs — it would have aborted a legitimate `--full`
    run partway through, with a message blaming the portal.

    Both of the portal's numbers are honoured, and the LARGER one wins. They agree
    today (`total_pages == ceil(total_orders / per_page)`, probed), but they are
    two independent claims and the failure they guard against is asymmetric: a
    budget one page short silently truncates an import, while a budget one page
    long costs one request that returns `No orders found.`
    """
    by_ref: dict[str, dict[str, Any]] = {}
    detail_ids: dict[str, str] = {}
    unknown: list[str] = []
    duplicates = 0
    contradictions = 0
    first_payload: Any = None
    page = 1
    # Until page 1 tells us what this window actually holds, only the runaway
    # guard applies.
    page_budget = ABSOLUTE_MAX_PAGES

    while True:
        if page > page_budget:
            raise PaginationError(
                f"pagination did not terminate: asked for {page - 1} page(s) with a budget of "
                f"{page_budget}. This is NOT portal drift — every row had its columns where they "
                f"should be. Either the portal keeps serving rows past its own reported total, or the "
                f"page parameter is being ignored."
            )
        payload = portal.get_json(ORDERS_API_PATH, orders_params(page, start, end))
        if page == 1:
            first_payload = payload

        try:
            result = parse_orders_json(payload)
        except PortalDrift:
            dump_drift(_as_json_text(payload), _label_for(ORDERS_API_PATH), suffix=".json")
            raise

        if page == 1:
            # Arithmetic, from the portal's own counts. `-(-a // b)` is ceiling
            # division without importing math for one line.
            needed = max(result.total_pages, -(-result.total_entries // PER_PAGE))
            page_budget = min(ABSOLUTE_MAX_PAGES, needed + PAGE_SLACK)
            LOG.info(
                "%d order(s) over %d page(s) reported → reading up to %d page(s)",
                result.total_entries,
                result.total_pages,
                page_budget,
            )

        LOG.info("page %d: %d row(s)", page, len(result.orders))
        for order in result.orders:
            if order["refNo"] in by_ref:
                duplicates += 1
            by_ref[order["refNo"]] = order
        detail_ids.update(result.detail_ids)
        unknown.extend(result.unknown_payment_types)

        if not result.orders:
            if page == 1 and result.total_entries > 0:
                # THE PORTAL CONTRADICTING ITSELF, and the one way this loop
                # could still end in a silent under-read. It says the window
                # holds N orders and then hands back none of them, so `parsed=0
                # exit=0` would go in the log looking exactly like a quiet day —
                # which is the failure this program spends most of its guards
                # avoiding. Nothing is imported either way, so this does not stop
                # the run; it just refuses to let it pass unremarked.
                contradictions += 1
                LOG.error(
                    "PORTAL DISAGREES WITH ITSELF — page 1 reported %d order(s) in %s..%s and then "
                    "returned no rows. Nothing has been imported. This is NOT a quiet day: either the "
                    "window parameters stopped being honoured or the row query is failing behind a "
                    "count that still works.",
                    result.total_entries,
                    start,
                    end,
                )
            break
        # `pagination.total_orders` is the authority, full stop — and it is always
        # there, because `parse_orders_json` treats its absence as drift. Stopping
        # on "fewer than per_page rows" instead would quietly lose every order
        # after page one whenever the portal declined to honour `per_page`, and
        # that would look exactly like a quiet day.
        if len(by_ref) + duplicates >= result.total_entries:
            break
        page += 1

    return Scraped(
        orders=list(by_ref.values()),
        pages=page,
        unknown_payment_types=unknown,
        duplicate_refs=duplicates,
        first_page_payload=first_payload,
        contradictions=contradictions,
        detail_ids=detail_ids,
    )


# ---------------------------------------------------------------------------
# The customer, from the endpoint behind the portal's own Details button
# ---------------------------------------------------------------------------


def parse_order_details(payload: Any) -> tuple[str | None, str | None]:
    """
    One `order_handler.php` response → `(customer_name, customer_phone)`.

    THIS FUNCTION NEVER RAISES, and that is the difference between it and every
    other parser in this file. The others guard an ORDER — money, a status, a date
    — where guessing writes a wrong number into a ledger and stopping is the only
    honest answer. This one guards a name. An order with no customer attached is
    the ordinary state of every row in this ledger written before 2026-08-08, the
    app and the ticket both simply print no customer line, and the import is
    additive: refusing the whole run because a name was missing would trade the
    restaurant's order sync for a nicety.

    So anything unexpected — not an object, keys absent, a number where a string
    should be — is `(None, None)`.

    The name is `"first last"` with the whitespace collapsed, because the portal's
    two fields are what a person typed into a delivery app and routinely carry
    trailing spaces and double spaces. Either half alone is a name too: `first_name`
    with no surname is extremely common and is still what a cook calls out.

    WHAT IS DELIBERATELY IGNORED. The response also carries `delivery_address`,
    `delivery_charge`, `sub_total`, `comment` and `ebm`. Every one of those facts
    already reaches the ledger from the order LIST, and a second, differently
    spelled copy of a number the books already have is how two writers start
    disagreeing about one order. The dishes are the exception, and they have their
    own parser below: the list has nothing like them.
    """
    if not isinstance(payload, dict):
        return None, None

    def text(key: str) -> str:
        value = payload.get(key)
        return collapse(value) if isinstance(value, str) else ""

    name = collapse(f"{text('first_name')} {text('last_name')}")
    phone = text("contact_phone")
    return (name or None), (phone or None)


def _detail_int(value: Any) -> int | None:
    """A portal number — `2`, `"2"`, `"3,000"` — as a whole one, or None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if not isinstance(value, str):
        return None
    text = collapse(value).upper().removesuffix("RWF").replace(",", "").replace(" ", "")
    return int(text) if _INTEGER.match(text) else None


def parse_order_items(payload: Any) -> list[dict[str, Any]] | None:
    """
    One `order_handler.php` response → the DISHES, or None when there is nothing
    here this program is willing to call a dish list.

    THIS FUNCTION NEVER RAISES either, and for `parse_order_details`' reason: what
    it guards is a richer ticket, not a number in a ledger, and the order behind it
    imports either way. None is the ordinary answer for a portal that changed shape
    or an endpoint that answered with something else entirely, and the ledger's
    fallback ladder (`item_data` → the parsed `Item` cell → the cell verbatim) is
    what makes None cost a detail rather than a dinner.

    ── ALL OR NOTHING, WHICH IS THE ONE RULE WORTH ARGUING ──────────────────────

    A row this parser cannot read refuses the WHOLE list. Not the row: the list.
    Dropping one dish would hand the kitchen a ticket that looks complete and is
    short by a plate, and there is nothing on it that could tell a cook so — while
    refusing the list drops the order back to exactly the ticket it would have got
    last week. A missing add-on is the same argument one level down: the add-ons
    ARE the order (`Choice of protein: Fish` is what makes it dinner rather than
    rice), so a malformed one refuses the list too.

    The QUANTITY is held to that same line and the PRICE is not, and the asymmetry
    is deliberate. A quantity we cannot read is a structural claim we cannot make —
    structure is the entire reason this endpoint is worth a second look at all — so
    it refuses. A price is display-only, never reaches a kitchen ticket (the
    owner's priceless-ticket rule) and is simply dropped when it will not parse: a
    dish nobody can price is still a dish somebody has to cook.

    ── THE SHAPE THAT GOES ON THE WIRE ─────────────────────────────────────────

    The portal's names are translated once, here, into the ledger's:

        {"name": "Party Jollof", "qty": 1, "size": "Large", "category": "Combos",
         "notes": "no pepper", "price": 9000,
         "addons": [{"name": "Fish", "category": "Choice of protein", "price": 0}]}

    `name` and `qty` are always there; everything else is omitted when the portal
    left it empty, which keeps the JSON well inside the ledger's ~8 KB column cap
    on the biggest order this restaurant has ever taken. `worker/sync/vubavuba.ts`
    shape-checks the same fields again on the way in — this program runs on
    somebody's laptop and the server is not entitled to trust it.
    """
    if not isinstance(payload, dict):
        return None
    rows = payload.get("item_data")
    if not isinstance(rows, list) or not rows:
        return None

    def refuse(why: str) -> None:
        LOG.warning("item_data ignored (%s) — the order imports with the list's Item cell", why)

    if len(rows) > MAX_DETAIL_ITEMS:
        refuse(f"{len(rows)} dishes, past the ceiling of {MAX_DETAIL_ITEMS}")
        return None

    items: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            refuse("a dish that is not an object")
            return None

        name = collapse(row.get("item_name")) if isinstance(row.get("item_name"), str) else ""
        if not name:
            refuse("a dish with no name")
            return None

        qty = _detail_int(row.get("quantity"))
        if qty is None or qty <= 0:
            refuse(f"{name}: cannot read {row.get('quantity')!r} as a quantity")
            return None

        item: dict[str, Any] = {"name": name, "qty": qty}
        for key, source_key in (("size", "size"), ("category", "category_name"), ("notes", "notes")):
            value = row.get(source_key)
            text = collapse(value) if isinstance(value, str) else ""
            if text:
                item[key] = text
        price = _detail_int(row.get("price"))
        if price is not None and price >= 0:
            item["price"] = price

        addons_raw = row.get("addons")
        if addons_raw is not None and not isinstance(addons_raw, list):
            refuse(f"{name}: an addons field that is not a list")
            return None
        addons: list[dict[str, Any]] = []
        if isinstance(addons_raw, list):
            if len(addons_raw) > MAX_DETAIL_ADDONS:
                refuse(f"{name}: {len(addons_raw)} add-ons, past the ceiling of {MAX_DETAIL_ADDONS}")
                return None
            for entry in addons_raw:
                if not isinstance(entry, dict):
                    refuse(f"{name}: an add-on that is not an object")
                    return None
                addon_name = collapse(entry.get("item_name")) if isinstance(entry.get("item_name"), str) else ""
                if not addon_name:
                    refuse(f"{name}: an add-on with no name")
                    return None
                addon: dict[str, Any] = {"name": addon_name}
                category = entry.get("category_name")
                if isinstance(category, str) and collapse(category):
                    addon["category"] = collapse(category)
                addon_price = _detail_int(entry.get("price"))
                if addon_price is not None and addon_price >= 0:
                    addon["price"] = addon_price
                addons.append(addon)
        if addons:
            item["addons"] = addons

        items.append(item)

    return items


@dataclass
class DetailFetch:
    """What one run's details fetching did, for the summary line."""

    #: Requests actually made. This IS the extra load on the portal.
    fetched: int = 0
    #: Of those, how many came back with a name.
    found: int = 0
    #: Of those, how many came back with a dish list this program could read.
    with_items: int = 0
    #: Fetches that failed or returned nothing usable. Never fails the run.
    failures: int = 0
    #: New orders left without a customer because the run hit its cap.
    over_cap: int = 0
    #: New orders whose row carried no readable Details link.
    no_detail_id: int = 0
    #: The first payload, verbatim, for `--capture-fixtures`.
    first_payload: Any = None


def fetch_order_details(
    portal: Portal,
    orders: Sequence[dict[str, Any]],
    detail_ids: dict[str, str],
    known_refs: dict[str, str],
) -> DetailFetch:
    """
    Attach `customerName`/`customerPhone` — and `itemData`, the dishes — to the
    orders this collector has never seen before. MUTATES the order dicts, and never
    raises.

    ONE REQUEST CARRIES BOTH. The dishes were added in 2026-08-13 and cost the
    portal nothing extra: `order_handler.php` was already being asked for the
    customer, and `item_data` is in the answer it was already sending. That is the
    whole reason this feature could be built at all without making the details
    endpoint load-bearing — the budget, the cap and the never-retry rule below are
    unchanged, and so is what a failure costs.

    ── WHY ONLY THE NEW ONES ────────────────────────────────────────────────────
    This is a SECOND request per order, against the same production server the
    window is being read from, and a customer's name does not change after the
    order is placed. The window holds three days; watch mode re-reads it every ten
    seconds. Fetching details for everything in the window would turn a poll that
    costs one request into a poll that costs thirty, for ever, to re-learn what we
    already know. So the question this asks is "which refs are new to us", and
    `known_refs` is the answer that survives a restart.

    ── WHAT A FAILURE COSTS, AND WHY IT IS NOT AN EXIT CODE ─────────────────────
    A name and a richer ticket, and nothing else — THE LADDER LAW. Each fetch is
    wrapped on its own: a 500, an HTML error page, a dropped connection or a
    payload we do not recognise leaves that one order with no customer fields and
    no `itemData`, is logged, and the run carries on and imports the order exactly
    as it would have last week — the portal's free-text `Item` cell, parsed as it
    always was. The ledger takes all three fields as optional and never erases a
    stored one with a null, so what is missed today can still land tomorrow if it
    is ever re-fetched.

    ── ONE EXCEPTION, AND IT IS NOT THIS FUNCTION'S ─────────────────────────────
    `AuthError` is re-raised. An expired session that `get_json`'s own re-login
    could not fix is not "no customer for this order" — it is the whole run about
    to fail, and swallowing it here would spend the cap knocking on a door that has
    just been locked.
    """
    result = DetailFetch()
    # NEWEST FIRST: when the cap bites, it must bite on February's backfill and not
    # on this evening's orders. `orderTime` can be None on either side, hence `or ""`.
    new_orders = sorted(
        (o for o in orders if o["refNo"] not in known_refs),
        key=lambda o: (o["orderDate"], o["orderTime"] or ""),
        reverse=True,
    )
    if not new_orders:
        return result

    for order in new_orders:
        detail_id = detail_ids.get(order["refNo"])
        if detail_id is None:
            result.no_detail_id += 1
            continue
        if result.fetched >= MAX_DETAIL_FETCHES_PER_RUN:
            result.over_cap += 1
            continue

        try:
            payload = portal.get_json(
                ORDER_DETAILS_PATH,
                {"id": detail_id},
                dump=False,
            )
        except AuthError:
            raise
        except (CollectorError, requests.RequestException) as err:
            result.fetched += 1
            result.failures += 1
            LOG.warning("no details for %s — %s: %s", order["refNo"], type(err).__name__, err)
            continue

        result.fetched += 1
        if result.first_payload is None:
            result.first_payload = payload

        # THE DISHES FIRST, and outside the customer branch below on purpose: an
        # order nobody left a name on is still an order with food in it, and an
        # early `continue` here is exactly how a combo ticket would have gone on
        # arriving empty for anonymous customers only.
        items = parse_order_items(payload)
        if items is not None:
            order["itemData"] = items
            result.with_items += 1

        name, phone = parse_order_details(payload)
        if name is None and phone is None:
            # Not a failure: a portal is entitled to hold an order nobody left a
            # name on. Counted so that "the endpoint changed shape" and "this
            # customer is anonymous" do not look identical in the summary line.
            result.failures += 1
            LOG.info("%s: the details endpoint carried no customer", order["refNo"])
            continue

        # Only what the ledger takes. The address and the money stay where they
        # are — see `parse_order_details`.
        order["customerName"] = name
        order["customerPhone"] = phone
        if name is not None:
            result.found += 1

    if result.over_cap:
        LOG.warning(
            "%d new order(s) past this run's %d-fetch details budget — imported without a customer, "
            "and not retried. (This is what a --full backfill looks like; an ordinary run never sees it.)",
            result.over_cap,
            MAX_DETAIL_FETCHES_PER_RUN,
        )
    LOG.info(
        "order details: %d new ref(s), %d fetched, %d with a name, %d with dishes, %d without",
        len(new_orders),
        result.fetched,
        result.found,
        result.with_items,
        result.failures,
    )
    return result


def known_refs_path() -> Path:
    return _state_dir / KNOWN_REFS_FILE


def load_known_refs() -> dict[str, str]:
    """
    The refs whose details this collector has already asked about → their order date.

    BEST-EFFORT, ALWAYS. A missing file is the first run; a corrupt one is a disk
    that lost a write. Both answer "we know nothing", which costs one window of
    re-asked details and nothing else — every one of those requests is idempotent
    and the ledger's COALESCE makes a second answer harmless. Refusing to run over
    a cache file would be the tail wagging the dog.
    """
    path = known_refs_path()
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as err:
        LOG.warning("%s is unreadable (%s) — treating every ref as new for this run", path, err)
        return {}

    refs = body.get("refs") if isinstance(body, dict) else None
    if not isinstance(refs, dict):
        LOG.warning("%s does not hold a 'refs' object — treating every ref as new for this run", path)
        return {}
    return {str(k): str(v) for k, v in refs.items() if isinstance(k, str)}


def save_known_refs(refs: dict[str, str], today: date) -> None:
    """
    Write the ref→date map back, pruned, and never fail the run over it.

    PRUNED BY THE ORDER'S OWN DATE, not by when we saw it: what bounds this file is
    how far back the windows this collector reads go, and that is a fact about
    order dates. A ref whose date we cannot parse is kept — the cost of keeping one
    row too long is a row, and the cost of dropping it is re-asking the portal.
    """
    floor = (today - timedelta(days=KNOWN_REFS_KEEP_DAYS)).isoformat()
    kept = {ref: day for ref, day in refs.items() if not (_DATE.match(day) and day < floor)}
    try:
        _state_dir.mkdir(parents=True, exist_ok=True)
        known_refs_path().write_text(
            json.dumps({"version": 1, "refs": kept}, indent=0, sort_keys=True),
            encoding="utf-8",
        )
    except OSError as err:  # pragma: no cover - read-only home, disk full
        # The import already happened. The only consequence of losing this is that
        # the next run re-asks for details it already has, which is one wasted
        # request per order and no wrong data anywhere.
        LOG.warning("could not write %s (%s) — the next run will re-ask for these details", known_refs_path(), err)


def scrape_item_summary(portal: Portal, business_date: date) -> tuple[list[dict[str, Any]], str]:
    """`sales-summary.php` for one day → the daily item snapshot."""
    response = portal.get_authenticated(
        SUMMARY_PATH,
        {"start_date": business_date.isoformat(), "end_date": business_date.isoformat()},
    )
    try:
        return parse_summary_page(response.text), response.text
    except PortalDrift:
        dump_drift(response.text, "sales-summary")
        raise


_state_dir = DEFAULT_STATE_DIR


def _label_for(path: str) -> str:
    """`/api/load_orders.php` → `load-orders`. What a drift dump gets named."""
    return path.rsplit("/", 1)[-1].removesuffix(".php").replace("_", "-") or "response"


def _as_json_text(payload: Any) -> str:
    """A payload, pretty, for a dump. Anything unserializable falls back to repr."""
    try:
        return json.dumps(payload, indent=2, ensure_ascii=False)
    except (TypeError, ValueError):  # pragma: no cover - json from requests is serializable
        return repr(payload)


def dump_drift(body: str, label: str, suffix: str = ".html") -> Path | None:
    """
    Write whatever surprised us where a human can look at it.

    Drift is diagnosed by reading the thing itself — a page, a JSON payload, the
    body behind an unexpected status — and by the time anybody reads the log the
    portal may well have been changed again. Best-effort: a failure to write the
    dump must never mask the drift error it belongs to.
    """
    try:
        _state_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = _state_dir / f"drift-{label}-{stamp}{suffix}"
        path.write_text(body or "", encoding="utf-8")
        LOG.error("PORTAL DRIFT — what surprised us is at %s", path)
        return path
    except OSError as err:  # pragma: no cover - disk full, read-only home
        LOG.error("could not write the drift dump: %s", err)
        return None


def _evidence(body: str, label: str, dump: bool) -> str:
    """
    Keep what surprised us, in a file or in the message, and say which.

    A dump is the right answer when a failure is rare and stops the run — somebody
    is going to read it. It is the wrong answer for a call that happens once per
    new order and whose failure costs a customer name: that produces a file per
    order per day in a directory nothing prunes. The line goes in the rotating log
    instead, truncated, which is where the person looking would go anyway.
    """
    if dump:
        dump_drift(body, label, suffix=".txt")
        return "the body is in the dump above."
    return f"body: {collapse(body)[:300]!r}"


# ---------------------------------------------------------------------------
# The ledger API
# ---------------------------------------------------------------------------


def chunks(items: Sequence[Any], size: int) -> Iterator[list[Any]]:
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def post_json(
    cfg: Config,
    path: str,
    payload: dict[str, Any],
    attempts: int = 3,
    sleeper=time.sleep,
) -> dict[str, Any]:
    """
    POST to the ledger, retrying what is worth retrying and nothing else.

    A 5xx or a dropped connection is the network or a deploy in flight — worth
    three tries with a growing pause. A 4xx is THIS program's fault (a payload the
    endpoint refuses, a token it does not accept) and no amount of repeating will
    change the answer; retrying it just delays the error a human needs to see.
    408 and 429 are the two 4xx that are genuinely about timing.
    """
    url = f"{cfg.api_base}{path}"
    headers = {
        "Content-Type": "application/json",
        # The csrf guard covers /api/imports — this prefix is deliberately NOT
        # exempt, and the collector satisfies it like any other caller.
        "X-Requested-With": "fetch",
        "X-Collector-Token": cfg.collector_token,
        "User-Agent": USER_AGENT,
    }

    last = "no attempt was made"
    for attempt in range(1, attempts + 1):
        # A REQUEST ABOUT TO BE MADE IS PROGRESS, and the deadman is told so here
        # rather than only when the import finishes: three tries at a 120-second
        # timeout is six minutes in the worst case, and a ledger having a slow
        # morning must not be read as a hung phone and answered with a restart.
        # A call that never returns still never beats again — which is the hang
        # the deadman exists for. See `Deadman`.
        beat()
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=120)
        except requests.RequestException as err:
            last = f"{type(err).__name__}: {err}"
        else:
            if 200 <= response.status_code < 300:
                try:
                    return response.json()
                except ValueError:
                    return {"ok": True, "raw": (response.text or "")[:400]}
            detail = (response.text or "")[:400]
            if 400 <= response.status_code < 500 and response.status_code not in (408, 429):
                raise ApiError(f"POST {path} → HTTP {response.status_code}: {detail}")
            last = f"HTTP {response.status_code}: {detail}"

        LOG.warning("POST %s attempt %d/%d failed — %s", path, attempt, attempts, last)
        if attempt < attempts:
            sleeper(min(2 ** (attempt - 1), 8))

    raise ApiError(f"POST {path} failed after {attempts} attempts — {last}")


def ping_ledger(cfg: Config, at: str | None = None) -> bool:
    """
    Tell the ledger this phone is still watching. ONE try, five seconds, no retry.

    **NOTHING ABOUT THIS IS ALLOWED TO MATTER TO AN IMPORT.** It is the bottom
    rung of the ladder: orders are the job, and a ping that cannot get out means
    only that the counter may be told the phone has stopped when it has not. So
    there is no retry ladder, no exception out of here, and no `ApiError` — a
    failure is a `False` the caller counts and shrugs at.

    THE TIMESTAMP IN THE BODY IS DIAGNOSTIC AND NOTHING ELSE. The Worker stamps
    the beat with ITS OWN clock (`worker/routes/imports.ts`), because a phone
    whose clock is wrong by an hour would otherwise be able to tell the ledger it
    is fine while it is dead, or that it is dead while it is fine. This is what a
    person reads in a log when they want to know what the phone believed the time
    was.
    """
    url = f"{cfg.api_base}{HEARTBEAT_PATH}"
    body = {"at": at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}
    headers = {
        "Content-Type": "application/json",
        # The csrf guard covers /api/imports — the ping satisfies it exactly like
        # the import does, because it lives under the same prefix on purpose.
        "X-Requested-With": "fetch",
        "X-Collector-Token": cfg.collector_token,
        "User-Agent": USER_AGENT,
    }
    try:
        response = requests.post(url, json=body, headers=headers, timeout=WATCH_PING_TIMEOUT)
    except requests.RequestException as err:
        LOG.debug("liveness ping did not reach %s — %s: %s", url, type(err).__name__, err)
        return False
    if 200 <= response.status_code < 300:
        return True
    LOG.debug("liveness ping → HTTP %d", response.status_code)
    return False


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------


@dataclass
class Summary:
    parsed: int = 0
    pages: int = 0
    duplicate_refs: int = 0
    unknown_payment_types: int = 0
    contradictions: int = 0
    upserted: int = 0
    unchanged: int = 0
    status_changes: int = 0
    item_parse_failures: int = 0
    item_snapshot_rows: int = 0
    #: Extra GETs of the details endpoint this run made, and how many of them
    #: produced a customer name and a dish list. `details` IS the answer to "what
    #: does this feature cost the portal", so it is on the one line a human reads —
    #: and it does not move when `dishes` starts appearing, because both ride the
    #: same request.
    detail_fetches: int = 0
    customers: int = 0
    dishes: int = 0
    exit_code: int = EXIT_OK

    def line(self) -> str:
        return (
            f"run finished: pages={self.pages} parsed={self.parsed} duplicates={self.duplicate_refs} "
            f"unknown_payment={self.unknown_payment_types} contradictions={self.contradictions} "
            f"upserted={self.upserted} "
            f"unchanged={self.unchanged} status_changes={self.status_changes} "
            f"item_parse_failures={self.item_parse_failures} item_snapshot={self.item_snapshot_rows} "
            f"details={self.detail_fetches} customers={self.customers} dishes={self.dishes} "
            f"exit={self.exit_code}"
        )


def kigali_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(KIGALI)


def window_for(args: argparse.Namespace, cfg: Config, today: date) -> tuple[date, date]:
    if args.full:
        return HISTORY_START, today
    if args.start and args.end:
        return date.fromisoformat(args.start), date.fromisoformat(args.end)
    return today - timedelta(days=cfg.window_days), today


def capture_fixtures(
    orders_payload: Any,
    summary_html: str,
    target: Path | None = None,
    details_payload: Any = None,
) -> None:
    """
    Save the real responses beside the committed ones, for a maintainer to diff.

    NOT done on every run. Fixtures are committed test data, and a program that
    overwrote them nightly would mean the tests silently start asserting whatever
    the portal did last — including whatever it did wrong. This is a flag somebody
    types on purpose, once, after a portal change.

    WHAT COMES OUT OF HERE IS NOT COMMITTABLE AS-IS. It is a real window of a real
    restaurant: every row carries a customer's delivery address, and any of them
    may carry the note that customer typed. THE DETAILS CAPTURE IS WORSE — it is
    one named customer's full name, working mobile number and delivery address in
    one small file. The committed fixtures are pseudonymized in every one of those
    cells (see PROMOTION, below), and the only thing standing between a fresh
    capture and a real person in git is somebody remembering that at the moment
    they copy a file. So this says it out loud, here, where they are — rather than
    in a document they read once.

    PROMOTION, the whole procedure:

      1. Diff `captured-load-orders.json` against `load-orders.json`, and
         `captured-load-order-details.json` against `load-order-details.json`.
         Almost always the answer is "nothing structural changed" and you stop.
      2. If the portal really moved, copy the rows across and REPLACE every cell
         that is about a person with stand-ins, keeping the SHAPE:
           * location → `KG 0NN St`, `Testville`, `Test District` (one address
             with commas in it, one bare place name);
           * any `Comment:` note → a stand-in sentence;
           * `first_name`/`last_name` → the fixture's `Testcustomer` /
             `Nyirahabimana`;
           * `contact_phone` → `+250 780 000 042` — a Rwandan mobile's shape,
             with a number nobody has.
      3. `uv run --project collector pytest`. The gate is
         `test_no_fixture_carries_a_real_customers_address_or_note`, which greps
         for the known real strings AND requires every location in every fixture
         to look like a stand-in AND requires the details fixture's name and phone
         to be the stand-ins. A real address or a real number fails it whether or
         not anybody thought to add it to the deny-list.

    The captured files are git-ignored, so the only way one reaches a commit is a
    deliberate copy — and step 3 is what catches it when that copy is careless.
    """
    target = target or Path(__file__).resolve().parent / "tests" / "fixtures"
    target.mkdir(parents=True, exist_ok=True)
    written: list[tuple[str, str]] = []
    if orders_payload is not None:
        written.append(("captured-load-orders.json", _as_json_text(orders_payload)))
    if details_payload is not None:
        written.append(("captured-load-order-details.json", _as_json_text(details_payload)))
    if summary_html:
        written.append(("captured-sales-summary.html", summary_html))
    for name, body in written:
        (target / name).write_text(body, encoding="utf-8")
        LOG.info("captured %s", target / name)
    if written:
        LOG.warning(
            "these captures hold REAL customer delivery addresses, notes, NAMES and PHONE NUMBERS — "
            "they are git-ignored on purpose. Do not commit them. To promote a change into the "
            "committed fixtures, replace every one of those with stand-ins first; pytest's "
            "test_no_fixture_carries_a_real_customers_address_or_note is what enforces it."
        )


def run(cfg: Config, args: argparse.Namespace, portal: Portal | None = None) -> Summary:
    summary = Summary()
    today = kigali_now().date()
    start, end = window_for(args, cfg, today)
    LOG.info("window %s .. %s (Kigali)", start, end)

    portal = portal or Portal(cfg)

    scraped = scrape_orders(portal, start, end)
    summary.parsed = len(scraped.orders)
    summary.pages = scraped.pages
    summary.duplicate_refs = scraped.duplicate_refs
    summary.unknown_payment_types = len(scraped.unknown_payment_types)
    summary.contradictions = scraped.contradictions
    if scraped.unknown_payment_types:
        LOG.warning(
            "payment types the ledger does not know, stored as null: %s",
            sorted(set(scraped.unknown_payment_types)),
        )

    # THE CUSTOMER AND THE DISHES, FOR THE ORDERS THAT ARE NEW TO US, AND ONLY
    # THEM. ONE extra GET each — both facts come out of the same response — inside
    # the same run that is about to POST them, so a watch-mode poll that found
    # nothing new costs the details endpoint nothing at all, which is the whole
    # reason `known_refs` is on disk rather than in this function.
    #
    # Deliberately BEFORE the item snapshot and the dry-run print: the fields it
    # attaches are part of the payload, so `--dry-run` has to show them or it is
    # not showing what a real run would send.
    known_refs = load_known_refs()
    customers = fetch_order_details(portal, scraped.orders, scraped.detail_ids, known_refs)
    summary.detail_fetches = customers.fetched
    summary.customers = customers.found
    summary.dishes = customers.with_items

    items, summary_html = scrape_item_summary(portal, end)
    summary.item_snapshot_rows = len(items)

    if args.capture_fixtures:
        capture_fixtures(scraped.first_page_payload, summary_html, details_payload=customers.first_payload)

    if args.dry_run:
        # stdout, not the log: this is output somebody is reading right now, and
        # piping it into `jq` should not also mean piping the log into `jq`.
        print(
            json.dumps(
                {
                    "orders": {
                        "source": "scrape",
                        "windowStart": start.isoformat(),
                        "windowEnd": end.isoformat(),
                        "orders": scraped.orders,
                    },
                    "items": {"businessDate": end.isoformat(), "items": items},
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        LOG.info("dry run — nothing was posted")
        return summary

    for chunk in chunks(scraped.orders, CHUNK_SIZE):
        result = post_json(
            cfg,
            "/api/imports/vubavuba",
            {
                "source": "scrape",
                "windowStart": start.isoformat(),
                "windowEnd": end.isoformat(),
                "orders": chunk,
            },
        )
        summary.upserted += int(result.get("upserted") or 0)
        summary.unchanged += int(result.get("unchanged") or 0)
        summary.status_changes += int(result.get("statusChanges") or 0)
        summary.item_parse_failures += int(result.get("itemParseFailures") or 0)
        LOG.info(
            "posted %d order(s): upserted=%s unchanged=%s statusChanges=%s",
            len(chunk),
            result.get("upserted"),
            result.get("unchanged"),
            result.get("statusChanges"),
        )

    # EVERY ref in the window is now known — not just the ones a detail fetch
    # succeeded for. One attempt per order is the rule: an order the portal has no
    # customer for, or whose fetch failed, must not be re-asked on every run for
    # ever, and the ledger keeps whatever it was told whenever it was told it.
    #
    # AFTER the POST, and only after: a ref recorded here is a ref this program
    # will never ask about again, so recording one whose import then failed would
    # mean an order that lands on the retry with no customer and no second chance.
    known_refs.update({order["refNo"]: order["orderDate"] for order in scraped.orders})
    save_known_refs(known_refs, today)

    if items:
        for chunk in chunks(items, PER_PAGE):
            post_json(cfg, "/api/imports/vubavuba-items", {"businessDate": end.isoformat(), "items": chunk})
        LOG.info("posted the %s item snapshot (%d row(s))", end.isoformat(), len(items))

    return summary


# ---------------------------------------------------------------------------
# Watch mode — one login, a poll every ten seconds
# ---------------------------------------------------------------------------


def poll_digest(payload: Any) -> str:
    """
    A `load_orders.php` page → a fingerprint that changes when the ORDERS do.

    This is the whole economy of watch mode. Ten-second polling only earns its
    keep if the expensive half — page through the window, normalize, POST it to
    the ledger — happens when something actually moved, and a hash of the rows is
    a cheaper answer to "did anything move" than parsing them is.

    IT IS THE ROWS AND THE COUNTS, NOT THE WHOLE PAYLOAD. `ordersHtml` carries
    every field the ledger stores, INCLUDING the status badge — which matters
    because a status change (`pending` → `successful`) is a change this loop must
    notice, and it is the only kind that leaves the row count alone.
    `pagination` is folded in as the second witness: it catches an order arriving
    or vanishing on a page we are not looking at. `paginationHtml` is left out
    deliberately — it is a rendered widget, and a portal that restyles it would
    otherwise re-import the window on every poll forever.

    A payload this function cannot recognise is hashed whole rather than refused.
    Digesting is not validating: whatever came back, the job here is only to tell
    it apart from what came back last time, and the guards in
    `parse_orders_json` are what get to have an opinion about its shape.
    """
    if not isinstance(payload, dict):
        return hashlib.sha256(repr(payload).encode("utf-8")).hexdigest()
    pagination = payload.get("pagination")
    parts = [str(payload.get("success")), str(payload.get("ordersHtml") or "")]
    if isinstance(pagination, dict):
        parts.extend(f"{key}={pagination.get(key)!r}" for key in PAGINATION_KEYS)
    else:
        parts.append(repr(pagination))
    return hashlib.sha256("\x00".join(parts).encode("utf-8")).hexdigest()


def seconds_until_active(now: datetime | None = None) -> float:
    """
    How long until 07:00 Kigali — `0.0` if the restaurant is open right now.

    The one-shot answers "the restaurant is shut" by exiting 0 and letting
    launchd call again in two hours. A daemon cannot do that: exiting is how it
    stops existing. So the night is a sleep, and this is the arithmetic behind
    it. `ACTIVE_HOUR_TO` is inclusive — hour 23 is open, because 23:40 is a
    perfectly ordinary time for the last delivery of the evening to be paid for.
    """
    now = now or kigali_now()
    if ACTIVE_HOUR_FROM <= now.hour <= ACTIVE_HOUR_TO:
        return 0.0
    opening = now.replace(hour=ACTIVE_HOUR_FROM, minute=0, second=0, microsecond=0)
    if opening <= now:
        opening += timedelta(days=1)
    return (opening - now).total_seconds()


def _duration(seconds: float) -> str:
    """`27000.0` → `"7h 30m"`. For the one line a human reads at 23:00."""
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = int(seconds // 60)
    return f"{minutes}m" if minutes < 60 else f"{minutes // 60}h {minutes % 60:02d}m"


class WatchStop:
    """
    The flag SIGINT and SIGTERM set, and the only clean way out of the loop.

    A signal handler must not do the stopping itself. It fires between two
    bytecodes — possibly in the middle of a POST that the ledger is already
    committing — and a handler that raised there would leave the run half
    reported. This records that somebody asked, and the loop reads it at the next
    point where nothing is in flight.
    """

    def __init__(self) -> None:
        self.reason = ""

    def set(self, signum: int | None = None, frame: Any = None) -> None:
        self.reason = signal.Signals(signum).name if signum else "asked to stop"

    def __bool__(self) -> bool:
        return bool(self.reason)


# ---------------------------------------------------------------------------
# The deadman — a hang becomes a crash, and a crash is already handled
# ---------------------------------------------------------------------------


def deadman_limit(poll_seconds: float) -> float:
    """How long a silent loop is allowed to be. `DEADMAN_*` carries the reasoning."""
    return max(DEADMAN_POLL_MULTIPLE * float(poll_seconds), DEADMAN_FLOOR_SECONDS)


class Deadman:
    """
    A daemon thread that kills this process if the loop stops making progress.

    ── WHY A PROCESS KILLS ITSELF ──────────────────────────────────────────────

    Every other failure in this program is already automatic: a network error
    backs off, an expired session logs back in, a crashed process is restarted by
    the phone's launcher in thirty seconds. The one the ladder does not cover is a
    HANG — the process alive, the loop stuck inside a call that never returns —
    because from outside it is indistinguishable from a quiet afternoon and
    nothing ever fires. That was the owner's actual outage: a phone on a charger,
    a screen that said "watching for orders", and no orders reaching the counter.
    Nothing INSIDE a hung thread can rescue it (a blocked C-level read cannot be
    interrupted from Python), so the only automatic move left is to end the
    process and let whatever started it start it again.

    ── WHAT COUNTS AS PROGRESS ─────────────────────────────────────────────────

    A completed poll, first of all — that is the loop's own pulse. But also every
    portal request (`Throttle.wait`), every ledger POST attempt (`post_json`) and
    every piece of a deliberate wait (`_watch_sleep`), because a long import and a
    five-minute backoff are the program working rather than the program stuck. A
    call that hangs beats NOTHING, whichever of those it is inside, which is
    exactly the property that makes this a hang detector and not a timeout on
    slowness.

    ── `time.monotonic`, AND WHY IT IS THE RIGHT CLOCK HERE ────────────────────

    It does not run while the device is suspended, and that is the behaviour we
    want: a phone whose whole process Android froze for ten minutes has not hung,
    and thawing it into an immediate suicide would turn power management into an
    outage. What DOES advance it is a frozen main thread beside a live one — the
    Pydroid failure this exists to catch.

    ── `os._exit`, AND NOT `sys.exit` ──────────────────────────────────────────

    `sys.exit` raises `SystemExit` **in the calling thread** — here, this monitor
    thread — where it kills nothing but the monitor and leaves the hang in place.
    `os._exit` ends the interpreter immediately, without unwinding and without
    waiting for a lock the hung thread may be holding. That is the whole point:
    there is nothing to clean up that is worth waiting on, and every write this
    program makes is an idempotent upsert the next run converges on.
    """

    def __init__(self, limit: float, clock: Any = time.monotonic) -> None:
        self.limit = float(limit)
        self._clock = clock
        self._last = clock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def beat(self) -> None:
        """Progress. Called from four places; see the class docstring."""
        self._last = self._clock()

    def silent_for(self) -> float:
        return self._clock() - self._last

    def overdue(self) -> bool:
        return self.silent_for() > self.limit

    def check(self) -> bool:
        """Look once. Returns True — and does not return at all — if it tripped."""
        if not self.overdue():
            return False
        self.trip()
        return True

    def trip(self) -> None:
        """
        Say what happened, loudly and with the evidence, then end the process.

        The stack of EVERY thread, not just the main one, because "which call did
        it stop in" is the entire diagnosis and the answer is as likely to be in a
        `requests` read as in this file. It goes through the ordinary log, which
        on the phone is a file somebody can be talked through opening.
        """
        LOG.critical(
            "THE WATCH LOOP HAS HUNG — no progress for %.0fs (limit %.0fs). "
            "Killing this process so the launcher can start a fresh one; "
            "every write this program makes is an idempotent upsert, so nothing is lost.",
            self.silent_for(),
            self.limit,
        )
        LOG.critical("what every thread was doing:\n%s", thread_dump())
        os._exit(EXIT_HANG)

    def start(self) -> "Deadman":
        self.beat()
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="deadman", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        """Stand the monitor down. The loop is ending on purpose, so it is not a hang."""
        self._stop.set()

    def _loop(self) -> None:
        # `Event.wait` and not `time.sleep`, so a clean shutdown does not have to
        # wait out the last check interval.
        while not self._stop.wait(DEADMAN_CHECK_SECONDS):
            self.check()


def thread_dump() -> str:
    """Every thread, and where it is. The one thing worth having about a hang."""
    frames = sys._current_frames()
    lines: list[str] = []
    for thread in threading.enumerate():
        lines.append(f"--- {thread.name} (daemon={thread.daemon}) ---")
        frame = frames.get(thread.ident or -1)
        if frame is None:
            lines.append("    (no frame — the thread ended while we were looking)")
            continue
        lines.extend(piece.rstrip() for piece in traceback.format_stack(frame))
    return "\n".join(lines)


#: The deadman the watch loop armed, or None everywhere else.
#:
#: A module-level one rather than an argument threaded through `run`, `Portal` and
#: `post_json`: the beat has to be reachable from the bottom of the call stack,
#: and adding a parameter to five functions so that four of them can pass it
#: along untouched is how a small safety net becomes a refactor. One-shot runs
#: leave it None and `beat()` is a no-op for them — they have launchd, a fixed
#: end, and no loop to hang.
_deadman: Deadman | None = None


def beat() -> None:
    """Progress, from anywhere. A no-op outside watch mode."""
    if _deadman is not None:
        _deadman.beat()


def _watch_sleep(seconds: float, stop: WatchStop, sleeper: Any) -> None:
    """
    Sleep, in pieces, so a signal is answered in seconds rather than at dawn.

    PEP 475 made `time.sleep` RESUME after a signal handler returns, which is
    almost always what you want and is exactly wrong here: `_watch_sleep(27000)`
    through the night would take the SIGTERM, set the flag, and then go back to
    sleep for another seven hours with nobody left to notice. Chopping the wait
    into `WATCH_SLEEP_CHUNK` pieces bounds how long the flag can go unread.

    Each piece also BEATS THE DEADMAN, because a deliberate wait is not a hang: a
    five-minute backoff against a portal that is down, and the seven-hour sleep
    through the night, are both this loop working exactly as designed. What the
    deadman is looking for is a wait nobody chose.
    """
    remaining = float(seconds)
    while remaining > 0 and not stop:
        beat()
        nap = min(WATCH_SLEEP_CHUNK, remaining)
        sleeper(nap)
        remaining -= nap


def _pinged(cfg: Config, streak: int) -> int:
    """
    Ping the ledger; say how long the failing streak is now. **NEVER RAISES.**

    THE LADDER LAW IS THIS FUNCTION. A ping is the bottom rung — below the
    orders, below the session, below everything a customer can feel — so it is
    written so that no failure of it can reach the loop that imports. The second
    `try` is not superstition: `ping_ledger` swallows its own network errors, and
    this one is the promise that a LATER bug in it still cannot cost anybody an
    order.

    ONE LINE PER STREAK, not one per ping. A ledger that is unreachable for an
    hour is 180 pings; a warning each would bury the line that says why. So the
    first failure says so, the rest are silent, and the recovery says how many
    there were — which is also the only line that tells a reader whether the
    counter was being told this phone had stopped.
    """
    try:
        ok = ping_ledger(cfg)
    except Exception as err:  # noqa: BLE001 - an order must never be lost to a ping
        LOG.debug("the liveness ping raised (%s: %s) — ignored", type(err).__name__, err)
        ok = False

    if ok:
        if streak:
            LOG.info("the ledger is hearing this phone again (after %d ping(s) it did not)", streak)
        return 0
    if streak == 0:
        LOG.warning(
            "the liveness ping is not reaching %s — the counter may be told this phone has stopped. "
            "Orders are unaffected: imports use their own retries.",
            cfg.api_base,
        )
    return streak + 1


def watch(
    cfg: Config,
    args: argparse.Namespace,
    portal: Portal | None = None,
    sleeper: Any = time.sleep,
    stop: WatchStop | None = None,
    deadman: Deadman | None = None,
) -> int:
    """
    Log in once; re-read the recent window every ~10s; import only what changed.

    WHAT ONE POLL COSTS THE PORTAL: exactly one GET of `api/load_orders.php`,
    page 1, on the session opened at start-up. At the default ten seconds that is
    six requests a minute — about 6,100 across a 17-hour trading day — and ONE
    login, against the 720 logins a day the two-minute one-shot cadence spends
    now. That is the trade this mode exists to make: many more cheap reads, three
    orders of magnitude fewer authentications.

    The customer-details fetch does NOT ride on the poll. It happens inside `run`,
    which only fires when the digest moved, and only for refs this collector has
    never seen — so its cost is one extra GET per ORDER PLACED (a handful a day),
    not one per poll. A quiet afternoon adds nothing to the numbers above.

    WHY PAGE 1 AND NOT THE WHOLE WINDOW. Page 1 is 100 rows, and this restaurant's
    three-day window is comfortably inside that, so on any ordinary day page 1 IS
    the window and its digest is an exact answer. When the window outgrows one
    page the digest stops being exact — a status moving on a row that has fallen
    onto page 2 would not show up in it — and rather than reason about a sort
    order the portal has never promised us, `WATCH_SWEEP_SECONDS` re-reads the
    whole window on a timer regardless of the digest. So the fast path is cheap
    and the slow path is complete, and the failure mode of the cheap one is
    bounded at fifteen minutes instead of forever.

    WHAT A CHANGE TRIGGERS: `run` — the same function the one-shot calls, with the
    same pagination, the same drift guards, the same idempotent upserts, and the
    same one summary line. Watch mode adds a trigger, not a second importer. It
    costs one extra fetch of page 1 (the poll's copy is a fingerprint, not an
    import), which is a request every few minutes against not having two code
    paths that can disagree about what an order is.

    WHY IT DOES NOT EXIT ON FAILURE. Every failure here is met with a capped
    exponential backoff instead of an exit code, because the thing that would
    "restart" it — the phone's supervisor, launchd — restarts it in 30 seconds,
    forever, and a program that dies every 30 seconds against a portal that is
    down is the login storm this whole design exists to avoid. Drift is the one
    that needs saying twice: a payload this program cannot parse is dumped, is
    logged as loudly as ever, and is then REMEMBERED BY DIGEST, so a portal that
    has genuinely changed shape is retried on the sweep timer rather than on
    every poll — a few dumps an hour instead of one every ten seconds, until the
    human they are addressed to reads one. Nothing is imported from any of them.

    WHAT IT SAYS ABOUT ITSELF, AND WHAT KILLS IT (2026-08-14). Two additions, both
    for the same outage: a phone that stops watching and nobody finds out until
    the morning's orders are missing.

      * every fourth completed poll POSTs a one-line liveness ping to the ledger
        (`ping_ledger`), so the Worker's hub can tell "watching" from "stopped"
        within seconds and ring the counter with the instructions when it stops;
      * a `Deadman` thread turns a HANG into an exit, because an exit is the one
        failure the phone's launcher already knows how to fix. Thirty seconds
        later there is a fresh process; without it there is a phone on a charger
        showing "watching for orders" and importing nothing.

    Neither can affect an import. The ping cannot raise into the loop and is never
    retried; the deadman is beaten by every request and every deliberate wait, so
    only a wait nobody chose can trip it.
    """
    global _deadman

    stop = stop if stop is not None else WatchStop()
    portal = portal or Portal(cfg)
    poll_seconds = max(WATCH_POLL_FLOOR, float(cfg.watch_poll_seconds))

    logged_in = False
    digest: str | None = None
    backoff = WATCH_BACKOFF_START
    polls = 0
    failures = 0
    ping_streak = 0
    last_change: datetime | None = None
    since_heartbeat = 0.0
    # Due immediately: the first poll of a session has nothing to compare against
    # anyway, so it may as well be the complete read.
    since_sweep = WATCH_SWEEP_SECONDS

    LOG.info(
        "watch mode: one login, then %s every %.0fs; active %02d:00-%02d:59 Kigali. "
        "SIGINT or SIGTERM stops it.",
        ORDERS_API_PATH,
        poll_seconds,
        ACTIVE_HOUR_FROM,
        ACTIVE_HOUR_TO,
    )

    # ARMED HERE AND NOWHERE ELSE, and stood down in the `finally` below, because
    # the module-level handle is what `beat()` reaches from the bottom of the call
    # stack. A monitor left armed after this function returns would be a thread
    # watching a clock nobody winds any more — and thirty to sixty seconds later
    # it would kill a perfectly healthy process for a loop that ended on purpose.
    monitor = deadman if deadman is not None else Deadman(deadman_limit(poll_seconds))
    _deadman = monitor
    monitor.start()
    LOG.info("deadman armed: %.0fs without progress ends this process (the launcher restarts it)", monitor.limit)
    try:
        while not stop:
            shut_for = seconds_until_active()
            if shut_for > 0:
                # ONE line for the whole night, not one per poll. The alternative is
                # 4,000 lines of "closed" between midnight and seven, which is how
                # the morning's real messages become unfindable.
                LOG.info(
                    "the restaurant is shut — sleeping %s, until %02d:00 Kigali",
                    _duration(shut_for),
                    ACTIVE_HOUR_FROM,
                )
                _watch_sleep(shut_for, stop, sleeper)
                # Nothing that happened before the night is worth carrying past it:
                # open with a complete read and a fresh hour on the heartbeat.
                since_sweep = WATCH_SWEEP_SECONDS
                since_heartbeat = 0.0
                continue

            wait = poll_seconds
            fresh: str | None = None
            try:
                if not logged_in:
                    # ONCE per session. Everything after this rides the cookie, and
                    # the mid-session expiry the portal does on its own schedule is
                    # handled a layer down, inside `get_json`.
                    portal.login()
                    logged_in = True

                start, end = window_for(args, cfg, kigali_now().date())
                payload = portal.get_json(ORDERS_API_PATH, orders_params(1, start, end))
                polls += 1
                # A COMPLETED POLL IS THE LOOP'S OWN PULSE — the beat the deadman
                # is really about, and the one place it is impossible to reach
                # without having gone all the way out to the portal and back.
                beat()
                # …and every fourth one, out to the ledger, so the counter can be
                # told within seconds when this stops. OUTSIDE the failure ladder
                # by construction: `_pinged` returns rather than raising, so a
                # ledger that cannot be reached costs a log line and never a
                # backoff, a re-login or one order's delay. See `ping_ledger`.
                if polls % WATCH_PING_EVERY_N_POLLS == 0:
                    ping_streak = _pinged(cfg, ping_streak)

                fresh = poll_digest(payload)
                changed = fresh != digest
                due_sweep = since_sweep >= WATCH_SWEEP_SECONDS
                if changed or due_sweep:
                    if changed:
                        last_change = kigali_now()
                    else:
                        LOG.info("watch sweep: re-reading %s..%s even though page 1 has not moved", start, end)
                    # Reset BEFORE the read, not after. The sweep is a timer on how
                    # long the one-page poll may be trusted on its own, and a read
                    # that failed still spent that trust; what decides whether a
                    # failed import is retried is the digest, which is the thing that
                    # knows if anything is outstanding. Resetting afterwards would
                    # mean a drifted portal re-reading, re-failing and re-dumping on
                    # every single poll, because the sweep would never come due.
                    since_sweep = 0.0
                    summary = run(cfg, args, portal)
                    LOG.info("%s", summary.line())
                    digest = fresh

                failures = 0
                backoff = WATCH_BACKOFF_START
            except Exception as err:  # noqa: BLE001 - the loop outlives everything below it
                if isinstance(err, (PortalDrift, PaginationError)) and fresh is not None:
                    # This exact payload has already been dumped and logged. Reading
                    # it again on every poll produces another identical dump and
                    # tells nobody anything they did not know at the first one, so it
                    # drops back to the sweep timer. The next genuine change to the
                    # window gets a fresh look and, if it is still broken, a fresh
                    # dump.
                    digest = fresh
                if isinstance(err, AuthError):
                    # The session is gone in a way `get_json`'s own re-login could not
                    # fix. Start the next attempt from a clean login rather than from
                    # a cookie we have already watched fail.
                    logged_in = False
                if isinstance(err, CollectorError):
                    LOG.error("%s", err)
                else:
                    LOG.exception("unexpected failure in the watch loop: %s", err)
                failures += 1
                wait = backoff
                backoff = min(backoff * 2, WATCH_BACKOFF_MAX)
                LOG.warning(
                    "backing off %s before the next poll (%d consecutive failure(s))",
                    _duration(wait),
                    failures,
                )

            since_heartbeat += wait
            since_sweep += wait
            if since_heartbeat >= WATCH_HEARTBEAT_SECONDS:
                LOG.info(
                    "watch alive, %d polls, last change %s",
                    polls,
                    last_change.strftime("%H:%M") if last_change else "none yet",
                )
                since_heartbeat = 0.0

            _watch_sleep(wait, stop, sleeper)

        LOG.info("watch stopped (%s) after %d poll(s)", stop.reason, polls)
        return EXIT_OK
    finally:
        # A loop that ended — on a signal, on an exception on its way out to
        # `main` — is not a hang, and the monitor must not outlive it. The global
        # is cleared too, so `beat()` from a one-shot run in the same process
        # (the phone's launcher calls `main` in a loop) is the no-op it should be.
        monitor.stop()
        _deadman = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="vubavuba-collect",
        description="Scrape merchant.vubavuba.rw and POST normalized orders to the resto-ledger Worker.",
    )
    parser.add_argument("--dry-run", action="store_true", help="print the normalized JSON; POST nothing")
    parser.add_argument("--full", action="store_true", help=f"backfill from {HISTORY_START} (implies --force)")
    parser.add_argument("--start", help="window start, YYYY-MM-DD (with --end)")
    parser.add_argument("--end", help="window end, YYYY-MM-DD (with --start)")
    parser.add_argument("--force", action="store_true", help="run even outside 07:00-23:00 Kigali")
    parser.add_argument(
        "--watch",
        action="store_true",
        help="stay running: log in once, re-read the recent window every ~10s, import what changed",
    )
    parser.add_argument("--capture-fixtures", action="store_true", help="save the raw pages into tests/fixtures/")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE, help=f"default {DEFAULT_ENV_FILE}")
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR, help=f"default {DEFAULT_STATE_DIR}")
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    global _state_dir
    _state_dir = args.state_dir

    # Logging is set up BEFORE the config is read, so a configuration failure is
    # also on the record — the launchd runs nobody watches are exactly the ones
    # that fail on a file permission somebody changed months ago.
    setup_logging(args.state_dir)
    try:
        cfg = load_config(args.env_file)
    except CollectorError as err:
        LOG.error("%s", err)
        return err.exit_code

    # Re-established now that the secrets are known, so nothing below can leak them.
    setup_logging(args.state_dir, cfg.secrets, args.verbose)

    if bool(args.start) != bool(args.end):
        LOG.error("--start and --end must be given together")
        return EXIT_CONFIG

    if args.watch and (args.full or args.capture_fixtures):
        # Both are one-shot errands with an end — a backfill from January, a
        # snapshot of today's markup — and neither means anything to a loop that
        # never finishes. Refusing beats doing the first iteration of a backfill
        # forever, or overwriting the captured fixtures every ten seconds.
        LOG.error("--watch is a daemon; it cannot be combined with --full or --capture-fixtures")
        return EXIT_CONFIG

    if args.watch:
        stop = WatchStop()
        # HANDLERS ARE BORROWED, NOT TAKEN. The phone's launcher imports this
        # module and calls `main` in-process, in a loop, so a SIGTERM handler
        # left pointing at a WatchStop nobody reads any more would make the whole
        # app unkillable by the one signal everything uses to ask politely.
        previous = {}
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                previous[sig] = signal.getsignal(sig)
                signal.signal(sig, stop.set)
            except (ValueError, OSError, AttributeError):  # pragma: no cover - not the main thread
                # Only the main thread may install a handler. Losing the clean
                # shutdown is not a reason to lose the collector, so this is a
                # shrug and not an error.
                previous.pop(sig, None)
                LOG.debug("could not install a handler for %s", sig)
        try:
            return watch(cfg, args, stop=stop)
        except CollectorError as err:
            LOG.error("%s", err)
            return err.exit_code
        except Exception as err:  # noqa: BLE001 - the same last line of defence the one-shot has
            LOG.exception("unexpected failure: %s", err)
            return EXIT_UNEXPECTED
        finally:
            for sig, handler in previous.items():
                try:
                    signal.signal(sig, handler)
                except (ValueError, OSError, TypeError):  # pragma: no cover
                    pass

    hour = kigali_now().hour
    if not (args.force or args.full or args.dry_run) and not (ACTIVE_HOUR_FROM <= hour <= ACTIVE_HOUR_TO):
        # Not an error, and deliberately not a failure code: launchd fires this
        # every two hours around the clock and the restaurant is shut. A run that
        # reported failure here would train everybody to ignore the exit status.
        LOG.info("night skip: %02d:00 Kigali is outside %02d:00-%02d:00", hour, ACTIVE_HOUR_FROM, ACTIVE_HOUR_TO)
        return EXIT_OK

    summary = Summary()
    try:
        summary = run(cfg, args)
    except CollectorError as err:
        summary.exit_code = err.exit_code
        LOG.error("%s", err)
    except Exception as err:  # noqa: BLE001 - the last line of defence; it is logged and coded
        summary.exit_code = EXIT_UNEXPECTED
        LOG.exception("unexpected failure: %s", err)

    # ONE summary line, always, whatever happened. It is what `tail collector.log`
    # is for, and what makes "is this thing working" answerable in one glance.
    LOG.info("%s", summary.line())
    return summary.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
