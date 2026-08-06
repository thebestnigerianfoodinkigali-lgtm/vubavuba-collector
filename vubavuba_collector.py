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
    4  PORTAL DRIFT — the page is not the page we know. HTML dumped for a human.
    5  the ledger API refused or could not be reached after retries
    6  the portal was unreachable (network, DNS, timeout) — nothing to fix here

USAGE

    uv run --project collector vubavuba-collect            # the launchd path
    uv run --project collector vubavuba-collect --dry-run  # print, do not POST
    uv run --project collector vubavuba-collect --full     # backfill from 2026-01-01
    uv run --project collector vubavuba-collect --capture-fixtures
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import stat
import sys
import time
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

#: Kigali hours in which a run is worth doing at all.
ACTIVE_HOUR_FROM = 7
ACTIVE_HOUR_TO = 23

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

    return Config(
        username=values["VUBAVUBA_USERNAME"],
        password=values["VUBAVUBA_PASSWORD"],
        api_base=api_base,
        collector_token=values["COLLECTOR_TOKEN"],
        window_days=window_days,
        portal_base=(values.get("VUBAVUBA_BASE_URL") or PORTAL_BASE).rstrip("/"),
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


def parse_orders_rows(orders_html: str) -> tuple[list[dict[str, Any]], list[str]]:
    """
    `ordersHtml` (bare `<tr>`s) → normalized orders, plus the payment rails we do
    not know.

    The fragment is wrapped in a table before parsing: `<tr>` outside a table is
    not valid HTML and a parser is entitled to throw the rows away, which would
    turn a busy day into a silent zero.
    """
    soup = BeautifulSoup(f"<table><tbody>{orders_html or ''}</tbody></table>", "html.parser")

    orders: list[dict[str, Any]] = []
    unknown: list[str] = []
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

    return orders, unknown


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

    orders, unknown = parse_orders_rows(orders_html)
    return PageResult(
        orders=orders,
        total_entries=int(pagination["total_orders"]),
        total_pages=int(pagination["total_pages"]),
        unknown_payment_types=unknown,
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
            # to retry blindly against somebody's production server. Dump it and
            # stop: whatever this is, a human reading the body is the fast path.
            dump_drift(body, label, suffix=".txt")
            raise PortalDrift(
                f"{path}: PORTAL DRIFT — HTTP {response.status_code} from an endpoint that should "
                f"answer 200 with orders. This is the portal refusing or failing, not a column that "
                f"moved; the body is in the dump above."
            )
        try:
            return response.json()
        except ValueError as err:
            dump_drift(body, label, suffix=".txt")
            raise PortalDrift(
                f"{path}: PORTAL DRIFT — the response is not JSON ({err}). "
                f"content-type was {response.headers.get('content-type')!r}."
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
        payload = portal.get_json(
            ORDERS_API_PATH,
            {
                "page": page,
                "per_page": PER_PAGE,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                # Sent empty, as the portal's own script sends them. An endpoint
                # that starts requiring them must not be able to fail us quietly.
                "search": "",
                "status": "",
            },
        )
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
    )


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
    exit_code: int = EXIT_OK

    def line(self) -> str:
        return (
            f"run finished: pages={self.pages} parsed={self.parsed} duplicates={self.duplicate_refs} "
            f"unknown_payment={self.unknown_payment_types} contradictions={self.contradictions} "
            f"upserted={self.upserted} "
            f"unchanged={self.unchanged} status_changes={self.status_changes} "
            f"item_parse_failures={self.item_parse_failures} item_snapshot={self.item_snapshot_rows} "
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


def capture_fixtures(orders_payload: Any, summary_html: str, target: Path | None = None) -> None:
    """
    Save the real responses beside the committed ones, for a maintainer to diff.

    NOT done on every run. Fixtures are committed test data, and a program that
    overwrote them nightly would mean the tests silently start asserting whatever
    the portal did last — including whatever it did wrong. This is a flag somebody
    types on purpose, once, after a portal change.

    WHAT COMES OUT OF HERE IS NOT COMMITTABLE AS-IS. It is a real window of a real
    restaurant: every row carries a customer's delivery address, and any of them
    may carry the note that customer typed. The committed fixtures are
    pseudonymized in those two cells (see PROMOTION, below), and the only thing
    standing between a fresh capture and a real address in git is somebody
    remembering that at the moment they copy a file. So this says it out loud,
    here, where they are — rather than in a document they read once.

    PROMOTION, the whole procedure:

      1. Diff `captured-load-orders.json` against `load-orders.json`. Almost
         always the answer is "nothing structural changed" and you stop.
      2. If the portal really moved, copy the rows across and REPLACE the
         location cell and any `Comment:` note with stand-ins — `KG 0NN St`,
         `Testville`, `Test District` — keeping the SHAPE (one address with
         commas in it, one bare place name).
      3. `uv run --project collector pytest`. The gate is
         `test_no_fixture_carries_a_real_customers_address_or_note`, which greps
         for the known real strings AND requires every location in every fixture
         to look like a stand-in. A real address fails it whether or not anybody
         thought to add it to the deny-list.

    The captured files are git-ignored, so the only way one reaches a commit is a
    deliberate copy — and step 3 is what catches it when that copy is careless.
    """
    target = target or Path(__file__).resolve().parent / "tests" / "fixtures"
    target.mkdir(parents=True, exist_ok=True)
    written: list[tuple[str, str]] = []
    if orders_payload is not None:
        written.append(("captured-load-orders.json", _as_json_text(orders_payload)))
    if summary_html:
        written.append(("captured-sales-summary.html", summary_html))
    for name, body in written:
        (target / name).write_text(body, encoding="utf-8")
        LOG.info("captured %s", target / name)
    if written:
        LOG.warning(
            "these captures hold REAL customer delivery addresses and notes — they are git-ignored "
            "on purpose. Do not commit them. To promote a change into the committed fixtures, "
            "replace the location and any Comment: note with stand-ins first; pytest's "
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

    items, summary_html = scrape_item_summary(portal, end)
    summary.item_snapshot_rows = len(items)

    if args.capture_fixtures:
        capture_fixtures(scraped.first_page_payload, summary_html)

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

    if items:
        for chunk in chunks(items, PER_PAGE):
            post_json(cfg, "/api/imports/vubavuba-items", {"businessDate": end.isoformat(), "items": chunk})
        LOG.info("posted the %s item snapshot (%d row(s))", end.isoformat(), len(items))

    return summary


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
