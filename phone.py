"""
One-file collector runner for an always-plugged-in Android phone (Pydroid 3).

Why this exists: the restaurant wants Vubavuba orders reaching the kitchen
within seconds, for free, on hardware it already owns. GitHub Actions can
only manage ~5-15 minutes and cloud free tiers rejected the signup, so the
collector runs on the shop's own phone instead.

First run: asks for the four credentials and stores them app-privately
(0600, same rule the laptop collector enforces). Every later run: refreshes
the collector script from the public repo, then starts it in WATCH MODE and
keeps it running.

WATCH MODE. This used to start the collector every 2 minutes, which meant a
fresh login every 2 minutes - 720 a day against the shop's own merchant
account - and an order could sit for two minutes before the kitchen heard
about it. Now the collector is started ONCE with --watch and is meant never
to return: it logs in a single time, then re-reads the recent orders every ten
seconds on that one session and books whatever changed.

SUPERVISION. If it ever does return - a crash, a bad exit code - this prints
why and starts it again 30 seconds later, forever. Thirty seconds because a
restart is a fresh login, and a relauncher with no pause in it would be the
login flood watch mode exists to avoid. A collector that stopped because
somebody asked it to is NOT restarted; run this file again for that.

Phone checklist (once):
  * Pydroid menu -> Settings -> tick "Wakelock" (keeps the loop alive).
  * Android Settings -> Battery -> Battery optimisation -> Pydroid 3 ->
    "Don't optimise".
  * Keep the phone on its charger. After any reboot, open Pydroid and run
    this file again - Android 6 gives us no way to auto-start.
"""

import os
import pathlib
import subprocess
import sys
import time
import urllib.request

HOME = pathlib.Path.home()
CFG = HOME / ".config" / "resto-ledger" / "collector.env"
SCRIPT = HOME / "vubavuba_collector.py"
RAW = (
    "https://raw.githubusercontent.com/"
    "thebestnigerianfoodinkigali-lgtm/vubavuba-collector/main/vubavuba_collector.py"
)
#: How long to wait before starting the collector again after it has stopped.
#: Not a poll interval any more - the collector does its own polling now, and
#: this is only ever reached when something went wrong.
RESTART_S = 30

KEYS = (
    ("VUBAVUBA_USERNAME", "Vubavuba portal username (email)"),
    ("VUBAVUBA_PASSWORD", "Vubavuba portal password"),
    ("LEDGER_API_BASE", "Ledger URL (https://...)"),
    ("COLLECTOR_TOKEN", "Collector token (long random string)"),
)


def ensure_deps() -> None:
    """requests + beautifulsoup4, installed once through Pydroid's own pip."""
    try:
        import requests  # noqa: F401
        import bs4  # noqa: F401
        return
    except ImportError:
        pass
    print("Installing requests + beautifulsoup4 (one time, needs internet)...")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "requests", "beautifulsoup4"],
        check=True,
    )


def ensure_cfg() -> None:
    """Prompt once, store 0600. The collector itself refuses looser modes."""
    if CFG.exists():
        return
    print("\nFirst-time setup - type each value exactly, then press enter.\n")
    lines = []
    for key, label in KEYS:
        value = ""
        while not value:
            value = input(f"{label}\n{key} = ").strip()
        lines.append(f"{key}={value}")
    lines.append("WINDOW_DAYS=3")
    CFG.parent.mkdir(parents=True, exist_ok=True)
    CFG.touch(mode=0o600)
    CFG.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(CFG, 0o600)
    print(f"\nSaved to {CFG}\n")


def refresh_script() -> None:
    """Best-effort update from the repo; a fetch failure keeps the old copy."""
    try:
        with urllib.request.urlopen(RAW, timeout=30) as resp:
            body = resp.read()
        if b"def main" in body:
            SCRIPT.write_bytes(body)
    except Exception as err:  # noqa: BLE001 - offline is normal, not fatal
        if not SCRIPT.exists():
            raise SystemExit(f"cannot download collector and no local copy: {err}")
        print(f"(update check failed, using existing copy: {err})")


def main() -> None:
    print(f"Python {sys.version.split()[0]} on {sys.platform}")
    ensure_deps()
    ensure_cfg()
    starts = 0
    while True:
        starts += 1
        print(f"\n--- collector start {starts} at {time.strftime('%H:%M:%S')} ---")
        try:
            # Re-fetched on every start, so a fix pushed while the phone was
            # running is picked up by the restart rather than by a walk to the
            # shop. A failed fetch keeps the copy already on the phone.
            refresh_script()
            print("watching for orders; this will not print again until something happens")
            # NO TIMEOUT and NO capture_output, on purpose. This process is meant
            # never to exit, so a timeout could only ever interrupt work that was
            # going fine, and captured output would be a buffer nobody ever gets
            # to read - the collector's own lines belong on the screen, live.
            proc = subprocess.run([sys.executable, str(SCRIPT), "--watch"])
            if proc.returncode == 0:
                # The only way watch mode ends with 0 is somebody asking it to
                # stop. Restarting after that would make it impossible to stop
                # from the phone, which is worse than not being supervised.
                print("stopped on request - run this file again to start watching")
                return
            print(f"collector stopped by itself with exit {proc.returncode}")
        except KeyboardInterrupt:
            print("stopped by hand")
            return
        except SystemExit as err:
            print(f"stopped: {err}")
        except Exception as err:  # noqa: BLE001 - the supervisor must never die
            print(f"collector failed: {err}")
        print(f"restarting in {RESTART_S}s")
        time.sleep(RESTART_S)


if __name__ == "__main__":
    main()
