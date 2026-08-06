"""
One-file collector runner for an always-plugged-in Android phone (Pydroid 3).

Why this exists: the restaurant wants Vubavuba orders reaching the kitchen
within ~2 minutes, for free, on hardware it already owns. GitHub Actions can
only manage ~5-15 minutes and cloud free tiers rejected the signup, so the
collector runs on the shop's own phone instead.

First run: asks for the four credentials and stores them app-privately
(0600, same rule the laptop collector enforces). Every later run: refreshes
the collector script from the public repo, then runs it every 2 minutes,
forever, until the app is closed.

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
PERIOD_S = 120
RUN_TIMEOUT_S = 600

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
    refresh_script()
    runs = 0
    while True:
        runs += 1
        stamp = time.strftime("%H:%M:%S")
        print(f"\n--- run {runs} at {stamp} ---")
        try:
            proc = subprocess.run(
                [sys.executable, str(SCRIPT)],
                timeout=RUN_TIMEOUT_S,
                capture_output=True,
                text=True,
            )
            tail = (proc.stdout + proc.stderr).strip().splitlines()[-3:]
            for line in tail:
                print(line)
            print("OK" if proc.returncode == 0 else f"exit {proc.returncode}")
        except subprocess.TimeoutExpired:
            print("run took too long and was stopped; will retry")
        except Exception as err:  # noqa: BLE001 - the loop must never die
            print(f"run failed: {err}")
        time.sleep(PERIOD_S)


if __name__ == "__main__":
    main()
