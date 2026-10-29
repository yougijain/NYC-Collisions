"""Keep the live dashboard awake, and say so when it will not come up.

Streamlit Community Cloud puts an app to sleep after a stretch with no
visitors, and the next visitor waits on a "this app has gone to sleep"
page while it boots. For a dashboard linked from a README, that first
visitor is usually the one who matters.

So a scheduled job visits it the way a person would: a real browser, since
a plain HTTP request reaches Streamlit's front door but never opens the
session that counts as traffic. If the app is asleep, the job presses the
wake button. Either way it waits for the dashboard's own title to render.

## Why this can fail the run

The weekly refresh never fails on things nobody can fix. This is the
opposite case: an app that does not render after a wake is something to
fix, usually with a reboot from Streamlit's Manage app panel. On 26
October 2026 the app sat on "Running" for minutes until exactly that, and
nothing noticed. A red run here is that notice.

Usage:
    python scripts/keep_awake.py
    python scripts/keep_awake.py --url https://example.streamlit.app/
"""

import argparse
import logging
import os
import sys
import time
from typing import List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

APP_URL = "https://nyc-collisions-current.streamlit.app/"

# Text Streamlit's own sleep page shows, and the title the dashboard draws
# once its script has run. The title is what proves the app is serving,
# not just that Streamlit's front door answered.
WAKE_BUTTON = "Yes, get this app back up!"
ASLEEP_TEXT = "gone to sleep"
READY_TEXT = "Which New York crashes hurt people"

# A cold boot downloads the dataset before it can draw anything; a couple
# of minutes is normal, five is not.
READY_DEADLINE = 300
POLL_SECONDS = 5

ASLEEP, READY, WAITING = "asleep", "ready", "waiting"


def classify(texts: List[str]) -> str:
    """What the page is doing, from the text of the page and its frames.

    The dashboard renders inside an iframe on streamlit.app, and the sleep
    page outside it, so each frame is read separately.
    """
    joined = "\n".join(texts)
    if READY_TEXT in joined:
        return READY
    if ASLEEP_TEXT in joined or WAKE_BUTTON in joined:
        return ASLEEP
    return WAITING


def _texts(page) -> List[str]:
    texts = []
    for frame in page.frames:
        try:
            texts.append(frame.inner_text("body", timeout=2000))
        except Exception:
            # Frames come and go while Streamlit boots.
            continue
    return texts


def visit(url: str, deadline: float = READY_DEADLINE, channel: str = None) -> dict:
    """Open the app, wake it if needed, and wait for it to render.

    Returns:
        `ready`, whether it had to be woken, and how long it took.
    """
    from playwright.sync_api import sync_playwright

    started = time.monotonic()
    woke = False
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel=channel)
        page = browser.new_page(viewport={"width": 1440, "height": 1100})
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)

        state = WAITING
        while time.monotonic() - started < deadline:
            state = classify(_texts(page))
            if state == READY:
                break
            if state == ASLEEP and not woke:
                logger.info("App was asleep; waking it")
                page.get_by_role("button", name=WAKE_BUTTON).click()
                woke = True
            time.sleep(POLL_SECONDS)

        browser.close()

    return {
        "ready": state == READY,
        "woke": woke,
        "seconds": round(time.monotonic() - started),
    }


def summarise(result: dict, url: str) -> str:
    """One line for the job summary, worded by what happened."""
    if not result["ready"]:
        return (
            f"**The dashboard did not render within {result['seconds']}s.** "
            f"{url} needs a look; a reboot from Manage app has cleared this "
            f"before."
        )
    how = "woken from sleep" if result["woke"] else "already awake"
    return f"Dashboard up in {result['seconds']}s ({how})."


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=APP_URL)
    parser.add_argument("--deadline", type=float, default=READY_DEADLINE)
    parser.add_argument(
        "--channel", default=None,
        help="a locally installed browser to drive instead of Playwright's "
             "bundled Chromium, e.g. msedge",
    )
    args = parser.parse_args()

    result = visit(args.url, args.deadline, args.channel)
    line = summarise(result, args.url)
    logger.info(line)

    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    return 0 if result["ready"] else 1


if __name__ == "__main__":
    sys.exit(main())
