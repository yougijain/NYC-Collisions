"""Is the source still publishing, and are we keeping up with it?

A pipeline that runs green every week is not the same as data that is
current. This one has run green every Monday since September while the
newest crash in it stayed at 2026-06-11, because NYC Open Data stopped
publishing. Nothing anywhere said so, and the README went on promising
coverage that "grows on its own".

So ask the source directly what its newest crash is, and compare.

## Two failures that look identical from inside

**The source is quiet.** Its newest record has not moved in weeks. The
pipeline is healthy and there is nothing to fix; the data is just old, and
whoever reads the dashboard deserves to know before they draw a conclusion
from it.

**We are behind the source.** The source has newer records than the build
does, which means the fetch, the merge or the publish is dropping them.
That is a bug, and it is ours.

They need different responses, so they are reported as different fields
rather than one "stale" flag. Confusing the two is how a team spends a
week debugging an ingest that was working.

## Why this never fails the build

A quiet upstream is not a broken pipeline, and a workflow that goes red
for something nobody can fix trains people to ignore red. The check
records what it found and exits zero; the banner on the dashboard and the
line in the job summary are what carry it.

Usage:
    python scripts/check_freshness.py
    python scripts/check_freshness.py --dataset data/clean/collisions.parquet
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "app"))
sys.path.insert(0, str(ROOT / "scripts"))

import socrata  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DATASET_ID = "h9gi-nx95"
DATE_COLUMN = "crash_date"
OUTPUT = ROOT / "docs" / "source_freshness.json"

# NYC publishes in batches with a reporting lag that routinely reaches two
# weeks, so a fortnight of silence is ordinary and says nothing. Three
# weeks is past what the normal lag explains.
STALE_AFTER_DAYS = 21

# Below this the build is simply mid-lag behind a source that publishes
# continuously; above it, records exist upstream that we did not ingest.
BEHIND_AFTER_DAYS = 2


def source_latest(app_token: Optional[str] = None) -> Optional[date]:
    """Ask the source for its newest crash date.

    One aggregate request rather than a page of rows: `$select=max(...)`
    is answered from the column's index, so this costs the API almost
    nothing and can run on every refresh.

    Args:
        app_token: Socrata token, which lifts anonymous throttling.

    Returns:
        The newest `crash_date`, or None if the source answered with
        nothing parseable.
    """
    http = socrata.session(app_token)
    rows = socrata.get_with_retry(
        http,
        {"$select": f"max({DATE_COLUMN}) as newest"},
        socrata.dataset_url(DATASET_ID),
    )
    if not rows or not rows[0].get("newest"):
        logger.warning("Source returned no maximum date")
        return None
    return pd.to_datetime(rows[0]["newest"]).date()


def dataset_latest(path: Path) -> date:
    """The newest crash in the build we are shipping."""
    frame = pd.read_parquet(path, columns=["crash_datetime"])
    return pd.to_datetime(frame["crash_datetime"]).max().date()


def _spoken(day: date) -> str:
    """"11 June 2026", with no leading zero on any platform.

    strftime's %-d is a glibc extension. It passes CI on Linux and raises
    ValueError on Windows, so the day is formatted by hand.
    """
    return f"{day.day} {day:%B %Y}"


def assess(
    source: Optional[date],
    dataset: date,
    today: Optional[date] = None,
    stale_after: int = STALE_AFTER_DAYS,
    behind_after: int = BEHIND_AFTER_DAYS,
) -> Dict:
    """Turn two dates into the two findings the dashboard reports.

    Args:
        source: Newest crash the source has, or None if it could not be read.
        dataset: Newest crash in the published build.
        today: Overridable for tests.
        stale_after: Days of upstream silence before the source counts as quiet.
        behind_after: Days the build may trail the source before it counts
            as a gap we caused.

    Returns:
        A record carrying both dates, both findings and a sentence a
        reader can act on.
    """
    today = today or datetime.now(timezone.utc).date()
    age = (today - dataset).days

    if source is None:
        return {
            "checked_at": today.isoformat(),
            "dataset_latest": dataset.isoformat(),
            "source_latest": None,
            "dataset_age_days": age,
            "source_quiet": None,
            "pipeline_behind": None,
            "headline": (
                "Could not reach the source to check how current this is. "
                f"The newest crash here is {_spoken(dataset)}."
            ),
        }

    behind_days = (source - dataset).days
    quiet = (today - source).days
    source_quiet = quiet >= stale_after
    pipeline_behind = behind_days > behind_after

    if pipeline_behind:
        headline = (
            f"This build is {behind_days} days behind the source, which has "
            f"crashes through {_spoken(source)}. Records exist upstream "
            f"that the pipeline has not ingested."
        )
    elif source_quiet:
        headline = (
            f"NYC Open Data has published no crashes since "
            f"{_spoken(source)}, {quiet} days ago. The pipeline is "
            f"current with the source; the source has gone quiet."
        )
    else:
        headline = (
            f"Current with the source, which has crashes through "
            f"{_spoken(source)}."
        )

    return {
        "checked_at": today.isoformat(),
        "dataset_latest": dataset.isoformat(),
        "source_latest": source.isoformat(),
        "dataset_age_days": age,
        "source_silent_days": quiet,
        "behind_source_days": behind_days,
        "source_quiet": source_quiet,
        "pipeline_behind": pipeline_behind,
        "headline": headline,
    }


def summarise(finding: Dict) -> str:
    """One markdown block for the workflow's job summary."""
    if finding["pipeline_behind"]:
        mark = "### :rotating_light: The pipeline is behind the source"
    elif finding["source_quiet"]:
        mark = "### :hourglass: The source has gone quiet"
    elif finding["source_quiet"] is None:
        mark = "### :grey_question: Freshness unknown"
    else:
        mark = "### :white_check_mark: Current with the source"
    return (
        f"{mark}\n\n{finding['headline']}\n\n"
        f"- Newest crash here: `{finding['dataset_latest']}`\n"
        f"- Newest crash upstream: `{finding['source_latest']}`\n"
    )


def run(
    dataset: Optional[Path] = None,
    output: Path = OUTPUT,
    app_token: Optional[str] = None,
) -> Dict:
    """Check, write the record, and report. Never raises on a quiet source."""
    import db

    path = Path(dataset) if dataset else Path(db.resolve_dataset())
    held = dataset_latest(path)

    try:
        upstream = source_latest(app_token)
    except Exception as exc:
        # The source being unreachable is itself not a build failure.
        logger.warning(f"Could not read the source's newest date: {exc}")
        upstream = None

    finding = assess(upstream, held)
    logger.info(finding["headline"])

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(finding, indent=2) + "\n", encoding="utf-8")

    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as handle:
            handle.write(summarise(finding))

    return finding


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default=None, help="Parquet to check")
    parser.add_argument("--output", type=Path, default=OUTPUT)
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    print(json.dumps(
        run(dataset=args.dataset, output=args.output,
            app_token=os.environ.get("SOCRATA_APP_TOKEN")),
        indent=2,
    ))


if __name__ == "__main__":
    main()
