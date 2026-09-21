"""Put traffic volumes next to the watchlist, and measure what they say.

Writes two committed files:

    data/clean/injury_exposure.csv    matched sites, volumes, rates
    data/clean/exposure_summary.json  coverage, and the correlation

The second is the point. The watchlist has always carried a caveat --
that it ranks by crash mix and not by danger, because there was no
exposure denominator to rank by. Now there is one for some of the city,
and the caveat can be a measurement rather than an assertion: the two
rankings agree at a Spearman correlation of about 0.27.

What this deliberately does not do is re-rank the watchlist by vehicles.
The head of a per-vehicle ranking is whichever junction has the most
under-measured traffic, which on this data means single-direction counts
on service roads. The rate is published per site with a confidence grade
and the volume behind it; the ranking stays with the crash-mix residual,
which covers every site rather than a third of them.

Usage:
    python scripts/build_exposure.py
    python scripts/build_exposure.py --counts data/clean/traffic_volume.parquet
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "app"))

import db  # noqa: E402
from models import exposure as E  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

CLEAN_DIR = ROOT / "data" / "clean"
DEFAULT_COUNTS = CLEAN_DIR / "traffic_volume.parquet"
EXPOSURE_PATH = CLEAN_DIR / "injury_exposure.csv"
SUMMARY_PATH = CLEAN_DIR / "exposure_summary.json"

# Published as a Release asset by .github/workflows/traffic-volume.yml,
# the same way the collisions build is.
COUNTS_URL = (
    "https://github.com/yougijain/NYC-Collisions/releases/download/"
    "data-latest/traffic_volume.parquet"
)

COLUMNS = [
    "site", "borough", "crashes", "observed_rate", "excess",
    "segment_id", "metres", "counters_in_range", "names_both_streets",
    "directions_counted", "counted_year", "confidence",
    "vehicles_per_day", "million_vehicles",
    "crashes_per_million", "harmful_per_million",
]
RATE_PRECISION = 3


def load_counts(path: Path = DEFAULT_COUNTS) -> pd.DataFrame:
    """Read the DOT counts, falling back to the published asset.

    Args:
        path: Local Parquet path.

    Returns:
        The raw counts.

    Raises:
        FileNotFoundError: If neither the local file nor the Release
            asset can be read.
    """
    if path.exists():
        logger.info(f"Reading counts from {path}")
        return pd.read_parquet(path)

    logger.info(f"{path} not found; reading the published asset")
    try:
        return pd.read_parquet(COUNTS_URL)
    except Exception as exc:
        raise FileNotFoundError(
            f"No traffic counts at {path} and the published asset could "
            f"not be read ({exc}). Run the 'Fetch traffic volume' workflow, "
            f"or python scripts/fetch_traffic_volume.py."
        ) from exc


def run(
    counts_path: Path = DEFAULT_COUNTS,
    output_dir: Path = CLEAN_DIR,
) -> Dict:
    """Join the counts to the watchlist and write both files.

    Args:
        counts_path: Local Parquet of DOT counts.
        output_dir: Where the two files go.

    Returns:
        The summary, as written to exposure_summary.json.

    Raises:
        RuntimeError: If the watchlist has not been built, or if no site
            matches a counter at all.
    """
    sites = db.load_watchlist()
    watchlist_summary = db.load_watchlist_summary()
    if sites.empty or not watchlist_summary:
        raise RuntimeError(
            "No watchlist to attach volumes to. Run "
            "`python scripts/build_watchlist.py` first."
        )

    window = watchlist_summary["window"]
    window_days = (
        pd.Timestamp(window["to"]) - pd.Timestamp(window["from"])
    ).days

    counts = load_counts(counts_path)
    volumes = E.daily_volumes(counts)
    points = E.count_points(counts).merge(volumes, on="segment_id", how="inner")
    logger.info(
        f"{len(points):,} counters with a usable volume, from "
        f"{counts['segmentid'].nunique():,} counted segments"
    )

    matches = E.match_sites(sites, points)
    rates = E.exposure_rates(sites, matches, volumes, window_days)
    if rates.empty:
        raise RuntimeError(
            "No watchlist site matched a traffic counter. Either the "
            "counts are empty or the site keys have changed shape."
        )

    summary = E.coverage(sites, rates)
    summary["window"] = window
    summary["window_days"] = window_days
    logger.info(
        f"{summary['matched']:,} of {summary['sites_located']:,} located "
        f"sites matched ({summary['matched_share']:.1%}); Spearman against "
        f"the crash-mix ranking {summary['spearman_excess_vs_per_vehicle']}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    published = rates[[c for c in COLUMNS if c in rates.columns]].copy()
    for column in ("observed_rate", "excess", "million_vehicles",
                   "crashes_per_million", "harmful_per_million"):
        if column in published:
            published[column] = published[column].round(RATE_PRECISION)
    published["vehicles_per_day"] = published["vehicles_per_day"].round(0).astype(int)

    published.to_csv(output_dir / EXPOSURE_PATH.name, index=False)
    (output_dir / SUMMARY_PATH.name).write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    logger.info(f"Wrote {EXPOSURE_PATH.name} and {SUMMARY_PATH.name}")
    return summary


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--counts", type=Path, default=DEFAULT_COUNTS,
                        help=f"DOT counts Parquet (default: {DEFAULT_COUNTS})")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    print(json.dumps(run(counts_path=args.counts), indent=2))


if __name__ == "__main__":
    main()
