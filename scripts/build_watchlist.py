"""Build the intersection watchlist the dashboard serves.

Writes three small files that are committed to the repository rather than
published as Release assets. They are a few hundred rows, they are the
output anyone would actually want to argue with, and CSV diffs readably --
a reviewer can see a site enter or leave the list between builds.

    data/clean/injury_watchlist.csv     ranked intersections
    data/clean/injury_risk_factors.csv  contributing factors by predicted risk
    data/clean/watchlist_summary.json   window, folds, headline numbers

Usage:
    python scripts/build_watchlist.py
    python scripts/build_watchlist.py --min-crashes 40
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
from build_dataset import provenance  # noqa: E402
from models import watchlist as W  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = ROOT / "data" / "clean"
WATCHLIST_PATH = OUTPUT_DIR / "injury_watchlist.csv"
FACTORS_PATH = OUTPUT_DIR / "injury_risk_factors.csv"
SUMMARY_PATH = OUTPUT_DIR / "watchlist_summary.json"

# Rates are rounded on the way out. Six figures of a rate estimated from
# 25 crashes is false precision, and it churns the diff on every rebuild.
RATE_PRECISION = 4
COORDINATE_PRECISION = 6

_RATE_COLUMNS = [
    "observed_rate", "expected_rate", "base_rate",
    "excess", "excess_z", "excess_lower_95",
]


def _round(frame: pd.DataFrame) -> pd.DataFrame:
    """Round rates and coordinates so rebuilds diff on substance."""
    out = frame.copy()
    for column in _RATE_COLUMNS:
        if column in out.columns:
            out[column] = out[column].round(RATE_PRECISION)
    for column in ("latitude", "longitude"):
        if column in out.columns:
            out[column] = out[column].round(COORDINATE_PRECISION)
    return out


def run(
    dataset: Optional[str] = None,
    first_scored_year: int = W.FIRST_SCORED_YEAR,
    min_crashes: int = W.MIN_SITE_CRASHES,
    output_dir: Path = OUTPUT_DIR,
) -> Dict:
    """Score, rank and write the watchlist.

    Args:
        dataset: Parquet path or URL; resolved through app/db.py when omitted.
        first_scored_year: Earliest year to score.
        min_crashes: Minimum scored crashes for a site to be listed.
        output_dir: Where the three files go.

    Returns:
        The summary, as written to watchlist_summary.json.

    Raises:
        RuntimeError: If no site clears the minimum crash count.
    """
    source = dataset or db.resolve_dataset()
    logger.info(f"Reading {source}")
    df = pd.read_parquet(source)

    logger.info(f"Scoring rolling-origin from {first_scored_year}")
    sites, factors, summary = W.build(df, first_scored_year, min_crashes)

    if sites.empty:
        raise RuntimeError(
            f"No intersection has {min_crashes} scored crashes. Either the "
            f"dataset is a sample or --min-crashes is set too high."
        )

    summary["dataset"] = provenance(source)
    logger.info(
        f"{summary['sites']:,} sites over {summary['scored_crashes']:,} scored "
        f"crashes; worst is {summary['worst_site']} at "
        f"{summary['worst_excess']:+.3f}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    _round(sites).to_csv(output_dir / WATCHLIST_PATH.name, index=False)
    _round(factors).to_csv(output_dir / FACTORS_PATH.name, index=False)
    (output_dir / SUMMARY_PATH.name).write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )

    logger.info(f"Wrote {WATCHLIST_PATH.name}, {FACTORS_PATH.name} and "
                f"{SUMMARY_PATH.name} to {output_dir}")
    return summary


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default=None,
                        help="Parquet path or URL (default: whatever app/db.py resolves)")
    parser.add_argument("--first-scored-year", type=int, default=W.FIRST_SCORED_YEAR,
                        help=f"earliest year to score (default: {W.FIRST_SCORED_YEAR})")
    parser.add_argument("--min-crashes", type=int, default=W.MIN_SITE_CRASHES,
                        help=f"minimum crashes per site (default: {W.MIN_SITE_CRASHES})")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    summary = run(
        dataset=args.dataset,
        first_scored_year=args.first_scored_year,
        min_crashes=args.min_crashes,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
