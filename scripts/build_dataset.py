"""Build the published collisions Parquet dataset from NYC Open Data.

Two modes:
  --full         fetch the whole window from --since and rewrite the file
  (default)      incremental: re-fetch only recent crashes and merge them in

Incremental runs re-request an overlap window rather than starting exactly at
the newest row already held. NYC back-fills late-reported crashes and amends
existing ones, so the tail of the dataset keeps changing after first
publication. De-duplication on collision_id makes the overlap idempotent.

Usage:
    python scripts/build_dataset.py --full
    python scripts/build_dataset.py
"""

import argparse
import logging
import sys
from datetime import timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from clean import CANONICAL_COLUMNS, clean  # noqa: E402
from fetch_data import fetch_collisions  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_SINCE = "2020-01-01"
DEFAULT_OUTPUT = Path("data/clean/collisions.parquet")
# Days of already-held data to re-request on an incremental run.
DEFAULT_OVERLAP_DAYS = 30
COMPRESSION = "zstd"


def read_existing(path: Path) -> Optional[pd.DataFrame]:
    """Load the current dataset, or None if it is absent or unreadable."""
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        logger.info(f"Existing dataset: {len(df):,} rows at {path}")
        return df
    except Exception as exc:
        logger.warning(f"Could not read {path} ({exc}); rebuilding from scratch")
        return None


def merge(existing: Optional[pd.DataFrame], incoming: pd.DataFrame) -> pd.DataFrame:
    """Combine held and freshly fetched rows, newest record winning per crash."""
    if existing is None or existing.empty:
        return incoming
    if incoming.empty:
        return existing

    combined = pd.concat([existing, incoming], ignore_index=True)
    # `incoming` is appended last, so keep="last" prefers the fresher record.
    combined = combined.drop_duplicates(subset="collision_id", keep="last")
    return combined.sort_values("crash_datetime").reset_index(drop=True)


def write(df: pd.DataFrame, path: Path) -> None:
    """Write the dataset to Parquet and log a summary."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, compression=COMPRESSION)

    size_mb = path.stat().st_size / (1024 * 1024)
    logger.info(
        f"Wrote {path}: {len(df):,} rows, {len(df.columns)} columns, "
        f"{size_mb:.1f} MB ({COMPRESSION})"
    )
    logger.info(
        f"Date range: {df['crash_datetime'].min()} to {df['crash_datetime'].max()}"
    )


def build(
    output: Path = DEFAULT_OUTPUT,
    since: str = DEFAULT_SINCE,
    full: bool = False,
    overlap_days: int = DEFAULT_OVERLAP_DAYS,
    max_rows: Optional[int] = None,
) -> pd.DataFrame:
    """Fetch, clean and merge collision data, then write it to Parquet.

    Args:
        output: Destination Parquet path.
        since: Earliest crash_date to include, as an ISO date.
        full: Rebuild the whole window instead of refreshing the tail.
        overlap_days: Days of held data to re-request on an incremental run.
        max_rows: Cap on rows fetched, for smoke tests.

    Returns:
        The dataset that was written.
    """
    existing = None if full else read_existing(output)

    fetch_from = since
    if existing is not None and not existing.empty:
        watermark = pd.to_datetime(existing["crash_datetime"]).max()
        fetch_from = max(
            (watermark - timedelta(days=overlap_days)).strftime("%Y-%m-%d"),
            since,
        )
        logger.info(
            f"Incremental refresh: newest held crash is {watermark}, "
            f"re-fetching from {fetch_from} ({overlap_days}-day overlap)"
        )
    else:
        logger.info(f"Full build from {since}")

    raw = fetch_collisions(since=fetch_from, max_rows=max_rows)
    if raw.empty:
        logger.warning("API returned no rows")
        if existing is None:
            raise RuntimeError("No existing dataset and no rows fetched")
        logger.info("Keeping existing dataset unchanged")
        return existing

    incoming = clean(raw)
    logger.info(f"Cleaned {len(incoming):,} rows")

    before = 0 if existing is None else len(existing)
    result = merge(existing, incoming)
    logger.info(f"Dataset: {before:,} -> {len(result):,} rows (+{len(result) - before:,})")

    write(result, output)
    return result


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT,
                        help=f"output Parquet path (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--since", default=DEFAULT_SINCE,
                        help=f"earliest crash date (default: {DEFAULT_SINCE})")
    parser.add_argument("--full", action="store_true",
                        help="rebuild the whole window instead of refreshing the tail")
    parser.add_argument("--overlap-days", type=int, default=DEFAULT_OVERLAP_DAYS,
                        help=f"incremental overlap (default: {DEFAULT_OVERLAP_DAYS})")
    parser.add_argument("--max-rows", type=int, default=None,
                        help="cap rows fetched, for smoke tests")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    build(
        output=args.out,
        since=args.since,
        full=args.full,
        overlap_days=args.overlap_days,
        max_rows=args.max_rows,
    )


if __name__ == "__main__":
    main()
