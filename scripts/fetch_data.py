"""Fetch the NYC collisions dataset.

Dataset: Motor Vehicle Collisions - Crashes (h9gi-nx95)
https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95

The paging, retrying and throttling live in scripts/socrata.py, which knows
nothing about crashes. What is left here is the part specific to this
dataset: which column gives a stable order, and how to bound it by date.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

import socrata  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DATASET_ID: str = "h9gi-nx95"
OUTPUT_DIR: Path = Path("data/raw")

# collision_id is unique, so it gives a total order and therefore stable paging.
PAGE_ORDER: str = "collision_id"

MAX_PAGE_SIZE: int = socrata.MAX_PAGE_SIZE


def build_where(
    since: Optional[str] = None, until: Optional[str] = None
) -> Optional[str]:
    """Compose a SoQL $where clause bounding crash_date.

    Args:
        since: Inclusive lower bound, ISO date or datetime (e.g. "2020-01-01").
        until: Exclusive upper bound, ISO date or datetime.

    Returns:
        The clause, or None when both bounds are omitted.
    """

    def _as_timestamp(value: str) -> str:
        # Socrata floating timestamps need a full ISO datetime literal.
        return value if "T" in value else f"{value}T00:00:00"

    clauses = []
    if since:
        clauses.append(f"crash_date >= '{_as_timestamp(since)}'")
    if until:
        clauses.append(f"crash_date < '{_as_timestamp(until)}'")
    return " AND ".join(clauses) if clauses else None


def fetch_collisions(
    since: Optional[str] = None,
    until: Optional[str] = None,
    page_size: int = MAX_PAGE_SIZE,
    app_token: Optional[str] = None,
    max_rows: Optional[int] = None,
) -> pd.DataFrame:
    """Fetch a date-bounded slice of the collisions dataset.

    Args:
        since: Inclusive lower bound on crash_date (ISO date or datetime).
        until: Exclusive upper bound on crash_date.
        page_size: Rows per request, capped at 50,000.
        app_token: Socrata app token; falls back to $SOCRATA_APP_TOKEN.
        max_rows: Cap on total rows returned.

    Returns:
        The concatenated result, empty if the filter matched nothing.
    """
    return socrata.fetch(
        DATASET_ID,
        PAGE_ORDER,
        where_clause=build_where(since, until),
        page_size=page_size,
        app_token=app_token,
        max_rows=max_rows,
    )


def save_data(df: pd.DataFrame, filename: str) -> Path:
    """Write a DataFrame to data/raw/<filename>.

    Args:
        df: DataFrame to save.
        filename: Target filename within the raw data directory.

    Returns:
        Path to the saved file.

    Raises:
        ValueError: If the DataFrame is empty.
        OSError: If the file cannot be written.
    """
    if df.empty:
        raise ValueError("Cannot save empty DataFrame")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / filename

    try:
        df.to_csv(output_path, index=False)
        size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"Saved {output_path} ({size_mb:.2f} MB)")
        return output_path
    except OSError as exc:
        logger.error(f"Failed to write {output_path}: {exc}")
        raise


def main() -> None:
    """Fetch a small recent sample and write it to data/raw/.

    This is a smoke test for API connectivity. The real pipeline entry point
    is scripts/build_dataset.py.
    """
    df = fetch_collisions(since="2024-01-01", max_rows=1000)
    if df.empty:
        logger.warning("No data fetched")
        return

    logger.info(f"Records: {len(df):,}  Columns: {len(df.columns)}")
    if "crash_date" in df.columns:
        logger.info(
            f"Date range: {df['crash_date'].min()} to {df['crash_date'].max()}"
        )
    save_data(df, "nyc_collisions_latest.csv")


if __name__ == "__main__":
    main()
