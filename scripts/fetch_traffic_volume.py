"""Fetch NYC DOT automated traffic volume counts.

Dataset: Automated Traffic Volume Counts (7ym2-wayt)
https://data.cityofnewyork.us/Transportation/Automated-Traffic-Volume-Counts/7ym2-wayt

This is the exposure denominator the watchlist has been missing. Without
it every rate in this project is per *crash*, so a junction with a lot of
traffic and a junction that is actually dangerous look the same.

It is a sample, not a census: DOT puts automated recorders out on a
street for a week or two at a time, so a segment either has counts for a
handful of days or none at all. How much of the city that reaches is the
first thing to measure, and it decides what can honestly be built on top.

The fetch deliberately takes every column as text and asks no questions
about the schema, so a wrong guess about a field name cannot break it.
Making sense of the columns happens later, against the real file.

Usage:
    python scripts/fetch_traffic_volume.py
    python scripts/fetch_traffic_volume.py --max-rows 5000 --describe
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import socrata  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DATASET_ID = "7ym2-wayt"
DEFAULT_OUTPUT = ROOT / "data" / "clean" / "traffic_volume.parquet"

# Socrata exposes a synthetic row id on every dataset. This one has no
# documented unique key, and paging without a total order silently drops
# and repeats rows, so the row id is what makes the walk stable.
PAGE_ORDER = ":id"

COMPRESSION = "zstd"
SOURCE_KEY = b"source_dataset"
FETCHED_AT_KEY = b"fetched_at"


def describe(df: pd.DataFrame) -> str:
    """Report the shape of whatever came back.

    The point of the first run is to learn the schema, so this prints it
    rather than assuming it.

    Args:
        df: The fetched frame.

    Returns:
        A human-readable report, also suitable for a CI job summary.
    """
    if df.empty:
        return "No rows returned."

    lines = [
        f"### {DATASET_ID}: {len(df):,} rows, {len(df.columns)} columns",
        "",
        "| Column | Non-null | Distinct | Example |",
        "|---|---|---|---|",
    ]
    for column in df.columns:
        values = df[column].dropna()
        example = str(values.iloc[0])[:40] if not values.empty else "-"
        lines.append(
            f"| `{column}` | {len(values):,} | {values.nunique():,} | "
            f"`{example}` |"
        )
    return "\n".join(lines)


def write(df: pd.DataFrame, path: Path) -> Path:
    """Write the counts to Parquet, stamped with where they came from.

    Args:
        df: The fetched frame.
        path: Destination Parquet path.

    Returns:
        The path written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    table = table.replace_schema_metadata({
        **(table.schema.metadata or {}),
        SOURCE_KEY: DATASET_ID.encode(),
        FETCHED_AT_KEY: datetime.now(timezone.utc).isoformat().encode(),
    })
    pq.write_table(table, path, compression=COMPRESSION)

    size_mb = path.stat().st_size / (1024 * 1024)
    logger.info(
        f"Wrote {path}: {len(df):,} rows, {len(df.columns)} columns, "
        f"{size_mb:.1f} MB ({COMPRESSION})"
    )
    return path


def run(
    output: Path = DEFAULT_OUTPUT,
    max_rows: Optional[int] = None,
    describe_only: bool = False,
) -> pd.DataFrame:
    """Fetch the counts and write them.

    Args:
        output: Destination Parquet path.
        max_rows: Cap on rows fetched, for a schema-discovery run.
        describe_only: Report the schema without writing anything.

    Returns:
        The fetched frame.

    Raises:
        RuntimeError: If the dataset comes back empty, which means the
            identifier or the portal is wrong rather than that New York
            has no traffic.
    """
    df = socrata.fetch(DATASET_ID, PAGE_ORDER, max_rows=max_rows)
    if df.empty:
        raise RuntimeError(
            f"{DATASET_ID} returned no rows. Check the dataset identifier "
            f"at {socrata.dataset_url(DATASET_ID)}."
        )

    report = describe(df)
    logger.info("\n" + report)

    # On a CI runner the schema report is the point of the run, so it goes
    # somewhere a person will actually see it.
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write(report + "\n")

    if not describe_only:
        write(df, output)
    return df


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT,
                        help=f"output Parquet path (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--max-rows", type=int, default=None,
                        help="cap rows fetched, for a schema-discovery run")
    parser.add_argument("--describe", action="store_true",
                        help="report the schema without writing anything")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    df = run(output=args.out, max_rows=args.max_rows, describe_only=args.describe)
    print(json.dumps({"rows": len(df), "columns": list(df.columns)}, indent=2))


if __name__ == "__main__":
    main()
