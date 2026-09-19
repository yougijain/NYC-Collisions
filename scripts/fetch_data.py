"""Paginated client for the NYC Open Data (Socrata) collisions dataset.

Dataset: Motor Vehicle Collisions - Crashes (h9gi-nx95)
https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95

Socrata caps a single response at 50,000 rows, so any slice bigger than that
has to be walked with $limit/$offset under a deterministic $order. Paging
without an explicit sort is not stable and silently drops or repeats rows.
"""

import logging
import os
import time
from pathlib import Path
from typing import Dict, Iterator, Optional, Union

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DATASET_ID: str = "h9gi-nx95"
BASE_URL: str = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"
OUTPUT_DIR: Path = Path("data/raw")
REQUEST_TIMEOUT: int = 120

# Socrata's hard per-request ceiling.
MAX_PAGE_SIZE: int = 50_000
# collision_id is unique, so it gives a total order and therefore stable paging.
PAGE_ORDER: str = "collision_id"

MAX_RETRIES: int = 5
BACKOFF_BASE: float = 2.0
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


def _session(app_token: Optional[str] = None) -> requests.Session:
    """Build a session carrying the Socrata app token, if one is available.

    Anonymous requests share a small throttling pool and get 429s under any
    real load. A token is free and lifts the per-app limit.
    """
    session = requests.Session()
    token = app_token or os.getenv("SOCRATA_APP_TOKEN")
    if token:
        session.headers["X-App-Token"] = token
        logger.info("Using Socrata app token")
    else:
        logger.warning(
            "No SOCRATA_APP_TOKEN set; requests are subject to strict "
            "anonymous throttling"
        )
    return session


def _get_with_retry(
    session: requests.Session, params: Dict[str, Union[int, str]]
) -> list:
    """GET one page, retrying throttling and transient server errors."""
    last_error: Optional[Exception] = None

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(
                BASE_URL, params=params, timeout=REQUEST_TIMEOUT
            )
            if response.status_code in RETRY_STATUSES:
                raise requests.exceptions.HTTPError(
                    f"retryable status {response.status_code}",
                    response=response,
                )
            response.raise_for_status()
            return response.json()

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
        ) as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status is not None and status not in RETRY_STATUSES:
                logger.error(f"Non-retryable HTTP error {status}: {exc}")
                raise
            last_error = exc
            if attempt == MAX_RETRIES - 1:
                break
            delay = BACKOFF_BASE**attempt
            logger.warning(
                f"Request failed ({exc}); retrying in {delay:.0f}s "
                f"[{attempt + 1}/{MAX_RETRIES}]"
            )
            time.sleep(delay)

    logger.error(f"Giving up after {MAX_RETRIES} attempts: {last_error}")
    raise RuntimeError(f"Socrata request failed: {last_error}") from last_error


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


def iter_pages(
    where_clause: Optional[str] = None,
    page_size: int = MAX_PAGE_SIZE,
    app_token: Optional[str] = None,
    max_rows: Optional[int] = None,
) -> Iterator[pd.DataFrame]:
    """Yield the dataset one page at a time.

    Args:
        where_clause: Optional SoQL $where filter.
        page_size: Rows per request, capped at Socrata's 50,000 limit.
        app_token: Socrata app token; falls back to $SOCRATA_APP_TOKEN.
        max_rows: Stop once this many rows have been yielded.

    Yields:
        A DataFrame per page, in collision_id order.
    """
    if page_size <= 0:
        raise ValueError("page_size must be a positive integer")
    page_size = min(page_size, MAX_PAGE_SIZE)

    session = _session(app_token)
    offset = 0
    total = 0

    logger.info(f"Fetching from {BASE_URL}")
    if where_clause:
        logger.info(f"Filter: {where_clause}")

    while True:
        remaining = None if max_rows is None else max_rows - total
        if remaining is not None and remaining <= 0:
            break

        limit = page_size if remaining is None else min(page_size, remaining)
        params: Dict[str, Union[int, str]] = {
            "$limit": limit,
            "$offset": offset,
            "$order": PAGE_ORDER,
        }
        if where_clause:
            params["$where"] = where_clause

        rows = _get_with_retry(session, params)
        if not rows:
            break

        total += len(rows)
        logger.info(f"  page at offset {offset:,}: {len(rows):,} rows "
                    f"({total:,} total)")
        yield pd.DataFrame(rows)

        # A short page means we reached the end of the result set.
        if len(rows) < limit:
            break
        offset += len(rows)

    logger.info(f"Fetched {total:,} rows")


def fetch_collisions(
    since: Optional[str] = None,
    until: Optional[str] = None,
    page_size: int = MAX_PAGE_SIZE,
    app_token: Optional[str] = None,
    max_rows: Optional[int] = None,
) -> pd.DataFrame:
    """Fetch a date-bounded slice of the dataset into a single DataFrame.

    Args:
        since: Inclusive lower bound on crash_date (ISO date or datetime).
        until: Exclusive upper bound on crash_date.
        page_size: Rows per request, capped at 50,000.
        app_token: Socrata app token; falls back to $SOCRATA_APP_TOKEN.
        max_rows: Cap on total rows returned.

    Returns:
        The concatenated result, empty if the filter matched nothing.
    """
    pages = list(
        iter_pages(
            where_clause=build_where(since, until),
            page_size=page_size,
            app_token=app_token,
            max_rows=max_rows,
        )
    )
    if not pages:
        logger.warning("API returned no rows")
        return pd.DataFrame()
    return pd.concat(pages, ignore_index=True)


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
