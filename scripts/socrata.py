"""A paginated client for any NYC Open Data (Socrata) dataset.

This was the collisions fetcher until a second dataset needed the same
machinery. Nothing here knows about crashes: it pages, it retries, and it
hands back frames.

Socrata caps a single response at 50,000 rows, so any slice bigger than
that has to be walked with $limit/$offset under a deterministic $order.
Paging without an explicit sort is not stable and silently drops or
repeats rows -- the server is free to return them in a different order on
every request, so a row can sit on both sides of a page boundary or
neither.
"""

import logging
import os
import time
from typing import Dict, Iterator, Optional, Union

import pandas as pd
import requests

logger = logging.getLogger(__name__)

PORTAL = "https://data.cityofnewyork.us/resource"
REQUEST_TIMEOUT: int = 120

# Socrata's hard per-request ceiling.
MAX_PAGE_SIZE: int = 50_000

MAX_RETRIES: int = 5
BACKOFF_BASE: float = 2.0
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


def dataset_url(dataset_id: str) -> str:
    """The JSON endpoint for a dataset's four-by-four identifier."""
    return f"{PORTAL}/{dataset_id}.json"


def session(app_token: Optional[str] = None) -> requests.Session:
    """Build a session carrying the Socrata app token, if one is available.

    Anonymous requests share a small throttling pool and get 429s under any
    real load. A token is free and lifts the per-app limit.
    """
    http = requests.Session()
    token = app_token or os.getenv("SOCRATA_APP_TOKEN")
    if token:
        http.headers["X-App-Token"] = token
        logger.info("Using Socrata app token")
    else:
        logger.warning(
            "No SOCRATA_APP_TOKEN set; requests are subject to strict "
            "anonymous throttling"
        )
    return http


def get_with_retry(
    http: requests.Session, params: Dict[str, Union[int, str]], url: str
) -> list:
    """GET one page, retrying throttling and transient server errors."""
    last_error: Optional[Exception] = None

    for attempt in range(MAX_RETRIES):
        try:
            response = http.get(url, params=params, timeout=REQUEST_TIMEOUT)
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


def iter_pages(
    dataset_id: str,
    order: str,
    where_clause: Optional[str] = None,
    page_size: int = MAX_PAGE_SIZE,
    app_token: Optional[str] = None,
    max_rows: Optional[int] = None,
) -> Iterator[pd.DataFrame]:
    """Yield a dataset one page at a time.

    Args:
        dataset_id: The dataset's four-by-four identifier.
        order: A column giving a total order, so paging is stable. A column
            with ties is not enough -- rows within a tie can move between
            requests.
        where_clause: Optional SoQL $where filter.
        page_size: Rows per request, capped at Socrata's 50,000 limit.
        app_token: Socrata app token; falls back to $SOCRATA_APP_TOKEN.
        max_rows: Stop once this many rows have been yielded.

    Yields:
        A DataFrame per page, in `order`.

    Raises:
        ValueError: If `page_size` is not positive.
    """
    if page_size <= 0:
        raise ValueError("page_size must be a positive integer")
    page_size = min(page_size, MAX_PAGE_SIZE)

    url = dataset_url(dataset_id)
    http = session(app_token)
    offset = 0
    total = 0

    logger.info(f"Fetching from {url}")
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
            "$order": order,
        }
        if where_clause:
            params["$where"] = where_clause

        rows = get_with_retry(http, params, url)
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


def fetch(
    dataset_id: str,
    order: str,
    where_clause: Optional[str] = None,
    page_size: int = MAX_PAGE_SIZE,
    app_token: Optional[str] = None,
    max_rows: Optional[int] = None,
) -> pd.DataFrame:
    """Fetch a whole dataset, or a filtered slice of one, into one frame.

    Args:
        dataset_id: The dataset's four-by-four identifier.
        order: A column giving a total order.
        where_clause: Optional SoQL $where filter.
        page_size: Rows per request, capped at 50,000.
        app_token: Socrata app token; falls back to $SOCRATA_APP_TOKEN.
        max_rows: Cap on total rows returned.

    Returns:
        The concatenated result, empty if the filter matched nothing.
    """
    pages = list(iter_pages(
        dataset_id, order, where_clause, page_size, app_token, max_rows
    ))
    if not pages:
        logger.warning("API returned no rows")
        return pd.DataFrame()
    return pd.concat(pages, ignore_index=True)
