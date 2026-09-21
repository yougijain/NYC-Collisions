"""DuckDB query layer over the published collisions Parquet dataset.

Resolution order for the dataset, first hit wins:

1. $NYC_COLLISIONS_DATA -- an explicit local path or URL (local dev, tests)
2. data/clean/collisions.parquet -- a build produced locally
3. the GitHub Release asset, downloaded into .cache/
4. data/clean/collisions_seed.parquet -- the committed fallback

Steps 3 and 4 are what keep the deployed dashboard alive: the scheduled
workflow republishes the Release asset, and if that download fails the app
serves the last good cache, or the seed, instead of erroring out.

This module deliberately does not import streamlit so the tests and the
notebook can use it directly.
"""

import json
import logging
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd
import requests

logger = logging.getLogger(__name__)

REPO = "yougijain/NYC-Collisions"
RELEASE_TAG = "data-latest"
ASSET_NAME = "collisions.parquet"
RELEASE_URL = (
    f"https://github.com/{REPO}/releases/download/{RELEASE_TAG}/{ASSET_NAME}"
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_ROOT / "sql"
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"
BUILT_PATH = CLEAN_DIR / "collisions.parquet"
SEED_PATH = CLEAN_DIR / "collisions_seed.parquet"
CACHE_PATH = PROJECT_ROOT / ".cache" / ASSET_NAME

# The watchlist is committed rather than published as a Release asset: it is
# a few hundred rows, it is the output worth arguing with, and a CSV diff
# shows a site entering or leaving the list between builds.
WATCHLIST_PATH = CLEAN_DIR / "injury_watchlist.csv"
FACTOR_RISK_PATH = CLEAN_DIR / "injury_risk_factors.csv"
WATCHLIST_SUMMARY_PATH = CLEAN_DIR / "watchlist_summary.json"

# Traffic volumes joined onto the watchlist, for the third of sites that
# sit near a DOT counter.
EXPOSURE_PATH = CLEAN_DIR / "injury_exposure.csv"
EXPOSURE_SUMMARY_PATH = CLEAN_DIR / "exposure_summary.json"

# Re-download the Release asset at most this often.
CACHE_TTL_SECONDS = 6 * 60 * 60
DOWNLOAD_TIMEOUT = 120

TABLE_NAME = "collisions_clean"
_PARAM_PATTERN = re.compile(r"\$([a-z_][a-z0-9_]*)", re.IGNORECASE)


def _download(url: str, destination: Path) -> bool:
    """Stream `url` to `destination` atomically. Returns False on failure."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(dir=destination.parent, suffix=".part")
    tmp_path = Path(tmp_name)
    os.close(tmp_fd)

    try:
        logger.info(f"Downloading dataset from {url}")
        with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as response:
            response.raise_for_status()
            with open(tmp_path, "wb") as handle:
                shutil.copyfileobj(response.raw, handle)
        # Move into place only once the body is fully written.
        tmp_path.replace(destination)
        size_mb = destination.stat().st_size / (1024 * 1024)
        logger.info(f"Cached dataset at {destination} ({size_mb:.1f} MB)")
        return True
    except (requests.exceptions.RequestException, OSError) as exc:
        logger.warning(f"Dataset download failed: {exc}")
        tmp_path.unlink(missing_ok=True)
        return False


def resolve_dataset(refresh: bool = False) -> str:
    """Locate the dataset to query.

    Args:
        refresh: Re-download the Release asset even if the cache is fresh.

    Returns:
        A path or URL that DuckDB's read_parquet can open.

    Raises:
        FileNotFoundError: If no dataset can be resolved at all.
    """
    override = os.getenv("NYC_COLLISIONS_DATA")
    if override:
        logger.info(f"Using dataset from $NYC_COLLISIONS_DATA: {override}")
        return override

    if BUILT_PATH.exists():
        logger.info(f"Using locally built dataset: {BUILT_PATH}")
        return str(BUILT_PATH)

    cache_is_fresh = (
        CACHE_PATH.exists()
        and (time.time() - CACHE_PATH.stat().st_mtime) < CACHE_TTL_SECONDS
    )
    if cache_is_fresh and not refresh:
        logger.info(f"Using cached dataset: {CACHE_PATH}")
        return str(CACHE_PATH)

    if _download(RELEASE_URL, CACHE_PATH):
        return str(CACHE_PATH)

    if CACHE_PATH.exists():
        logger.warning("Download failed; serving stale cached dataset")
        return str(CACHE_PATH)

    if SEED_PATH.exists():
        logger.warning("Download failed; serving the committed seed dataset")
        return str(SEED_PATH)

    raise FileNotFoundError(
        "No dataset available. Run `python scripts/build_dataset.py --full`, "
        "or set $NYC_COLLISIONS_DATA to a Parquet path or URL."
    )


def connect(dataset: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB with the dataset exposed as a view.

    Args:
        dataset: Parquet path or URL; resolved automatically when omitted.

    Returns:
        A connection where `collisions_clean` is queryable.
    """
    source = dataset or resolve_dataset()
    connection = duckdb.connect(database=":memory:")

    if source.startswith(("http://", "https://")):
        # httpfs lets DuckDB read the asset over the network directly.
        connection.execute("INSTALL httpfs; LOAD httpfs;")

    # CREATE VIEW cannot be prepared, so the path is escaped and inlined.
    escaped = source.replace("'", "''")
    connection.execute(
        f"CREATE VIEW {TABLE_NAME} AS "
        f"SELECT * FROM read_parquet('{escaped}')"
    )
    return connection


def read_sql_file(sql_file: str) -> str:
    """Return the text of a query in the sql/ directory."""
    return (SQL_DIR / sql_file).read_text(encoding="utf-8")


def bind_params(sql: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Narrow `params` to the placeholders the query actually references.

    DuckDB raises on named parameters a statement does not use, so passing a
    uniform filter dict to every query requires filtering it per query first.
    """
    referenced = set(_PARAM_PATTERN.findall(sql))
    missing = referenced - set(params)
    if missing:
        raise KeyError(f"Query references unbound parameters: {sorted(missing)}")
    return {k: v for k, v in params.items() if k in referenced}


def query(
    connection: duckdb.DuckDBPyConnection,
    sql_file: str,
    params: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Run a query from the sql/ directory and return the result.

    Args:
        connection: An open connection from `connect`.
        sql_file: Filename within sql/, e.g. "02_aggregate.sql".
        params: Named parameters; extras are dropped automatically.

    Returns:
        The result as a DataFrame.
    """
    sql = read_sql_file(sql_file)
    bound = bind_params(sql, params or {})
    return connection.execute(sql, bound).df() if bound else connection.execute(sql).df()


def filter_params(
    start_date: Any, end_date: Any, boroughs: List[str], row_limit: int = 50_000
) -> Dict[str, Any]:
    """Build the standard filter bound to every dashboard query.

    Args:
        start_date: Inclusive start of the window.
        end_date: Inclusive end of the window; widened to cover the whole day.
        boroughs: Borough names to keep, with 'UNKNOWN' for unlabelled rows.
        row_limit: Cap on rows returned by the map query.

    Returns:
        A dict suitable for passing to `query`.
    """
    return {
        "start_date": pd.Timestamp(start_date).normalize(),
        # end_date is a calendar day, so include everything up to midnight.
        "end_date": pd.Timestamp(end_date).normalize() + pd.Timedelta(days=1),
        "boroughs": list(boroughs),
        "row_limit": int(row_limit),
    }


def _read_csv(path: Path) -> pd.DataFrame:
    """Read a committed CSV, or an empty frame if it has not been built yet."""
    if not path.exists():
        logger.warning(f"{path.name} is missing; run scripts/build_watchlist.py")
        return pd.DataFrame()
    return pd.read_csv(path)


def load_watchlist() -> pd.DataFrame:
    """The ranked intersections, or an empty frame before the first build."""
    return _read_csv(WATCHLIST_PATH)


def load_factor_risk() -> pd.DataFrame:
    """Contributing factors by predicted injury risk."""
    return _read_csv(FACTOR_RISK_PATH)


def load_watchlist_summary() -> Dict[str, Any]:
    """The window, the folds and the headline numbers behind the watchlist."""
    if not WATCHLIST_SUMMARY_PATH.exists():
        return {}
    return json.loads(WATCHLIST_SUMMARY_PATH.read_text(encoding="utf-8"))


def load_exposure() -> pd.DataFrame:
    """Traffic volumes for the watchlist sites that have one."""
    return _read_csv(EXPOSURE_PATH)


def load_exposure_summary() -> Dict[str, Any]:
    """Coverage, and how far the two rankings actually agree."""
    if not EXPOSURE_SUMMARY_PATH.exists():
        return {}
    return json.loads(EXPOSURE_SUMMARY_PATH.read_text(encoding="utf-8"))
