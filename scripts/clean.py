"""Reusable cleaning for raw NYC collisions records.

Extracted from notebooks/cleaning.ipynb so the scheduled pipeline and the
notebook apply exactly the same transforms.

Handles both shapes of the source data:
  - the CSV export, with headers like "VEHICLE TYPE CODE 1"
  - the Socrata JSON API, which uses snake_case but irregularly emits
    vehicle_type_code1 / vehicle_type_code2 (no underscore) alongside
    vehicle_type_code_3 / _4 / _5 (with one)
"""

import logging
import re
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)

# Column order of the published dataset.
CANONICAL_COLUMNS: List[str] = [
    "borough",
    "zip_code",
    "latitude",
    "longitude",
    "on_street_name",
    "cross_street_name",
    "off_street_name",
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_pedestrians_injured",
    "number_of_pedestrians_killed",
    "number_of_cyclist_injured",
    "number_of_cyclist_killed",
    "number_of_motorist_injured",
    "number_of_motorist_killed",
    "contributing_factor_vehicle_1",
    "contributing_factor_vehicle_2",
    "contributing_factor_vehicle_3",
    "collision_id",
    "vehicle_type_code_1",
    "vehicle_type_code_2",
    "vehicle_type_code_3",
    "crash_datetime",
]

COUNT_COLUMNS: List[str] = [c for c in CANONICAL_COLUMNS if c.startswith("number_of_")]

# >98% null in the source; dropped rather than carried as dead weight.
SPARSE_COLUMNS: List[str] = [
    "contributing_factor_vehicle_4",
    "contributing_factor_vehicle_5",
    "vehicle_type_code_4",
    "vehicle_type_code_5",
]

# Redundant with latitude/longitude, and a nested dict over the API.
REDUNDANT_COLUMNS: List[str] = ["location"]

# Generous bounding box around the five boroughs. The source encodes unknown
# positions as 0.0 rather than null, which would otherwise plot in the
# Gulf of Guinea.
NYC_LAT_RANGE = (40.4, 41.0)
NYC_LON_RANGE = (-74.35, -73.6)

_TIME_PATTERN = re.compile(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lower-case and snake_case column names, reconciling API/CSV variants.

    Args:
        df: Raw DataFrame from either source.

    Returns:
        A copy with normalized column names.
    """
    normalized = []
    for col in df.columns:
        name = re.sub(r"[^\w\s]", "", str(col).strip())
        name = re.sub(r"\s+", "_", name).lower()
        # Socrata emits vehicle_type_code1/2 but vehicle_type_code_3/4/5.
        name = re.sub(r"^(vehicle_type_code)(\d)$", r"\1_\2", name)
        name = re.sub(r"^(contributing_factor_vehicle)(\d)$", r"\1_\2", name)
        normalized.append(name)

    out = df.copy()
    out.columns = normalized
    return out


def _parse_crash_datetime(df: pd.DataFrame) -> pd.Series:
    """Combine crash_date and crash_time into a single timestamp."""
    raw_date = df["crash_date"].astype("string").str.strip()

    # The API returns ISO timestamps, the CSV export MM/DD/YYYY.
    date = pd.to_datetime(raw_date, errors="coerce", format="ISO8601")
    missing = date.isna() & raw_date.notna()
    if missing.any():
        date = date.fillna(
            pd.to_datetime(raw_date[missing], errors="coerce", format="%m/%d/%Y")
        )
    date = date.dt.normalize()

    # Normalize H:MM / HH:MM / HH:MM:SS to HH:MM:SS before parsing.
    raw_time = df["crash_time"].astype("string").str.strip()
    extracted = raw_time.str.extract(_TIME_PATTERN)
    time_str = (
        extracted[0].str.zfill(2)
        + ":"
        + extracted[1]
        + ":"
        + extracted[2].fillna("00")
    )
    offset = pd.to_timedelta(time_str, errors="coerce").fillna(pd.Timedelta(0))

    unparsed = raw_time.notna() & extracted[0].isna()
    if unparsed.any():
        logger.warning(
            f"{int(unparsed.sum()):,} unparseable crash_time values "
            f"defaulted to midnight"
        )

    return date + offset


def _mask_invalid_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """Null out coordinates that fall outside the NYC bounding box."""
    lat = pd.to_numeric(df["latitude"], errors="coerce")
    lon = pd.to_numeric(df["longitude"], errors="coerce")

    valid = (
        lat.between(*NYC_LAT_RANGE)
        & lon.between(*NYC_LON_RANGE)
    )
    dropped = int((lat.notna() & lon.notna() & ~valid).sum())
    if dropped:
        logger.info(f"Masked {dropped:,} out-of-bounds coordinate pairs")

    df["latitude"] = lat.where(valid)
    df["longitude"] = lon.where(valid)
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the full cleaning pipeline to raw collision records.

    Normalizes column names, builds crash_datetime, coerces types, masks
    invalid coordinates, drops sparse and redundant columns, and de-duplicates
    on collision_id.

    Args:
        df: Raw DataFrame from the CSV export or the Socrata API.

    Returns:
        A DataFrame with exactly CANONICAL_COLUMNS, sorted by crash_datetime.

    Raises:
        KeyError: If a required source column is absent.
    """
    if df.empty:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    out = normalize_columns(df)

    required = {"crash_date", "crash_time", "collision_id"}
    missing = required - set(out.columns)
    if missing:
        raise KeyError(f"Source data is missing required columns: {sorted(missing)}")

    out["crash_datetime"] = _parse_crash_datetime(out)
    out = out.drop(columns=["crash_date", "crash_time"])

    out = out.drop(
        columns=[c for c in SPARSE_COLUMNS + REDUNDANT_COLUMNS if c in out.columns]
    )

    # Zip codes are identifiers, not quantities; keep them as digit strings.
    if "zip_code" in out.columns:
        zips = pd.to_numeric(out["zip_code"], errors="coerce")
        out["zip_code"] = (
            zips.astype("Int64").astype("string").str.zfill(5)
        )

    for col in COUNT_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")

    out["collision_id"] = pd.to_numeric(
        out["collision_id"], errors="coerce"
    ).astype("Int64")

    if {"latitude", "longitude"}.issubset(out.columns):
        out = _mask_invalid_coordinates(out)

    # Any column the source did not supply is still expected downstream.
    for col in CANONICAL_COLUMNS:
        if col not in out.columns:
            logger.warning(f"Source data had no {col}; filling with nulls")
            out[col] = pd.NA

    out = out[CANONICAL_COLUMNS]

    before = len(out)
    out = out.dropna(subset=["collision_id", "crash_datetime"])
    out = out.drop_duplicates(subset="collision_id", keep="last")
    if before != len(out):
        logger.info(f"Dropped {before - len(out):,} duplicate/unusable rows")

    return out.sort_values("crash_datetime").reset_index(drop=True)
