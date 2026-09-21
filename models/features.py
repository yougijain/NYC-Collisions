"""Turn cleaned crash records into features a reader can name.

Every column this produces is something you could say out loud in a review:
"a moped hit a sedan on an avenue in Brooklyn at 2am". Nothing here is an
embedding, a hash or a learned encoding, because the point of the model is
to support an argument about which intersections deserve attention, and an
argument made of opaque features is not one anybody can check.

Borough comes from the pipeline's resolved column where the build has one,
since the raw field is blank on 31% of crashes and not at random.

Two things are deliberately absent:

  - the casualty counts, which are the target
  - coordinates, street names and anything else identifying *where* a crash
    happened

The second matters for how the model gets used downstream. The watchlist
ranks sites by how much their observed injury rate exceeds what the model
expects, so the model has to predict from the crash mix alone. Give it the
location and it learns the site, and the difference collapses to nothing.
"""

import re
from typing import List

import pandas as pd

# Feature columns, in the order the model sees them.
CATEGORICAL_FEATURES: List[str] = [
    "borough",
    "weekday",
    "vehicle_1",
    "vehicle_2",
    "factor_1",
    "factor_2",
    "road_class",
]
NUMERIC_FEATURES: List[str] = [
    "hour",
    "month",
    "vehicle_count",
    "at_intersection",
]
FEATURES: List[str] = CATEGORICAL_FEATURES + NUMERIC_FEATURES

TARGET = "injured"

# Stand-ins for a field the source left blank. Kept as named categories
# rather than nulls: a crash with no second vehicle is a single-vehicle
# crash, which is a fact about it, not a gap in the record.
UNKNOWN = "Unknown"
NONE_RECORDED = "None recorded"

# scripts/build_dataset.py fills the boroughs the source leaves blank by
# placing each crash on a coordinate grid. Using it takes the share of
# crashes whose borough reads "Unknown" from 21% to 0.8%, which barely moves
# the model (ROC-AUC 0.7943 to 0.7946) but turns a category nobody can
# explain into a footnote.
BOROUGH_RESOLVED = "borough_resolved"

WEEKDAYS: List[str] = [
    "Monday", "Tuesday", "Wednesday", "Thursday",
    "Friday", "Saturday", "Sunday",
]

# Ordered: the first pattern that matches wins, so the narrow cases come
# before the broad ones. "E-BIKE" before "BIKE", "FIRE TRUCK" and
# "PICK-UP TRUCK" before "TRUCK", "MOTORBIKE" before "BIKE".
_VEHICLE_RULES = [
    (r"E-?BIKE|ELECTRIC BIKE", "E-bike"),
    (r"E-?SCOOTER|\bE-SCO", "E-scooter"),
    (r"MOTORCYCLE|MOTORBIKE|MOTORSCOOTER|MOTOR SCOOTER|MOPED|SCOOTER", "Motorcycle or moped"),
    (r"\bBIKE\b|BICYCLE|\bBICY", "Bicycle"),
    (r"AMBUL|FIRE|\bEMS\b", "Emergency vehicle"),
    (r"\bBUS\b", "Bus"),
    (r"TAXI|LIMO|LIVERY", "Taxi or livery"),
    (r"PICK-?UP|\bPK\b", "Pickup truck"),
    (
        r"TRUCK|TRACTOR|\bDUMP\b|FLAT ?BED|FLAT ?RACK|GARBAGE|REFUSE|TANKER"
        r"|MIXER|CHASSIS|TRAILER|\bDELV\b|\bTOW\b|\bLARGE COM",
        "Truck",
    ),
    (r"\bVAN\b|CARRY ALL|\bCARRYALL", "Van"),
    (r"STATION WAGON|SPORT UTILITY|\bSUBN\b|\bSUV\b", "SUV or station wagon"),
    (r"SEDAN|CONVERTIBLE|\d ?-? ?DR\b|\d-DOOR|\bCAR\b", "Car"),
]

# Ordered the same way: a name matching more than one class takes the
# heavier road. "3 AVENUE BRIDGE" is a bridge, not an avenue.
_ROAD_RULES = [
    (r"EXPRESSWAY|EXPWY|\bEXPY\b|HIGHWAY|\bHWY\b|\bBQE\b|\bRAMP\b", "Expressway or ramp"),
    (r"PARKWAY|\bPKWY\b|\bPKY\b", "Parkway"),
    (r"BRIDGE|TUNNEL", "Bridge or tunnel"),
    (r"BOULEVARD|\bBLVD\b", "Boulevard"),
    (r"AVENUE|\bAVE\b", "Avenue"),
    (r"STREET|\bST\b", "Street"),
    (r"\bROAD\b|\bRD\b", "Road"),
]

_OTHER_VEHICLE = "Other"
_OTHER_ROAD = "Other named road"
_OTHER_FACTOR = "Other"

# A category thinner than this is noise the model would overfit and a reader
# would not recognise, so it folds into "Other".
RARE_FACTOR_SHARE = 0.001

_VEHICLE_PATTERNS = [(re.compile(p), label) for p, label in _VEHICLE_RULES]
_ROAD_PATTERNS = [(re.compile(p), label) for p, label in _ROAD_RULES]


def _upper(series: pd.Series) -> pd.Series:
    """Upper-case and single-space a free-text column for matching."""
    return (
        series.astype("string")
        .str.upper()
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def _classify(series: pd.Series, patterns, other: str, missing: str) -> pd.Series:
    """Map free text onto a small named vocabulary.

    Args:
        series: The raw column.
        patterns: Ordered (compiled regex, label) pairs; first match wins.
        other: Label for a value that matches nothing.
        missing: Label for a value the source left blank.

    Returns:
        A string Series over the vocabulary.
    """
    text = _upper(series)
    out = pd.Series(other, index=series.index, dtype="object")

    unassigned = text.notna()
    for pattern, label in patterns:
        if not unassigned.any():
            break
        hit = unassigned & text.str.contains(pattern, regex=True, na=False)
        out[hit] = label
        unassigned &= ~hit

    out[text.isna()] = missing
    return out


def classify_vehicle(series: pd.Series, missing: str = NONE_RECORDED) -> pd.Series:
    """Collapse the source's 1,385 vehicle spellings into named classes."""
    return _classify(series, _VEHICLE_PATTERNS, _OTHER_VEHICLE, missing)


def classify_road(series: pd.Series) -> pd.Series:
    """Read the road type out of a street name.

    The source has no road-class field, but it is in the name: an expressway
    is called an expressway. This is the "street type where available" the
    model needs to tell a crash on the Cross Bronx from one on a side street.
    """
    return _classify(series, _ROAD_PATTERNS, _OTHER_ROAD, UNKNOWN)


def _collapse_rare(series: pd.Series, min_share: float = RARE_FACTOR_SHARE) -> pd.Series:
    """Fold categories below `min_share` of the column into "Other"."""
    share = series.value_counts(normalize=True)
    rare = set(share[share < min_share].index) - {NONE_RECORDED}
    return series.where(~series.isin(rare), _OTHER_FACTOR)


def label(df: pd.DataFrame) -> pd.Series:
    """The thing being predicted: did this crash hurt anyone.

    Args:
        df: Cleaned collision records.

    Returns:
        A boolean Series, True when at least one person was injured or killed.
    """
    # Coerced rather than filled directly: an all-null column arrives as
    # object dtype, and filling that downcasts with a deprecation warning.
    injured = pd.to_numeric(df["number_of_persons_injured"], errors="coerce").fillna(0)
    killed = pd.to_numeric(df["number_of_persons_killed"], errors="coerce").fillna(0)
    return ((injured > 0) | (killed > 0)).astype(bool)


def build(df: pd.DataFrame) -> pd.DataFrame:
    """Build the model's feature frame from cleaned collision records.

    Categorical columns come back as pandas `category` dtype, which is what
    HistGradientBoostingClassifier's native categorical support reads.

    Args:
        df: Cleaned collision records, with crash_datetime parsed.

    Returns:
        A frame of exactly FEATURES, indexed like `df`.
    """
    when = pd.to_datetime(df["crash_datetime"])
    vehicles = ["vehicle_type_code_1", "vehicle_type_code_2", "vehicle_type_code_3"]

    borough = BOROUGH_RESOLVED if BOROUGH_RESOLVED in df.columns else "borough"

    out = pd.DataFrame(index=df.index)
    out["borough"] = df[borough].fillna(UNKNOWN)
    out["weekday"] = pd.Categorical(
        when.dt.day_name(), categories=WEEKDAYS, ordered=False
    )
    out["hour"] = when.dt.hour.astype("int16")
    out["month"] = when.dt.month.astype("int8")

    # The first vehicle is the one the report is written around; the second
    # is what it hit, and its absence means a single-vehicle crash.
    out["vehicle_1"] = classify_vehicle(df["vehicle_type_code_1"], missing=UNKNOWN)
    out["vehicle_2"] = classify_vehicle(df["vehicle_type_code_2"])
    out["vehicle_count"] = df[vehicles].notna().sum(axis=1).astype("int8")

    out["factor_1"] = _collapse_rare(
        _upper(df["contributing_factor_vehicle_1"]).fillna(UNKNOWN).astype(object)
    )
    out["factor_2"] = _collapse_rare(
        _upper(df["contributing_factor_vehicle_2"]).fillna(NONE_RECORDED).astype(object)
    )

    out["road_class"] = classify_road(df["on_street_name"])
    # An intersection is on_street plus cross_street; the source records the
    # pair only when the crash happened at one.
    out["at_intersection"] = df["cross_street_name"].notna().astype("int8")

    for column in CATEGORICAL_FEATURES:
        out[column] = out[column].astype("category")

    return out[FEATURES]
