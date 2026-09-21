"""Put a denominator under the watchlist.

Every rate in this project has been per *crash*: of the crashes here, how
many hurt somebody. That is a real question, but it is not the one a
traffic engineer asks. They ask how often a vehicle passing through this
junction ends up in a crash, and answering it needs to know how many
vehicles pass through -- which the collision data does not say.

NYC DOT's automated traffic volume counts say it, for some of the city.
This joins them on.

## What the counts are, and are not

Automated recorders go out on a street for a week or two, so a segment
either has a few days of readings or none at all. 3,391 segments carry
counts against tens of thousands of intersections, and a segment's most
recent count can be a decade old.

So this produces a second lens, not a replacement. The crash-mix residual
the watchlist already ranks by covers every site; this covers the ones
that happen to sit near a counter, and says which those are.

## Three things it does not measure

**A count is one approach, not the junction.** Traffic through an
intersection is the sum over every arm; a recorder sits on one segment.
The volume here is what passed the counter, and a junction fed by four
busy roads with a counter on the quietest one will look worse than it is.

**Most segments are counted in one direction.** 2,858 of 3,391 have a
single direction, so for a two-way street the figure is roughly half the
traffic. The direction count travels with every row rather than being
silently averaged away.

**The count is from one year, applied across the window.** Treating a
2016 count as 2024 traffic assumes the street did not change. The year
travels with every row too, so a reader can discount an old one.
"""

import re
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# NY State Plane Long Island, in US survey feet -- what DOT publishes
# wktgeom in -- and plain latitude/longitude, which everything else here
# speaks.
STATE_PLANE = "EPSG:2263"
WGS84 = "EPSG:4326"

_POINT = re.compile(r"POINT\s*\(\s*(-?[\d.]+)\s+(-?[\d.]+)\s*\)", re.IGNORECASE)

# Roughly one long New York block. Beyond this the nearest counter stops
# being about the junction: at 150m, 95% of matches still name one of the
# site's own streets, and by 300m that has fallen to 90% while the match
# quality keeps dropping.
MATCH_RADIUS_M = 150.0

# A day of readings only counts if all 24 hours are there. Recorders are
# pulled mid-day, and a partial day summed as a whole one understates
# traffic badly.
REQUIRED_HOURS = 24

# Every hour present is not the same as every bin present: 376 segment-days
# carry exactly one fifteen-minute reading per hour, which sums to a
# quarter of the traffic and passed an hours-only check. Recorders bin by
# ten minutes or by fifteen depending on the campaign, so the expected
# count is derived per day rather than assumed.
MIN_BIN_COVERAGE = 0.9

# A recorder that logged nothing all day was broken, not parked on an
# empty street.
MIN_DAILY_VOLUME = 1

# Below this a per-vehicle rate is division by almost nothing, and one
# extra crash swings it by an order of magnitude. It is also the strongest
# signal left that a match is wrong: a junction carrying 25 crashes a year
# is not fed by a road with 200 vehicles a day, so the counter belongs to
# some side street that happens to be nearby.
MIN_VEHICLES_PER_DAY = 500

# Local planar approximation. Over a few hundred metres in one city this
# is accurate to centimetres, and it avoids a geodesic library in the
# inner loop of a nearest-neighbour search.
METRES_PER_DEGREE_LAT = 111_320.0
NYC_LATITUDE = 40.73

SITE_SEPARATOR = " @ "

# DOT writes street names as abbreviations, and its `street` field is
# free text describing where the recorder sat -- "RALPH AVENUE SOUTH O
# CLARENDON ROAD", not "Ralph Avenue". Both sides get flattened to the
# same vocabulary before they are compared.
_ABBREVIATIONS: Dict[str, str] = {
    "AV": "AVENUE", "AVE": "AVENUE", "ST": "STREET", "STR": "STREET",
    "PL": "PLACE", "BLVD": "BOULEVARD", "BL": "BOULEVARD", "BLV": "BOULEVARD",
    "RD": "ROAD", "DR": "DRIVE", "PKWY": "PARKWAY", "PKY": "PARKWAY",
    "PWY": "PARKWAY", "PARKWY": "PARKWAY", "EXPY": "EXPRESSWAY",
    "EXPWY": "EXPRESSWAY", "EXWY": "EXPRESSWAY", "EXP": "EXPRESSWAY",
    "EP": "EXPRESSWAY", "LN": "LANE", "LA": "LANE", "CT": "COURT",
    "TER": "TERRACE", "TERR": "TERRACE", "HWY": "HIGHWAY", "BRG": "BRIDGE",
    "BR": "BRIDGE", "SQ": "SQUARE", "PLZ": "PLAZA", "CIR": "CIRCLE",
    "TPKE": "TURNPIKE", "CONC": "CONCOURSE",
}
_ORDINAL = re.compile(r"^(\d+)(ST|ND|RD|TH)$")


def normalise_name(name: object) -> str:
    """Flatten a street name to a common vocabulary.

    Expands DOT's abbreviations and drops ordinal suffixes, so "E 165TH
    ST" and "EAST 165 STREET" come out the same.

    Args:
        name: A street name, or anything else.

    Returns:
        The normalised name, empty for a non-string.
    """
    if not isinstance(name, str):
        return ""
    words = []
    for word in re.sub(r"[^A-Z0-9 ]", " ", name.upper()).split():
        ordinal = _ORDINAL.match(word)
        words.append(ordinal.group(1) if ordinal else _ABBREVIATIONS.get(word, word))
    return " ".join(words)


def count_points(counts: pd.DataFrame) -> pd.DataFrame:
    """Locate each counted segment in latitude and longitude.

    Args:
        counts: Raw rows from the DOT dataset.

    Returns:
        One row per distinct recorder position: segment_id, latitude,
        longitude, and the normalised text describing where it sat.

    Raises:
        ImportError: If pyproj is missing. It converts State Plane feet to
            degrees and is only needed when building exposure, which is
            why it lives in requirements-ml.txt rather than the app's.
    """
    try:
        import pyproj
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise ImportError(
            "Building exposure needs pyproj to convert DOT's State Plane "
            "coordinates. Install requirements-ml.txt."
        ) from exc

    points = counts.drop_duplicates("wktgeom").copy()
    parsed = points["wktgeom"].str.extract(_POINT)
    usable = parsed[0].notna()

    points = points[usable].copy()
    transformer = pyproj.Transformer.from_crs(STATE_PLANE, WGS84, always_xy=True)
    longitude, latitude = transformer.transform(
        parsed.loc[usable, 0].astype(float).to_numpy(),
        parsed.loc[usable, 1].astype(float).to_numpy(),
    )
    points["longitude"], points["latitude"] = longitude, latitude

    # The location text names the street the recorder was on and usually
    # the two it sat between, which is what makes a match checkable.
    points["described"] = (
        points["street"].fillna("") + " "
        + points.get("fromst", pd.Series("", index=points.index)).fillna("") + " "
        + points.get("tost", pd.Series("", index=points.index)).fillna("")
    ).map(normalise_name)

    return points[["segmentid", "latitude", "longitude", "described"]].rename(
        columns={"segmentid": "segment_id"}
    )


def daily_volumes(counts: pd.DataFrame) -> pd.DataFrame:
    """Estimate vehicles per day for each counted segment.

    Sums each complete day of readings, averages those days within the
    segment's most recent counted year, and adds the directions together.
    Summing is correct whether the recorder binned by ten minutes or by
    fifteen, since every row is the vehicles in its own bin.

    Args:
        counts: Raw rows from the DOT dataset.

    Returns:
        One row per segment: vehicles_per_day, the directions and days
        behind it, and the year it was counted.
    """
    numbers = counts.copy()
    for column in ("yr", "m", "d", "hh", "vol"):
        numbers[column] = pd.to_numeric(numbers[column], errors="coerce")
    numbers = numbers.dropna(subset=["yr", "m", "d", "hh", "vol", "segmentid"])

    by_day = numbers.groupby(
        ["segmentid", "direction", "yr", "m", "d"], observed=True
    ).agg(
        volume=("vol", "sum"),
        hours=("hh", "nunique"),
        readings=("vol", "size"),
        bins_per_hour=("mm", "nunique"),
    )
    # Expected readings for the interval this campaign actually used.
    expected = REQUIRED_HOURS * by_day["bins_per_hour"].clip(lower=1)
    complete = by_day[
        (by_day["hours"] >= REQUIRED_HOURS)
        & (by_day["readings"] >= MIN_BIN_COVERAGE * expected)
        & (by_day["volume"] >= MIN_DAILY_VOLUME)
    ].reset_index()
    if complete.empty:
        return pd.DataFrame()

    # Only the most recent year a direction was counted; older campaigns
    # describe a street that may no longer exist.
    latest = complete.groupby(["segmentid", "direction"])["yr"].transform("max")
    recent = complete[complete["yr"] == latest]

    per_direction = recent.groupby(["segmentid", "direction"], observed=True).agg(
        vehicles_per_day=("volume", "mean"),
        days=("volume", "size"),
        counted_year=("yr", "max"),
    )
    return per_direction.groupby("segmentid").agg(
        vehicles_per_day=("vehicles_per_day", "sum"),
        directions_counted=("vehicles_per_day", "size"),
        count_days=("days", "sum"),
        counted_year=("counted_year", "max"),
    ).reset_index().rename(columns={"segmentid": "segment_id"})


def _planar(frame: pd.DataFrame) -> np.ndarray:
    """Project degrees onto a local metre grid for distance work."""
    metres_per_degree_lon = METRES_PER_DEGREE_LAT * np.cos(np.radians(NYC_LATITUDE))
    return np.c_[
        frame["latitude"].to_numpy() * METRES_PER_DEGREE_LAT,
        frame["longitude"].to_numpy() * metres_per_degree_lon,
    ]


def match_sites(
    sites: pd.DataFrame,
    points: pd.DataFrame,
    radius_m: float = MATCH_RADIUS_M,
    candidates: int = 15,
) -> pd.DataFrame:
    """Attach each site to the nearest counter that is actually on it.

    Proximity alone is not a match: a recorder 150m away on a different
    road measures different traffic. A candidate only counts if its
    location text names one of the site's own two streets, which rules
    out the parallel street one block over.

    Args:
        sites: Watchlist rows carrying `site`, latitude and longitude.
        points: Output of `count_points`.
        radius_m: How far a counter may sit from the junction.
        candidates: Nearest neighbours to consider per site.

    Returns:
        One row per matched site: site, segment_id, metres, and whether
        the counter's description named one or both of its streets.
    """
    from scipy.spatial import cKDTree

    located = sites.dropna(subset=["latitude", "longitude"])
    if located.empty or points.empty:
        return pd.DataFrame()

    tree = cKDTree(_planar(points))
    neighbours = min(candidates, len(points))
    distances, indices = tree.query(_planar(located), k=neighbours, workers=-1)
    distances = np.atleast_2d(distances.T).T
    indices = np.atleast_2d(indices.T).T

    halves = located["site"].str.split(SITE_SEPARATOR, regex=False)
    first = [normalise_name(pair[0]) for pair in halves]
    second = [normalise_name(pair[-1]) for pair in halves]

    matches: List[Dict] = []
    for row, (row_distances, row_indices) in enumerate(zip(distances, indices)):
        street_a, street_b = first[row], second[row]
        for metres, index in zip(row_distances, row_indices):
            if metres > radius_m:
                break
            described = points["described"].iat[index]
            names_a = bool(street_a) and street_a in described
            names_b = bool(street_b) and street_b in described
            if not (names_a or names_b):
                continue
            matches.append({
                "site": located["site"].iat[row],
                "segment_id": points["segment_id"].iat[index],
                "metres": round(float(metres), 1),
                "names_both_streets": bool(names_a and names_b),
            })
            break  # the nearest qualifying counter wins

    return pd.DataFrame(matches)


def exposure_rates(
    sites: pd.DataFrame,
    matches: pd.DataFrame,
    volumes: pd.DataFrame,
    window_days: float,
) -> pd.DataFrame:
    """Express each matched site's risk per vehicle instead of per crash.

    Args:
        sites: Watchlist rows, carrying `site`, `crashes` and
            `observed_rate`.
        matches: Output of `match_sites`.
        volumes: Output of `daily_volumes`.
        window_days: Days the crash counts were accumulated over.

    Returns:
        The matched sites with vehicles_per_day, the millions of vehicles
        behind the window, and crash and injury-crash rates per million.

    Raises:
        ValueError: If `window_days` is not positive.
    """
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if matches.empty or volumes.empty:
        return pd.DataFrame()

    joined = (
        sites.merge(matches, on="site", how="inner")
        .merge(volumes, on="segment_id", how="inner")
    )
    # Too small a denominator is no denominator: see MIN_VEHICLES_PER_DAY.
    joined = joined[joined["vehicles_per_day"] >= MIN_VEHICLES_PER_DAY].copy()
    if joined.empty:
        return pd.DataFrame()

    joined["million_vehicles"] = (
        joined["vehicles_per_day"] * window_days / 1_000_000
    )
    joined["crashes_per_million"] = joined["crashes"] / joined["million_vehicles"]
    joined["harmful_per_million"] = (
        joined["crashes"] * joined["observed_rate"] / joined["million_vehicles"]
    )
    return joined.sort_values("harmful_per_million", ascending=False).reset_index(
        drop=True
    )


def coverage(sites: pd.DataFrame, rates: pd.DataFrame) -> Dict:
    """How much of the watchlist got a denominator, and how good it is."""
    located = int(sites["latitude"].notna().sum())
    if rates.empty:
        return {"sites": int(len(sites)), "sites_located": located, "matched": 0}

    return {
        "sites": int(len(sites)),
        "sites_located": located,
        "matched": int(len(rates)),
        "matched_share": round(len(rates) / located, 4) if located else 0.0,
        "named_both_streets": int(rates["names_both_streets"].sum()),
        "median_metres": round(float(rates["metres"].median()), 1),
        "median_vehicles_per_day": int(rates["vehicles_per_day"].median()),
        "single_direction_counts": int((rates["directions_counted"] == 1).sum()),
        "median_count_age_years": int(
            pd.Timestamp.now().year - rates["counted_year"].median()
        ),
        "oldest_count_year": int(rates["counted_year"].min()),
    }
