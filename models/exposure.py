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
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from street_names import canonical as normalise_name  # noqa: E402

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

# Every hour present is not the same as every bin present. 240 segment-days
# have all 24 hours but only half their readings, which sums to half the
# traffic and passed an hours-only check.
#
# How many readings an hour should hold is a property of the campaign, not
# of the day: recorders bin by the hour, by fifteen minutes or by ten
# depending on which request they were deployed under. Deriving it per day
# cannot see this failure at all -- a day that lost half its bins looks
# exactly like a campaign that used half as many -- which is why the
# expectation comes from the requestid the readings were filed under.
MIN_BIN_COVERAGE = 0.9

# Readings filed under one deployment request. Every segment-day in a
# request was recorded at the same interval.
CAMPAIGN = "requestid"

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
# CLARENDON ROAD", not "Ralph Avenue". Both sides are flattened through
# scripts/street_names.py, the same vocabulary the crash records are keyed
# by, so a site and a counter that name one street agree on its spelling.
# Two tables would drift: DOT writes "E 165 ST" where the watchlist, after
# cleaning, holds "EAST 165 STREET", and a local table missing the
# directional expansion silently loses every directional street. Sharing
# one takes matched sites from 416 to 425, and the ones where the counter
# names both of the junction's streets from 175 to 196.


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
    if numbers.empty:
        return pd.DataFrame()

    # The interval each deployment recorded at, carried down to its rows.
    campaign = numbers.get(
        CAMPAIGN, pd.Series("", index=numbers.index)
    ).fillna("")
    numbers["bins_per_hour"] = campaign.map(
        numbers.groupby(campaign)["mm"].nunique()
    )

    by_day = numbers.groupby(
        ["segmentid", "direction", "yr", "m", "d"], observed=True
    ).agg(
        volume=("vol", "sum"),
        hours=("hh", "nunique"),
        readings=("vol", "size"),
        bins_per_hour=("bins_per_hour", "max"),
    )
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
    candidates: int = 25,
) -> pd.DataFrame:
    """Attach each site to the busiest counter that is actually on it.

    Two rules, and the second is the one that matters.

    A candidate only counts if its location text names one of the site's
    own two streets. Proximity alone is not a match: a recorder 150m away
    on the parallel street measures different traffic, and requiring the
    name keeps 95% of matches on the right road where distance alone
    manages 69%.

    Among those that qualify, the busiest wins rather than the nearest.
    Half of matched sites have more than one counter in range, and the
    nearest is as likely to sit on a service road as on the arterial --
    which is how Bruckner Boulevard came back at 1,312 vehicles a day and
    topped a per-vehicle ranking it has no business being on. The busiest
    approach is still an undercount of what crosses a junction, but it is
    an undercount that does not invert the order.

    Args:
        sites: Watchlist rows carrying `site`, latitude and longitude.
        points: Output of `count_points`, merged with `daily_volumes` so
            each carries `vehicles_per_day`.
        radius_m: How far a counter may sit from the junction.
        candidates: Nearest neighbours to consider per site.

    Returns:
        One row per matched site: site, segment_id, metres, how many
        counters qualified, and whether the chosen one named both streets.

    Raises:
        KeyError: If `points` carries no `vehicles_per_day`.
    """
    from scipy.spatial import cKDTree

    if not points.empty and "vehicles_per_day" not in points.columns:
        raise KeyError(
            "match_sites needs points merged with daily_volumes(): the "
            "busiest qualifying counter cannot be chosen without volumes."
        )

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
        qualifying = []
        for metres, index in zip(row_distances, row_indices):
            if metres > radius_m:
                break
            described = points["described"].iat[index]
            names_a = bool(street_a) and street_a in described
            names_b = bool(street_b) and street_b in described
            if names_a or names_b:
                qualifying.append((index, float(metres), names_a and names_b))

        if not qualifying:
            continue
        index, metres, both = max(
            qualifying, key=lambda c: points["vehicles_per_day"].iat[c[0]]
        )
        matches.append({
            "site": located["site"].iat[row],
            "segment_id": points["segment_id"].iat[index],
            "metres": round(metres, 1),
            "counters_in_range": len(qualifying),
            "names_both_streets": bool(both),
        })

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
    joined["confidence"] = _confidence(joined)
    return joined.sort_values(
        ["confidence", "harmful_per_million"], ascending=[True, False]
    ).reset_index(drop=True)


def _confidence(rates: pd.DataFrame) -> pd.Series:
    """How much weight a site's denominator can carry.

    A single-direction count on a two-way street measures roughly half
    the traffic, and nothing in this data says which streets are one-way.
    That is the difference between a rate worth quoting and a rate worth
    only glancing at, so it is on the row rather than in a footnote.
    """
    both_streets = rates["names_both_streets"]
    both_directions = rates["directions_counted"] >= 2
    return pd.Series(
        np.where(
            both_streets & both_directions, "high",
            np.where(both_streets | both_directions, "medium", "low"),
        ),
        index=rates.index,
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
        "by_confidence": rates["confidence"].value_counts().to_dict(),
        "median_harmful_per_million": round(
            float(rates["harmful_per_million"].median()), 3
        ),
        # The number the watchlist's caveat has only ever asserted: how
        # much the crash-mix ranking and a per-vehicle one actually agree.
        "spearman_excess_vs_per_vehicle": round(
            float(rates["excess"].corr(
                rates["harmful_per_million"], method="spearman"
            )), 3
        ),
    }
