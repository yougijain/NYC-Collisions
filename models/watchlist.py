"""Turn per-crash risk into a list of intersections worth looking at.

A model that produces an AUC is a model nobody uses. The question an
engineer can act on is which junctions to send someone to, and the answer
this produces is deliberately narrow:

    at this intersection, more crashes injured someone than the crashes
    themselves account for

Not "this intersection is dangerous". A site can rank here because its
crashes involve e-bikes and mopeds rather than because the geometry is bad,
and it can stay off the list while injuring more people than anywhere else
in the city simply by having a lot of traffic. There is no exposure
denominator anywhere in this dataset. See docs/model_card.md.

## How a site earns its place

Every crash at the site is scored for the probability it injured someone,
using only the crash mix -- vehicles, factors, road type, hour -- and never
the location. The site's **excess** is what actually happened minus what
those crashes predicted:

    excess = observed injury rate - mean predicted injury rate

centred so the city as a whole nets to zero, which removes any residual
level bias in the model and makes the number a comparison between sites
rather than a claim about absolute risk.

Sites are then ranked not by that excess but by a **conservative lower bound
on it**, because a site with 25 crashes and an excess of 0.30 is a weaker
finding than one with 400 crashes and an excess of 0.15. A minimum crash
count on top of that keeps the thinnest denominators off the list entirely.

## Why the scores are rolled forward

Scoring a crash with a model that trained on it shrinks its residual, and
the residual is the entire product here. So predictions are generated
rolling-origin: each year is scored by a model trained on everything before
the previous year and calibrated on the previous one. Nothing on this list
was scored by a model that had seen it.

That costs the first two years -- 2020 has nothing before it and 2021 has
only 2020 -- which is no great loss, since those years sit in a different
reporting regime anyway.
"""

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from models import features as F
from models import injury_risk as IR

# Rolling-origin scoring needs a training year and a calibration year before
# the year it scores, so 2022 is the earliest that can be scored at all.
FIRST_SCORED_YEAR = 2022

# Below this a site's injury rate is an anecdote. At 25 crashes the 95%
# interval on a rate near 0.4 is still about +/- 0.19 wide, which is why
# ranking uses the lower bound rather than the point estimate.
MIN_SITE_CRASHES = 25

# Two-sided 95%.
Z_95 = 1.96

# A factor needs this many crashes behind it before its rate is worth
# printing next to the others.
MIN_FACTOR_CRASHES = 500

# Names nothing, and it is the single most common value in the column.
# Excluded from the per-site summary so the line a reader repeats back is
# about something.
UNINFORMATIVE_FACTOR = "UNSPECIFIED"

SITE_SEPARATOR = " @ "

# sql/00_bounds.sql builds the dashboard's borough filter with
# COALESCE(borough, 'UNKNOWN'), so the watchlist has to spell it the same
# way or those sites silently drop out of every selection.
UNKNOWN_BOROUGH = "UNKNOWN"

# Written by scripts/build_dataset.py over the whole dataset, filling the
# boroughs the source leaves blank.
BOROUGH_RESOLVED = "borough_resolved"


def site_key(df: pd.DataFrame) -> pd.Series:
    """Name each crash's intersection, or null if it was not at one.

    The two street names are sorted before joining, so a crash logged as
    "A and B" and one logged as "B and A" land on the same intersection
    rather than on two half-sized ones. That alone merges 94,433 apparent
    sites into 63,132 real ones.

    Args:
        df: Cleaned collision records.

    Returns:
        "STREET A @ STREET B" per row, null where the source recorded an
        address rather than a cross street.
    """
    on = df["on_street_name"]
    cross = df["cross_street_name"]
    at_intersection = on.notna() & cross.notna()

    pair = np.sort(
        np.stack([
            on.where(at_intersection, "").astype(str).to_numpy(),
            cross.where(at_intersection, "").astype(str).to_numpy(),
        ], axis=1),
        axis=1,
    )
    key = pd.Series(pair[:, 0] + SITE_SEPARATOR + pair[:, 1], index=df.index)
    return key.where(at_intersection)


def rolling_scores(
    df: pd.DataFrame,
    first_scored_year: int = FIRST_SCORED_YEAR,
    **overrides,
) -> Tuple[pd.DataFrame, List[Dict]]:
    """Score every crash with a model that never saw it.

    One fold per year: train on everything before the previous year,
    calibrate on the previous year, score the year itself. The baseline gets
    the same treatment, so the two are compared on equal information.

    Features are built once over the whole dataset and then sliced, which
    keeps one category vocabulary across every fold.

    Args:
        df: Cleaned collision records.
        first_scored_year: Earliest year to score. Anything before it comes
            back null, because there is not enough history behind it.
        **overrides: Model parameter overrides.

    Returns:
        A frame of `model` and `baseline` probabilities indexed like `df`,
        null for unscored rows, and one record per fold describing it.

    Raises:
        ValueError: If no year has enough history to be scored.
    """
    features = F.build(df)
    labels = F.label(df)
    year = pd.to_datetime(df["crash_datetime"]).dt.year

    scores = pd.DataFrame(
        {"model": np.nan, "baseline": np.nan}, index=df.index, dtype=float
    )
    folds: List[Dict] = []

    for target in range(first_scored_year, int(year.max()) + 1):
        train = year < target - 1
        calibrate = year == target - 1
        scored = year == target
        if not (train.any() and calibrate.any() and scored.any()):
            continue

        estimator = IR.train(features[train], labels[train], **overrides)
        shift = IR.LevelShift().fit(
            estimator.predict_proba(features[calibrate])[:, 1], labels[calibrate]
        )
        scores.loc[scored, "model"] = shift.transform(
            estimator.predict_proba(features[scored])[:, 1]
        )

        # The baseline may use the calibration year too: it is data from
        # before the scored year, so withholding it would handicap the
        # comparison rather than make it fair.
        known = train | calibrate
        baseline = IR.BoroughHourBaseline().fit(features[known], labels[known])
        scores.loc[scored, "baseline"] = baseline.predict_proba(features[scored])

        folds.append({
            "scored_year": target,
            "train_rows": int(train.sum()),
            "calibration_year": target - 1,
            "scored_rows": int(scored.sum()),
            "boosting_rounds": int(estimator.n_iter_),
            "level_shift": round(shift.shift_, 4),
        })

    if not folds:
        raise ValueError(
            f"No year from {first_scored_year} on has two years of history "
            f"behind it; the dataset spans {int(year.min())} to {int(year.max())}."
        )
    return scores, folds


def _modal(frame: pd.DataFrame, by: str, of: str) -> pd.Series:
    """The most common value of `of` within each `by`, ties broken by name."""
    counted = frame.value_counts([by, of]).reset_index(name="n")
    return (
        counted.sort_values([by, "n", of], ascending=[True, False, True])
        .drop_duplicates(by)
        .set_index(by)[of]
    )


def borough_column(df: pd.DataFrame) -> str:
    """Prefer the dataset's resolved borough, falling back to the raw one.

    scripts/build_dataset.py fills the 31% of crashes the source leaves
    blank by placing them on a coordinate grid, which is 99.9% accurate
    against held-out rows. An older build without that column still works,
    just with more sites coming back unknown.
    """
    return BOROUGH_RESOLVED if BOROUGH_RESOLVED in df.columns else "borough"


def _site_borough(
    df: pd.DataFrame, sites: pd.Series, index: pd.Index
) -> pd.Series:
    """The borough each site is in, by majority of its own crashes.

    This used to infer a borough from the site's street names when none of
    its crashes named one. The pipeline now does better upstream -- placing
    a crash by its coordinates is 99.9% accurate where reading its street
    name is 90% -- so there is nothing left to guess at here.

    Args:
        df: Cleaned collision records.
        sites: Site key per crash, aligned to `df`.
        index: The sites to resolve.

    Returns:
        Borough per site, null where none of its crashes name one.
    """
    column = borough_column(df)
    named = df.loc[sites.notna() & df[column].notna(), [column]]
    if named.empty:
        return pd.Series(index=index, dtype="object")

    modal = _modal(named.assign(site=sites.loc[named.index]), "site", column)
    return modal.reindex(index)


def _top_factors(factors: pd.Series, sites: pd.Series, keep: int = 2) -> pd.Series:
    """The most common named contributing factors at each site."""
    named = factors[factors.notna() & (factors != UNINFORMATIVE_FACTOR)]
    if named.empty:
        return pd.Series(dtype="object")

    counted = (
        pd.DataFrame({"site": sites.loc[named.index], "factor": named})
        .value_counts()
        .reset_index(name="crashes")
        .sort_values(["site", "crashes"], ascending=[True, False])
    )
    return (
        counted.groupby("site", sort=False)
        .head(keep)
        .groupby("site")["factor"]
        .agg(lambda names: "; ".join(name.title() for name in names))
    )


def rank_sites(
    df: pd.DataFrame,
    scores: pd.DataFrame,
    min_crashes: int = MIN_SITE_CRASHES,
) -> pd.DataFrame:
    """Rank intersections by injury risk their crash mix does not account for.

    Args:
        df: Cleaned collision records.
        scores: Output of `rolling_scores`.
        min_crashes: Sites with fewer scored crashes are dropped outright.

    Returns:
        One row per qualifying site, ordered by `excess_lower_95` descending:

        site, borough, crashes, observed_rate, expected_rate, base_rate,
        excess, excess_z, excess_lower_95, latitude, longitude, top_factors

        `excess` is the observed rate minus the predicted rate, centred so
        the scored population nets to zero. `excess_lower_95` is the
        conservative end of its 95% interval, and is what the order is by.
        `excess_z` is there so a reader can see how much of the ordering is
        signal: across several hundred sites, a handful will clear z = 1.96
        on chance alone, and this is a screening list rather than a test.
    """
    sites = site_key(df)
    usable = scores["model"].notna() & sites.notna()
    if not usable.any():
        return pd.DataFrame()

    injured = F.label(df)[usable].astype(float)
    expected = scores.loc[usable, "model"]

    frame = pd.DataFrame({
        "site": sites[usable],
        "injured": injured,
        "expected": expected,
        "base_rate": scores.loc[usable, "baseline"],
        # Variance of a sum of independent Bernoullis with differing
        # probabilities, which is what a site's crash count is.
        "variance": expected * (1 - expected),
        "latitude": df.loc[usable, "latitude"],
        "longitude": df.loc[usable, "longitude"],
    })

    # Any level bias left in the model lands on every site equally, so
    # removing the city-wide mean turns the number into a comparison
    # between sites instead of a claim about absolute risk.
    citywide_excess = frame["injured"].mean() - frame["expected"].mean()

    grouped = frame.groupby("site", sort=False).agg(
        crashes=("injured", "size"),
        observed_rate=("injured", "mean"),
        expected_rate=("expected", "mean"),
        base_rate=("base_rate", "mean"),
        total_variance=("variance", "sum"),
        latitude=("latitude", "median"),
        longitude=("longitude", "median"),
    )
    qualifying = grouped[grouped["crashes"] >= min_crashes].copy()
    if qualifying.empty:
        return pd.DataFrame()

    qualifying["excess"] = (
        qualifying["observed_rate"] - qualifying["expected_rate"] - citywide_excess
    )
    standard_error = np.sqrt(qualifying["total_variance"]) / qualifying["crashes"]
    qualifying["excess_z"] = qualifying["excess"] / standard_error
    qualifying["excess_lower_95"] = qualifying["excess"] - Z_95 * standard_error

    # Enrich only the survivors: the per-site mode and factor counts are the
    # slow part, and there are three orders of magnitude fewer of these.
    survivors = frame.index[frame["site"].isin(qualifying.index)]
    qualifying["borough"] = _site_borough(
        df, sites, qualifying.index
    ).fillna(UNKNOWN_BOROUGH)
    qualifying["top_factors"] = _top_factors(
        df.loc[survivors, "contributing_factor_vehicle_1"].str.upper().str.strip(),
        frame.loc[survivors, "site"],
    )
    qualifying["top_factors"] = qualifying["top_factors"].fillna("None recorded")

    columns = [
        "borough", "crashes", "observed_rate", "expected_rate", "base_rate",
        "excess", "excess_z", "excess_lower_95",
        "latitude", "longitude", "top_factors",
    ]
    return (
        qualifying[columns]
        .sort_values("excess_lower_95", ascending=False)
        .reset_index()
    )


def factor_risk(
    df: pd.DataFrame,
    scores: pd.DataFrame,
    min_crashes: int = MIN_FACTOR_CRASHES,
) -> pd.DataFrame:
    """Rank contributing factors by the injury risk the model gives them.

    This is the table a non-technical reader repeats back: crashes where the
    officer wrote down X injure someone Y% of the time.

    Args:
        df: Cleaned collision records.
        scores: Output of `rolling_scores`.
        min_crashes: Factors with fewer scored crashes are dropped.

    Returns:
        One row per factor -- crashes, predicted rate, observed rate --
        ordered by predicted rate descending.
    """
    scored = scores["model"].notna()
    if not scored.any():
        return pd.DataFrame()

    frame = pd.DataFrame({
        "factor": df.loc[scored, "contributing_factor_vehicle_1"]
        .str.upper().str.strip().str.title().fillna("Not recorded"),
        "predicted_rate": scores.loc[scored, "model"],
        "observed_rate": F.label(df)[scored].astype(float),
    })

    grouped = frame.groupby("factor", sort=False).agg(
        crashes=("observed_rate", "size"),
        predicted_rate=("predicted_rate", "mean"),
        observed_rate=("observed_rate", "mean"),
    )
    return (
        grouped[grouped["crashes"] >= min_crashes]
        .sort_values("predicted_rate", ascending=False)
        .reset_index()
    )


def summarise(watchlist: pd.DataFrame, scores: pd.DataFrame) -> Dict:
    """Headline numbers for the watchlist, for the dashboard and the docs."""
    scored = scores["model"].notna()
    if watchlist.empty:
        return {"sites": 0, "scored_crashes": int(scored.sum())}

    return {
        "sites": int(len(watchlist)),
        "scored_crashes": int(scored.sum()),
        "crashes_at_listed_sites": int(watchlist["crashes"].sum()),
        "worst_site": str(watchlist.iloc[0]["site"]),
        "worst_excess": round(float(watchlist.iloc[0]["excess"]), 4),
        "median_excess": round(float(watchlist["excess"].median()), 4),
        "top_decile_excess": round(float(watchlist["excess"].quantile(0.9)), 4),
        "sites_above_z_95": int((watchlist["excess_z"] > Z_95).sum()),
        # How many of those the multiple-comparison arithmetic expects for
        # free, given one two-sided test per site.
        "sites_above_z_95_by_chance": round(len(watchlist) * 0.025, 1),
    }


def build(
    df: pd.DataFrame,
    first_scored_year: int = FIRST_SCORED_YEAR,
    min_crashes: int = MIN_SITE_CRASHES,
    scores: Optional[pd.DataFrame] = None,
    **overrides,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """Score, rank and summarise in one call.

    Args:
        df: Cleaned collision records.
        first_scored_year: Earliest year to score.
        min_crashes: Minimum scored crashes for a site to be listed.
        scores: Precomputed scores, to skip refitting.
        **overrides: Model parameter overrides.

    Returns:
        The watchlist, the factor table, and a summary including the fold
        descriptions.
    """
    folds: List[Dict] = []
    if scores is None:
        scores, folds = rolling_scores(df, first_scored_year, **overrides)

    watchlist = rank_sites(df, scores, min_crashes)
    factors = factor_risk(df, scores)

    scored = scores["model"].notna()
    when = pd.to_datetime(df.loc[scored, "crash_datetime"])

    # Both counts, because the gap between them is the argument for keying
    # sites direction-free in the first place.
    at_intersection = df["on_street_name"].notna() & df["cross_street_name"].notna()
    directed = (
        df.loc[at_intersection, "on_street_name"]
        + SITE_SEPARATOR
        + df.loc[at_intersection, "cross_street_name"]
    ).nunique()

    summary = {
        "window": {"from": str(when.min().date()), "to": str(when.max().date())},
        "min_site_crashes": min_crashes,
        "intersections_directed": int(directed),
        "intersections_canonical": int(site_key(df).nunique()),
        "folds": folds,
        **summarise(watchlist, scores),
    }
    return watchlist, factors, summary
