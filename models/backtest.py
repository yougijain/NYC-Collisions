"""Does the watchlist find sites that stay bad, or sites that had a bad year?

Every number this project publishes about the watchlist is measured inside
one window. The rolling-origin scoring means no crash is scored by a model
that trained on it, and the z-scores say how many sites clear what chance
would produce. Neither answers the question a reader actually has, which is
whether a site flagged in 2023 was still worth flagging in 2025.

So: fit on an early window, look at a later one, and ask what the earlier
ranking predicted.

## Why this is a fair test

The model never sees where a crash happened -- no coordinates, no street
names, enforced by tests in test_features.py. A model trained on the early
window therefore carries no memory of which site is which, and the late
window's site-level residuals are not contaminated by the early window's.
Without that exclusion this comparison would be circular.

## Two things that would fool a careless version

**Regression to the mean.** A site flagged for a high residual will show a
smaller one next window from noise alone. Shrinkage is arithmetic, not
failure, so "the excess got smaller" proves nothing on its own.

**Persistence is not skill.** Busy junctions stay busy and dangerous-looking
junctions keep looking dangerous. The question is not whether the ranking
persists but whether it persists *better than the naive alternative*: just
sorting by observed injury rate and ignoring the model. If the crash-mix
adjustment cannot beat that, it is decoration on a sort.

Both are handled by comparing predictors against the same target, and by a
permutation null that breaks the site-to-score link.
"""

from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from models import watchlist as W

# Sites need enough crashes in *both* windows to be comparable, which is a
# stiffer bar than the published watchlist's 25 in one window. Twenty keeps
# a few hundred sites; twenty-five leaves too few to cut into deciles.
MIN_WINDOW_CRASHES = 20

# Shuffles behind the null. Enough to resolve a p-value at this sample size
# without making the build slow.
PERMUTATIONS = 2000
SEED = 20260101

# How many sites a person would actually be sent to look at.
TOP_N = 50


def windows(
    df: pd.DataFrame,
    scores: pd.DataFrame,
    split_year: int,
    min_crashes: int = MIN_WINDOW_CRASHES,
) -> Dict[str, pd.DataFrame]:
    """Rank sites separately in the window before and after `split_year`.

    Args:
        df: Cleaned collision records.
        scores: Output of `watchlist.rolling_scores`, aligned to `df`.
        split_year: First year of the later window.
        min_crashes: Minimum scored crashes per site, per window.

    Returns:
        `early` and `late` rankings, each as `rank_sites` returns them.

    Raises:
        ValueError: If either window has no qualifying site.
    """
    year = pd.to_datetime(df["crash_datetime"]).dt.year
    before, after = year < split_year, year >= split_year

    ranked = {}
    for name, mask in (("early", before), ("late", after)):
        ranked[name] = W.rank_sites(
            df[mask], scores[mask], min_crashes=min_crashes
        )
        if ranked[name].empty:
            raise ValueError(
                f"no site has {min_crashes} scored crashes in the {name} "
                f"window; lower min_crashes or move the split"
            )
    return ranked


def paired(early: pd.DataFrame, late: pd.DataFrame) -> pd.DataFrame:
    """The sites that qualify in both windows, with both windows' numbers."""
    return early.merge(late, on="site", suffixes=("_early", "_late"))


def _spearman(a: pd.Series, b: pd.Series) -> float:
    return float(a.corr(b, method="spearman"))


def _null(
    predictor: pd.Series,
    target: pd.Series,
    permutations: int = PERMUTATIONS,
    seed: int = SEED,
) -> Dict:
    """What correlation chance produces when the pairing is broken.

    Shuffling the predictor against a fixed target destroys the
    site-to-score link while preserving both marginal distributions, so
    anything the real correlation has above this spread is signal.
    """
    rng = np.random.default_rng(seed)
    observed = _spearman(predictor, target)
    values = predictor.to_numpy()
    draws = np.array([
        _spearman(pd.Series(rng.permutation(values)), target)
        for _ in range(permutations)
    ])
    # One-sided: the claim is that the ranking predicts, not that it differs.
    exceeded = int((draws >= observed).sum())
    return {
        "observed": round(observed, 4),
        "null_mean": round(float(draws.mean()), 4),
        "null_sd": round(float(draws.std(ddof=1)), 4),
        "null_p95": round(float(np.quantile(draws, 0.95)), 4),
        # (exceeded + 1) / (n + 1) never reports an impossible p of zero.
        "p_value": round((exceeded + 1) / (permutations + 1), 5),
        "permutations": permutations,
    }


def predictors(pair: pd.DataFrame) -> List[Dict]:
    """Rank each candidate ordering by how well it predicts the late window.

    Four orderings, two targets. The comparison that decides whether the
    model earns its place is `excess` against `observed_rate`, both
    predicting late excess: same target, same sites, one uses the crash-mix
    adjustment and the other does not.
    """
    rows = []
    for target_name, target in (
        ("late excess", pair["excess_late"]),
        ("late injury rate", pair["observed_rate_late"]),
    ):
        for label, column in (
            ("model excess", "excess_early"),
            ("model excess, lower 95%", "excess_lower_95_early"),
            ("observed injury rate", "observed_rate_early"),
            ("crash count", "crashes_early"),
        ):
            rows.append({
                "predictor": label,
                "target": target_name,
                "spearman": round(_spearman(pair[column], target), 4),
            })
    return rows


def deciles(pair: pd.DataFrame, bins: int = 10) -> List[Dict]:
    """Mean late excess for each decile of the early ranking.

    A monotone column here is the claim a reader can check by eye: sites the
    early window ranked worst stayed worst.
    """
    ranks = pair["excess_early"].rank(method="first")
    group = pd.qcut(ranks, bins, labels=False, duplicates="drop")
    out = []
    for index, rows in pair.groupby(group, sort=True):
        out.append({
            "decile": int(index) + 1,
            "sites": int(len(rows)),
            "early_excess": round(float(rows["excess_early"].mean()), 4),
            "late_excess": round(float(rows["excess_late"].mean()), 4),
            "late_injury_rate": round(float(rows["observed_rate_late"].mean()), 4),
        })
    return out


def shortlist(pair: pd.DataFrame, top_n: int = TOP_N) -> Dict:
    """What a person sent to the top of the early list would have found.

    Precision against the base rate, which is the form the question takes
    in practice: of the sites worth visiting, how many were still worth it?
    """
    top_n = min(top_n, len(pair))
    ordered = pair.sort_values("excess_lower_95_early", ascending=False)
    top = ordered.head(top_n)

    base = float((pair["excess_late"] > 0).mean())
    hit = float((top["excess_late"] > 0).mean())
    return {
        "top_n": top_n,
        "still_above_expectation": round(hit, 4),
        "base_rate": round(base, 4),
        "lift": round(hit / base, 3) if base else None,
        "mean_late_excess": round(float(top["excess_late"].mean()), 4),
        "mean_late_excess_rest": round(
            float(ordered.tail(len(pair) - top_n)["excess_late"].mean()), 4
        ),
    }


def shrinkage(pair: pd.DataFrame, top_n: int = TOP_N) -> Dict:
    """How much the early excess fades, which regression to the mean predicts.

    Reported rather than hidden: a reader who knows the effect exists will
    look for it, and a backtest that does not mention it has not understood
    its own result.
    """
    top = pair.sort_values("excess_lower_95_early", ascending=False).head(
        min(top_n, len(pair))
    )
    early, late = float(top["excess_early"].mean()), float(top["excess_late"].mean())
    return {
        "top_n_early_excess": round(early, 4),
        "top_n_late_excess": round(late, 4),
        "retained": round(late / early, 3) if early else None,
    }


# Splits and thresholds the sensitivity sweep walks. 2024 is the headline
# because it was chosen before the first run, not because of how it came
# out: reporting whichever split flatters the model is how a backtest
# becomes a press release.
SWEEP_SPLITS = (2023, 2024, 2025)
SWEEP_MINIMUMS = (15, 20, 25)
SWEEP_PERMUTATIONS = 500
SWEEP_MIN_SITES = 30


def sensitivity(
    df: pd.DataFrame,
    scores: pd.DataFrame,
    splits=SWEEP_SPLITS,
    minimums=SWEEP_MINIMUMS,
    permutations: int = SWEEP_PERMUTATIONS,
) -> List[Dict]:
    """Rerun the comparison across splits and thresholds.

    One split proves nothing on its own. A result that only holds at the
    window it was measured at is a property of that window, and the only
    way a reader can tell is to be shown the others.
    """
    rows = []
    for split in splits:
        for minimum in minimums:
            try:
                ranked = windows(df, scores, split, minimum)
            except ValueError:
                continue
            pair = paired(ranked["early"], ranked["late"])
            if len(pair) < SWEEP_MIN_SITES:
                rows.append({
                    "split_year": split, "min_window_crashes": minimum,
                    "sites": int(len(pair)), "underpowered": True,
                })
                continue
            rows.append({
                "split_year": split,
                "min_window_crashes": minimum,
                "sites": int(len(pair)),
                "model_excess": round(
                    _spearman(pair["excess_early"], pair["excess_late"]), 4),
                "observed_rate": round(
                    _spearman(pair["observed_rate_early"], pair["excess_late"]), 4),
                "p_value": _null(
                    pair["excess_early"], pair["excess_late"], permutations
                )["p_value"],
                "underpowered": False,
            })
    return rows


def overlap(pair: pd.DataFrame) -> Dict:
    """How far the crash-mix adjustment moves the ranking at all.

    If predicted rates barely varied between sites, excess would be
    observed rate minus a constant, the two predictors would be the same
    ordering, and comparing them would answer nothing.
    """
    return {
        "spearman_excess_vs_observed_rate": round(
            _spearman(pair["excess_early"], pair["observed_rate_early"]), 4),
        "expected_rate_sd": round(float(pair["expected_rate_early"].std()), 4),
        "expected_rate_min": round(float(pair["expected_rate_early"].min()), 4),
        "expected_rate_max": round(float(pair["expected_rate_early"].max()), 4),
        "observed_rate_sd": round(float(pair["observed_rate_early"].std()), 4),
    }


def run(
    df: pd.DataFrame,
    scores: pd.DataFrame,
    split_year: int,
    min_crashes: int = MIN_WINDOW_CRASHES,
    permutations: int = PERMUTATIONS,
) -> Dict:
    """Backtest the ranking across one split.

    Args:
        df: Cleaned collision records.
        scores: Output of `watchlist.rolling_scores`, aligned to `df`.
        split_year: First year of the later window.
        min_crashes: Minimum scored crashes per site, per window.
        permutations: Shuffles behind the null.

    Returns:
        Everything the report quotes, as plain JSON-serialisable values.
    """
    ranked = windows(df, scores, split_year, min_crashes)
    pair = paired(ranked["early"], ranked["late"])
    if len(pair) < 2:
        raise ValueError(
            f"only {len(pair)} site qualifies in both windows; nothing to "
            f"correlate"
        )

    year = pd.to_datetime(df["crash_datetime"]).dt.year
    scored = scores["model"].notna()

    return {
        "split_year": split_year,
        "min_window_crashes": min_crashes,
        "early_window": {
            "from": int(year[scored & (year < split_year)].min()),
            "to": int(year[scored & (year < split_year)].max()),
            "sites": int(len(ranked["early"])),
        },
        "late_window": {
            "from": int(year[scored & (year >= split_year)].min()),
            "to": int(year[scored & (year >= split_year)].max()),
            "sites": int(len(ranked["late"])),
        },
        "sites_in_both": int(len(pair)),
        "predictors": predictors(pair),
        "headline": _null(pair["excess_early"], pair["excess_late"], permutations),
        "naive_headline": _null(
            pair["observed_rate_early"], pair["excess_late"], permutations
        ),
        "overlap": overlap(pair),
        "deciles": deciles(pair),
        "shortlist": shortlist(pair),
        "shrinkage": shrinkage(pair),
        "sensitivity": sensitivity(df, scores),
    }
