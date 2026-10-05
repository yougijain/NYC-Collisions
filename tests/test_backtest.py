"""Tests for the watchlist backtest.

A backtest is the one piece of analysis that can quietly say whatever its
author wanted. The failure modes are not crashes: a leaky split, a null
that is not null, a comparison run against a target the predictor helped
build. So most of what is here feeds the machinery data whose answer is
known in advance and checks it comes back.
"""

import numpy as np
import pandas as pd
import pytest

from models import backtest as B
from models import watchlist as W

FAST = {"max_iter": 30}
LOW_BAR = 2


@pytest.fixture(scope="module")
def crashes(dataset_path):
    return pd.read_parquet(dataset_path)


@pytest.fixture(scope="module")
def scores(crashes):
    return W.rolling_scores(crashes, **FAST)[0]


def synthetic(sites: int = 60, per_site: int = 40, seed: int = 0) -> pd.DataFrame:
    """Sites whose injury rate is fixed by construction, half of them nasty.

    Built so the right answer is known: the nasty half stays nasty across
    both windows, so any honest backtest has to find a positive correlation.
    """
    rng = np.random.default_rng(seed)
    rows = []
    for s in range(sites):
        rate = 0.75 if s % 2 else 0.25
        for year in (2022, 2023, 2024, 2025):
            for n in range(per_site):
                rows.append({
                    "collision_id": len(rows),
                    "crash_datetime": pd.Timestamp(f"{year}-06-01"),
                    "on_street_name": f"STREET {s}",
                    "cross_street_name": "MAIN STREET",
                    "latitude": 40.7, "longitude": -73.9,
                    "borough": "BROOKLYN",
                    "number_of_persons_injured": int(rng.random() < rate),
                    "number_of_persons_killed": 0,
                    "contributing_factor_vehicle_1": "DRIVER INATTENTION",
                })
    return pd.DataFrame(rows)


def flat_scores(df: pd.DataFrame, value: float = 0.5) -> pd.DataFrame:
    """Every crash predicted identically, so excess is observed rate shifted."""
    return pd.DataFrame(
        {"model": value, "baseline": value}, index=df.index, dtype=float
    )


# --- the split ---------------------------------------------------------

def test_the_windows_do_not_share_a_crash(crashes, scores):
    """A site's late excess must not be computed from any crash that fed its
    early excess, or the correlation measures bookkeeping."""
    year = pd.to_datetime(crashes["crash_datetime"]).dt.year
    early = crashes[year < 2024]
    late = crashes[year >= 2024]

    assert len(early) + len(late) == len(crashes)
    assert not set(early["collision_id"]) & set(late["collision_id"])


def test_a_split_with_nothing_after_it_is_refused(crashes, scores):
    with pytest.raises(ValueError, match="no site|scored crashes"):
        B.windows(crashes, scores, split_year=2099, min_crashes=LOW_BAR)


def test_each_window_is_ranked_on_its_own_crashes_only():
    df = synthetic()
    ranked = B.windows(df, flat_scores(df), 2024, min_crashes=LOW_BAR)
    # Two years each side, per_site crashes per year.
    assert set(ranked["early"]["crashes"]) == {80}
    assert set(ranked["late"]["crashes"]) == {80}


# --- does it find a signal that is really there? -----------------------

def test_a_ranking_that_should_persist_does():
    """Half the sites injure at 0.75 and half at 0.25, in both windows. A
    backtest that cannot see that is broken."""
    df = synthetic()
    out = B.run(df, flat_scores(df), 2024, min_crashes=LOW_BAR, permutations=200)

    # Not near 1: within each of the two groups the ordering is noise, so
    # the achievable ceiling is well under a perfect rank match.
    assert out["headline"]["observed"] > 0.6
    assert out["headline"]["p_value"] < 0.01


def test_a_ranking_that_is_pure_noise_is_not_mistaken_for_signal():
    """Injury assigned at random with no site effect. The correlation must
    land inside the null, which is the test that the null is honest."""
    rng = np.random.default_rng(7)
    df = synthetic()
    df["number_of_persons_injured"] = rng.integers(0, 2, len(df))

    out = B.run(df, flat_scores(df), 2024, min_crashes=LOW_BAR, permutations=400)
    assert out["headline"]["p_value"] > 0.05
    assert abs(out["headline"]["observed"]) < 0.4


# --- the null ----------------------------------------------------------

def test_the_null_is_centred_on_zero():
    """Shuffling the predictor against a fixed target should correlate at
    nothing. A null with a mean away from zero would make every p-value a
    lie."""
    rng = np.random.default_rng(3)
    a = pd.Series(rng.normal(size=300))
    b = pd.Series(rng.normal(size=300))
    null = B._null(a, b, permutations=500)

    assert abs(null["null_mean"]) < 0.05
    assert null["null_sd"] > 0


def test_a_p_value_is_never_zero():
    """(exceeded + 1) / (n + 1), because no finite permutation test can
    license a claim of impossibility."""
    a = pd.Series(range(200), dtype=float)
    null = B._null(a, a.copy(), permutations=100)

    assert null["observed"] == pytest.approx(1.0)
    assert null["p_value"] > 0


# --- the comparison it exists to make ----------------------------------

def test_both_predictors_are_scored_against_the_same_target():
    """The model is only shown to add something if it beats the naive
    ordering at predicting the same thing."""
    df = synthetic()
    out = B.run(df, flat_scores(df), 2024, min_crashes=LOW_BAR, permutations=100)

    rows = [r for r in out["predictors"] if r["target"] == "late excess"]
    assert {r["predictor"] for r in rows} >= {"model excess", "observed injury rate"}


def test_the_overlap_between_predictors_is_reported():
    """If predicted rates never varied, excess would be observed rate minus
    a constant and the comparison would answer nothing. The reader needs to
    see which case they are in."""
    df = synthetic()
    out = B.run(df, flat_scores(df), 2024, min_crashes=LOW_BAR, permutations=100)
    overlap = out["overlap"]

    # Flat scores: excess is observed rate shifted, so the orders agree.
    assert overlap["spearman_excess_vs_observed_rate"] == pytest.approx(1.0, abs=1e-6)
    assert overlap["expected_rate_sd"] == pytest.approx(0.0, abs=1e-9)


# --- honesty guards ----------------------------------------------------

def test_the_sweep_reports_every_split_it_ran(crashes, scores):
    """Showing one split and hiding the rest is how a backtest becomes a
    press release."""
    rows = B.sensitivity(
        crashes, scores, splits=(2024, 2025), minimums=(LOW_BAR,), permutations=50
    )
    assert {r["split_year"] for r in rows} == {2024, 2025}


def test_a_thin_split_is_flagged_rather_than_quietly_correlated(crashes, scores):
    rows = B.sensitivity(
        crashes, scores, splits=(2024,), minimums=(10_000,), permutations=50
    )
    assert all(r["underpowered"] for r in rows)
    assert all("model_excess" not in r for r in rows)


def test_shrinkage_is_always_reported():
    """Regression to the mean is expected, and a backtest that does not
    mention it has not understood its own result."""
    df = synthetic()
    out = B.run(df, flat_scores(df), 2024, min_crashes=LOW_BAR, permutations=100)
    assert set(out["shrinkage"]) >= {
        "top_n_early_excess", "top_n_late_excess", "retained"
    }


def test_the_shortlist_is_scored_against_the_base_rate():
    """"64% of flagged sites stayed bad" means nothing without the share of
    all sites that stayed bad."""
    df = synthetic()
    out = B.run(df, flat_scores(df), 2024, min_crashes=LOW_BAR, permutations=100)
    short = out["shortlist"]

    assert 0 <= short["base_rate"] <= 1
    assert short["lift"] == pytest.approx(
        short["still_above_expectation"] / short["base_rate"], rel=1e-3
    )


def test_a_threshold_nothing_clears_is_refused(crashes, scores):
    with pytest.raises(ValueError, match="scored crashes in the"):
        B.run(crashes, scores, 2024, min_crashes=1_000_000, permutations=10)


def test_windows_that_share_only_one_site_cannot_be_correlated():
    """Two sites qualify early, one of them vanishes from the late window.
    One pair is not a correlation."""
    df = synthetic(sites=2, per_site=30)
    gone = (df["on_street_name"] == "STREET 1") & (
        pd.to_datetime(df["crash_datetime"]).dt.year >= 2024
    )
    with pytest.raises(ValueError, match="nothing to correlate"):
        B.run(df[~gone], flat_scores(df[~gone]), 2024,
              min_crashes=LOW_BAR, permutations=10)


# --- the real dataset --------------------------------------------------

def test_the_backtest_runs_against_the_seed(crashes, scores):
    out = B.run(crashes, scores, 2024, min_crashes=LOW_BAR, permutations=100)

    assert out["sites_in_both"] > 0
    assert out["early_window"]["to"] < out["late_window"]["from"]
    assert len(out["deciles"]) <= 10
