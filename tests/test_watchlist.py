"""Tests for the site key, the rolling scores and the ranking.

The committed seed is a twentieth of the full dataset, so no intersection in
it has anything like the production minimum crash count. These tests lower
that threshold rather than asserting on the shape of the real watchlist.
"""

import numpy as np
import pandas as pd
import pytest

from models import watchlist as W

# Small folds and short fits: five models get trained per scoring run.
FAST = {"max_iter": 30}
LOW_BAR = 2


@pytest.fixture(scope="module")
def crashes(dataset_path):
    return pd.read_parquet(dataset_path)


@pytest.fixture(scope="module")
def scores(crashes):
    return W.rolling_scores(crashes, **FAST)


@pytest.fixture(scope="module")
def sites(crashes, scores):
    return W.rank_sites(crashes, scores[0], min_crashes=LOW_BAR)


def frame(**columns) -> pd.DataFrame:
    return pd.DataFrame(columns)


# --- the site key ------------------------------------------------------

def test_the_same_junction_written_either_way_is_one_site():
    """Half the crashes at a junction name the streets in the other order,
    which would otherwise split it into two half-sized sites."""
    df = frame(
        on_street_name=["ATLANTIC AVENUE", "LOGAN STREET"],
        cross_street_name=["LOGAN STREET", "ATLANTIC AVENUE"],
    )
    keys = W.site_key(df)
    assert keys.iloc[0] == keys.iloc[1] == "ATLANTIC AVENUE @ LOGAN STREET"


def test_a_crash_recorded_at_an_address_has_no_site():
    df = frame(
        on_street_name=[None, "ATLANTIC AVENUE"],
        cross_street_name=["LOGAN STREET", None],
    )
    assert W.site_key(df).isna().all()


def test_real_crashes_split_into_sites_and_addresses(crashes):
    keys = W.site_key(crashes)
    at_intersection = (
        crashes["on_street_name"].notna() & crashes["cross_street_name"].notna()
    )
    assert (keys.notna() == at_intersection).all()
    assert keys.nunique() < at_intersection.sum()


# --- rolling scores ----------------------------------------------------

def test_nothing_before_the_first_scorable_year_is_scored(crashes, scores):
    """2020 has no history behind it and 2021 has only one year."""
    values, _ = scores
    year = pd.to_datetime(crashes["crash_datetime"]).dt.year
    assert values.loc[year < W.FIRST_SCORED_YEAR, "model"].isna().all()
    assert values.loc[year >= W.FIRST_SCORED_YEAR, "model"].notna().all()


def test_every_fold_trains_only_on_its_past(scores):
    _, folds = scores
    for fold in folds:
        assert fold["calibration_year"] == fold["scored_year"] - 1
        assert fold["train_rows"] > 0
        assert fold["scored_rows"] > 0


def test_the_folds_grow(scores):
    """Each year is scored by a model that has seen one more year than the
    last one did."""
    _, folds = scores
    rows = [fold["train_rows"] for fold in folds]
    assert rows == sorted(rows)
    assert len(set(rows)) == len(rows)


def test_both_predictors_are_scored_on_the_same_rows(scores):
    values, _ = scores
    assert (values["model"].notna() == values["baseline"].notna()).all()


def test_scores_are_probabilities(scores):
    values, _ = scores
    scored = values.dropna()
    assert ((scored >= 0) & (scored <= 1)).all().all()


def test_a_dataset_with_no_history_cannot_be_scored(crashes):
    one_year = crashes[pd.to_datetime(crashes["crash_datetime"]).dt.year == 2023]
    with pytest.raises(ValueError, match="history"):
        W.rolling_scores(one_year, **FAST)


# --- ranking -----------------------------------------------------------

def test_the_watchlist_has_the_columns_it_promises(sites):
    assert list(sites.columns) == [
        "site", "borough", "crashes", "observed_rate", "expected_rate",
        "base_rate", "excess", "excess_z", "excess_lower_95",
        "latitude", "longitude", "top_factors",
    ]


def test_thin_sites_are_dropped(crashes, scores):
    listed = W.rank_sites(crashes, scores[0], min_crashes=8)
    assert (listed["crashes"] >= 8).all()
    assert len(listed) < len(W.rank_sites(crashes, scores[0], min_crashes=LOW_BAR))


def test_an_impossible_threshold_returns_nothing(crashes, scores):
    assert W.rank_sites(crashes, scores[0], min_crashes=10**6).empty


def test_the_city_as_a_whole_nets_to_zero(crashes, scores):
    """Excess is a comparison between sites, so any level bias left in the
    model has to come out first."""
    values = scores[0]
    listed = W.rank_sites(crashes, values, min_crashes=1)
    weighted = (listed["excess"] * listed["crashes"]).sum() / listed["crashes"].sum()
    assert weighted == pytest.approx(0, abs=0.02)


def test_the_ranking_is_by_the_conservative_end(sites):
    assert sites["excess_lower_95"].is_monotonic_decreasing


def test_the_lower_bound_sits_below_the_estimate(sites):
    assert (sites["excess_lower_95"] <= sites["excess"]).all()


def test_a_thin_site_is_penalised_more_than_a_thick_one(crashes, scores):
    """Two sites with the same excess should not rank equally when one is
    built from four times as many crashes."""
    listed = W.rank_sites(crashes, scores[0], min_crashes=LOW_BAR)
    margin = listed["excess"] - listed["excess_lower_95"]
    assert margin.corr(listed["crashes"]) < 0


def test_observed_and_expected_are_rates(sites):
    for column in ("observed_rate", "expected_rate", "base_rate"):
        assert ((sites[column] >= 0) & (sites[column] <= 1)).all()


def test_every_site_gets_a_borough(sites):
    assert sites["borough"].notna().all()


def test_boroughs_are_spelled_the_way_the_dashboard_filters_them(sites):
    """sql/00_bounds.sql builds the filter with COALESCE(borough,'UNKNOWN'),
    so a differently-spelled unknown would silently vanish from every
    selection."""
    allowed = {
        "BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND",
        W.UNKNOWN_BOROUGH,
    }
    assert set(sites["borough"]) <= allowed


def test_a_site_inherits_the_borough_of_its_streets():
    """The Belt Parkway carries 9,534 crashes and a borough on 102 of them,
    so a site's own rows often do not say."""
    df = frame(
        borough=["BROOKLYN", None, None],
        on_street_name=["BELT PARKWAY", "BELT PARKWAY", "BELT PARKWAY"],
        cross_street_name=["OCEAN PARKWAY", "KNAPP STREET", "KNAPP STREET"],
    )
    sites = W.site_key(df)
    resolved = W._site_borough(df, sites, pd.Index(["BELT PARKWAY @ KNAPP STREET"]))
    assert resolved.iloc[0] == "BROOKLYN"


def test_top_factors_skip_the_one_that_names_nothing(sites):
    assert not sites["top_factors"].str.contains("Unspecified").any()


# --- factors -----------------------------------------------------------

def test_factors_are_ordered_by_predicted_risk(crashes, scores):
    factors = W.factor_risk(crashes, scores[0], min_crashes=1)
    assert factors["predicted_rate"].is_monotonic_decreasing


def test_thin_factors_are_dropped(crashes, scores):
    factors = W.factor_risk(crashes, scores[0], min_crashes=200)
    assert (factors["crashes"] >= 200).all()


def test_predicted_tracks_observed_across_factors(crashes, scores):
    """A per-factor calibration check: if these diverge, the model is
    telling a story about factors that the data does not support."""
    factors = W.factor_risk(crashes, scores[0], min_crashes=200)
    assert factors["predicted_rate"].corr(factors["observed_rate"]) > 0.9


# --- the whole thing ---------------------------------------------------

def test_build_returns_a_summary_that_matches_its_output(crashes):
    listed, factors, summary = W.build(crashes, min_crashes=LOW_BAR, **FAST)
    assert summary["sites"] == len(listed)
    assert summary["min_site_crashes"] == LOW_BAR
    assert summary["worst_site"] == listed.iloc[0]["site"]
    assert len(summary["folds"]) >= 1
    assert not factors.empty


def test_build_reuses_scores_it_is_handed(crashes, scores):
    """Scoring is five model fits; the dashboard build should not pay for
    them twice."""
    listed, _, summary = W.build(crashes, scores=scores[0], min_crashes=LOW_BAR)
    assert summary["folds"] == []
    assert len(listed) > 0


def test_the_build_script_writes_its_three_files(dataset_path, tmp_path):
    import build_watchlist

    summary = build_watchlist.run(
        dataset=dataset_path, min_crashes=LOW_BAR, output_dir=tmp_path
    )
    written = {path.name for path in tmp_path.iterdir()}
    assert written == {
        "injury_watchlist.csv",
        "injury_risk_factors.csv",
        "watchlist_summary.json",
    }
    assert summary["sites"] == len(pd.read_csv(tmp_path / "injury_watchlist.csv"))


def test_the_build_script_refuses_to_write_an_empty_watchlist(dataset_path, tmp_path):
    import build_watchlist

    with pytest.raises(RuntimeError, match="min-crashes"):
        build_watchlist.run(
            dataset=dataset_path, min_crashes=10**6, output_dir=tmp_path
        )


# --- what the dashboard reads ------------------------------------------

def test_the_committed_watchlist_is_readable():
    import db

    listed = db.load_watchlist()
    factors = db.load_factor_risk()
    summary = db.load_watchlist_summary()

    assert not listed.empty and not factors.empty
    assert summary["sites"] == len(listed)
    assert (listed["crashes"] >= summary["min_site_crashes"]).all()
    assert np.isfinite(listed["excess"]).all()
