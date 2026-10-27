"""Tests for partial pooling across sites.

Shrinkage is easy to get subtly wrong in ways that still produce plausible
numbers: a weight inverted, a variance where a standard deviation belongs.
So the checks here are against answers known in advance -- a spread built
in by construction, a hand-worked example, and the property the whole
exercise exists for, that pooled estimates sit closer to the truth.
"""

import numpy as np
import pandas as pd
import pytest

from models import shrinkage as SH


def simulated(true_sd: float, sites: int = 2000, seed: int = 0):
    """Sites with a known spread of true effects, observed with noise that
    varies the way crash counts make it vary."""
    rng = np.random.default_rng(seed)
    truth = rng.normal(0.0, true_sd, sites)
    crashes = rng.integers(25, 400, sites)
    variance = 0.25 / crashes
    estimate = truth + rng.normal(0.0, np.sqrt(variance))
    return pd.Series(truth), pd.Series(estimate), pd.Series(variance)


def test_a_built_in_spread_is_recovered():
    _, estimate, variance = simulated(true_sd=0.05)
    assert np.sqrt(SH.between_site_variance(estimate, variance)) == pytest.approx(
        0.05, abs=0.006
    )


def test_noise_alone_is_not_mistaken_for_a_spread():
    _, estimate, variance = simulated(true_sd=0.0)
    assert np.sqrt(SH.between_site_variance(estimate, variance)) < 0.01


def test_the_estimator_matches_a_hand_worked_example():
    """Three sites, all variances 1: weights are equal, Q is the plain sum
    of squares about the mean, and the scale is 3 - 3/3 = 2."""
    estimate = pd.Series([0.0, 2.0, 4.0])
    variance = pd.Series([1.0, 1.0, 1.0])
    # Q = 4 + 0 + 4 = 8; tau2 = (8 - 2) / 2 = 3.
    assert SH.between_site_variance(estimate, variance) == pytest.approx(3.0)


def test_pooled_estimates_sit_closer_to_the_truth():
    """The reason to do any of this."""
    truth, estimate, variance = simulated(true_sd=0.05)
    pooled = SH.shrink(estimate, variance)["shrunk"]

    raw_error = ((estimate - truth) ** 2).mean()
    pooled_error = ((pooled - truth) ** 2).mean()
    assert pooled_error < 0.7 * raw_error


def test_a_precise_site_keeps_more_of_itself_than_a_noisy_one():
    _, estimate, variance = simulated(true_sd=0.05)
    out = SH.shrink(estimate, variance)
    assert out["weight"].corr(variance, method="spearman") == pytest.approx(-1.0)


def test_every_estimate_moves_toward_the_mean_and_never_past_it():
    _, estimate, variance = simulated(true_sd=0.05)
    out = SH.shrink(estimate, variance)
    w = 1.0 / (variance + SH.between_site_variance(estimate, variance))
    mean = float(np.sum(w * estimate) / np.sum(w))

    assert ((out["shrunk"] - mean).abs() <= (estimate - mean).abs() + 1e-12).all()
    assert (np.sign(out["shrunk"] - mean) == np.sign(estimate - mean)).all()
    assert out["weight"].between(0, 1).all()


def test_with_no_spread_every_site_collapses_onto_the_mean():
    estimate = pd.Series([0.1, -0.1, 0.05, -0.05])
    variance = pd.Series([1.0, 1.0, 1.0, 1.0])
    out = SH.shrink(estimate, variance)
    assert (out["weight"] == 0).all()
    assert out["shrunk"].nunique() == 1


def test_the_lower_bound_sits_below_the_pooled_estimate():
    _, estimate, variance = simulated(true_sd=0.05)
    out = SH.shrink(estimate, variance)
    assert (out["shrunk_lower_95"] < out["shrunk"]).all()


def test_pooling_keeps_the_index_it_was_given():
    estimate = pd.Series([0.3, -0.2, 0.1], index=["A @ B", "C @ D", "E @ F"])
    out = SH.shrink(estimate, pd.Series([0.01, 0.02, 0.03], index=estimate.index))
    assert list(out.index) == ["A @ B", "C @ D", "E @ F"]


def test_one_site_has_no_spread_to_estimate():
    with pytest.raises(ValueError, match="two sites"):
        SH.between_site_variance(pd.Series([0.1]), pd.Series([0.01]))


def test_a_zero_variance_is_refused():
    with pytest.raises(ValueError, match="positive"):
        SH.between_site_variance(pd.Series([0.1, 0.2]), pd.Series([0.01, 0.0]))


def test_the_summary_reports_how_hard_it_pulled():
    _, estimate, variance = simulated(true_sd=0.05)
    found = SH.describe(estimate, variance)
    assert found["weight_min"] <= found["weight_median"] <= found["weight_max"]
    assert found["between_site_sd"] == pytest.approx(0.05, abs=0.006)
