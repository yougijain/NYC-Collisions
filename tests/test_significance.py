"""Tests for the false-discovery arithmetic behind the watchlist.

The Benjamini-Hochberg step-up is easy to write wrongly in a way that
produces plausible numbers: stopping at the first p-value above its
threshold instead of taking the largest rank below one is a different and
more conservative test, and nothing about its output looks wrong. So the
procedure is checked against cases whose answer is known by hand.
"""

import numpy as np
import pandas as pd
import pytest

from models import significance as S


def series(values) -> pd.Series:
    return pd.Series(np.asarray(values, dtype=float))


# --- the procedure itself ----------------------------------------------

def test_the_largest_passing_rank_wins_not_the_first_failing_one():
    """The step-up's defining property. With thresholds i/m*q at
    0.0125/0.025/0.0375/0.05, the third p-value fails its own threshold but
    the fourth passes, so all four are rejected. A procedure that stopped
    at the first failure would reject two."""
    p = series([0.001, 0.02, 0.03, 0.04])
    survived = S.benjamini_hochberg(p, q=0.05)

    assert survived.sum() == 4


def test_nothing_survives_when_nothing_is_small_enough():
    assert S.benjamini_hochberg(series([0.4, 0.5, 0.6, 0.9]), 0.05).sum() == 0


def test_everything_survives_when_everything_is_tiny():
    assert S.benjamini_hochberg(series([1e-9] * 5), 0.05).sum() == 5


def test_it_is_less_conservative_than_bonferroni():
    """The reason to use it. One false alarm costs an afternoon, not a
    catastrophe, so bounding the share of wrong flags beats bounding the
    chance of any."""
    p = series([0.001, 0.008, 0.02, 0.2, 0.4])
    bh = S.benjamini_hochberg(p, 0.05).sum()
    bonferroni = (p <= 0.05 / len(p)).sum()

    assert bh >= bonferroni


def test_a_looser_rate_never_rejects_fewer():
    p = series(np.random.default_rng(0).uniform(0, 1, 300))
    counts = [S.benjamini_hochberg(p, q).sum() for q in (0.01, 0.05, 0.10, 0.20)]

    assert counts == sorted(counts)


def test_survivors_keep_their_place_in_the_frame():
    """The mask is used to index the watchlist, so a reordering here would
    flag the wrong junctions."""
    p = pd.Series([0.9, 1e-9, 0.8], index=["a", "b", "c"])
    survived = S.benjamini_hochberg(p, 0.05)

    assert survived["b"] and not survived["a"] and not survived["c"]


def test_an_impossible_rate_is_refused():
    for q in (0.0, 1.0, -0.1, 2.0):
        with pytest.raises(ValueError, match="between 0 and 1"):
            S.benjamini_hochberg(series([0.01]), q)


def test_no_sites_is_no_survivors():
    assert S.benjamini_hochberg(series([]), 0.05).sum() == 0


# --- p-values ----------------------------------------------------------

def test_the_test_is_one_sided():
    """The list asks which junctions injure people *more* than predicted.
    A site that injures fewer is not a finding, so spending half the
    budget on that tail would be waste."""
    assert S.p_values(series([1.959964]))[0] == pytest.approx(0.025, abs=1e-4)
    assert S.p_values(series([0.0]))[0] == pytest.approx(0.5, abs=1e-9)


def test_a_worse_site_gets_a_smaller_p_value():
    p = S.p_values(series([0.5, 1.5, 2.5, 3.5]))
    assert list(p) == sorted(p, reverse=True)


def test_p_values_need_no_scipy():
    """The dashboard shows this and the deployment installs only
    requirements.txt, which has no scipy."""
    import models.significance as module

    source = open(module.__file__).read()
    assert "import scipy" not in source
    assert "from scipy" not in source


def test_it_agrees_with_scipy_where_scipy_is_available():
    scipy_stats = pytest.importorskip("scipy.stats")
    z = series([-1.0, 0.0, 1.0, 1.96, 3.0, 4.5])

    assert np.allclose(S.p_values(z), scipy_stats.norm.sf(z), atol=1e-12)


# --- the funnel --------------------------------------------------------

def test_the_band_narrows_as_the_crash_count_grows():
    """The whole point of the chart: a site with 25 crashes has to be far
    from expectation to mean anything, and one with 400 does not."""
    limits = S.funnel_limits(series([20, 400]), reference_rate=0.4)

    assert limits["outer_upper"].iloc[0] > limits["outer_upper"].iloc[-1]
    assert limits["crashes"].is_monotonic_increasing


def test_the_outer_band_sits_outside_the_inner_one():
    limits = S.funnel_limits(series([25, 300]), reference_rate=0.4)

    assert (limits["outer_upper"] > limits["inner_upper"]).all()
    assert (limits["outer_lower"] < limits["inner_lower"]).all()


def test_the_band_is_symmetric_about_zero():
    limits = S.funnel_limits(series([25, 300]), reference_rate=0.4)

    assert np.allclose(limits["inner_upper"], -limits["inner_lower"])


def test_a_reference_rate_that_is_not_a_probability_is_refused():
    for rate in (0.0, 1.0, -0.5, 1.5):
        with pytest.raises(ValueError, match="probability"):
            S.funnel_limits(series([25, 300]), rate)


# --- both answers, reported together -----------------------------------

def test_it_reports_the_aggregate_and_the_per_site_reading():
    """Quoting only the flattering one is the same failure as quoting a
    model's training accuracy."""
    rng = np.random.default_rng(1)
    sites = pd.DataFrame({
        "site": [f"S{i}" for i in range(500)],
        "excess_z": rng.normal(size=500),
        "crashes": rng.integers(25, 400, 500),
    })
    found = S.assess(sites)

    assert {"above_z_95", "above_z_95_by_chance", "survivors",
            "survivors_bonferroni"} <= set(found)


def test_pure_noise_produces_about_the_chance_rate_and_no_survivors():
    """500 sites of pure noise: roughly 2.5% clear z = 1.96 by
    construction, and essentially none should survive the correction."""
    rng = np.random.default_rng(4)
    sites = pd.DataFrame({
        "site": [f"S{i}" for i in range(500)],
        "excess_z": rng.normal(size=500),
        "crashes": rng.integers(25, 400, 500),
    })
    found = S.assess(sites)

    assert 0 <= found["above_z_95"] <= 30
    assert found["survivors"] <= 1


def test_a_genuinely_bad_site_survives_the_correction():
    rng = np.random.default_rng(5)
    z = rng.normal(size=300)
    z[0] = 6.0
    sites = pd.DataFrame({
        "site": [f"S{i}" for i in range(300)],
        "excess_z": z,
        "crashes": rng.integers(25, 400, 300),
    })
    found = S.assess(sites)

    assert found["survivors"] >= 1
    assert "S0" in found["survivor_sites"]


def test_bonferroni_never_finds_more_than_the_false_discovery_rate_does():
    rng = np.random.default_rng(6)
    z = rng.normal(size=400); z[:5] = [5.0, 4.5, 4.2, 4.0, 3.8]
    sites = pd.DataFrame({
        "site": [f"S{i}" for i in range(400)],
        "excess_z": z,
        "crashes": rng.integers(25, 400, 400),
    })
    found = S.assess(sites)

    assert found["survivors_bonferroni"] <= found["survivors"]


def test_an_empty_watchlist_is_not_an_error():
    assert S.assess(pd.DataFrame(columns=["site", "excess_z", "crashes"]))["sites"] == 0


def test_assess_reports_the_lower_tail_as_well():
    """The one-sided test still has to show its mirror image.

    A wide lower tail means the excess above is dispersion, not danger,
    so the number has to be published next to the one it qualifies.
    """
    z = pd.Series([-4.0, -2.5, -2.1, 0.0, 0.3, 2.1, 2.5, 4.0])
    sites = pd.DataFrame({"excess_z": z, "crashes": 30, "site": range(len(z))})
    found = S.assess(sites)

    assert found["above_z_95"] == 3
    assert found["below_z_95"] == 3
