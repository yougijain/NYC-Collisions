"""Which sites survive the arithmetic of testing a thousand of them.

The watchlist has reported significance one way: 101 of 1,093 sites clear
z = 1.96 against the 27.3 that chance produces for free. That is a true
statement and a useful one, but it answers a question about the *list* --
there are roughly seventy-four more flagged sites than chance explains, so
the list carries signal -- and readers hear it as a question about *sites*,
as though seventy-four junctions had been individually established.

They have not. Testing 1,093 hypotheses at 5% each means expecting dozens
of false ones, and the usual correction is brutal here: at a 5% false
discovery rate, two sites survive.

Both numbers are right and they answer different questions:

    aggregate      does this list contain more than chance?    yes, clearly
    per site       can I defend this junction on its own?      almost never

A screening tool lives on the first. An engineer sent to one corner with a
clipboard needs the second. Reporting only the flattering one would be the
same failure as quoting a model's training accuracy.

## Why Benjamini-Hochberg rather than Bonferroni

Bonferroni controls the chance of *any* false positive, which is the right
guarantee when one false alarm is a catastrophe. Here a false alarm costs
an engineer an afternoon. Benjamini-Hochberg instead bounds the expected
share of the flagged list that is wrong, which is what a screening queue
actually wants to know, and it is far less conservative: at the same 5%
it finds two sites where Bonferroni finds one.

## Why one-sided

The watchlist asks which junctions injure people *more* than their crash
mix predicts. A site that injures fewer is not a finding it is looking for,
so spending half the significance budget on that tail would be waste.
"""

from typing import Dict, Optional

import numpy as np
import pandas as pd

# The usual false discovery rate. At q = 0.05, one in twenty sites on the
# surviving list is expected to be there by chance.
DEFAULT_Q = 0.05

# Funnel control limits, as a p-chart draws them: the 95% and 99.8% bands.
# The outer pair is the one quality engineers read as "investigate".
INNER_SIGMA = 1.959964
OUTER_SIGMA = 3.090232

# Enough points to draw the curves smoothly across the crash-count range.
CURVE_POINTS = 200


def _normal_sf(z: np.ndarray) -> np.ndarray:
    """Upper-tail probability of the standard normal.

    Written out rather than imported so the dashboard can show the funnel
    without scipy, which lives in requirements-ml.txt and is not installed
    on the deployment.
    """
    from math import erfc, sqrt

    return np.array([0.5 * erfc(value / sqrt(2.0)) for value in np.ravel(z)])


def p_values(z: pd.Series) -> pd.Series:
    """One-sided p-values: the chance of a z this high under the null."""
    return pd.Series(_normal_sf(z.to_numpy()), index=z.index)


def benjamini_hochberg(p: pd.Series, q: float = DEFAULT_Q) -> pd.Series:
    """Which hypotheses survive at a false discovery rate of `q`.

    The step-up procedure: sort the p-values, find the largest rank i whose
    p-value is at or below i/m * q, and reject everything up to it. Taking
    the largest such rank rather than the first failure is what makes this
    valid; stopping at the first p above its threshold would be a different,
    wrong test.

    Args:
        p: One-sided p-values, one per site.
        q: Target false discovery rate.

    Returns:
        A boolean aligned to `p`, true where the site survives.

    Raises:
        ValueError: If `q` is outside (0, 1).
    """
    if not 0 < q < 1:
        raise ValueError(f"q must be between 0 and 1, got {q}")

    m = len(p)
    if m == 0:
        return pd.Series(dtype=bool)

    order = np.argsort(p.to_numpy(), kind="stable")
    ranked = p.to_numpy()[order]
    thresholds = np.arange(1, m + 1) / m * q

    below = np.nonzero(ranked <= thresholds)[0]
    survivors = np.zeros(m, dtype=bool)
    if len(below):
        survivors[order[: below.max() + 1]] = True
    return pd.Series(survivors, index=p.index)


def funnel_limits(
    crashes: pd.Series,
    reference_rate: float,
    points: int = CURVE_POINTS,
) -> pd.DataFrame:
    """Control limits for a p-chart with varying sample size.

    The band narrows as 1/sqrt(n), which is the whole point of the chart: a
    site with 25 crashes has to be far from expectation to mean anything,
    and a site with 400 does not. Reading that off a plot takes a second,
    where reading it off a column of z-scores takes training.

    The limits use one reference rate for every site rather than each
    site's own predicted rate. Per-site limits would be a cloud of points
    instead of two curves, and the approximation is what makes the chart
    readable; the exact per-site arithmetic is in `excess_z`, which is what
    the Benjamini-Hochberg step actually uses.

    Args:
        crashes: Per-site crash counts, used for the range to draw over.
        reference_rate: The expected injury rate the band is centred on.
        points: How many points to draw each curve with.

    Returns:
        `crashes` and the four limits, ordered for plotting.

    Raises:
        ValueError: If `reference_rate` is not a probability.
    """
    if not 0 < reference_rate < 1:
        raise ValueError(
            f"reference_rate must be a probability, got {reference_rate}"
        )

    low, high = int(crashes.min()), int(crashes.max())
    grid = np.unique(np.linspace(max(low, 1), high, points).astype(int))
    sigma = np.sqrt(reference_rate * (1 - reference_rate) / grid)

    return pd.DataFrame({
        "crashes": grid,
        "inner_upper": INNER_SIGMA * sigma,
        "inner_lower": -INNER_SIGMA * sigma,
        "outer_upper": OUTER_SIGMA * sigma,
        "outer_lower": -OUTER_SIGMA * sigma,
    })


def assess(sites: pd.DataFrame, q: float = DEFAULT_Q) -> Dict:
    """Both answers: what the list shows, and what each site can carry.

    Args:
        sites: The watchlist, carrying `excess_z` and `crashes`.
        q: Target false discovery rate.

    Returns:
        Counts for both framings, plus the sites that survive.
    """
    if sites.empty:
        return {"sites": 0, "survivors": 0}

    p = p_values(sites["excess_z"])
    survived = benjamini_hochberg(p, q)
    m = len(sites)

    bonferroni = int((p <= q / m).sum())
    above = int((sites["excess_z"] > INNER_SIGMA).sum())
    # The mirror image. The test is one-sided because the question is which
    # junctions are worse than their crashes explain, but the lower tail is
    # the honest check on it: if the excess above were an artefact of the
    # model's residual scale, the tail below would be just as fat.
    below = int((sites["excess_z"] < -INNER_SIGMA).sum())

    return {
        "sites": m,
        "q": q,
        # The aggregate reading: more flagged than chance explains.
        "above_z_95": above,
        "above_z_95_by_chance": round(m * 0.025, 1),
        "below_z_95": below,
        "excess_over_chance": round(above - m * 0.025, 1),
        # The per-site reading: what one junction can carry on its own.
        "survivors": int(survived.sum()),
        "survivors_bonferroni": bonferroni,
        "smallest_p": float(p.min()),
        "largest_z": round(float(sites["excess_z"].max()), 2),
        "survivor_sites": sites.loc[survived, "site"].tolist(),
    }
