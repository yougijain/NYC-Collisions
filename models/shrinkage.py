"""Borrow strength across sites before ranking them.

Every site's excess is an estimate, and how noisy it is depends almost
entirely on how many crashes sit behind it. Ranking on the raw estimate
lets the thinnest sites reach both ends of the list on noise alone; the
backtest's 29% retention among the top fifty is that effect, measured.

The watchlist's answer so far has been the lower confidence bound, which
penalises thin sites but does not say how much of an extreme estimate to
believe. Partial pooling does. Treat each site's true excess as a draw from
a citywide distribution, estimate how wide that distribution is from the
sites themselves, and pull every estimate toward the middle in proportion
to how little its own crashes can carry:

    shrunk = mean + weight * (excess - mean)
    weight = between-site variance / (between-site variance + sampling variance)

A site with 400 crashes keeps most of its excess. A site with 25 keeps
whatever fraction of it the spread between real sites can explain.

## Why DerSimonian-Laird

The between-site variance is the one number this needs, and the
DerSimonian-Laird moment estimator is the standard way to get it from
estimates with known, unequal sampling variances. It is closed-form, needs
no optimiser, and is what a reviewer from a meta-analysis background will
check first. Its known weakness -- it underestimates the spread when there
are few studies -- does not bite at a thousand sites.

## What the sampling variance is

The same Bernoulli variance `rank_sites` already uses for its z-scores:
each crash injures someone with its own predicted probability, so a site's
count of injury crashes has variance sum(p * (1 - p)), and its rate that
divided by crashes squared. Nothing new is assumed here.

## The fair comparison

Pooling helps any noisy ranking, not just the model's. So the naive
baseline it is compared against in the backtest is pooled too: raw injury
rates shrunk the same way. Otherwise a win would only show that pooling
beats not pooling, which is known.
"""

from typing import Dict

import numpy as np
import pandas as pd

# Two-sided 95%, matching the raw lower bound the watchlist ranks on.
Z_95 = 1.959964


def between_site_variance(estimate: pd.Series, variance: pd.Series) -> float:
    """How much sites truly differ, net of how noisy each estimate is.

    The DerSimonian-Laird moment estimator: the precision-weighted spread
    of the estimates around their weighted mean, less what sampling noise
    alone would produce, floored at zero.

    Args:
        estimate: One estimate per site.
        variance: Each estimate's sampling variance.

    Returns:
        The between-site variance, zero if the estimates are no more
        spread out than their noise explains.

    Raises:
        ValueError: If any sampling variance is not positive, or fewer
            than two sites are given.
    """
    if len(estimate) < 2:
        raise ValueError("need at least two sites to estimate a spread")
    if not (variance > 0).all():
        raise ValueError("every sampling variance must be positive")

    w = 1.0 / variance.to_numpy()
    x = estimate.to_numpy()
    fixed_mean = np.sum(w * x) / np.sum(w)
    q = np.sum(w * (x - fixed_mean) ** 2)
    scale = np.sum(w) - np.sum(w ** 2) / np.sum(w)
    return float(max(0.0, (q - (len(x) - 1)) / scale))


def shrink(estimate: pd.Series, variance: pd.Series) -> pd.DataFrame:
    """Pull each estimate toward the citywide mean by what it cannot carry.

    Args:
        estimate: One estimate per site.
        variance: Each estimate's sampling variance.

    Returns:
        Aligned to `estimate`: `shrunk` (the posterior mean), `shrunk_sd`,
        `shrunk_lower_95`, and `weight`, the share of the site's own
        deviation from the mean that it keeps.
    """
    tau2 = between_site_variance(estimate, variance)
    # The mean is re-estimated with the random-effects weights, which is
    # what the shrinkage is toward. With tau2 at zero this is the
    # fixed-effect mean and every site collapses onto it, as it should.
    w = 1.0 / (variance + tau2)
    mean = float(np.sum(w * estimate) / np.sum(w))

    weight = tau2 / (tau2 + variance)
    shrunk = mean + weight * (estimate - mean)
    sd = np.sqrt(weight * variance)
    return pd.DataFrame({
        "shrunk": shrunk,
        "shrunk_sd": sd,
        "shrunk_lower_95": shrunk - Z_95 * sd,
        "weight": weight,
    }, index=estimate.index)


def describe(estimate: pd.Series, variance: pd.Series) -> Dict:
    """The numbers a reader needs to judge how hard the pooling pulled."""
    tau2 = between_site_variance(estimate, variance)
    weight = tau2 / (tau2 + variance)
    return {
        "between_site_sd": round(float(np.sqrt(tau2)), 4),
        "median_sampling_sd": round(float(np.sqrt(variance).median()), 4),
        "weight_median": round(float(weight.median()), 3),
        "weight_min": round(float(weight.min()), 3),
        "weight_max": round(float(weight.max()), 3),
    }
