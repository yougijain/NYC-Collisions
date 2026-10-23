"""Run the watchlist backtest and write the report.

Answers the question the watchlist's own figures cannot: do the sites it
flags stay flagged? Fits on an early window, checks a later one, and
compares against sorting by raw injury rate with no model at all.

Writes:

    reports/backtest/metrics.json   every figure, machine-readable
    reports/backtest/results.md     the same thing as a table

Usage:
    python scripts/build_backtest.py
    python scripts/build_backtest.py --split-year 2024 --min-crashes 20
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "app"))
sys.path.insert(0, str(ROOT / "scripts"))

import db  # noqa: E402
from models import backtest as B  # noqa: E402
from models import watchlist as W  # noqa: E402
from build_dataset import provenance  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

REPORT_DIR = ROOT / "reports" / "backtest"
SPLIT_YEAR = 2024

# Model seeds behind the noise floor, the default included. Each one is a
# full rolling-origin rescoring, about two minutes on a CI runner, so five
# buys a usable spread without doubling the weekly refresh.
SEEDS = 5


def pooling_section(metrics: Dict) -> List[str]:
    """Partial pooling: how hard it pulled, and what it changed."""
    pool, fade = metrics["pooling"], metrics["pooled_fade"]
    head, naive = metrics["pooled_headline"], metrics["pooled_naive_headline"]
    short = metrics["pooled_shortlist"]
    retained = "n/a" if fade["retained"] is None else f"{fade['retained']:.0%}"
    sweep = [r for r in metrics["sensitivity"] if not r["underpowered"]]
    helps = sum(1 for r in sweep if r["model_excess_pooled"] > r["model_excess"])
    beats = sum(
        1 for r in sweep if r["model_excess_pooled"] > r["observed_rate_pooled"]
    )
    return [
        "",
        "## Partial pooling",
        "",
        f"Sites in the early window truly differ by about "
        f"{pool['between_site_sd'] * 100:.1f} points of excess (between-site "
        f"sd), against a median sampling sd of "
        f"{pool['median_sampling_sd'] * 100:.1f} points for one site's own "
        f"estimate. So a typical site keeps "
        f"{pool['weight_median']:.0%} of its deviation from the mean, "
        f"ranging from {pool['weight_min']:.0%} to {pool['weight_max']:.0%}.",
        "",
        f"- Pooled early excess predicts late excess at **{head['observed']}** "
        f"(p = {head['p_value']}), against **{naive['observed']}** for raw "
        f"injury rates pooled the same way.",
        f"- Across {len(sweep)} split-and-threshold combinations, pooling "
        f"improves the model's ordering in {helps} and the pooled model "
        f"beats the pooled naive ordering in {beats}.",
        f"- The top {short['top_n']} by pooled excess: "
        f"{short['still_above_expectation']:.0%} still above expectation "
        f"against a {short['base_rate']:.0%} base rate.",
        f"- Their pooled early excess was "
        f"{fade['top_n_pooled_early_excess'] * 100:+.1f} points and their "
        f"late excess {fade['top_n_late_excess'] * 100:+.1f}, retaining "
        f"{retained}. A calibrated pooled estimate would retain about all "
        f"of it; well under that means it was not pulled hard enough.",
    ]


def seed_section(metrics: Dict) -> List[str]:
    """How much of any difference is the model's seed."""
    spread = metrics.get("seed_spread")
    if not spread:
        return []
    lines = [
        "",
        "## Noise floor",
        "",
        f"Everything above rescored under {spread['seeds']} model seeds. "
        f"Only the early-stopping holdout changes between them, so any "
        f"difference smaller than this spread is not a difference.",
        "",
        "| Predictor | Mean | sd | Range |",
        "|---|---|---|---|",
    ]
    for label, row in spread["predictors"].items():
        if row["mean"] is None:
            continue
        lines.append(
            f"| {label} | {row['mean']:.3f} | {row['sd']:.3f} | "
            f"{row['min']:.3f} to {row['max']:.3f} |"
        )
    lines += [
        "",
        "| Comparison | Mean difference | sd | Wins |",
        "|---|---|---|---|",
    ]
    for name, row in spread["comparisons"].items():
        lines.append(
            f"| {name} | {row['mean']:+.4f} | {row['sd']:.4f} | "
            f"{row['wins']} of {spread['seeds']} |"
        )
    return lines


def report(metrics: Dict) -> str:
    """Render the findings as markdown."""
    early, late = metrics["early_window"], metrics["late_window"]
    head, naive = metrics["headline"], metrics["naive_headline"]
    short, shrink = metrics["shortlist"], metrics["shrinkage"]

    lines = [
        "# Does the watchlist find sites that stay bad?",
        "",
        f"Fit on **{early['from']}–{early['to']}**, checked against "
        f"**{late['from']}–{late['to']}**. "
        f"{metrics['sites_in_both']:,} sites clear "
        f"{metrics['min_window_crashes']} scored crashes in both windows.",
        "",
        "## Headline",
        "",
        f"Early excess predicts late excess at a Spearman correlation of "
        f"**{head['observed']}** "
        f"(permutation null {head['null_mean']} ± {head['null_sd']}, "
        f"p = {head['p_value']}).",
        "",
        "Sorting by raw injury rate instead, with no model, predicts the "
        f"same target at **{naive['observed']}**.",
        "",
        "## Every ordering, against both targets",
        "",
        "| Predictor | Target | Spearman |",
        "|---|---|---|",
    ]
    for row in metrics["predictors"]:
        lines.append(
            f"| {row['predictor']} | {row['target']} | {row['spearman']:.3f} |"
        )

    lines += [
        "",
        "## By decile of the early ranking",
        "",
        "| Decile | Sites | Early excess | Late excess | Late injury rate |",
        "|---|---|---|---|---|",
    ]
    for row in metrics["deciles"]:
        lines.append(
            f"| {row['decile']} | {row['sites']} | "
            f"{row['early_excess'] * 100:+.1f} pts | "
            f"{row['late_excess'] * 100:+.1f} pts | "
            f"{row['late_injury_rate']:.1%} |"
        )

    over = metrics["overlap"]
    lines += [
        "",
        "## Does the model adjustment change the ranking at all?",
        "",
        f"Early excess and raw injury rate order the sites at a Spearman of "
        f"**{over['spearman_excess_vs_observed_rate']}**, so the two "
        f"predictors above are related but not the same list. Predicted "
        f"rates run {over['expected_rate_min']:.1%} to "
        f"{over['expected_rate_max']:.1%} across sites "
        f"(sd {over['expected_rate_sd']:.3f}, against "
        f"{over['observed_rate_sd']:.3f} for observed rates), so the "
        f"adjustment is substantial rather than cosmetic.",
        "",
        "## Sensitivity",
        "",
        "Neither the split year nor the crash threshold was tuned. 2024 is "
        "the headline because it was chosen before the first run, not "
        "because of how it came out.",
        "",
        "| Split | Min crashes | Sites | Model excess | Injury rate "
        "| Model, pooled | Rate, pooled | p |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in metrics["sensitivity"]:
        if row["underpowered"]:
            lines.append(
                f"| {row['split_year']} | {row['min_window_crashes']} | "
                f"{row['sites']} | _too few to rank_ | | | | |"
            )
        else:
            lines.append(
                f"| {row['split_year']} | {row['min_window_crashes']} | "
                f"{row['sites']} | {row['model_excess']:.3f} | "
                f"{row['observed_rate']:.3f} | "
                f"{row['model_excess_pooled']:.3f} | "
                f"{row['observed_rate_pooled']:.3f} | {row['p_value']} |"
            )
    lines += pooling_section(metrics)
    lines += seed_section(metrics)

    lift = "n/a" if short["lift"] is None else f"{short['lift']:.2f}x"
    retained = "n/a" if shrink["retained"] is None else f"{shrink['retained']:.0%}"
    lines += [
        "",
        "## What a visit to the top of the list would have found",
        "",
        f"- Of the top {short['top_n']} by the early ranking, "
        f"**{short['still_above_expectation']:.0%}** were still above "
        f"expectation in the later window, against a base rate of "
        f"{short['base_rate']:.0%} ({lift} lift).",
        f"- Their mean late excess was "
        f"{short['mean_late_excess'] * 100:+.1f} points, against "
        f"{short['mean_late_excess_rest'] * 100:+.1f} for the rest.",
        "",
        "## Regression to the mean",
        "",
        f"The top {short['top_n']} sites "
        f"averaged {shrink['top_n_early_excess'] * 100:+.1f} points of excess "
        f"in the early window and {shrink['top_n_late_excess'] * 100:+.1f} "
        f"in the later one, retaining {retained}. Some shrinkage is "
        f"arithmetic rather than failure: ranking on a noisy estimate "
        f"selects sites whose noise ran high. The permutation null above is "
        f"what separates the two.",
        "",
    ]
    return "\n".join(lines)


def run(
    dataset: Optional[str] = None,
    split_year: int = SPLIT_YEAR,
    min_crashes: int = B.MIN_WINDOW_CRASHES,
    permutations: int = B.PERMUTATIONS,
    output_dir: Path = REPORT_DIR,
    seeds: int = SEEDS,
    **overrides,
) -> Dict:
    """Score every crash, split the window, and write the report.

    `seeds` counts full rescorings under different model seeds, including
    the default one; 1 skips the seed spread.
    """
    path = dataset or db.resolve_dataset()
    logger.info(f"Reading {path}")
    df = pd.read_parquet(path)

    logger.info("Scoring rolling-origin; this is the slow part")
    scores, folds = W.rolling_scores(df, **overrides)

    metrics = B.run(df, scores, split_year, min_crashes, permutations)
    metrics["folds"] = len(folds)

    # The default model is seed 0; the others refit everything with a
    # different early-stopping holdout and nothing else changed.
    score_sets = [scores]
    for seed in range(1, seeds):
        logger.info(f"Rescoring under seed {seed} of {seeds - 1}")
        score_sets.append(
            W.rolling_scores(df, **{**overrides, "random_state": seed})[0]
        )
    if len(score_sets) > 1:
        metrics["seed_spread"] = B.seed_spread(
            df, score_sets, split_year, min_crashes
        )
    metrics["dataset"] = provenance(path)

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "metrics.json").write_text(
        json.dumps(metrics, indent=2) + "\n", encoding="utf-8"
    )
    (output_dir / "results.md").write_text(report(metrics), encoding="utf-8")
    logger.info(f"Wrote {output_dir / 'metrics.json'} and results.md")
    return metrics


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-year", type=int, default=SPLIT_YEAR)
    parser.add_argument("--min-crashes", type=int, default=B.MIN_WINDOW_CRASHES)
    parser.add_argument("--permutations", type=int, default=B.PERMUTATIONS)
    parser.add_argument("--dataset", default=None)
    parser.add_argument(
        "--seeds", type=int, default=SEEDS,
        help="model seeds behind the noise floor, the default included; "
             "1 skips it",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    metrics = run(
        dataset=args.dataset,
        split_year=args.split_year,
        min_crashes=args.min_crashes,
        permutations=args.permutations,
        seeds=args.seeds,
    )
    print(json.dumps(metrics["headline"], indent=2))


if __name__ == "__main__":
    main()
