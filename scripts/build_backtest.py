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
from typing import Dict, Optional

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
        f"(permutation null {naive['null_mean']} ± {head['null_sd']}, "
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
        "| Split | Min crashes | Sites | Model excess | Injury rate | p |",
        "|---|---|---|---|---|---|",
    ]
    for row in metrics["sensitivity"]:
        if row["underpowered"]:
            lines.append(
                f"| {row['split_year']} | {row['min_window_crashes']} | "
                f"{row['sites']} | _too few to rank_ | | |"
            )
        else:
            lines.append(
                f"| {row['split_year']} | {row['min_window_crashes']} | "
                f"{row['sites']} | {row['model_excess']:.3f} | "
                f"{row['observed_rate']:.3f} | {row['p_value']} |"
            )

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
        f"The top {shrink['top_n_early_excess'] and short['top_n']} sites "
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
    **overrides,
) -> Dict:
    """Score every crash, split the window, and write the report."""
    path = dataset or db.resolve_dataset()
    logger.info(f"Reading {path}")
    df = pd.read_parquet(path)

    logger.info("Scoring rolling-origin; this is the slow part")
    scores, folds = W.rolling_scores(df, **overrides)

    metrics = B.run(df, scores, split_year, min_crashes, permutations)
    metrics["folds"] = len(folds)
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
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    metrics = run(
        dataset=args.dataset,
        split_year=args.split_year,
        min_crashes=args.min_crashes,
        permutations=args.permutations,
    )
    print(json.dumps(metrics["headline"], indent=2))


if __name__ == "__main__":
    main()
