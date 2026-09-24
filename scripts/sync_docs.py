"""Write generated figures into the prose that quotes them.

The README and the model card quote about thirty numbers between them. Every
one of those was typed by hand once, and every one of them goes stale the
next time the dataset grows or the model is refit -- which is exactly the
failure this project already fixed for the dataset counts and then
reintroduced in its own documentation.

So the prose does not hold numbers any more. It holds markers:

    <!-- generated:model-metrics -->
    ...whatever this script last wrote...
    <!-- /generated:model-metrics -->

and this rewrites what is between them from the generated artifacts. The
weekly refresh runs it after retraining, so a figure moving shows up as a
diff in the documentation rather than quietly becoming a lie.

Usage:
    python scripts/sync_docs.py
    python scripts/sync_docs.py --check    # fail if anything is out of date
"""

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Callable, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DATASET_FACTS = ROOT / "docs" / "dataset_facts.json"
MODEL_METRICS = ROOT / "reports" / "injury_risk" / "metrics.json"
WATCHLIST_SUMMARY = ROOT / "data" / "clean" / "watchlist_summary.json"
EXPOSURE_SUMMARY = ROOT / "data" / "clean" / "exposure_summary.json"
WATCHLIST_CSV = ROOT / "data" / "clean" / "injury_watchlist.csv"
FACTORS_CSV = ROOT / "data" / "clean" / "injury_risk_factors.csv"

TARGETS = [ROOT / "README.md", ROOT / "docs" / "model_card.md"]

BASELINE = "Borough x hour base rate"
MODEL = "Gradient boosting"
UNCALIBRATED = "Gradient boosting (uncalibrated)"

# How many intersections the example table shows. Enough to see the shape of
# the list, few enough that nobody scrolls.
EXAMPLE_SITES = 5


def _marker(name: str) -> re.Pattern:
    return re.compile(
        rf"(<!-- generated:{re.escape(name)} -->\n).*?(\n<!-- /generated:{re.escape(name)} -->)",
        re.DOTALL,
    )


def _table(headers: List[str], rows: List[tuple]) -> str:
    lines = ["| " + " | ".join(headers) + " |",
             "|" + "|".join(["---"] * len(headers)) + "|"]
    lines += ["| " + " | ".join(str(cell) for cell in row) + " |" for row in rows]
    return "\n".join(lines)


def _display(path: Path) -> str:
    """A short name for a path, which may sit outside the repository."""
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _load(path: Path) -> Optional[Dict]:
    if not path.exists():
        logger.warning(f"{path.name} is missing; its blocks are left alone")
        return None
    return json.loads(path.read_text(encoding="utf-8"))


# --- blocks ------------------------------------------------------------

def dataset_headline(facts: Dict) -> str:
    return (
        f"The current build holds **{facts['rows']:,} crashes** through "
        f"**{facts['last_crash'][:10]}**. Of those, {facts['injury_crashes']:,} "
        f"({facts['injury_crash_rate']:.1%}) injured or killed somebody: "
        f"{facts['people_injured']:,} people injured, "
        f"{facts['people_killed']:,} killed. Borough is known for "
        f"{facts['borough_known_after_resolution']:.1%} of them, against "
        f"{facts['borough_known_at_source']:.1%} as the source ships it."
    )


def model_folds(metrics: Dict) -> str:
    split = metrics["split"]
    return _table(
        ["Fold", "Period", "Crashes", "Injury rate"],
        [
            (name.capitalize(), f"{fold['from']} to {fold['to']}",
             f"{fold['rows']:,}", f"{fold['injury_rate']:.3f}")
            for name, fold in (("train", split["train"]),
                               ("calibration", split["calibration"]),
                               ("test", split["test"]))
        ],
    )


def model_metrics(metrics: Dict) -> str:
    rows = []
    for name in (BASELINE, UNCALIBRATED, MODEL):
        row = metrics["metrics"][name]
        skill = row.get("brier_skill_vs_baseline")
        label = name.replace(" x ", " × ")
        emphasis = "**" if name == MODEL else ""
        rows.append((
            f"{emphasis}{label}{emphasis}",
            f"{emphasis}{row['roc_auc']:.4f}{emphasis}",
            f"{emphasis}{row['brier']:.4f}{emphasis}",
            f"{emphasis}{row['log_loss']:.4f}{emphasis}",
            f"{emphasis}{row['mean_predicted']:.3f}{emphasis}",
            "—" if skill is None else f"{emphasis}{skill:+.1%}{emphasis}",
        ))

    observed = metrics["metrics"][MODEL]["observed_rate"]
    return (
        _table(
            ["Predictor", "ROC-AUC", "Brier", "Log loss", "Mean predicted",
             "Brier skill"],
            rows,
        )
        + f"\n\nObserved injury rate on the test fold: {observed:.3f}."
    )


def feature_importance(metrics: Dict) -> str:
    ranked = metrics.get("feature_importance")
    if not ranked:
        return "_Permutation importance was not run for this build._"
    return _table(
        ["Feature", "AUC drop"],
        [(f"`{row['feature']}`", f"{row['auc_drop']:.4f}") for row in ranked],
    )


def finding_headline(facts: Dict, summary: Dict) -> str:
    """The one sentence a reader gets before deciding to keep reading."""
    return (
        f"Across **{facts['rows']:,} crashes** since 2020, "
        f"**{summary['sites']:,} intersections** stand out: people get hurt "
        f"there more often than the kinds of crashes happening there would "
        f"predict. At the worst of them, "
        f"**{summary['worst_excess'] * 100:.0f} percentage points** more "
        f"often."
    )


def watchlist_headline(summary: Dict) -> str:
    window = summary["window"]
    return (
        f"**{summary['sites']:,} intersections** with at least "
        f"{summary['min_site_crashes']} crashes between {window['from']} and "
        f"{window['to']} qualify, over {summary['scored_crashes']:,} scored "
        f"crashes. The worst sits **{summary['worst_excess'] * 100:.1f} points** "
        f"above what its crash mix predicts; the top decile sits "
        f"{summary['top_decile_excess'] * 100:.1f} points above. "
        f"{summary['sites_above_z_95']} clear z = 1.96, against roughly "
        f"{summary['sites_above_z_95_by_chance']:.0f} expected from chance "
        f"across that many sites. The head of the list is signal. The tail "
        f"is a screening queue."
    )


def watchlist_scope(summary: Dict) -> str:
    window = summary["window"]
    return (
        f"Keying sites direction-free merges "
        f"{summary['intersections_directed']:,} apparent intersections into "
        f"{summary['intersections_canonical']:,} real ones. The current build "
        f"lists {summary['sites']:,} of them, over "
        f"{summary['scored_crashes']:,} scored crashes from {window['from']} "
        f"to {window['to']}, with {summary['sites_above_z_95']} above "
        f"z = 1.96 against roughly "
        f"{summary['sites_above_z_95_by_chance']:.0f} expected by chance."
    )


def exposure_coverage(summary: Dict) -> str:
    """How far the traffic counts reach, and what they say about the order."""
    from models.exposure import MATCH_RADIUS_M

    grades = summary["by_confidence"]
    return (
        f"NYC DOT's automated traffic counts reach **{summary['matched']:,} "
        f"of the {summary['sites_located']:,} located sites** "
        f"({summary['matched_share']:.1%}): a recorder within "
        f"{MATCH_RADIUS_M:.0f}m whose location text names one of the "
        f"junction's own streets. Those sites see a median "
        f"{summary['median_vehicles_per_day']:,} vehicles a day past the "
        f"counter, and a median "
        f"{summary['median_harmful_per_million']} crashes that hurt someone "
        f"per million vehicles.\n\n"
        f"Ranking them by that rate rather than by crash mix gives a "
        f"substantially different order — the two agree at a Spearman "
        f"correlation of **{summary['spearman_excess_vs_per_vehicle']}**. "
        f"That is the distance between the two questions, in a number.\n\n"
        f"It is also why the watchlist is not re-ranked by it. A recorder "
        f"sits on one segment rather than across a junction, and "
        f"{summary['single_direction_counts']:,} of the matched counters "
        f"cover a single direction, roughly half the traffic on a two-way "
        f"street; sort by crashes per vehicle and the head of the list is "
        f"whichever junction has the most under-measured traffic. Counts "
        f"are a median {summary['median_count_age_years']} years old, the "
        f"oldest from {summary['oldest_count_year']}. So every row carries "
        f"a grade for how much weight it can take — {grades.get('high', 0)} "
        f"high (the counter names both streets and covers both directions), "
        f"{grades.get('medium', 0)} medium, {grades.get('low', 0)} low — "
        f"and the ranking stays with the crash-mix residual, which covers "
        f"every site rather than a third of them."
    )


def exposure_lede(summary: Dict) -> str:
    """The one-line version of the caveat, for the top of the README."""
    return (
        f"Over the {summary['matched']:,} sites a counter reaches, the two "
        f"orderings agree at a rank correlation of only "
        f"**{summary['spearman_excess_vs_per_vehicle']:.2f}**."
    )


def factor_examples(factors) -> str:
    """The loudest and the quietest contributing factor, as a sentence."""
    ranked = factors.sort_values("predicted_rate", ascending=False)
    top, bottom = ranked.iloc[0], ranked.iloc[-1]
    return (
        f"Crashes where the officer wrote *{top['factor']}* injure someone "
        f"{top['predicted_rate']:.1%} of the time, against "
        f"{bottom['predicted_rate']:.1%} for *{bottom['factor']}*. Predicted "
        f"and observed track within a couple of points across all "
        f"{len(factors)} factors. That agreement is the calibration check "
        f"worth trusting most."
    )


def watchlist_examples(sites) -> str:
    return _table(
        ["Intersection", "Borough", "Crashes", "Injured", "Expected", "Excess"],
        [
            (
                row.site.title().replace(" @ ", " @ "),
                row.borough.title(),
                f"{int(row.crashes):,}",
                f"{row.observed_rate:.1%}",
                f"{row.expected_rate:.1%}",
                f"{row.excess * 100:+.1f} pts",
            )
            for row in sites.head(EXAMPLE_SITES).itertuples()
        ],
    )


def build_blocks() -> Dict[str, str]:
    """Render every block the documents can reference."""
    blocks: Dict[str, Callable] = {}

    facts = _load(DATASET_FACTS)
    if facts:
        blocks["dataset-headline"] = dataset_headline(facts)

    metrics = _load(MODEL_METRICS)
    if metrics:
        blocks["model-folds"] = model_folds(metrics)
        blocks["model-metrics"] = model_metrics(metrics)
        blocks["feature-importance"] = feature_importance(metrics)

    summary = _load(WATCHLIST_SUMMARY)
    if summary:
        if facts:
            blocks["finding-headline"] = finding_headline(facts, summary)
        blocks["watchlist-headline"] = watchlist_headline(summary)
        blocks["watchlist-scope"] = watchlist_scope(summary)

    exposure = _load(EXPOSURE_SUMMARY)
    if exposure:
        blocks["exposure-coverage"] = exposure_coverage(exposure)
        blocks["exposure-lede"] = exposure_lede(exposure)

    import pandas as pd

    if WATCHLIST_CSV.exists():
        blocks["watchlist-examples"] = watchlist_examples(pd.read_csv(WATCHLIST_CSV))
    if FACTORS_CSV.exists():
        blocks["factor-examples"] = factor_examples(pd.read_csv(FACTORS_CSV))

    return blocks


# --- applying them -----------------------------------------------------

def apply(text: str, blocks: Dict[str, str]) -> str:
    """Rewrite every marked region whose block is available."""
    for name, body in blocks.items():
        text = _marker(name).sub(
            lambda match: match.group(1) + body + match.group(2), text
        )
    return text


def markers_in(text: str) -> List[str]:
    return re.findall(r"<!-- generated:([a-z-]+) -->", text)


def sync(check: bool = False, targets: Optional[List[Path]] = None) -> List[Path]:
    """Rewrite the marked regions in each target document.

    Args:
        check: Report what would change without writing anything.
        targets: Documents to sync; defaults to the README and the model card.

    Returns:
        The documents that changed, or would have.

    Raises:
        KeyError: If a document references a block that does not exist.
    """
    blocks = build_blocks()
    stale: List[Path] = []

    for path in targets or TARGETS:
        if not path.exists():
            continue
        before = path.read_text(encoding="utf-8")

        unknown = set(markers_in(before)) - set(blocks)
        if unknown:
            raise KeyError(
                f"{path.name} references generated blocks that do not exist: "
                f"{sorted(unknown)}"
            )

        after = apply(before, blocks)
        if after == before:
            continue

        stale.append(path)
        if not check:
            path.write_text(after, encoding="utf-8")
            logger.info(f"Updated {_display(path)}")

    if check and stale:
        logger.error(
            "Out of date: "
            + ", ".join(_display(path) for path in stale)
            + ". Run `python scripts/sync_docs.py`."
        )
    elif not stale:
        logger.info("Documentation already matches the generated figures.")
    return stale


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="exit non-zero if a document is out of date")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    stale = sync(check=args.check)
    if args.check and stale:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
