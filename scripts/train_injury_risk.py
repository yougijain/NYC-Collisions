"""Train the injury risk model and write down what it is worth.

Produces, under reports/injury_risk/:

    metrics.json      every number below, for anything that wants to read them
    results.md        the baseline and the model side by side
    calibration.png   predicted against observed, both predictors

and the fitted model under models/artifacts/, which the watchlist scores with.

Usage:
    python scripts/train_injury_risk.py
    python scripts/train_injury_risk.py --test-start 2024-01-01 --no-importance
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Optional

import matplotlib
import pandas as pd

matplotlib.use("Agg")  # No display on a CI runner.
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "app"))

import db  # noqa: E402
import palette  # noqa: E402
from build_dataset import provenance  # noqa: E402
from models import injury_risk as IR  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

REPORT_DIR = ROOT / "reports" / "injury_risk"
MODEL_PATH = ROOT / "models" / "artifacts" / "injury_risk.joblib"

UNCALIBRATED_NAME = "Gradient boosting (uncalibrated)"

# Enough repeats to separate the features that matter from the ones that do
# not, without spending ten minutes on a number nobody reads to four places.
IMPORTANCE_REPEATS = 5


def importances(model: IR.InjuryRiskModel, split: IR.Split, repeats: int) -> pd.DataFrame:
    """Rank features by the test ROC-AUC lost when each is shuffled.

    Permutation rather than split gain: gain rewards a feature for being
    high-cardinality, and the question here is what the model would actually
    miss, measured on data it has not seen.

    Args:
        model: The fitted model.
        split: The split it was fitted on.
        repeats: Shuffles per feature.

    Returns:
        Features ordered by mean AUC drop, descending.
    """
    from sklearn.inspection import permutation_importance

    result = permutation_importance(
        model.estimator, split.X_test, split.y_test,
        scoring="roc_auc", n_repeats=repeats, random_state=0,
    )
    return pd.DataFrame({
        "feature": split.X_test.columns,
        "auc_drop": result.importances_mean.round(4),
        "sd": result.importances_std.round(4),
    }).sort_values("auc_drop", ascending=False).reset_index(drop=True)


def plot_calibration(curves: Dict[str, pd.DataFrame], path: Path) -> Path:
    """Draw predicted against observed for each predictor.

    Args:
        curves: Predictor name to the frame from `injury_risk.calibration`.
        path: Destination PNG.

    Returns:
        The path written.
    """
    figure, axes = plt.subplots(figsize=(6.5, 6.0), facecolor=palette.SURFACE)
    axes.set_facecolor(palette.SURFACE)
    axes.plot([0, 1], [0, 1], linestyle="--", color=palette.INK_MUTED,
              linewidth=1, label="Perfect calibration", zorder=1)

    # Same two series colours the dashboard uses, so the plot does not
    # arrive on the page in a palette of its own.
    colours = [palette.SERIES_ALT, palette.SERIES]
    for (name, curve), colour in zip(curves.items(), colours):
        axes.plot(curve["predicted"], curve["observed"], marker="o",
                  markersize=5, linewidth=2, label=name, color=colour,
                  zorder=2)

    axes.set_xlabel("Predicted probability of injury", color=palette.INK_SECONDARY)
    axes.set_ylabel("Observed share of crashes causing injury",
                    color=palette.INK_SECONDARY)
    axes.set_title("Calibration on held-out crashes\nequal-count bins",
                   color=palette.INK)
    axes.set_xlim(0, 1)
    axes.set_ylim(0, 1)
    axes.grid(color=palette.GRIDLINE, linewidth=0.6)
    axes.set_axisbelow(True)
    for side in ("top", "right"):
        axes.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        axes.spines[side].set_color(palette.GRIDLINE)
    axes.tick_params(colors=palette.INK_MUTED)
    axes.legend(loc="upper left", frameon=False, labelcolor=palette.INK_SECONDARY)
    axes.set_aspect("equal")

    path.parent.mkdir(parents=True, exist_ok=True)
    figure.tight_layout()
    figure.savefig(path, dpi=150)
    plt.close(figure)
    return path


def _table(rows, headers) -> str:
    """Render a Markdown table."""
    lines = ["| " + " | ".join(headers) + " |",
             "|" + "|".join(["---"] * len(headers)) + "|"]
    lines += ["| " + " | ".join(str(cell) for cell in row) + " |" for row in rows]
    return "\n".join(lines)


def render(results: Dict, importance: Optional[pd.DataFrame]) -> str:
    """Write the results up as Markdown."""
    split = results["split"]
    metrics = results["metrics"]

    fold_rows = [
        (name.capitalize(), f"{fold['from']} to {fold['to']}",
         f"{fold['rows']:,}", f"{fold['injury_rate']:.3f}")
        for name, fold in (("train", split["train"]),
                           ("calibration", split["calibration"]),
                           ("test", split["test"]))
    ]

    metric_rows = []
    for name, row in metrics.items():
        skill = row.get("brier_skill_vs_baseline")
        metric_rows.append((
            name,
            f"{row['roc_auc']:.4f}",
            f"{row['brier']:.4f}",
            f"{row['log_loss']:.4f}",
            f"{row['mean_predicted']:.3f}",
            "--" if skill is None else f"{skill:+.1%}",
        ))

    observed = metrics[IR.MODEL_NAME]["observed_rate"]
    sections = [
        "# Injury risk: results",
        "",
        "Generated by `scripts/train_injury_risk.py`. Every figure below is",
        "measured on crashes from after the test boundary, which neither the",
        "model nor its calibration has seen.",
        "",
        "## Folds",
        "",
        _table(fold_rows, ["Fold", "Period", "Crashes", "Injury rate"]),
        "",
        "## Baseline against model",
        "",
        _table(metric_rows, ["Predictor", "ROC-AUC", "Brier", "Log loss",
                             "Mean predicted", "Brier skill"]),
        "",
        f"The observed injury rate on the test period is {observed:.3f}.",
        "",
        "Brier skill is the share of the baseline's squared error removed.",
        "The uncalibrated row is the same model without its level shift; the",
        "gap between the two rows is what drift costs, and the identical",
        "ROC-AUC is the point -- a level shift cannot reorder anything.",
        "",
    ]

    if importance is not None:
        importance_rows = [
            (row.feature, f"{row.auc_drop:.4f}", f"{row.sd:.4f}")
            for row in importance.itertuples()
        ]
        sections += [
            "## What the model is using",
            "",
            "Test ROC-AUC lost when each feature is shuffled, "
            f"{IMPORTANCE_REPEATS} repeats.",
            "",
            _table(importance_rows, ["Feature", "AUC drop", "SD"]),
            "",
        ]

    sections += [
        "## Calibration",
        "",
        "![Calibration curve](calibration.png)",
        "",
    ]
    return "\n".join(sections)


def run(
    calibration_start: str = IR.DEFAULT_CALIBRATION_START,
    test_start: str = IR.DEFAULT_TEST_START,
    dataset: Optional[str] = None,
    with_importance: bool = True,
    report_dir: Path = REPORT_DIR,
    model_path: Path = MODEL_PATH,
) -> Dict:
    """Train, evaluate and write every artifact.

    Args:
        calibration_start: First crash date used for calibration.
        test_start: First crash date held out.
        dataset: Parquet path or URL; resolved through app/db.py when omitted.
        with_importance: Run permutation importance, which is the slow part.
        report_dir: Where to write metrics, results and the plot.
        model_path: Where to write the fitted model.

    Returns:
        The metrics dictionary, as written to metrics.json.
    """
    source = dataset or db.resolve_dataset()
    logger.info(f"Reading {source}")
    df = pd.read_parquet(source)

    split = IR.split_by_time(df, calibration_start, test_start)
    logger.info(
        f"Folds: {len(split.train):,} train / {len(split.calibration):,} "
        f"calibration / {len(split.test):,} test"
    )

    model = IR.fit(split)
    logger.info(
        f"Fitted in {model.metadata['boosting_rounds']} rounds; "
        f"level shift {model.metadata['level_shift']:+.4f}"
    )

    baseline = IR.BoroughHourBaseline().fit(
        pd.concat([split.X_train, split.X_calibration]),
        pd.concat([split.y_train, split.y_calibration]),
    )

    predictions = {
        IR.BASELINE_NAME: baseline.predict_proba(split.X_test),
        UNCALIBRATED_NAME: model.predict_proba(split.X_test, calibrated=False),
        IR.MODEL_NAME: model.predict_proba(split.X_test),
    }
    results = {
        "dataset": provenance(source),
        "split": split.describe(),
        "model": model.metadata,
        "metrics": IR.compare(split.y_test, predictions),
    }

    importance = importances(model, split, IMPORTANCE_REPEATS) if with_importance else None
    if importance is not None:
        results["feature_importance"] = importance.to_dict(orient="records")

    plot_calibration(
        {
            name: IR.calibration(split.y_test, p)
            for name, p in predictions.items()
            if name != UNCALIBRATED_NAME
        },
        report_dir / "calibration.png",
    )

    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "metrics.json").write_text(
        json.dumps(results, indent=2) + "\n", encoding="utf-8"
    )
    (report_dir / "results.md").write_text(
        render(results, importance), encoding="utf-8"
    )
    model.save(model_path)

    logger.info(f"Wrote {report_dir}/ and {model_path}")
    return results


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calibration-start", default=IR.DEFAULT_CALIBRATION_START,
                        help=f"first calibration crash date "
                             f"(default: {IR.DEFAULT_CALIBRATION_START})")
    parser.add_argument("--test-start", default=IR.DEFAULT_TEST_START,
                        help=f"first held-out crash date "
                             f"(default: {IR.DEFAULT_TEST_START})")
    parser.add_argument("--dataset", default=None,
                        help="Parquet path or URL (default: whatever app/db.py resolves)")
    parser.add_argument("--no-importance", action="store_true",
                        help="skip permutation importance, which dominates the runtime")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    results = run(
        calibration_start=args.calibration_start,
        test_start=args.test_start,
        dataset=args.dataset,
        with_importance=not args.no_importance,
    )
    print(json.dumps(results["metrics"], indent=2))


if __name__ == "__main__":
    main()
