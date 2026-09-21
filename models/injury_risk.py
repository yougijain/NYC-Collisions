"""Given a crash, what is the probability it injured someone.

Not a forecast. The features are what a responding officer writes down --
the vehicles, the contributing factors, the road, the hour -- so the model
scores a crash that has already happened and asks whether that combination
of circumstances usually hurts people. That is the question the watchlist
needs answered: it compares what a site's crash mix predicts against what
the site actually produced.

Four decisions worth stating up front.

**The split is by time.** A random split would let the model see crashes
from the same week it is scored on, and this dataset drifts hard: the share
of reported crashes that injured someone climbed from 0.30 in 2020 to 0.44
in 2024. Most of that is reporting practice rather than roads getting
deadlier -- the share of crashes logged to a street address instead of an
intersection rose over the same period, which is what a property-damage
report looks like when it stops being filed.

**There are three folds, not two.** Train, then a calibration year, then the
test period. The drift above means a model trained through 2023 predicts a
city-wide injury rate of 0.366 for a test period that ran at 0.432. Fitting a
single log-odds shift on the most recent training year closes most of that
gap without touching the test period, and the watchlist depends on it: the
watchlist subtracts predicted rates from observed ones, so a model that is
uniformly 7 points low makes every site in the city look dangerous.

**The baseline is the borough-by-hour injury rate**, not a coin flip. Anyone
can compute it in one SQL query, so a model that does not beat it is not
worth deploying. It sees every crash before the test period, the same as the
model, and gets reported next to it every time.

**Brier matters more than AUC here**, because the watchlist uses the
probabilities rather than only their order. A model can rank crashes
perfectly and still be uniformly wrong about how likely injury is.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

from models import features as F

# Fold boundaries. Train on crashes before the first, calibrate on the year
# between the two, hold out everything from the second onward.
DEFAULT_CALIBRATION_START = "2024-01-01"
DEFAULT_TEST_START = "2025-01-01"

# Deliberately modest. The features are coarse and the signal sits in
# low-order interactions, so a deeper model buys noise.
#
# max_iter is set high enough that early stopping is what ends training
# rather than the cap; it settles around 700 rounds. The curve is flat well
# before that -- 200 rounds gives ROC-AUC 0.7929 against 0.7943 at 716 -- so
# the extra rounds are cheap insurance, not a tuned choice.
DEFAULT_PARAMS: Dict = {
    "learning_rate": 0.06,
    "max_iter": 1200,
    "max_leaf_nodes": 31,
    "min_samples_leaf": 100,
    "l2_regularization": 1.0,
    "early_stopping": True,
    "validation_fraction": 0.1,
    "n_iter_no_change": 20,
    "random_state": 0,
}

BASELINE_NAME = "Borough x hour base rate"
MODEL_NAME = "Gradient boosting"

_EPSILON = 1e-6


def _logit(p: np.ndarray) -> np.ndarray:
    """Log-odds, with probabilities held off 0 and 1."""
    clipped = np.clip(p, _EPSILON, 1 - _EPSILON)
    return np.log(clipped / (1 - clipped))


def _sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-z))


# --- folds -------------------------------------------------------------

@dataclass
class Split:
    """A time-ordered division of the crash record into three folds."""

    train: pd.DataFrame
    calibration: pd.DataFrame
    test: pd.DataFrame
    calibration_start: pd.Timestamp
    test_start: pd.Timestamp

    X_train: pd.DataFrame = field(init=False)
    y_train: pd.Series = field(init=False)
    X_calibration: pd.DataFrame = field(init=False)
    y_calibration: pd.Series = field(init=False)
    X_test: pd.DataFrame = field(init=False)
    y_test: pd.Series = field(init=False)

    def __post_init__(self) -> None:
        self.X_train, self.y_train = F.build(self.train), F.label(self.train)
        self.X_calibration = F.build(self.calibration)
        self.y_calibration = F.label(self.calibration)
        self.X_test, self.y_test = F.build(self.test), F.label(self.test)

    def describe(self) -> Dict:
        """Row counts, date ranges and injury rates for each fold."""
        def fold(rows: pd.DataFrame, y: pd.Series) -> Dict:
            when = pd.to_datetime(rows["crash_datetime"])
            return {
                "rows": int(len(rows)),
                "from": str(when.min().date()),
                "to": str(when.max().date()),
                "injury_rate": round(float(y.mean()), 4),
            }

        return {
            "calibration_start": str(self.calibration_start.date()),
            "test_start": str(self.test_start.date()),
            "train": fold(self.train, self.y_train),
            "calibration": fold(self.calibration, self.y_calibration),
            "test": fold(self.test, self.y_test),
        }


def split_by_time(
    df: pd.DataFrame,
    calibration_start: str = DEFAULT_CALIBRATION_START,
    test_start: str = DEFAULT_TEST_START,
) -> Split:
    """Divide crashes into a training past, a calibration year and a future.

    Args:
        df: Cleaned collision records.
        calibration_start: First crash date used for calibration.
        test_start: First crash date held out entirely.

    Returns:
        The split, with features and labels built for each fold.

    Raises:
        ValueError: If the boundaries are out of order or leave a fold empty.
    """
    first, second = pd.Timestamp(calibration_start), pd.Timestamp(test_start)
    if first >= second:
        raise ValueError(
            f"calibration_start ({calibration_start}) must precede "
            f"test_start ({test_start})"
        )

    when = pd.to_datetime(df["crash_datetime"])
    folds = {
        "train": df[when < first],
        "calibration": df[(when >= first) & (when < second)],
        "test": df[when >= second],
    }
    empty = [name for name, rows in folds.items() if rows.empty]
    if empty:
        raise ValueError(
            f"Boundaries {calibration_start}/{test_start} leave "
            f"{', '.join(empty)} empty; the dataset spans "
            f"{when.min().date()} to {when.max().date()}."
        )

    return Split(**folds, calibration_start=first, test_start=second)


# --- predictors --------------------------------------------------------

class BoroughHourBaseline:
    """The injury rate for a borough at an hour, and nothing else.

    This is the comparison that makes the model's numbers mean something. It
    needs no library and no training loop, and it is what a competent analyst
    would produce in an afternoon.
    """

    def __init__(self, min_count: int = 30):
        """
        Args:
            min_count: Cells thinner than this fall back to a wider average,
                so a borough-hour with nine crashes cannot claim a rate.
        """
        self.min_count = min_count
        self.rates_: Optional[pd.Series] = None
        self.borough_rates_: Optional[pd.Series] = None
        self.overall_rate_: Optional[float] = None

    def fit(self, X: pd.DataFrame, y: pd.Series) -> "BoroughHourBaseline":
        """Learn the rate table.

        Args:
            X: Feature frame carrying `borough` and `hour`.
            y: Whether each crash injured someone.

        Returns:
            self.
        """
        frame = pd.DataFrame(
            {"borough": X["borough"].astype(str), "hour": X["hour"], "y": y.to_numpy()}
        )
        cells = frame.groupby(["borough", "hour"], observed=True)["y"].agg(["mean", "size"])

        self.rates_ = cells.loc[cells["size"] >= self.min_count, "mean"]
        self.borough_rates_ = frame.groupby("borough", observed=True)["y"].mean()
        self.overall_rate_ = float(frame["y"].mean())
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Look up P(injury) for each crash.

        Args:
            X: Feature frame carrying `borough` and `hour`.

        Returns:
            One probability per row.

        Raises:
            RuntimeError: If called before `fit`.
        """
        if self.rates_ is None:
            raise RuntimeError("BoroughHourBaseline must be fitted first")

        keys = pd.MultiIndex.from_arrays(
            [X["borough"].astype(str), X["hour"]], names=["borough", "hour"]
        )
        rates = pd.Series(self.rates_.reindex(keys).to_numpy(), index=X.index)

        # A borough-hour unseen or too thin in training falls back to the
        # borough, then to the city.
        fallback = X["borough"].astype(str).map(self.borough_rates_)
        return rates.fillna(fallback).fillna(self.overall_rate_).to_numpy()


class LevelShift:
    """One additive correction in log-odds, and nothing else.

    The drift in this dataset is almost purely a level change: fitting a full
    Platt scaling on the calibration year returns a slope of 0.967, and both
    it and isotonic regression land within 0.0001 Brier of this. Given that,
    one parameter that cannot overfit and can be read out loud -- "the city
    ran 0.34 log-odds hotter than the model was trained for" -- beats two
    that cannot.
    """

    def __init__(self) -> None:
        self.shift_: float = 0.0

    def fit(self, probabilities: np.ndarray, y: pd.Series) -> "LevelShift":
        """Find the shift that matches the mean prediction to the mean outcome.

        The mean of a sigmoid is monotone in the shift, so bisection finds it
        exactly and without a solver dependency.

        Args:
            probabilities: Uncalibrated P(injury) on the calibration fold.
            y: Observed outcomes on the same fold.

        Returns:
            self.
        """
        target = float(np.asarray(y, dtype=float).mean())
        base = _logit(np.asarray(probabilities, dtype=float))

        low, high = -10.0, 10.0
        for _ in range(100):
            middle = (low + high) / 2
            if _sigmoid(base + middle).mean() < target:
                low = middle
            else:
                high = middle

        self.shift_ = (low + high) / 2
        return self

    def transform(self, probabilities: np.ndarray) -> np.ndarray:
        """Apply the shift."""
        return _sigmoid(_logit(np.asarray(probabilities, dtype=float)) + self.shift_)


@dataclass
class InjuryRiskModel:
    """A fitted estimator plus its level correction: the thing you score with.

    Attributes:
        estimator: The gradient boosting model.
        level_shift: The correction fitted on the calibration fold.
        metadata: Provenance -- fold boundaries, row counts, rounds used.
    """

    estimator: HistGradientBoostingClassifier
    level_shift: LevelShift
    metadata: Dict = field(default_factory=dict)

    def predict_proba(self, X: pd.DataFrame, calibrated: bool = True) -> np.ndarray:
        """P(injury) for each crash.

        Args:
            X: A feature frame from `models.features.build`.
            calibrated: Apply the level shift. False exposes the raw model,
                which is only useful for showing what the shift is worth.

        Returns:
            One probability per row.
        """
        raw = self.estimator.predict_proba(X)[:, 1]
        return self.level_shift.transform(raw) if calibrated else raw

    def save(self, path: Path) -> Path:
        """Persist the model and its correction together.

        Args:
            path: Destination `.joblib` file.

        Returns:
            The path written.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self, path)
        return path

    @staticmethod
    def load(path: Path) -> "InjuryRiskModel":
        """Read back a model saved by `save`."""
        return joblib.load(Path(path))


def train(X: pd.DataFrame, y: pd.Series, **overrides) -> HistGradientBoostingClassifier:
    """Fit the gradient boosting estimator.

    Categorical columns are handed over as pandas `category` dtype and split
    natively, so nothing is one-hot expanded and "Sedan" stays "Sedan" in
    whatever the model reports about itself.

    Early stopping holds out a random 10% of the training fold to pick the
    number of boosting rounds. That is the one random split here; it never
    touches the calibration or test periods, and no reported number comes
    from it.

    Args:
        X: Training features.
        y: Training labels.
        **overrides: Any HistGradientBoostingClassifier parameter.

    Returns:
        The fitted estimator.
    """
    params = {**DEFAULT_PARAMS, **overrides}
    model = HistGradientBoostingClassifier(categorical_features="from_dtype", **params)
    model.fit(X, y)
    return model


def fit(split: Split, **overrides) -> InjuryRiskModel:
    """Train on the training fold and calibrate on the calibration fold.

    Args:
        split: A time-ordered split.
        **overrides: Model parameter overrides.

    Returns:
        The fitted, calibrated model.
    """
    estimator = train(split.X_train, split.y_train, **overrides)
    shift = LevelShift().fit(
        estimator.predict_proba(split.X_calibration)[:, 1], split.y_calibration
    )
    return InjuryRiskModel(
        estimator=estimator,
        level_shift=shift,
        metadata={
            "boosting_rounds": int(estimator.n_iter_),
            "level_shift": round(shift.shift_, 4),
            "features": list(F.FEATURES),
            **split.describe(),
        },
    )


# --- measurement -------------------------------------------------------

def evaluate(y_true: pd.Series, probabilities: np.ndarray) -> Dict[str, float]:
    """Measure one set of predictions.

    Args:
        y_true: The observed outcomes.
        probabilities: P(injury) per row.

    Returns:
        ROC-AUC, Brier score, log loss, and the mean predicted rate next to
        the observed one, which is what shows a level problem.
    """
    truth = np.asarray(y_true, dtype=float)
    return {
        "roc_auc": round(float(roc_auc_score(truth, probabilities)), 4),
        "brier": round(float(brier_score_loss(truth, probabilities)), 4),
        "log_loss": round(float(log_loss(truth, probabilities)), 4),
        "mean_predicted": round(float(np.mean(probabilities)), 4),
        "observed_rate": round(float(truth.mean()), 4),
    }


def compare(y_true: pd.Series, predictions: Dict[str, np.ndarray]) -> Dict[str, Dict]:
    """Score several predictors against the same outcomes.

    Args:
        y_true: The observed outcomes on the test period.
        predictions: Named probability arrays. The baseline's name is used as
            the reference for the skill score.

    Returns:
        A row of metrics per predictor. Everything but the baseline also
        carries its Brier skill score: the share of the baseline's squared
        error it removes.
    """
    results = {name: evaluate(y_true, p) for name, p in predictions.items()}

    reference = results.get(BASELINE_NAME, {}).get("brier")
    if reference:
        for name, row in results.items():
            if name != BASELINE_NAME:
                row["brier_skill_vs_baseline"] = round(1 - row["brier"] / reference, 4)
    return results


def calibration(
    y_true: pd.Series, probabilities: np.ndarray, bins: int = 10
) -> pd.DataFrame:
    """Bin predictions by confidence and check what actually happened.

    Equal-count bins rather than equal-width: predictions cluster, and
    equal-width bins leave the busy part of the range in one bucket and the
    empty part in nine.

    Args:
        y_true: The observed outcomes.
        probabilities: P(injury) per row.
        bins: Number of quantile bins.

    Returns:
        One row per bin, with the mean prediction, the observed rate and the
        number of crashes behind it.
    """
    frame = pd.DataFrame(
        {"p": probabilities, "y": np.asarray(y_true, dtype=float)}
    )
    # duplicates="drop" matters for the baseline, whose 144 distinct values
    # cannot always be cut into the requested number of equal-count bins.
    frame["bin"] = pd.qcut(frame["p"], bins, labels=False, duplicates="drop")

    return (
        frame.groupby("bin", observed=True)
        .agg(predicted=("p", "mean"), observed=("y", "mean"), crashes=("y", "size"))
        .reset_index(drop=True)
    )
