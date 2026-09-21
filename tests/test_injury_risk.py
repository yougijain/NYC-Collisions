"""Tests for the split, the baseline, the calibration and the model.

Everything here trains on the committed seed with a shortened round count.
The point is that the pieces behave, not that the seed reproduces the
figures in reports/injury_risk/, which come from the full build.
"""

import numpy as np
import pandas as pd
import pytest

from models import features as F
from models import injury_risk as IR

# The seed is a twentieth of the full dataset, so a full-length fit buys
# nothing a test can see.
FAST = {"max_iter": 60}


@pytest.fixture(scope="module")
def crashes(dataset_path):
    return pd.read_parquet(dataset_path)


@pytest.fixture(scope="module")
def split(crashes):
    return IR.split_by_time(crashes)


@pytest.fixture(scope="module")
def model(split):
    return IR.fit(split, **FAST)


@pytest.fixture(scope="module")
def baseline(split):
    return IR.BoroughHourBaseline().fit(split.X_train, split.y_train)


# --- the split ---------------------------------------------------------

def test_the_folds_partition_the_dataset(crashes, split):
    assert len(split.train) + len(split.calibration) + len(split.test) == len(crashes)


def test_no_crash_lands_in_two_folds(split):
    ids = pd.concat([
        split.train["collision_id"],
        split.calibration["collision_id"],
        split.test["collision_id"],
    ])
    assert not ids.duplicated().any()


def test_the_folds_run_forwards(split):
    train_end = pd.to_datetime(split.train["crash_datetime"]).max()
    calibration_start = pd.to_datetime(split.calibration["crash_datetime"]).min()
    calibration_end = pd.to_datetime(split.calibration["crash_datetime"]).max()
    test_start = pd.to_datetime(split.test["crash_datetime"]).min()

    assert train_end < calibration_start
    assert calibration_end < test_start


def test_no_test_crash_predates_the_boundary(split):
    assert pd.to_datetime(split.test["crash_datetime"]).min() >= split.test_start


def test_boundaries_out_of_order_are_rejected(crashes):
    with pytest.raises(ValueError, match="must precede"):
        IR.split_by_time(crashes, "2025-01-01", "2024-01-01")


def test_a_boundary_that_empties_a_fold_is_rejected(crashes):
    with pytest.raises(ValueError, match="empty"):
        IR.split_by_time(crashes, "2019-01-01", "2019-06-01")


def test_the_split_describes_itself(split):
    described = split.describe()
    assert described["train"]["rows"] == len(split.train)
    assert 0 < described["test"]["injury_rate"] < 1


# --- the baseline ------------------------------------------------------

def test_the_baseline_returns_a_probability_per_crash(baseline, split):
    predicted = baseline.predict_proba(split.X_test)
    assert len(predicted) == len(split.X_test)
    assert ((predicted > 0) & (predicted < 1)).all()


def test_the_baseline_reproduces_a_cell_it_learned(split):
    fitted = IR.BoroughHourBaseline(min_count=1).fit(split.X_train, split.y_train)
    frame = pd.DataFrame({
        "borough": split.X_train["borough"].astype(str),
        "hour": split.X_train["hour"],
        "y": split.y_train.to_numpy(),
    })
    cell = frame[(frame["borough"] == "BROOKLYN") & (frame["hour"] == 8)]

    asked = pd.DataFrame({"borough": pd.Categorical(["BROOKLYN"]), "hour": [8]})
    assert fitted.predict_proba(asked)[0] == pytest.approx(cell["y"].mean())


def test_a_borough_never_seen_falls_back_to_the_city(baseline):
    asked = pd.DataFrame({"borough": pd.Categorical(["ATLANTIS"]), "hour": [3]})
    assert baseline.predict_proba(asked)[0] == pytest.approx(baseline.overall_rate_)


def test_a_thin_cell_does_not_get_to_claim_a_rate(split):
    """Nine crashes in a borough-hour is not a rate, it is nine crashes."""
    strict = IR.BoroughHourBaseline(min_count=10**9).fit(split.X_train, split.y_train)
    assert strict.rates_.empty

    predicted = strict.predict_proba(split.X_test)
    boroughs = split.X_test["borough"].astype(str)
    assert predicted == pytest.approx(boroughs.map(strict.borough_rates_).to_numpy())


def test_predicting_before_fitting_is_an_error(split):
    with pytest.raises(RuntimeError):
        IR.BoroughHourBaseline().predict_proba(split.X_test)


# --- the level shift ---------------------------------------------------

def test_the_shift_matches_the_mean_prediction_to_the_mean_outcome():
    probabilities = np.linspace(0.05, 0.6, 5000)
    y = pd.Series(np.random.default_rng(0).random(5000) < 0.7)

    shifted = IR.LevelShift().fit(probabilities, y).transform(probabilities)
    assert shifted.mean() == pytest.approx(y.mean(), abs=1e-6)


def test_a_model_already_at_the_right_level_is_left_alone():
    rng = np.random.default_rng(1)
    probabilities = rng.uniform(0.05, 0.95, 20000)
    y = pd.Series(rng.random(20000) < probabilities)

    shift = IR.LevelShift().fit(probabilities, y)
    assert shift.shift_ == pytest.approx(0, abs=0.05)


def test_the_shift_cannot_reorder_anything():
    """It is monotone in log-odds, which is why it moves Brier and leaves
    ROC-AUC untouched."""
    probabilities = np.array([0.1, 0.3, 0.5, 0.9])
    shift = IR.LevelShift()
    shift.shift_ = 1.7

    moved = shift.transform(probabilities)
    assert (np.diff(moved) > 0).all()


# --- the model ---------------------------------------------------------

def test_the_model_predicts_a_probability_per_crash(model, split):
    predicted = model.predict_proba(split.X_test)
    assert len(predicted) == len(split.X_test)
    assert ((predicted >= 0) & (predicted <= 1)).all()


def test_the_model_beats_the_borough_hour_baseline(model, split, baseline):
    """The whole justification for the model. If this fails, ship the SQL."""
    scored = IR.compare(split.y_test, {
        IR.BASELINE_NAME: baseline.predict_proba(split.X_test),
        IR.MODEL_NAME: model.predict_proba(split.X_test),
    })
    assert scored[IR.MODEL_NAME]["roc_auc"] > scored[IR.BASELINE_NAME]["roc_auc"] + 0.1
    assert scored[IR.MODEL_NAME]["brier"] < scored[IR.BASELINE_NAME]["brier"]
    assert scored[IR.MODEL_NAME]["brier_skill_vs_baseline"] > 0


def test_calibration_only_moves_the_level(model, split):
    from sklearn.metrics import roc_auc_score

    raw = model.predict_proba(split.X_test, calibrated=False)
    adjusted = model.predict_proba(split.X_test)
    assert roc_auc_score(split.y_test, raw) == pytest.approx(
        roc_auc_score(split.y_test, adjusted)
    )


def test_calibration_lands_the_level_near_the_calibration_year(model, split):
    predicted = model.predict_proba(split.X_calibration).mean()
    assert predicted == pytest.approx(split.y_calibration.mean(), abs=0.01)


def test_the_model_records_how_it_was_built(model):
    assert model.metadata["features"] == F.FEATURES
    assert model.metadata["boosting_rounds"] > 0
    assert "level_shift" in model.metadata


def test_a_saved_model_scores_the_same_as_the_one_in_memory(model, split, tmp_path):
    path = model.save(tmp_path / "injury_risk.joblib")
    reloaded = IR.InjuryRiskModel.load(path)
    assert reloaded.predict_proba(split.X_test) == pytest.approx(
        model.predict_proba(split.X_test)
    )


# --- measurement -------------------------------------------------------

def test_a_perfect_predictor_scores_perfectly():
    y = pd.Series([True, False, True, False])
    scored = IR.evaluate(y, np.array([1.0, 0.0, 1.0, 0.0]))
    assert scored["roc_auc"] == 1.0
    assert scored["brier"] == 0.0


def test_skill_is_measured_against_the_baseline():
    y = pd.Series([True, False] * 50)
    scored = IR.compare(y, {
        IR.BASELINE_NAME: np.full(100, 0.5),
        IR.MODEL_NAME: np.where(y, 0.9, 0.1),
    })
    assert IR.BASELINE_NAME not in scored[IR.BASELINE_NAME]
    assert scored[IR.MODEL_NAME]["brier_skill_vs_baseline"] > 0.8


def test_the_calibration_table_accounts_for_every_crash(model, split):
    curve = IR.calibration(split.y_test, model.predict_proba(split.X_test))
    assert curve["crashes"].sum() == len(split.y_test)
    assert len(curve) == 10


def test_a_predictor_with_few_distinct_values_still_bins(baseline, split):
    """The baseline has at most one value per borough-hour, so equal-count
    bin edges collide and have to be dropped rather than raising."""
    curve = IR.calibration(split.y_test, baseline.predict_proba(split.X_test))
    assert 0 < len(curve) <= 10
    assert curve["crashes"].sum() == len(split.y_test)


# --- the training entry point ------------------------------------------

def test_training_writes_every_artifact_it_promises(dataset_path, tmp_path):
    """A smoke test on the script the workflow runs, not a check on the
    numbers: those come from the full build, not the seed."""
    import train_injury_risk

    reports = tmp_path / "reports"
    results = train_injury_risk.run(
        dataset=dataset_path,
        with_importance=False,
        report_dir=reports,
        model_path=tmp_path / "injury_risk.joblib",
    )

    assert (reports / "metrics.json").exists()
    assert (reports / "results.md").exists()
    assert (reports / "calibration.png").stat().st_size > 0
    assert (tmp_path / "injury_risk.joblib").exists()

    assert set(results["metrics"]) == {
        IR.BASELINE_NAME,
        IR.MODEL_NAME,
        train_injury_risk.UNCALIBRATED_NAME,
    }
    assert "Baseline against model" in (reports / "results.md").read_text()
