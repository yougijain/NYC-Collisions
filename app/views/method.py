"""The method tab: how the number was produced, and what it does not prove.

A reader who does not trust the finding needs this without leaving for
GitHub. Every figure on it is read from the same generated artifacts the
README quotes, so the page cannot drift from the model that is actually
serving it.
"""

import json
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import streamlit as st

import db
import palette
from views.common import exposure_claim, exposure_data

ROOT = Path(__file__).resolve().parent.parent.parent
METRICS_PATH = ROOT / "reports" / "injury_risk" / "metrics.json"
CALIBRATION_PLOT = ROOT / "reports" / "injury_risk" / "calibration.png"

BASELINE = "Borough x hour base rate"
MODEL = "Gradient boosting"

PIPELINE = """
NYC Open Data (h9gi-nx95)
     │  paginated fetch, stable ordering
     ▼
  clean  ── reconcile the API's swapped street columns
     │      place missing boroughs from coordinates
     ▼
  merge + dedupe on collision_id  ──►  Parquet, published weekly
     │
     ├──►  this dashboard (DuckDB over the Parquet)
     │
     └──►  injury risk model  ──►  intersection watchlist
"""

# Everything here is fixed prose. The exposure limitation is not -- it is
# built by limitations() below, because how far the traffic counts reach
# changes every time they are refetched.
LIMITATIONS = [
    (
        "Reported crashes are not all crashes, and the gap moves",
        "The share of reported crashes that injured someone climbed from "
        "0.30 in 2020 to 0.44 in 2024. New York's roads did not get 50% "
        "more dangerous in four years — fewer property-damage-only crashes "
        "are being filed, which mechanically raises the share of what is "
        "left that hurt somebody.",
    ),
    (
        "The features are a person's judgement, written after the fact",
        "\"Driver Inattention/Distraction\" is an officer's call, and a "
        "quarter of crashes get \"Unspecified\". The model partly learns "
        "how reports get written, not only what happened.",
    ),
    (
        "Nothing here is causal",
        "\"Unsafe speed raises predicted injury risk\" is a statement about "
        "what co-occurs in crash reports. It is not an estimate of what "
        "would happen if speeds fell.",
    ),
]


def _metrics() -> Optional[Dict]:
    if not METRICS_PATH.exists():
        return None
    return json.loads(METRICS_PATH.read_text(encoding="utf-8"))



def limitations() -> list:
    """The caveats, with the exposure one measured from the current join.

    It led the list as "no exposure denominator" long after the denominator
    arrived, because it was prose in a constant while the watchlist tab was
    reading the join. Now it is built from the same numbers.
    """
    exposure, summary = exposure_data()
    covered = (
        "The exposure denominator is partial"
        if not exposure.empty and summary
        else "No exposure denominator"
    )
    return [(
        covered,
        exposure_claim(exposure, summary)
        + " Every rate the model produces is per crash, never per vehicle "
          "passing through, so a busy intersection and a dangerous one can "
          "still look alike. Closing the gap needs a count at every "
          "approach to a junction, taken in the window the crashes are "
          "drawn from; DOT's counts are week-long deployments, not a "
          "network.",
    )] + LIMITATIONS


def render() -> None:
    """Explain the pipeline, the model, and the limits, from the artifacts."""
    st.subheader("How a crash gets scored")
    st.code(PIPELINE.strip(), language=None)
    st.caption(
        "The dataset rebuilds itself every Monday from NYC Open Data, "
        "retrains the model, rescores the watchlist and commits the "
        "result. Nothing on this page is typed in by hand."
    )

    metrics = _metrics()
    if not metrics:
        st.info(
            "No model report yet. Run `python scripts/train_injury_risk.py`."
        )
        return

    st.divider()
    st.subheader("Is the model worth having")
    st.markdown(
        "The comparison that matters is not against nothing, it is against "
        "what an analyst produces in one SQL query: the injury rate for "
        "this borough at this hour. A model that cannot beat that is not "
        "worth deploying."
    )

    rows = metrics["metrics"]
    left, right = st.columns(2)
    left.metric(
        "Baseline ROC-AUC", f"{rows[BASELINE]['roc_auc']:.3f}",
        border=True, help="Borough and hour, and nothing else.",
    )
    right.metric(
        "Model ROC-AUC", f"{rows[MODEL]['roc_auc']:.3f}",
        delta=f"{rows[MODEL]['roc_auc'] - rows[BASELINE]['roc_auc']:+.3f}",
        border=True,
    )
    left.metric("Baseline Brier", f"{rows[BASELINE]['brier']:.4f}", border=True)
    right.metric(
        "Model Brier", f"{rows[MODEL]['brier']:.4f}",
        delta=f"{rows[MODEL]['brier'] - rows[BASELINE]['brier']:+.4f}",
        delta_color="inverse", border=True,
        help="Lower is better: it measures how far the probabilities are "
             "from what happened.",
    )
    st.caption(
        f"Brier matters more than ROC-AUC here, because the watchlist "
        f"subtracts predicted rates from observed ones — the probabilities "
        f"have to be right, not just ordered right. The model removes "
        f"{rows[MODEL].get('brier_skill_vs_baseline', 0):.1%} of the "
        f"baseline's squared error."
    )

    st.divider()
    st.subheader("Trained on the past, tested on the future")
    split = metrics["split"]
    st.dataframe(
        pd.DataFrame([
            {
                "Fold": name.capitalize(),
                "Period": f"{fold['from']} to {fold['to']}",
                "Crashes": fold["rows"],
                "Injury rate": fold["injury_rate"],
            }
            for name, fold in (("train", split["train"]),
                               ("calibration", split["calibration"]),
                               ("test", split["test"]))
        ]),
        hide_index=True,
        use_container_width=True,
        column_config={
            "Crashes": st.column_config.NumberColumn(format="localized"),
            "Injury rate": st.column_config.NumberColumn(format="percent"),
        },
    )
    st.caption(
        "A random split would let the model see crashes from the same week "
        "it is scored on. The injury rate climbs steadily across these "
        "folds, which is the drift a random split would hide — and the "
        "reason for a calibration year between training and test."
    )

    if CALIBRATION_PLOT.exists():
        st.subheader("Are the probabilities honest")
        st.image(str(CALIBRATION_PLOT), use_container_width=True)
        st.caption(
            "Predicted against observed, in equal-count bins. On the "
            "diagonal means a crash the model calls 70% likely to hurt "
            "somebody does so about 70% of the time. The baseline barely "
            "leaves the middle of the range, which is what no "
            "discrimination looks like."
        )

    importance = metrics.get("feature_importance")
    if importance:
        st.divider()
        st.subheader("What the model is actually using")
        st.bar_chart(
            pd.DataFrame(importance).set_index("feature")["auc_drop"],
            horizontal=True,
            color=palette.SERIES,
        )
        st.caption(
            "Test ROC-AUC lost when each feature is shuffled. What was hit "
            "and why dominates; when and where barely register — which is "
            "why the borough-and-hour baseline does so badly."
        )

    st.divider()
    st.subheader("What this does not prove")
    for title, body in limitations():
        with st.expander(title):
            st.write(body)

    facts = db.load_watchlist_summary()
    built = metrics.get("dataset", {})
    st.caption(
        f"Model trained on dataset version {built.get('dataset_version', '?')}, "
        f"built {str(built.get('built_at', 'unknown'))[:19]}. "
        f"Watchlist scored over "
        f"{facts.get('scored_crashes', 0):,} crashes."
    )
