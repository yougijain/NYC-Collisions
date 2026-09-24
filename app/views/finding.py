"""The landing tab: what this project found, in ninety seconds."""

import pandas as pd
import streamlit as st

import narrative
from views.common import exposure_claim, exposure_data, query, watchlist_data

# Enough of the watchlist to show what the list is, not enough to scroll.
PREVIEW_SITES = 6

def exposure_caveat() -> str:
    """What the list is not. The middle of it is measured, not asserted."""
    return (
        "**This is not a ranking of dangerous intersections.** "
        + exposure_claim(*exposure_data())
        + " Every rate here is per crash, never per vehicle passing "
          "through. It says one thing only: these sites injure people more "
          "often than the kinds of crashes happening there would predict."
    )


def render(con, bounds: pd.Series, metrics: pd.Series) -> None:
    """Lead with the finding, then the evidence, then the caveat."""
    st.markdown(narrative.headline(bounds, metrics))

    sites, _, summary = watchlist_data()
    if sites.empty:
        st.info(
            "The watchlist has not been built yet. Run "
            "`python scripts/build_watchlist.py`."
        )
        return

    window = summary.get("window", {})
    st.subheader("The intersections worth looking at")
    st.markdown(
        f"Every crash is scored for how likely it was to hurt somebody, "
        f"from the **crash mix alone**: the vehicles, the contributing "
        f"factors, the road type, the hour. Never from where it "
        f"happened. Comparing that against what actually happened leaves "
        f"**{summary.get('sites', len(sites)):,} intersections** where more "
        f"crashes injured someone than the kinds of crashes there would predict."
    )

    preview = sites.head(PREVIEW_SITES).copy()
    preview["Excess (pts)"] = preview["excess"] * 100
    st.dataframe(
        preview.rename(columns={
            "site": "Intersection",
            "borough": "Borough",
            "crashes": "Crashes",
            "observed_rate": "Hurt someone",
            "expected_rate": "Expected",
        })[["Intersection", "Borough", "Crashes", "Hurt someone",
            "Expected", "Excess (pts)"]],
        hide_index=True,
        use_container_width=True,
        column_config={
            "Hurt someone": st.column_config.NumberColumn(format="percent"),
            "Expected": st.column_config.NumberColumn(format="percent"),
            "Excess (pts)": st.column_config.NumberColumn(format="%+.1f"),
        },
    )
    st.caption(
        f"Scored over {summary.get('scored_crashes', 0):,} crashes from "
        f"{window.get('from', '?')} to {window.get('to', '?')}. The full "
        f"list, the map and the contributing factors are in the watchlist "
        f"tab."
    )

    st.warning(exposure_caveat())

    st.caption(
        "The crashes tab has the counts and trends behind all of this. "
        "How it works has the model, what it beats, and what it does not "
        "prove."
    )
