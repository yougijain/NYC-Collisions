"""Shared plumbing: the connection, cached queries, and the filter control."""

from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st

import db

QUERY_CACHE_TTL = 3600

# A browser cannot usefully render more points than this, and shipping them
# all would exhaust the app's memory on the full dataset.
MAP_POINT_LIMIT = 50_000


@st.cache_resource(show_spinner="Loading collision data...")
def connection():
    """Open the DuckDB connection once per app container."""
    return db.connect()


@st.cache_data(ttl=QUERY_CACHE_TTL, show_spinner=False)
def query(_con, sql_file: str, params: Optional[dict] = None) -> pd.DataFrame:
    """Run a query, caching on the SQL file and its parameters.

    The connection is prefixed with an underscore so Streamlit skips hashing
    it; the cache key is the filename and params, which is what actually
    identifies a result.
    """
    return db.query(_con, sql_file, params)


@st.cache_data(ttl=QUERY_CACHE_TTL, show_spinner=False)
def watchlist_data():
    """The committed watchlist, its factor table and the summary behind them."""
    return db.load_watchlist(), db.load_factor_risk(), db.load_watchlist_summary()


@st.cache_data(ttl=QUERY_CACHE_TTL, show_spinner=False)
def exposure_data():
    """Traffic volumes for the sites that have them, and their coverage."""
    return db.load_exposure(), db.load_exposure_summary()


def say(sentence: Optional[str]) -> None:
    """Put a chart's takeaway underneath it, if there is one to make."""
    if sentence:
        st.caption(sentence)


def filter_control(bounds: pd.Series) -> Tuple[dict, List[str]]:
    """Draw the date and borough filters and return what they select.

    In a popover rather than the sidebar: the sidebar is collapsed on a
    phone, so a reader there never discovers it, and on a desktop it opens
    by default and makes a control panel the first thing anybody sees.
    Selections live in session state so they survive switching tabs.
    """
    earliest = pd.Timestamp(bounds["min_datetime"]).date()
    latest = pd.Timestamp(bounds["max_datetime"]).date()
    all_boroughs = list(bounds["boroughs"])

    state = st.session_state.setdefault(
        "filters", {"dates": (earliest, latest), "boroughs": all_boroughs}
    )

    with st.popover("Filters", use_container_width=False):
        chosen_dates = st.date_input(
            "Date range", state["dates"],
            min_value=earliest, max_value=latest,
        )
        # Streamlit returns a 1-tuple while the reader is picking the second
        # date, so the old value stands until both are in.
        if isinstance(chosen_dates, (list, tuple)) and len(chosen_dates) == 2:
            state["dates"] = tuple(chosen_dates)

        state["boroughs"] = st.multiselect(
            "Boroughs", all_boroughs, default=state["boroughs"]
        )
        if st.button("Reset", use_container_width=True):
            state["dates"] = (earliest, latest)
            state["boroughs"] = all_boroughs
            st.rerun()

    boroughs = state["boroughs"] or all_boroughs
    start, end = state["dates"]
    if not state["boroughs"]:
        st.caption("No borough selected, so all of them are shown.")

    narrowed = (start, end) != (earliest, latest) or len(boroughs) != len(all_boroughs)
    if narrowed:
        st.caption(
            f"Showing {start} to {end} in "
            + (", ".join(b.title() for b in boroughs) if len(boroughs) < 4
               else f"{len(boroughs)} boroughs")
            + "."
        )

    return db.filter_params(start, end, boroughs, MAP_POINT_LIMIT), boroughs
