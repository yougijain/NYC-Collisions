"""NYC collision injury risk: the dashboard entry point.

Four tabs rather than st.navigation pages. Streamlit's navigation lives in
the sidebar, which collapses behind a hamburger on a phone, and a phone is
what most people open a link like this on. Tabs stay visible there.

The tabs themselves are in app/views/; this module only resolves the data,
draws the title and the filter control, and hands each tab what it needs.
"""

import os
import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))

from views import crashes, finding, method, watchlist  # noqa: E402
from views.common import connection, filter_control, query  # noqa: E402

st.set_page_config(
    page_title="NYC crash injury risk",
    page_icon="🚦",
    layout="wide",
)


def _mapbox_token() -> str | None:
    """Read the Mapbox key from Streamlit secrets, falling back to the env.

    st.secrets raises rather than returning None when no secrets.toml exists,
    which is the normal case for a fresh clone, so the lookup is guarded.
    """
    try:
        token = st.secrets.get("MAPBOX_API_KEY")
    except Exception:
        token = None
    return token or os.getenv("MAPBOX_API_KEY")


def basemap() -> str | None:
    """The Mapbox style to draw under the maps, or None for OpenStreetMap."""
    token = _mapbox_token()
    if not token:
        return None
    os.environ["MAPBOX_API_KEY"] = token
    return "mapbox://styles/mapbox/dark-v10"


def main():
    st.title("Which New York crashes hurt people")

    con = connection()
    bounds = query(con, "00_bounds.sql").iloc[0]
    params, boroughs = filter_control(bounds)
    metrics = query(con, "01_metrics.sql", params).iloc[0]

    style = basemap()
    tabs = st.tabs(["The finding", "The crashes", "Watchlist", "How it works"])
    with tabs[0]:
        finding.render(con, bounds, metrics)
    with tabs[1]:
        crashes.render(con, params, metrics, style)
    with tabs[2]:
        watchlist.render(boroughs, style)
    with tabs[3]:
        method.render()


if __name__ == "__main__":
    main()
