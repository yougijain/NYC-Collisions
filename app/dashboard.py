import os
import sys
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))

import db  # noqa: E402

# A browser cannot usefully render more points than this, and shipping them
# all would exhaust the app's memory on the full dataset.
MAP_POINT_LIMIT = 50_000
QUERY_CACHE_TTL = 3600

st.set_page_config(page_title="NYC Collisions Dashboard", layout="wide")

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


mapbox_token = _mapbox_token()
if mapbox_token:
    os.environ["MAPBOX_API_KEY"] = mapbox_token
    BASEMAP = "mapbox://styles/mapbox/dark-v10"
else:
    # Without a token the OpenStreetMap TileLayer below provides the basemap.
    BASEMAP = None


@st.cache_resource(show_spinner="Loading collision data...")
def get_connection():
    """Open the DuckDB connection once per app container."""
    return db.connect()


@st.cache_data(ttl=QUERY_CACHE_TTL, show_spinner=False)
def run(_con, sql_file: str, params: dict | None = None) -> pd.DataFrame:
    """Run a query, caching on the SQL file and its parameters.

    The connection is prefixed with an underscore so Streamlit skips hashing
    it; the cache key is the filename and params, which is what actually
    identifies a result.
    """
    return db.query(_con, sql_file, params)


def main():
    st.title("NYC Collisions Dashboard")

    con = get_connection()
    bounds = run(con, "00_bounds.sql").iloc[0]

    min_date = pd.Timestamp(bounds["min_datetime"]).date()
    max_date = pd.Timestamp(bounds["max_datetime"]).date()
    all_boroughs = list(bounds["boroughs"])

    st.caption(
        f"{int(bounds['total_crashes']):,} crashes  ·  "
        f"{min_date} to {max_date}  ·  source: NYC Open Data (h9gi-nx95)"
    )

    st.sidebar.header("Filter Options")
    date_range = st.sidebar.date_input(
        "Date range", (min_date, max_date),
        min_value=min_date, max_value=max_date,
    )
    # Streamlit returns a 1-tuple while the user is picking the second date.
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = min_date, max_date

    selected = st.sidebar.multiselect(
        "Boroughs", all_boroughs, default=all_boroughs
    )
    if not selected:
        st.warning("Select at least one borough.")
        st.stop()

    params = db.filter_params(start_date, end_date, selected, MAP_POINT_LIMIT)

    # ---------------- Key metrics ----------------
    metrics = run(con, "01_metrics.sql", params).iloc[0]
    st.subheader("Key Metrics")
    st.caption("Crashes causing injury or death, within the selected filters.")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Incidents", f"{int(metrics['crash_count']):,}")
    col2.metric("Total Injuries", f"{int(metrics['total_injuries']):,}")
    col3.metric("Total Fatalities", f"{int(metrics['total_fatalities']):,}")
    avg = metrics["avg_injuries_per_crash"]
    col4.metric("Avg Injuries/Crash", f"{avg:.2f}" if pd.notna(avg) else "0.00")

    st.divider()

    # ---------------- Charts ----------------
    by_borough = run(con, "02_aggregate.sql", params)
    st.subheader("Total Injuries by Borough")
    st.bar_chart(by_borough.set_index("borough")["total_injuries"])

    by_hour = run(con, "03_time_analysis.sql", params)
    st.subheader("Crashes by Hour (AM/PM)")
    st.line_chart(by_hour.set_index("hour_label")["crash_count"])

    trends = run(con, "04_trends.sql", params)
    st.subheader("Monthly Crash Trends")
    trends["month"] = pd.to_datetime(trends["month"])
    st.line_chart(
        trends.set_index("month")[
            ["crash_count", "total_injuries", "total_fatalities"]
        ]
    )

    # ---------------- Crash heatmap ----------------
    st.subheader("Crash Heatmap")
    points = run(con, "05_map_points.sql", params)

    if points.empty:
        st.info("No geolocated crashes match the current filters.")
        return

    if len(points) >= MAP_POINT_LIMIT:
        st.caption(
            f"Showing a random {MAP_POINT_LIMIT:,}-point sample of the "
            f"matching crashes."
        )

    view_state = pdk.ViewState(
        latitude=40.734,
        longitude=-73.9,
        zoom=9.7,   # lower = zoomed out
        pitch=0,    # 0 = top-down
    )

    heat = pdk.Layer(
        "HeatmapLayer",
        points,
        get_position=["longitude", "latitude"],
        pickable=True,
        radius_pixels=30,   # radius of influence per point
        intensity=1.4,      # heat strength multiplier
        threshold=0.3,      # minimum normalized weight to render
        color_range=[       # gradient stops [R, G, B, A]
            [0,   0,   0,   0],
            [0,   255, 0,   70],
            [255, 255, 0,   95],
            [255, 0,   0,   130],
        ],
    )

    # Drawn only when there is no Mapbox token to supply a basemap.
    tiles = pdk.Layer(
        "TileLayer",
        data="https://c.tile.openstreetmap.org/{z}/{x}/{y}.png",
        tile_size=256,
        opacity=1.0,
    )

    # Transparent, purely to carry tooltips for the heatmap.
    scatter = pdk.Layer(
        "ScatterplotLayer",
        points,
        get_position=["longitude", "latitude"],
        get_radius=100,
        get_fill_color=[0, 0, 0, 0],
        pickable=True,
    )

    layers = [heat, scatter] if BASEMAP else [tiles, heat, scatter]
    st.pydeck_chart(
        pdk.Deck(
            map_style=BASEMAP,
            initial_view_state=view_state,
            layers=layers,
            tooltip={
                "text": "Date: {crash_datetime_str}\nBorough: {borough}",
                "style": {
                    "backgroundColor": "rgba(0, 0, 0, 0.8)",
                    "color": "white",
                },
            },
        )
    )

    st.subheader("Sample of Filtered Records")
    st.dataframe(points.head(7))


if __name__ == "__main__":
    main()
