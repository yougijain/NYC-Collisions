import os
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))

import db  # noqa: E402
import narrative  # noqa: E402

# A browser cannot usefully render more points than this, and shipping them
# all would exhaust the app's memory on the full dataset.
MAP_POINT_LIMIT = 50_000
QUERY_CACHE_TTL = 3600

# Enough of the watchlist to see on a map without it becoming a blur.
WATCHLIST_MAP_SITES = 60

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


@st.cache_data(ttl=QUERY_CACHE_TTL, show_spinner=False)
def load_watchlist():
    """The committed watchlist, its factor table and the summary behind them."""
    return db.load_watchlist(), db.load_factor_risk(), db.load_watchlist_summary()


def _say(sentence: str | None) -> None:
    """Put a chart's takeaway underneath it, if there is one to make."""
    if sentence:
        st.caption(sentence)


def overview(con, params, metrics):
    """Counts, trends and the crash heatmap, over the sidebar's filters."""
    # ---------------- Key metrics ----------------
    crashes = int(metrics["crash_count"])
    harmful = int(metrics["harmful_crash_count"])
    injured = int(metrics["total_injuries"])
    killed = int(metrics["total_fatalities"])

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Crashes", f"{crashes:,}")
    col2.metric(
        "Crashes that hurt someone", f"{harmful:,}",
        help="At least one person injured or killed.",
    )
    col3.metric("People injured", f"{injured:,}")
    col4.metric("People killed", f"{killed:,}")

    if crashes:
        st.caption(
            f"That is {injured / crashes:.2f} injuries per crash across all "
            f"of them, or {(injured / harmful if harmful else 0):.2f} per "
            f"crash that caused one."
        )

    st.divider()

    # ---------------- Charts ----------------
    # Each chart carries the sentence it is making. app/narrative.py computes
    # those from the same frame the chart is drawn from, so the words cannot
    # drift from the picture or go stale when a filter changes.
    by_borough = run(con, "02_aggregate.sql", params)
    st.subheader("Where people get hurt")
    st.bar_chart(by_borough.set_index("borough")["total_injuries"])
    _say(narrative.borough_takeaway(by_borough))

    by_hour = run(con, "03_time_analysis.sql", params)
    st.subheader("When crashes happen")
    st.line_chart(by_hour.set_index("hour_label")["crash_count"])
    _say(narrative.hour_takeaway(by_hour))

    trends = narrative.drop_partial_month(run(con, "04_trends.sql", params))
    trends["month"] = pd.to_datetime(trends["month"])

    st.subheader("Crashes and injuries, month by month")
    st.line_chart(trends.set_index("month")[["crash_count", "total_injuries"]])
    _say(narrative.trend_takeaway(trends))

    # On its own axis. Sharing one with injuries, which run two orders of
    # magnitude higher, flattened this into a line along the bottom -- the
    # series about people dying was the one you could not see.
    st.subheader("People killed, month by month")
    st.line_chart(trends.set_index("month")["total_fatalities"], color="#c1272d")
    _say(narrative.fatality_takeaway(trends))

    # ---------------- Crash heatmap ----------------
    st.subheader("Where the harmful crashes are")
    points = run(con, "05_map_points.sql", params)

    if points.empty:
        st.info("No geolocated crashes match the current filters.")
        return

    _say(narrative.map_takeaway(len(points), MAP_POINT_LIMIT, metrics))

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


def watchlist_map(sites: pd.DataFrame) -> None:
    """Plot the worst sites, scaled by how far they sit above expectation."""
    located = sites.dropna(subset=["latitude", "longitude"])
    if located.empty:
        st.info("No listed site has coordinates to plot.")
        return

    top = located.head(WATCHLIST_MAP_SITES).copy()
    # Radius reads as excess, floored so the mildest site is still clickable.
    top["radius"] = 120 + 1400 * top["excess"].clip(lower=0)
    top["excess_label"] = (top["excess"] * 100).round(1).astype(str) + " pts"

    st.pydeck_chart(
        pdk.Deck(
            map_style=BASEMAP,
            initial_view_state=pdk.ViewState(
                latitude=40.734, longitude=-73.9, zoom=9.7, pitch=0
            ),
            layers=(
                []
                if BASEMAP
                else [pdk.Layer(
                    "TileLayer",
                    data="https://c.tile.openstreetmap.org/{z}/{x}/{y}.png",
                    tile_size=256,
                )]
            ) + [pdk.Layer(
                "ScatterplotLayer",
                top,
                get_position=["longitude", "latitude"],
                get_radius="radius",
                get_fill_color=[255, 70, 40, 150],
                get_line_color=[255, 255, 255, 200],
                line_width_min_pixels=1,
                stroked=True,
                pickable=True,
            )],
            tooltip={
                "text": "{site}\n{crashes} crashes\n"
                        "{excess_label} above expectation",
                "style": {
                    "backgroundColor": "rgba(0, 0, 0, 0.8)",
                    "color": "white",
                },
            },
        )
    )


def watchlist(boroughs: list[str]) -> None:
    """Intersections whose crashes injure people more often than they should.

    Built over a fixed window by scripts/build_watchlist.py, so the sidebar's
    date range does not apply here; the borough selection does.
    """
    sites, factors, summary = load_watchlist()

    if sites.empty:
        st.warning(
            "No watchlist has been built yet. Run "
            "`python scripts/build_watchlist.py`."
        )
        return

    window = summary.get("window", {})
    st.subheader("Intersections worth a look")
    st.caption(
        f"{summary.get('sites', len(sites)):,} intersections with at least "
        f"{summary.get('min_site_crashes', '?')} crashes between "
        f"{window.get('from', '?')} and {window.get('to', '?')}. "
        "The sidebar date range does not apply to this tab."
    )

    st.markdown(
        "Every crash is scored for the probability it injured someone, from "
        "the **crash mix alone** — the vehicles, the contributing factors, "
        "the road type, the hour — and never from where it happened. A site's "
        "**excess** is how much more often its crashes actually injured "
        "someone than that mix accounts for. Sites are ordered by the "
        "conservative end of that estimate, so a site with 400 crashes is not "
        "outranked by one with 25 and a lucky run."
    )

    st.warning(
        "**This does not measure how dangerous an intersection is.** There "
        "are crashes in this data but no traffic counts, so there is no "
        "exposure denominator: a junction with many crashes may simply be a "
        "junction with many vehicles. Every rate here is per crash, never per "
        "vehicle passing through. Joining NYC DOT automated volume counts is "
        "what would turn this into a danger ranking. Until then it says one "
        "thing only — these sites injure people more often than their crash "
        "mix explains."
    )

    filtered = sites[sites["borough"].isin(boroughs)] if boroughs else sites
    if filtered.empty:
        st.info("No listed intersection is in the selected boroughs.")
        return

    minimum = st.slider(
        "Minimum crashes at the site",
        int(filtered["crashes"].min()),
        int(filtered["crashes"].max()),
        int(filtered["crashes"].min()),
        help="Raise this to trade coverage for confidence.",
    )
    shown = filtered[filtered["crashes"] >= minimum]
    if shown.empty:
        st.info("No listed intersection meets that crash count.")
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("Intersections listed", f"{len(shown):,}")
    col2.metric("Crashes behind them", f"{int(shown['crashes'].sum()):,}")
    col3.metric(
        "Worst excess",
        f"+{shown['excess'].max() * 100:.1f} pts",
        help="Percentage points above what the site's crash mix predicts.",
    )

    display = shown.rename(columns={
        "site": "Intersection",
        "borough": "Borough",
        "crashes": "Crashes",
        "observed_rate": "Injured",
        "expected_rate": "Expected",
        "base_rate": "Borough-hour base",
        "top_factors": "Most common factors",
    })
    # Shown in points rather than as a percentage: it is a difference
    # between two rates, and a reader who subtracts the two columns should
    # not find a third number disagreeing with them by the city-wide
    # centring.
    display["Excess (pts)"] = shown["excess"] * 100

    st.dataframe(
        display[[
            "Intersection", "Borough", "Crashes", "Injured", "Expected",
            "Borough-hour base", "Excess (pts)", "Most common factors",
        ]],
        hide_index=True,
        use_container_width=True,
        column_config={
            "Injured": st.column_config.NumberColumn(
                format="percent", help="Share of the site's crashes that injured someone"
            ),
            "Expected": st.column_config.NumberColumn(
                format="percent", help="What the site's crash mix predicts"
            ),
            "Borough-hour base": st.column_config.NumberColumn(
                format="percent", help="The baseline: this borough, at these hours"
            ),
            "Excess (pts)": st.column_config.NumberColumn(
                format="%+.1f",
                help="Percentage points of observed minus expected, after "
                     "centring so the city as a whole nets to zero",
            ),
        },
    )

    st.subheader(f"Worst {min(WATCHLIST_MAP_SITES, len(shown))} on the map")
    watchlist_map(shown)

    st.divider()
    st.subheader("Contributing factors by predicted injury risk")
    st.caption(
        "Across every scored crash. The predicted column is the model's; the "
        "observed column is what happened. They track closely, which is the "
        "calibration check worth trusting most."
    )

    if factors.empty:
        st.info("No factor table has been built yet.")
        return

    # Altair rather than st.bar_chart: the latter sorts its index
    # alphabetically, and the whole point of this chart is the order.
    st.altair_chart(
        alt.Chart(factors)
        .mark_bar(color="#e4572e")
        .encode(
            x=alt.X("predicted_rate:Q",
                    axis=alt.Axis(format="%", title="Predicted injury rate")),
            y=alt.Y("factor:N", sort="-x", title=None),
            tooltip=[
                alt.Tooltip("factor:N", title="Factor"),
                alt.Tooltip("crashes:Q", title="Crashes", format=","),
                alt.Tooltip("predicted_rate:Q", title="Predicted", format=".1%"),
                alt.Tooltip("observed_rate:Q", title="Observed", format=".1%"),
            ],
        )
        .properties(height=700),
        use_container_width=True,
    )
    st.dataframe(
        factors.rename(columns={
            "factor": "Contributing factor",
            "crashes": "Crashes",
            "predicted_rate": "Predicted injury rate",
            "observed_rate": "Observed injury rate",
        }),
        hide_index=True,
        use_container_width=True,
        column_config={
            "Predicted injury rate": st.column_config.NumberColumn(format="percent"),
            "Observed injury rate": st.column_config.NumberColumn(format="percent"),
        },
    )


def main():
    st.title("Which New York crashes hurt people")

    con = get_connection()
    bounds = run(con, "00_bounds.sql").iloc[0]

    min_date = pd.Timestamp(bounds["min_datetime"]).date()
    max_date = pd.Timestamp(bounds["max_datetime"]).date()
    all_boroughs = list(bounds["boroughs"])

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

    boroughs = st.sidebar.multiselect(
        "Boroughs", all_boroughs, default=all_boroughs
    )
    if not boroughs:
        st.warning("Select at least one borough.")
        st.stop()

    params = db.filter_params(start_date, end_date, boroughs, MAP_POINT_LIMIT)

    # The lede, before any chart. A reader who stops here should still leave
    # knowing what the data says.
    metrics = run(con, "01_metrics.sql", params).iloc[0]
    st.markdown(narrative.headline(bounds, metrics))
    st.caption(
        "Every crash the NYPD reported, cleaned nightly from NYC Open Data "
        "(h9gi-nx95) and scored by a model that estimates how likely each "
        "one was to hurt somebody. Use the watchlist tab for the "
        "intersections where more crashes do than their circumstances "
        "account for."
    )

    overview_tab, watchlist_tab = st.tabs(
        ["The crashes", "Injury risk watchlist"]
    )
    with overview_tab:
        overview(con, params, metrics)
    with watchlist_tab:
        watchlist(boroughs)


if __name__ == "__main__":
    main()
