"""The exploration tab: counts, trends and the heatmap, under filters."""

import pandas as pd
import pydeck as pdk
import streamlit as st

import narrative
import palette
from views.common import MAP_POINT_LIMIT, query, say


def render(con, params: dict, metrics: pd.Series, basemap) -> None:
    """Draw every chart with the sentence it is making underneath it."""
    crashes = int(metrics["crash_count"])
    if not crashes:
        st.info("No crashes match the current filters.")
        return

    harmful = int(metrics["harmful_crash_count"])
    injured = int(metrics["total_injuries"])
    killed = int(metrics["total_fatalities"])

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Crashes", f"{crashes:,}", border=True)
    col2.metric("Hurt someone", f"{harmful:,}", border=True,
                help="At least one person injured or killed.")
    col3.metric("People injured", f"{injured:,}", border=True)
    col4.metric("People killed", f"{killed:,}", border=True)
    st.caption(
        f"{injured / crashes:.2f} injuries per crash across all of them, or "
        f"{(injured / harmful if harmful else 0):.2f} per crash that caused "
        f"one."
    )

    st.divider()

    by_borough = query(con, "02_aggregate.sql", params)
    st.subheader("Where people get hurt")
    st.bar_chart(by_borough.set_index("borough")["total_injuries"],
                 color=palette.SERIES)
    say(narrative.borough_takeaway(by_borough))

    by_hour = query(con, "03_time_analysis.sql", params)
    st.subheader("When crashes happen")
    st.line_chart(by_hour.set_index("hour_label")["crash_count"],
                  color=palette.SERIES)
    say(narrative.hour_takeaway(by_hour))

    trends = narrative.drop_partial_month(query(con, "04_trends.sql", params))
    trends["month"] = pd.to_datetime(trends["month"])

    st.subheader("Crashes and injuries, month by month")
    st.line_chart(
        trends.set_index("month")[["crash_count", "total_injuries"]],
        color=[palette.SERIES, palette.SERIES_ALT],
    )
    say(narrative.trend_takeaway(trends))

    # On its own axis. Sharing one with injuries, which run two orders of
    # magnitude higher, flattened this into a line along the bottom -- the
    # series about people dying was the one you could not see.
    st.subheader("People killed, month by month")
    st.line_chart(trends.set_index("month")["total_fatalities"],
                  color=palette.SERIES)
    say(narrative.fatality_takeaway(trends))

    st.subheader("Where the harmful crashes are")
    points = query(con, "05_map_points.sql", params)
    if points.empty:
        st.info("No geolocated crashes match the current filters.")
        return

    say(narrative.map_takeaway(len(points), MAP_POINT_LIMIT, metrics))
    _heatmap(points, basemap)


def _heatmap(points: pd.DataFrame, basemap) -> None:
    heat = pdk.Layer(
        "HeatmapLayer",
        points,
        get_position=["longitude", "latitude"],
        pickable=True,
        radius_pixels=30,   # radius of influence per point
        intensity=1.4,      # heat strength multiplier
        threshold=0.3,      # minimum normalized weight to render
        # One hue, light to dark. The green-yellow-red gradient this
        # replaces was a rainbow encoding a single continuous quantity:
        # not perceptually ordered, so its yellow band read as a peak that
        # was not there.
        color_range=palette.DENSITY_RAMP,
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

    st.pydeck_chart(
        pdk.Deck(
            map_style=basemap,
            initial_view_state=pdk.ViewState(
                latitude=40.734, longitude=-73.9, zoom=9.7, pitch=0
            ),
            layers=[heat, scatter] if basemap else [tiles, heat, scatter],
            tooltip={
                "text": "Date: {crash_datetime_str}\nBorough: {borough}",
                "style": {
                    "backgroundColor": "rgba(0, 0, 0, 0.8)",
                    "color": "white",
                },
            },
        )
    )
