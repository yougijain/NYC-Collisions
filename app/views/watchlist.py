"""The watchlist tab: intersections whose crashes injure more than they should."""

import pandas as pd
import pydeck as pdk
import altair as alt
import streamlit as st

from views.common import watchlist_data

# Enough of the watchlist to see on a map without it becoming a blur.
WATCHLIST_MAP_SITES = 60


def _map(sites: pd.DataFrame, basemap) -> None:
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
            map_style=basemap,
            initial_view_state=pdk.ViewState(
                latitude=40.734, longitude=-73.9, zoom=9.7, pitch=0
            ),
            layers=(
                []
                if basemap
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


def render(boroughs: list[str], basemap) -> None:
    """Intersections whose crashes injure people more often than they should.

    Built over a fixed window by scripts/build_watchlist.py, so the sidebar's
    date range does not apply here; the borough selection does.
    """
    sites, factors, summary = watchlist_data()

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
    _map(shown, basemap)

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
