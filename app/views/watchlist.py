"""The watchlist tab: intersections whose crashes injure more than they should."""

import numpy as np
import pandas as pd
import pydeck as pdk
import altair as alt
import streamlit as st

import palette
from models import significance as sig
from views.common import exposure_claim, exposure_data, watchlist_data

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
                get_fill_color=palette.rgba(palette.FLAGGED, 150),
                get_line_color=palette.rgba(palette.SURFACE, 220),
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


def funnel_chart(sites: pd.DataFrame, survivors: set) -> alt.LayerChart:
    """Build the p-chart. Separated from the tab so it can be rendered alone.

    The band is drawn at the average expected rate rather than each site's
    own, which a strict reading of the control limits would require. The
    approximation is tight here: sqrt(p(1-p)) moves by under 5% across the
    middle 90% of sites, and the z-scores the ranking and the correction
    both rest on use each site's own rate regardless.
    """
    plotted = sites.copy()
    plotted["Excess (pts)"] = plotted["excess"] * 100
    plotted["Standing"] = np.where(
        plotted["site"].isin(survivors),
        "Survives correction",
        "Within the noise a site this size makes",
    )

    reference = float(plotted["expected_rate"].mean())
    limits = sig.funnel_limits(plotted["crashes"], reference)
    span = [plotted["crashes"].min() * 0.9, plotted["crashes"].max() * 1.1]
    scale = alt.Scale(type="log", domain=span, nice=False)
    bands = pd.concat([
        pd.DataFrame({"crashes": limits["crashes"],
                      "value": limits[column] * 100,
                      "edge": column})
        for column in ("outer_upper", "outer_lower",
                       "inner_upper", "inner_lower")
    ])

    rules = (
        alt.Chart(bands)
        .mark_line(strokeDash=[5, 4], color=palette.INK_MUTED, strokeWidth=1.5,
                   opacity=0.8)
        .encode(
            x=alt.X("crashes:Q", title="Crashes at the site", scale=scale),
            y=alt.Y("value:Q", title="Excess (percentage points)"),
            detail="edge:N",
        )
    )
    dots = (
        alt.Chart(plotted)
        .mark_circle(opacity=0.5, stroke=palette.SURFACE, strokeWidth=0.6)
        .encode(
            x=alt.X("crashes:Q", scale=scale),
            y=alt.Y("Excess (pts):Q"),
            color=alt.Color(
                "Standing:N",
                scale=alt.Scale(
                    domain=["Within the noise a site this size makes",
                            "Survives correction"],
                    range=[palette.SERIES, palette.FLAGGED],
                ),
                legend=alt.Legend(title=None, orient="top", labelLimit=320),
            ),
            size=alt.Size("Standing:N", scale=alt.Scale(
                domain=["Within the noise a site this size makes",
                        "Survives correction"], range=[30, 170]), legend=None),
            tooltip=[
                alt.Tooltip("site:N", title="Intersection"),
                alt.Tooltip("crashes:Q", title="Crashes", format=","),
                alt.Tooltip("observed_rate:Q", title="Injured", format=".1%"),
                alt.Tooltip("expected_rate:Q", title="Expected", format=".1%"),
                alt.Tooltip("excess_z:Q", title="z", format=".2f"),
                alt.Tooltip("Standing:N", title="Standing"),
            ],
        )
    )
    return (rules + dots).properties(height=440)


def _funnel(sites: pd.DataFrame, summary: dict) -> None:
    """Plot every site against the precision its crash count buys it.

    A p-chart with varying sample size. The band narrows as 1/sqrt(n),
    which is the argument the z-scores make in a column of numbers and
    this makes in a glance: a junction with 25 crashes has to sit far
    from expectation before it means anything, and one with 200 does not.

    The two readings of the same evidence are both on the chart. Sites
    outside the outer band are what the eye screens for; the handful that
    survive a false-discovery correction across all of them are marked
    separately, because those are the only ones defensible on their own.
    """
    if sites.empty:
        return
    found = (summary or {}).get("false_discovery") or {}

    st.subheader("How much of this is sample size")
    st.caption(
        f"All {len(sites):,} intersections, so the filters above do not "
        f"apply here. Each dot is one of them. The curves are what random "
        f"variation alone produces at that many crashes, so a dot inside "
        f"them is an ordinary junction having an unlucky stretch. They are "
        f"drawn at the average expected rate; each dot's own rate sets its "
        f"z-score, and the two differ by under a twentieth across the "
        f"middle 90% of sites."
    )
    st.altair_chart(
        funnel_chart(sites, set(found.get("survivor_sites") or [])),
        use_container_width=True,
    )
    _significance_note(found)


def _significance_note(found: dict) -> None:
    """Say what the chart shows and what it refuses to show."""
    if not found:
        return
    with st.expander("Why so few sites are marked"):
        st.markdown(
            f"Two readings of the same evidence, and they disagree sharply.\n\n"
            f"**As a list**, it clearly holds more than chance: "
            f"{found['above_z_95']} sites clear z = 1.96 where chance alone "
            f"would produce about {found['above_z_95_by_chance']:.0f} across "
            f"{found['sites']:,} of them, an excess of roughly "
            f"{found['excess_over_chance']:.0f}. The backtest agrees "
            f"independently: the ranking predicts a later window well above "
            f"chance.\n\n"
            f"**As individual junctions**, almost none stand up. Testing "
            f"{found['sites']:,} sites at once means expecting dozens of "
            f"false positives, and at a {found['q']:.0%} false discovery "
            f"rate only **{found['survivors']}** survive "
            f"(Bonferroni: {found['survivors_bonferroni']}). The largest z "
            f"anywhere on the list is {found['largest_z']}.\n\n"
            f"Neither number is the honest one on its own. The list is worth "
            f"working through; a single site on it is a lead rather than a "
            f"finding, and that is what a screening tool is.\n\n"
            f"The test runs one-sided, so the {found['below_z_95']} sites "
            f"below the lower curve -- junctions whose crashes injure people "
            f"*less* often than their mix predicts -- are not marked. They "
            f"are the check on the ones above: if the upper tail were just "
            f"the model's residuals being wider than the formula assumes, "
            f"the lower tail would be the same size."
        )


def _exposure_section(boroughs: list[str]) -> None:
    """The sites that have a traffic count, and what it says.

    Kept apart from the main table rather than added as columns to it.
    Only a third of sites have a volume, so as columns it would be mostly
    blanks -- and blanks next to numbers read as zero.
    """
    exposure, summary = exposure_data()
    if exposure.empty or not summary:
        return

    shown = exposure[exposure["borough"].isin(boroughs)] if boroughs else exposure
    if shown.empty:
        return

    st.divider()
    st.subheader("Where we know how much traffic goes through")
    st.markdown(
        f"NYC DOT puts automated recorders on a street for a week or two "
        f"at a time. {summary['matched']:,} of the "
        f"{summary['sites_located']:,} located sites have one within 150m "
        f"that names one of their own streets. Close enough to say how "
        f"often a vehicle passing through ends up in a crash that hurt "
        f"somebody."
    )

    table = shown.rename(columns={
        "site": "Intersection",
        "borough": "Borough",
        "crashes": "Crashes",
        "vehicles_per_day": "Vehicles/day",
        "harmful_per_million": "Per million",
        "counted_year": "Counted",
        "confidence": "Volume quality",
    })
    st.dataframe(
        table[["Intersection", "Borough", "Crashes", "Vehicles/day",
               "Per million", "Counted", "Volume quality"]],
        hide_index=True,
        use_container_width=True,
        column_config={
            "Vehicles/day": st.column_config.NumberColumn(
                format="localized",
                help="On the busiest DOT-counted approach to the junction.",
            ),
            "Per million": st.column_config.NumberColumn(
                format="%.2f",
                help="Crashes that hurt someone, per million vehicles past "
                     "that counter.",
            ),
            "Counted": st.column_config.NumberColumn(
                format="%d", help="The year DOT last counted this segment."
            ),
            "Volume quality": st.column_config.TextColumn(
                help="high: the counter names both streets and covers both "
                     "directions. low: one street, one direction, so the "
                     "volume is an undercount.",
            ),
        },
    )
    _exposure_note()


def _exposure_note() -> None:
    """Explain why the per-vehicle figure is not what the list is sorted by."""
    exposure, summary = exposure_data()
    if exposure.empty or not summary:
        return

    with st.expander("Why the list is not sorted by crashes per vehicle"):
        st.markdown(
            f"Because the denominator is not good enough to rank on. A DOT "
            f"recorder sits on one segment, not across a junction, and "
            f"{summary['single_direction_counts']:,} of "
            f"{summary['matched']:,} matched counters cover a single "
            f"direction, roughly half the traffic on a two-way street. "
            f"Sort by crashes per vehicle and the top is whichever junction "
            f"has the most under-measured traffic, not the most dangerous "
            f"one.\n\n"
            f"The grades say how much weight a row can take. Where a "
            f"counter names both streets and covers both directions "
            f"(**{summary['by_confidence'].get('high', 0)} sites**) it "
            f"reports a median "
            f"{exposure[exposure.confidence == 'high']['vehicles_per_day'].median():,.0f} "
            f"vehicles a day; where it names one street in one direction "
            f"(**{summary['by_confidence'].get('low', 0)} sites**) it "
            f"reports "
            f"{exposure[exposure.confidence == 'low']['vehicles_per_day'].median():,.0f}. "
            f"Same kind of junction, different measurement.\n\n"
            f"Counts are also a median **"
            f"{summary['median_count_age_years']} years old**, the oldest "
            f"from {summary['oldest_count_year']}, and are applied across "
            f"a window they were not taken in."
        )


def _exposure_caveat() -> None:
    """Say what the list is not, using the number rather than the assertion.

    This used to be an assertion: no traffic counts exist, so the ranking
    cannot be about danger. NYC DOT counts now cover part of the city, so
    the claim is measurable -- and measuring it says the same thing, which
    is worth more than saying it.
    """
    exposure, summary = exposure_data()
    measured = not exposure.empty and bool(summary)
    st.warning(
        ("**This is not a ranking of dangerous intersections, and that is "
         "now measured rather than assumed.** " if measured else
         "**This is not a ranking of dangerous intersections.** ")
        + exposure_claim(exposure, summary)
        + " So a site high on this list is one whose crashes injure people "
          "more often than the kinds of crashes happening there would predict, "
          "not a site you are most likely to be hurt at."
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
        "the **crash mix alone**: the vehicles, the contributing factors, "
        "the road type, the hour. Never from where it happened. A site's "
        "**excess** is how much more often its crashes actually injured "
        "someone than that mix accounts for. Sites are ordered by the "
        "conservative end of that estimate, so a site with 200 crashes is not "
        "outranked by one with 25 and a lucky run."
    )

    _exposure_caveat()

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
    # The raw excess of the top sites overstates what they go on to do by
    # about a factor of three; the pooled one is close. See
    # models/shrinkage.py and the backtest's partial-pooling section.
    display["Pooled (pts)"] = shown["excess_shrunk"] * 100

    st.dataframe(
        display[[
            "Intersection", "Borough", "Crashes", "Injured", "Expected",
            "Borough-hour base", "Excess (pts)", "Pooled (pts)",
            "Most common factors",
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
            "Pooled (pts)": st.column_config.NumberColumn(
                format="%+.1f",
                help="The same excess pulled toward the city-wide mean by "
                     "how little the site's own crashes can carry. The "
                     "better guess at what the site will do next.",
            ),
        },
    )

    st.subheader(f"Worst {min(WATCHLIST_MAP_SITES, len(shown))} on the map")
    _map(shown, basemap)

    st.divider()
    _funnel(sites, summary)

    _exposure_section(boroughs)

    st.divider()
    st.subheader("Contributing factors by predicted injury risk")
    st.caption(
        "Across every scored crash. The predicted column is the model's; the "
        "observed column is what happened. They track closely. That agreement "
        "is the calibration check worth trusting most."
    )

    if factors.empty:
        st.info("No factor table has been built yet.")
        return

    # Altair rather than st.bar_chart: the latter sorts its index
    # alphabetically, and the whole point of this chart is the order.
    st.altair_chart(
        alt.Chart(factors)
        .mark_bar(color=palette.SERIES)
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
