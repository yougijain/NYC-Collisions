"""Say out loud what each chart shows.

A chart without a sentence under it asks the reader to do the work of
noticing. Most readers will not, and the ones who would are the ones who
already know. So every chart on the dashboard carries a line saying what it
says.

Those lines are computed here rather than typed into the page, for the same
reason the README's figures are generated: a sentence claiming crashes peak
at 5pm becomes a lie the moment the data moves or the reader changes a
filter, and nobody goes back to check. Every function takes the same frame
the chart is drawn from, so the words and the picture cannot disagree.

Nothing here imports streamlit, so it can be tested directly.
"""

from typing import Optional

import pandas as pd

UNKNOWN_BOROUGH = "UNKNOWN"

# A month still being reported is a cliff at the right-hand edge of every
# trend line, and reading it as a collapse in crashes is the obvious mistake.
PARTIAL_MONTH_SHARE = 0.6


def _pct(value: float) -> str:
    return f"{value:.0%}" if value >= 0.1 else f"{value:.1%}"


def _titled(name: str) -> str:
    """Borough names arrive shouting."""
    return name.title() if name != UNKNOWN_BOROUGH else "an unrecorded borough"


def headline(bounds: pd.Series, metrics: pd.Series) -> str:
    """The one sentence a reader gets if they read nothing else."""
    crashes = int(metrics["crash_count"])
    if not crashes:
        return "No crashes match the current filters."

    harmful = int(metrics["harmful_crash_count"])
    injured = int(metrics["total_injuries"])
    killed = int(metrics["total_fatalities"])
    first = pd.Timestamp(bounds["min_datetime"]).date()
    last = pd.Timestamp(bounds["max_datetime"]).date()

    return (
        f"Between {first} and {last}, New York City recorded "
        f"**{crashes:,} crashes**. **{harmful:,}** of them hurt somebody: "
        f"**{injured:,} people injured** and **{killed:,} killed**. "
        f"That is {_pct(harmful / crashes)} of every crash reported."
    )


def borough_takeaway(by_borough: pd.DataFrame) -> Optional[str]:
    """Separate the borough with the most injuries from the worst one.

    Usually that is a volume story -- Brooklyn leads because Brooklyn is
    big -- and the interesting part is how little the rates differ once you
    divide through. That flatness is the same fact the model reports when
    borough turns out to be one of its weakest features.
    """
    known = by_borough[by_borough["borough"] != UNKNOWN_BOROUGH].copy()
    if len(known) < 2:
        return None

    known["harm_rate"] = known["harmful_crash_count"] / known["crash_count"]
    most = known.loc[known["total_injuries"].idxmax()]
    safest = known.loc[known["harm_rate"].idxmin()]
    worst = known.loc[known["harm_rate"].idxmax()]

    return (
        f"{_titled(most['borough'])} has the most injuries "
        f"({int(most['total_injuries']):,}), mostly because it has the most "
        f"crashes. Per crash the boroughs barely differ: "
        f"{_pct(safest['harm_rate'])} of crashes hurt someone in "
        f"{_titled(safest['borough'])} against "
        f"{_pct(worst['harm_rate'])} in {_titled(worst['borough'])}. That "
        f"is why knowing the borough tells you so little about whether a "
        f"crash was serious."
    )


def hour_takeaway(by_hour: pd.DataFrame) -> Optional[str]:
    """Say when crashes happen, then how little that changes their severity.

    The count swings by a factor of four across the day. The share that
    hurt someone swings by about ten points, and saying so is what stops a
    reader over-reading the peak.
    """
    if by_hour.empty or by_hour["crash_count"].sum() == 0:
        return None

    hours = by_hour.copy()
    hours["harm_rate"] = hours["harmful_crash_count"] / hours["crash_count"]
    busiest = hours.loc[hours["crash_count"].idxmax()]

    # Ignore hours too thin for a rate to mean anything.
    solid = hours[hours["crash_count"] >= max(30, 0.005 * hours["crash_count"].sum())]
    if solid.empty:
        return None
    worst = solid.loc[solid["harm_rate"].idxmax()]
    safest = solid.loc[solid["harm_rate"].idxmin()]

    return (
        f"Crashes peak at {busiest['hour_label']}, with "
        f"{int(busiest['crash_count']):,} of them. How bad they are moves "
        f"far less than how many: from {_pct(safest['harm_rate'])} hurting "
        f"someone at {safest['hour_label']} to "
        f"{_pct(worst['harm_rate'])} at {worst['hour_label']}. The clock "
        f"tells you when to expect a crash, not how bad it will be."
    )


def drop_partial_month(trends: pd.DataFrame) -> pd.DataFrame:
    """Remove a trailing month the city is still filing reports for.

    Left in, it draws a cliff at the right-hand edge of every trend line
    that looks like crashes collapsing.
    """
    if len(trends) < 4:
        return trends

    typical = trends["crash_count"].iloc[:-1].median()
    if trends["crash_count"].iloc[-1] < PARTIAL_MONTH_SHARE * typical:
        return trends.iloc[:-1]
    return trends


def trend_takeaway(trends: pd.DataFrame) -> Optional[str]:
    """Compare the first and last full year in view.

    Crash counts have fallen and the share causing injury has risen, which
    reads as roads getting safer and more dangerous at once until somebody
    says which is which.
    """
    if len(trends) < 24:
        return None

    months = trends.copy()
    months["year"] = pd.to_datetime(months["month"]).dt.year
    by_year = months.groupby("year").agg(
        crashes=("crash_count", "sum"),
        harmful=("harmful_crash_count", "sum"),
        months=("crash_count", "size"),
    )
    # Only years the data covers in full can be compared with each other.
    full = by_year[by_year["months"] == 12]
    if len(full) < 2:
        return None

    first, last = full.iloc[0], full.iloc[-1]
    change = (last["crashes"] - first["crashes"]) / first["crashes"]
    first_rate = first["harmful"] / first["crashes"]
    last_rate = last["harmful"] / last["crashes"]

    direction = "fell" if change < 0 else "rose"
    return (
        f"Reported crashes {direction} {abs(change):.0%} between "
        f"{full.index[0]} and {full.index[-1]}, but the share causing injury "
        f"went from {_pct(first_rate)} to {_pct(last_rate)}. Most of that "
        f"second move is reporting rather than road safety: minor "
        f"property-damage crashes stopped being filed, which raises the "
        f"share of what is left that involved somebody getting hurt."
    )


def fatality_takeaway(trends: pd.DataFrame) -> Optional[str]:
    """Put the death toll in a unit a person can hold."""
    if trends.empty or "total_fatalities" not in trends:
        return None

    total = int(trends["total_fatalities"].sum())
    if not total:
        return None

    per_month = total / len(trends)
    worst = trends.loc[trends["total_fatalities"].idxmax()]
    return (
        f"{total:,} people were killed over these {len(trends)} months, "
        f"about {per_month:.0f} a month. The worst was "
        f"{pd.Timestamp(worst['month']).strftime('%B %Y')}, with "
        f"{int(worst['total_fatalities'])}."
    )


def map_takeaway(shown: int, limit: int, metrics: pd.Series) -> str:
    """Say what the heatmap is, and what it is quietly leaving out."""
    harmful = int(metrics["harmful_crash_count"])
    mappable = int(metrics.get("mappable_crash_count", shown))
    unplaceable = harmful - mappable

    if shown >= limit:
        opening = (
            f"A fixed random {limit:,} of the {mappable:,} harmful crashes "
            f"that carry coordinates, so the map does not shimmer when you "
            f"change a filter."
        )
    else:
        opening = f"{shown:,} harmful crashes with coordinates."

    if unplaceable > 0:
        return (
            f"{opening} A further {unplaceable:,} hurt someone but were "
            f"logged without a position, and are not on here at all."
        )
    return opening
