"""Tests for the sentences under the charts.

These are claims about the data shown on a public page, so they get the
same treatment as the numbers: pinned to frames where the right answer is
known, and checked against the real dataset for the ones that depend on it.
"""

import pandas as pd
import pytest

import db
import narrative as N


@pytest.fixture(scope="module")
def connection_and_params(dataset_path):
    con = db.connect(dataset_path)
    bounds = db.query(con, "00_bounds.sql").iloc[0]
    params = db.filter_params(
        bounds["min_datetime"], bounds["max_datetime"], list(bounds["boroughs"])
    )
    yield con, bounds, params
    con.close()


def boroughs(**rates) -> pd.DataFrame:
    """A borough frame with given (crashes, harmful, injuries) per borough."""
    return pd.DataFrame([
        {"borough": name, "crash_count": crashes,
         "harmful_crash_count": harmful, "total_injuries": injuries}
        for name, (crashes, harmful, injuries) in rates.items()
    ])


def hours(*counts) -> pd.DataFrame:
    """An hour frame from (label, crashes, harmful) triples."""
    return pd.DataFrame([
        {"hour_label": label, "hour_24": i,
         "crash_count": crashes, "harmful_crash_count": harmful}
        for i, (label, crashes, harmful) in enumerate(counts)
    ])


def months(start: str, crashes: list, harmful: list, killed: list) -> pd.DataFrame:
    index = pd.period_range(start, periods=len(crashes), freq="M").astype(str)
    return pd.DataFrame({
        "month": index,
        "crash_count": crashes,
        "harmful_crash_count": harmful,
        "total_injuries": [h * 2 for h in harmful],
        "total_fatalities": killed,
    })


# --- the lede ----------------------------------------------------------

def test_the_headline_states_every_figure_beside_it(connection_and_params):
    con, bounds, params = connection_and_params
    metrics = db.query(con, "01_metrics.sql", params).iloc[0]

    said = N.headline(bounds, metrics)
    for value in ("crash_count", "harmful_crash_count", "total_injuries",
                  "total_fatalities"):
        assert f"{int(metrics[value]):,}" in said


def test_the_headline_survives_an_empty_selection():
    empty = pd.Series({"crash_count": 0, "harmful_crash_count": 0,
                       "total_injuries": 0, "total_fatalities": 0})
    assert "No crashes" in N.headline(pd.Series(dtype="object"), empty)


# --- boroughs ----------------------------------------------------------

def test_volume_and_rate_are_reported_separately():
    """The biggest borough is not the worst one, and saying only the first
    is how a chart misleads."""
    said = N.borough_takeaway(boroughs(
        BROOKLYN=(1000, 300, 400),      # most injuries, 30% harmful
        BRONX=(200, 100, 120),          # fewer crashes, 50% harmful
        QUEENS=(400, 80, 90),           # 20% harmful
    ))
    assert "Brooklyn has the most injuries" in said
    assert "20%" in said and "50%" in said


def test_the_unrecorded_borough_is_not_ranked():
    """It is a reporting artifact, not a place, so it cannot be 'the worst'."""
    said = N.borough_takeaway(boroughs(
        UNKNOWN=(1000, 900, 1200),
        BROOKLYN=(500, 100, 150),
        QUEENS=(400, 120, 140),
    ))
    assert "UNKNOWN" not in said
    assert "Brooklyn" in said or "Queens" in said


def test_one_borough_is_not_a_comparison():
    assert N.borough_takeaway(boroughs(BROOKLYN=(100, 40, 50))) is None


# --- hours -------------------------------------------------------------

def test_the_busiest_hour_is_named_with_its_count():
    said = N.hour_takeaway(hours(
        ("12 AM", 100, 50), ("8 AM", 900, 300), ("5 PM", 1000, 350),
    ))
    assert "5 PM" in said and "1,000" in said


def test_a_thin_hour_cannot_claim_the_worst_rate():
    """Three crashes at 4am, all of them nasty, is not a finding."""
    said = N.hour_takeaway(hours(
        ("4 AM", 3, 3), ("8 AM", 5000, 1000), ("5 PM", 6000, 1500),
    ))
    assert "4 AM" not in said


def test_an_empty_hour_frame_says_nothing():
    assert N.hour_takeaway(pd.DataFrame(
        columns=["hour_label", "crash_count", "harmful_crash_count"]
    )) is None


# --- months ------------------------------------------------------------

def test_a_month_still_being_reported_is_dropped():
    """Left in, it draws a cliff that reads as crashes collapsing."""
    frame = months("2024-01", [1000] * 11 + [120], [400] * 11 + [50],
                   [20] * 11 + [2])
    kept = N.drop_partial_month(frame)
    assert len(kept) == 11
    assert kept["month"].iloc[-1] == "2024-11"


def test_a_complete_final_month_is_kept():
    frame = months("2024-01", [1000] * 12, [400] * 12, [20] * 12)
    assert len(N.drop_partial_month(frame)) == 12


def test_a_short_series_is_left_alone():
    frame = months("2024-01", [1000, 100], [400, 40], [20, 2])
    assert len(N.drop_partial_month(frame)) == 2


def test_the_trend_separates_volume_from_severity():
    """Crashes down and the injury share up is the single most misreadable
    thing in this dataset."""
    frame = months(
        "2020-01",
        [1000] * 12 + [500] * 12,
        [300] * 12 + [250] * 12,
        [10] * 24,
    )
    said = N.trend_takeaway(frame)
    assert "fell 50%" in said
    assert "30%" in said and "50%" in said
    assert "reporting" in said


def test_a_rise_in_crashes_is_described_as_a_rise():
    frame = months("2020-01", [500] * 12 + [1000] * 12,
                   [150] * 12 + [300] * 12, [10] * 24)
    assert "rose 100%" in N.trend_takeaway(frame)


def test_a_partial_year_is_not_compared_against_a_full_one():
    frame = months("2020-01", [1000] * 12 + [500] * 6,
                   [300] * 12 + [200] * 6, [10] * 18)
    assert N.trend_takeaway(frame) is None


def test_too_short_a_series_makes_no_trend_claim():
    frame = months("2024-01", [1000] * 12, [400] * 12, [20] * 12)
    assert N.trend_takeaway(frame) is None


def test_the_death_toll_names_its_worst_month():
    frame = months("2024-01", [1000] * 12, [400] * 12,
                   [10] * 6 + [55] + [10] * 5)
    said = N.fatality_takeaway(frame)
    assert "July 2024" in said
    assert "55" in said
    assert "165" in said  # 11 * 10 + 55


def test_no_deaths_makes_no_claim():
    frame = months("2024-01", [1000] * 12, [400] * 12, [0] * 12)
    assert N.fatality_takeaway(frame) is None


# --- the map -----------------------------------------------------------

def test_the_map_admits_what_it_cannot_draw():
    """Crashes without coordinates are not on the map and not mentioned by
    it unless it says so."""
    metrics = pd.Series({"harmful_crash_count": 1000, "mappable_crash_count": 900})
    said = N.map_takeaway(shown=900, limit=50_000, metrics=metrics)
    assert "100" in said
    assert "not on here" in said


def test_a_capped_map_says_it_is_a_sample():
    metrics = pd.Series({"harmful_crash_count": 90_000,
                         "mappable_crash_count": 90_000})
    said = N.map_takeaway(shown=50_000, limit=50_000, metrics=metrics)
    assert "50,000" in said and "90,000" in said


def test_a_complete_map_claims_nothing_extra():
    metrics = pd.Series({"harmful_crash_count": 40, "mappable_crash_count": 40})
    said = N.map_takeaway(shown=40, limit=50_000, metrics=metrics)
    assert "not on here" not in said


# --- against the real dataset ------------------------------------------

@pytest.mark.parametrize("sentence", [
    "borough_takeaway", "hour_takeaway",
])
def test_every_takeaway_is_a_sentence_on_the_real_data(
    connection_and_params, sentence
):
    con, _, params = connection_and_params
    frames = {
        "borough_takeaway": "02_aggregate.sql",
        "hour_takeaway": "03_time_analysis.sql",
    }
    said = getattr(N, sentence)(db.query(con, frames[sentence], params))
    assert said and said[0].isupper() and said.rstrip().endswith(".")
