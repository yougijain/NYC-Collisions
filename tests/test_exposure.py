"""Tests for the traffic-volume join.

The join is where this project is easiest to be confidently wrong. Every
number it produces is a ratio, and a denominator that is quietly a
quarter of the truth -- a partial day summed as a whole one, a recorder
on the service road instead of the arterial -- does not look wrong. It
looks like a dangerous intersection.

So most of what is here guards a denominator. The cases are the real
ones: Bruckner Boulevard came back at 15.5 vehicles a day under an
hours-only completeness check, and at 1,312 under nearest-counter
matching, and topped a per-vehicle ranking both times.
"""

import json

import pandas as pd
import pytest

import build_exposure
from models import exposure as E

# Three recorder positions in the State Plane feet DOT publishes, and
# the junction they sit around. NEARBY is 91m from the site, FAR is 1.2km.
HERE = "POINT (1011000.0 236000.0)"
NEARBY = "POINT (1011300.0 236000.0)"
FAR = "POINT (1015000.0 236000.0)"
SITE_LATITUDE, SITE_LONGITUDE = 40.814401, -73.903362


def readings(
    segment="1", geom=HERE, street="BRUCKNER BLVD", fromst="LONGWOOD AV",
    tost="HUNTS POINT AV", direction="NB", year=2024, day=1,
    per_hour=4, volume=100, hours=24, campaign="1",
) -> pd.DataFrame:
    """One segment-day of counts, as the API serves them: all strings."""
    rows = []
    for hour in range(hours):
        for bin_index in range(per_hour):
            rows.append({
                "requestid": campaign,
                "segmentid": segment, "wktgeom": geom, "street": street,
                "fromst": fromst, "tost": tost, "direction": direction,
                "yr": str(year), "m": "6", "d": str(day),
                "hh": str(hour), "mm": str(bin_index * (60 // per_hour)),
                "vol": str(volume),
            })
    return pd.DataFrame(rows)


def site(name="BRUCKNER BOULEVARD @ LONGWOOD AVENUE",
         latitude=SITE_LATITUDE, longitude=SITE_LONGITUDE,
         crashes=100, observed_rate=0.5, excess=0.05):
    return {
        "site": name, "borough": "BRONX", "crashes": crashes,
        "observed_rate": observed_rate, "excess": excess,
        "latitude": latitude, "longitude": longitude,
    }


@pytest.fixture
def one_counter():
    """A segment with a complete day, located and merged with its volume."""
    counts = readings()
    volumes = E.daily_volumes(counts)
    return counts, volumes, E.count_points(counts).merge(volumes, on="segment_id")


# --- locating a counter ------------------------------------------------

def test_state_plane_feet_come_back_as_new_york(one_counter):
    _, _, points = one_counter
    assert 40.5 < points["latitude"].iat[0] < 41.0
    assert -74.3 < points["longitude"].iat[0] < -73.7


def test_a_counter_with_no_geometry_is_dropped():
    broken = readings(geom="")
    assert E.count_points(broken).empty


def test_the_location_text_carries_all_three_street_fields(one_counter):
    _, _, points = one_counter
    described = points["described"].iat[0]
    assert "BRUCKNER BOULEVARD" in described
    assert "LONGWOOD AVENUE" in described
    assert "HUNTS POINT AVENUE" in described


def test_dot_abbreviations_are_flattened_to_the_projects_spelling():
    """The watchlist keys junctions through scripts/street_names.py. A
    second vocabulary here would match DOT's "E 165 ST" against nothing."""
    assert E.normalise_name("E 165TH ST") == "EAST 165 STREET"
    assert E.normalise_name("BRUCKNER BLVD") == "BRUCKNER BOULEVARD"


# --- what counts as a day ----------------------------------------------

def test_a_complete_day_sums_every_bin(one_counter):
    _, volumes, _ = one_counter
    assert volumes["vehicles_per_day"].iat[0] == 24 * 4 * 100


def test_a_recorder_pulled_at_lunchtime_is_not_a_day():
    """Half a day summed as a whole one halves the denominator."""
    assert E.daily_volumes(readings(hours=12)).empty


def test_a_day_missing_most_of_its_bins_is_not_a_day():
    """Every hour present is not every bin present: 240 segment-days have
    all 24 hours and half their readings, which halves the denominator."""
    campaign = pd.concat([readings(day=1), readings(day=2)])
    lost_half = campaign[
        (campaign["d"] == "1") | campaign["mm"].isin(["0", "30"])
    ]
    volumes = E.daily_volumes(lost_half)
    assert volumes["count_days"].iat[0] == 1


def test_a_thin_day_is_judged_against_its_own_campaign():
    """199 requests recorded hourly by design. A campaign that binned by
    the hour must not be thrown out for looking like one that lost bins,
    which is what deriving the interval per day would do."""
    hourly = readings(per_hour=1)
    assert E.daily_volumes(hourly)["vehicles_per_day"].iat[0] == 24 * 100


def test_counts_with_no_campaign_field_still_build():
    """The fetch is schema-agnostic on purpose; the join should not fall
    over if DOT drops a column."""
    without = readings().drop(columns=["requestid"])
    assert not E.daily_volumes(without).empty


def test_a_recorder_that_logged_nothing_was_broken():
    assert E.daily_volumes(readings(volume=0)).empty


def test_directions_are_added_together_not_averaged():
    both = pd.concat([
        readings(direction="NB", volume=100),
        readings(direction="SB", volume=300),
    ])
    volumes = E.daily_volumes(both)
    assert volumes["vehicles_per_day"].iat[0] == 24 * 4 * 400
    assert volumes["directions_counted"].iat[0] == 2


def test_only_the_most_recent_campaign_counts():
    """A 2009 count describes a street that may not exist any more."""
    both = pd.concat([
        readings(year=2009, volume=1000),
        readings(year=2024, volume=100),
    ])
    volumes = E.daily_volumes(both)
    assert volumes["counted_year"].iat[0] == 2024
    assert volumes["vehicles_per_day"].iat[0] == 24 * 4 * 100


def test_days_within_the_recent_campaign_are_averaged():
    week = pd.concat([readings(day=d, volume=100 * d) for d in (1, 2, 3)])
    volumes = E.daily_volumes(week)
    assert volumes["vehicles_per_day"].iat[0] == 24 * 4 * 200
    assert volumes["count_days"].iat[0] == 3


# --- matching a counter to a junction ----------------------------------

def test_a_counter_on_the_junctions_street_matches(one_counter):
    _, _, points = one_counter
    assert len(E.match_sites(pd.DataFrame([site()]), points)) == 1


def test_being_close_is_not_being_on_the_street(one_counter):
    """A recorder 100m away on the parallel street measures different
    traffic. Distance alone puts 31% of matches on the wrong road."""
    _, _, points = one_counter
    elsewhere = site(name="FAILE STREET @ GARRISON AVENUE")
    assert E.match_sites(pd.DataFrame([elsewhere]), points).empty


def test_a_counter_beyond_the_radius_does_not_match():
    counts = readings(geom=FAR)
    volumes = E.daily_volumes(counts)
    points = E.count_points(counts).merge(volumes, on="segment_id")
    assert E.match_sites(pd.DataFrame([site()]), points).empty


def test_the_busiest_qualifying_counter_wins_not_the_nearest():
    """Two counters name the street; the near one is a service road.
    Taking the nearest gave Bruckner Boulevard 1,312 vehicles a day and
    inverted the whole per-vehicle order."""
    counts = pd.concat([
        readings(segment="quiet", geom=HERE, volume=10),
        readings(segment="busy", geom=NEARBY, volume=1000),
    ])
    volumes = E.daily_volumes(counts)
    points = E.count_points(counts).merge(volumes, on="segment_id")

    matched = E.match_sites(pd.DataFrame([site()]), points)
    assert matched["segment_id"].iat[0] == "busy"
    assert matched["counters_in_range"].iat[0] == 2


def test_matching_without_volumes_is_refused_rather_than_guessed():
    """Choosing the busiest is impossible without them, and falling back
    to the nearest silently would reintroduce the bug it replaced."""
    counts = readings()
    with pytest.raises(KeyError, match="daily_volumes"):
        E.match_sites(pd.DataFrame([site()]), E.count_points(counts))


def test_a_site_with_no_coordinates_cannot_be_matched(one_counter):
    _, _, points = one_counter
    nowhere = site(latitude=None, longitude=None)
    assert E.match_sites(pd.DataFrame([nowhere]), points).empty


def test_naming_both_streets_is_recorded(one_counter):
    _, _, points = one_counter
    matched = E.match_sites(pd.DataFrame([site()]), points)
    assert bool(matched["names_both_streets"].iat[0])

    one_street = site(name="BRUCKNER BOULEVARD @ TIFFANY STREET")
    matched = E.match_sites(pd.DataFrame([one_street]), points)
    assert not bool(matched["names_both_streets"].iat[0])


# --- turning a volume into a rate --------------------------------------

@pytest.fixture
def rates(one_counter):
    _, volumes, points = one_counter
    sites = pd.DataFrame([site()])
    matches = E.match_sites(sites, points)
    return E.exposure_rates(sites, matches, volumes, window_days=1000)


def test_the_rate_is_crashes_over_vehicles_past_the_counter(rates):
    millions = 9600 * 1000 / 1_000_000
    assert rates["million_vehicles"].iat[0] == pytest.approx(millions)
    assert rates["crashes_per_million"].iat[0] == pytest.approx(100 / millions)


def test_the_published_rate_counts_only_the_crashes_that_hurt_someone(rates):
    """observed_rate is the share of the site's crashes that injured."""
    assert rates["harmful_per_million"].iat[0] == pytest.approx(
        rates["crashes_per_million"].iat[0] * 0.5
    )


def test_a_denominator_too_small_to_divide_by_is_dropped():
    """A junction with 100 crashes is not fed by a road carrying 200
    vehicles a day; the counter belongs to some side street nearby."""
    counts = readings(volume=1)  # 96 vehicles a day
    volumes = E.daily_volumes(counts)
    points = E.count_points(counts).merge(volumes, on="segment_id")
    sites = pd.DataFrame([site()])
    matches = E.match_sites(sites, points)

    assert not matches.empty
    assert E.exposure_rates(sites, matches, volumes, 1000).empty


def test_a_window_of_no_days_is_refused(one_counter):
    _, volumes, points = one_counter
    sites = pd.DataFrame([site()])
    matches = E.match_sites(sites, points)
    with pytest.raises(ValueError, match="window_days"):
        E.exposure_rates(sites, matches, volumes, window_days=0)


def test_nothing_matched_is_no_rates(one_counter):
    _, volumes, _ = one_counter
    assert E.exposure_rates(
        pd.DataFrame([site()]), pd.DataFrame(), volumes, 1000
    ).empty


# --- how much weight a row can take ------------------------------------

def grade(names_both, directions):
    return E._confidence(pd.DataFrame({
        "names_both_streets": [names_both],
        "directions_counted": [directions],
    })).iat[0]


def test_both_streets_and_both_directions_is_the_only_high_grade():
    assert grade(True, 2) == "high"
    assert grade(True, 1) == "medium"
    assert grade(False, 2) == "medium"
    assert grade(False, 1) == "low"


def test_the_grade_reaches_the_published_row(rates):
    assert rates["confidence"].iat[0] == "medium"


# --- what the coverage summary claims ----------------------------------

def test_coverage_is_measured_against_sites_that_could_match(rates):
    """A site with no coordinates was never a candidate, and counting it
    as a miss understates the join."""
    sites = pd.DataFrame([site(), site(name="A @ B", latitude=None,
                                       longitude=None)])
    summary = E.coverage(sites, rates)
    assert summary["sites"] == 2
    assert summary["sites_located"] == 1
    assert summary["matched_share"] == 1.0


def test_coverage_reports_the_agreement_between_the_two_rankings():
    """The watchlist's caveat quotes this number, so it has to be there."""
    many = pd.DataFrame({
        "excess": [0.1, 0.2, 0.3, 0.4],
        "harmful_per_million": [4.0, 3.0, 2.0, 1.0],
        "names_both_streets": [True] * 4,
        "metres": [10.0] * 4,
        "vehicles_per_day": [10_000] * 4,
        "directions_counted": [2] * 4,
        "counted_year": [2024] * 4,
        "confidence": ["high"] * 4,
    })
    summary = E.coverage(pd.DataFrame([site()] * 4), many)
    assert summary["spearman_excess_vs_per_vehicle"] == -1.0


def test_matching_nothing_is_a_summary_rather_than_a_crash():
    summary = E.coverage(pd.DataFrame([site()]), pd.DataFrame())
    assert summary["matched"] == 0


# --- the build script --------------------------------------------------

def test_the_build_writes_both_files(monkeypatch, tmp_path, one_counter):
    counts, _, _ = one_counter
    monkeypatch.setattr(build_exposure.db, "load_watchlist",
                        lambda: pd.DataFrame([site()]))
    monkeypatch.setattr(
        build_exposure.db, "load_watchlist_summary",
        lambda: {"window": {"from": "2022-01-01", "to": "2026-06-11"}},
    )
    monkeypatch.setattr(build_exposure, "load_counts", lambda path: counts)

    summary = build_exposure.run(output_dir=tmp_path)

    written = pd.read_csv(tmp_path / "injury_exposure.csv")
    assert written["site"].iat[0] == site()["site"]
    assert written["vehicles_per_day"].iat[0] == 9600
    assert json.loads(
        (tmp_path / "exposure_summary.json").read_text()
    )["matched"] == summary["matched"] == 1
    assert summary["window_days"] == 1622


def test_the_build_refuses_to_run_without_a_watchlist(monkeypatch, tmp_path):
    monkeypatch.setattr(build_exposure.db, "load_watchlist", pd.DataFrame)
    monkeypatch.setattr(build_exposure.db, "load_watchlist_summary", dict)
    with pytest.raises(RuntimeError, match="build_watchlist"):
        build_exposure.run(output_dir=tmp_path)


def test_matching_no_site_at_all_is_an_error_not_an_empty_file(
    monkeypatch, tmp_path, one_counter
):
    """An empty CSV would quietly blank the dashboard section."""
    counts, _, _ = one_counter
    monkeypatch.setattr(
        build_exposure.db, "load_watchlist",
        lambda: pd.DataFrame([site(name="FAILE STREET @ GARRISON AVENUE")]),
    )
    monkeypatch.setattr(
        build_exposure.db, "load_watchlist_summary",
        lambda: {"window": {"from": "2022-01-01", "to": "2026-06-11"}},
    )
    monkeypatch.setattr(build_exposure, "load_counts", lambda path: counts)

    with pytest.raises(RuntimeError, match="no watchlist site|No watchlist site"):
        build_exposure.run(output_dir=tmp_path)
    assert not (tmp_path / "injury_exposure.csv").exists()
