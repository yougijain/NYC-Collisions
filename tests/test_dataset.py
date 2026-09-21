"""Contract tests for the published collisions dataset and its queries."""

import pandas as pd
import pytest

import db
from build_dataset import DATASET_COLUMNS

QUERY_FILES = [
    "01_metrics.sql",
    "02_aggregate.sql",
    "03_time_analysis.sql",
    "04_trends.sql",
    "05_map_points.sql",
]


@pytest.fixture(scope="module")
def bounds(connection):
    return db.query(connection, "00_bounds.sql").iloc[0]


@pytest.fixture(scope="module")
def params(bounds):
    return db.filter_params(
        bounds["min_datetime"], bounds["max_datetime"], list(bounds["boroughs"])
    )


def test_schema_matches_the_published_columns(connection):
    columns = connection.execute(
        f"SELECT * FROM {db.TABLE_NAME} LIMIT 0"
    ).df().columns.tolist()
    assert columns == DATASET_COLUMNS


def test_the_resolved_borough_covers_more_than_the_source(connection):
    """The source omits borough on 31% of crashes, mostly expressway and
    parkway reports, which otherwise makes UNKNOWN the largest bar on every
    borough chart."""
    at_source, resolved = connection.execute(
        f"""SELECT COUNT(borough), COUNT(borough_resolved)
            FROM {db.TABLE_NAME}"""
    ).fetchone()
    assert resolved > at_source


def test_the_resolved_borough_never_contradicts_the_source(connection):
    """It fills blanks; it does not overrule a borough the source gave."""
    overruled = connection.execute(
        f"""SELECT COUNT(*) FROM {db.TABLE_NAME}
            WHERE borough IS NOT NULL AND borough_resolved != borough"""
    ).fetchone()[0]
    assert overruled == 0


def test_the_resolved_borough_invents_no_new_names(connection):
    extra = connection.execute(
        f"""SELECT COUNT(DISTINCT borough_resolved) FROM {db.TABLE_NAME}
            WHERE borough_resolved NOT IN (
                SELECT DISTINCT borough FROM {db.TABLE_NAME}
                WHERE borough IS NOT NULL)"""
    ).fetchone()[0]
    assert extra == 0


def test_dataset_is_not_empty(bounds):
    assert bounds["total_crashes"] > 0


def test_no_null_crash_datetime(connection):
    nulls = connection.execute(
        f"SELECT COUNT(*) FROM {db.TABLE_NAME} WHERE crash_datetime IS NULL"
    ).fetchone()[0]
    assert nulls == 0


def test_collision_id_is_unique(connection):
    total, distinct = connection.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT collision_id) FROM {db.TABLE_NAME}"
    ).fetchone()
    assert total == distinct


def test_no_negative_casualty_counts(connection):
    negatives = connection.execute(
        f"""SELECT COUNT(*) FROM {db.TABLE_NAME}
            WHERE number_of_persons_injured < 0
               OR number_of_persons_killed  < 0"""
    ).fetchone()[0]
    assert negatives == 0


def test_coordinates_fall_within_nyc(connection):
    """Unknown positions are encoded as 0.0 upstream and must be masked."""
    stray = connection.execute(
        f"""SELECT COUNT(*) FROM {db.TABLE_NAME}
            WHERE latitude IS NOT NULL
              AND (latitude NOT BETWEEN 40.4 AND 41.0
                OR longitude NOT BETWEEN -74.35 AND -73.6)"""
    ).fetchone()[0]
    assert stray == 0


def test_intersections_are_identifiable(connection):
    """on_street_name and cross_street_name together name an intersection.

    The Socrata API returns the cross-street and off-street fields under each
    other's names. A build that does not reconcile them has house numbers
    filed as cross streets and no row carrying both halves of a pair, which
    makes any intersection-level analysis impossible.
    """
    paired, total = connection.execute(
        f"""SELECT COUNT(*) FILTER (WHERE on_street_name    IS NOT NULL
                                      AND cross_street_name IS NOT NULL),
                   COUNT(*)
            FROM {db.TABLE_NAME}"""
    ).fetchone()
    assert paired / total > 0.25


def test_one_junction_has_one_spelling(connection):
    """The source wrote a single junction as many as fourteen ways, which
    split its crashes across that many keys and listed it on the watchlist
    more than once."""
    variants = connection.execute(
        f"""SELECT COUNT(*) FROM {db.TABLE_NAME}
            WHERE on_street_name != upper(on_street_name)
               OR cross_street_name != upper(cross_street_name)
               OR off_street_name != upper(off_street_name)"""
    ).fetchone()[0]
    assert variants == 0


def test_no_street_name_is_left_abbreviated(connection):
    """An abbreviation surviving the clean would key as its own street."""
    abbreviated = connection.execute(
        f"""SELECT COUNT(*) FROM {db.TABLE_NAME}
            WHERE regexp_matches(on_street_name,
                  '(^| )(AVE?|ST|PL|BLVD|RD|PKWY|EXPY|LN|HWY)( |$)')"""
    ).fetchone()[0]
    assert abbreviated == 0


def test_street_names_carry_no_padding(connection):
    """The source pads street names, which stops one intersection's records
    from grouping together."""
    padded = connection.execute(
        f"""SELECT COUNT(*) FROM {db.TABLE_NAME}
            WHERE on_street_name    != trim(on_street_name)
               OR cross_street_name != trim(cross_street_name)
               OR off_street_name   != trim(off_street_name)
               OR on_street_name    LIKE '%  %'
               OR cross_street_name LIKE '%  %'
               OR off_street_name   LIKE '%  %'"""
    ).fetchone()[0]
    assert padded == 0


def test_an_address_is_never_filed_as_a_cross_street(connection):
    """A cross street is a street name; a leading house number means the
    address column has leaked into it."""
    numbered = connection.execute(
        f"""SELECT COUNT(*) FROM {db.TABLE_NAME}
            WHERE regexp_matches(cross_street_name, '^[0-9]+(-[0-9]+)? [0-9]*[A-Z]')
              AND on_street_name IS NULL"""
    ).fetchone()[0]
    assert numbered == 0


@pytest.mark.parametrize("sql_file", QUERY_FILES)
def test_query_returns_rows(connection, sql_file, params):
    assert not db.query(connection, sql_file, params).empty


def test_the_headline_counts_agree_with_the_table(connection, params):
    """The tiles used to say 244,916 while the page header said 637,256,
    because the metrics query silently filtered to harmful crashes."""
    row = db.query(connection, "01_metrics.sql", params).iloc[0]
    total = connection.execute(f"SELECT COUNT(*) FROM {db.TABLE_NAME}").fetchone()[0]

    assert row["crash_count"] == total
    assert 0 < row["harmful_crash_count"] < row["crash_count"]


def test_filters_narrow_results(connection, bounds, params):
    wide = db.query(connection, "01_metrics.sql", params).iloc[0]["crash_count"]

    one_borough = db.filter_params(
        bounds["min_datetime"], bounds["max_datetime"], [list(bounds["boroughs"])[0]]
    )
    narrow = db.query(connection, "01_metrics.sql", one_borough).iloc[0]["crash_count"]
    assert 0 < narrow < wide


def test_date_filter_is_inclusive_of_the_final_day(connection, bounds):
    """end_date is a calendar day, so its crashes must be counted."""
    last_day = pd.Timestamp(bounds["max_datetime"]).date()
    same_day = db.filter_params(last_day, last_day, list(bounds["boroughs"]))
    counted = db.query(connection, "02_aggregate.sql", same_day)["crash_count"].sum()
    assert counted > 0


def test_map_respects_row_limit(connection, bounds):
    capped = db.filter_params(
        bounds["min_datetime"], bounds["max_datetime"],
        list(bounds["boroughs"]), row_limit=100,
    )
    points = db.query(connection, "05_map_points.sql", capped)
    assert len(points) == 100
    assert points[["latitude", "longitude"]].notna().all().all()


def test_map_sample_is_deterministic(connection, bounds):
    capped = db.filter_params(
        bounds["min_datetime"], bounds["max_datetime"],
        list(bounds["boroughs"]), row_limit=100,
    )
    first = db.query(connection, "05_map_points.sql", capped)
    second = db.query(connection, "05_map_points.sql", capped)
    assert first.equals(second)


def test_empty_borough_selection_returns_no_rows(connection, bounds):
    none_selected = db.filter_params(
        bounds["min_datetime"], bounds["max_datetime"], []
    )
    assert db.query(connection, "02_aggregate.sql", none_selected).empty


def test_bind_params_drops_unreferenced_keys():
    """DuckDB rejects named parameters a statement does not reference."""
    bound = db.bind_params("SELECT $start_date", db.filter_params(
        "2024-01-01", "2024-01-31", ["QUEENS"]
    ))
    assert set(bound) == {"start_date"}


def test_bind_params_rejects_unbound_placeholders():
    with pytest.raises(KeyError):
        db.bind_params("SELECT $missing", {})
