"""Tests for the cleaning pipeline shared by the notebook and the scheduler."""

import pandas as pd
import pytest

from clean import CANONICAL_COLUMNS, clean, normalize_columns


def api_row(**overrides):
    """A minimal record shaped like the Socrata JSON API."""
    row = {
        "crash_date": "2025-06-14T00:00:00.000",
        "crash_time": "9:05",
        "borough": "BROOKLYN",
        "zip_code": "11201",
        "latitude": "40.6955",
        "longitude": "-73.9895",
        "location": {"latitude": "40.6955", "longitude": "-73.9895"},
        "number_of_persons_injured": "2",
        "number_of_persons_killed": "0",
        "collision_id": "4900001",
        "vehicle_type_code1": "Sedan",
        "vehicle_type_code2": "Bike",
        "vehicle_type_code_3": "Taxi",
        "vehicle_type_code_5": "Van",
    }
    row.update(overrides)
    return row


def test_normalize_handles_csv_headers():
    df = pd.DataFrame(columns=["CRASH DATE", "NUMBER OF PERSONS INJURED", "COLLISION_ID"])
    assert list(normalize_columns(df).columns) == [
        "crash_date", "number_of_persons_injured", "collision_id",
    ]


def test_normalize_reconciles_socrata_vehicle_code_quirk():
    """The API emits vehicle_type_code1/2 but vehicle_type_code_3/4/5."""
    df = pd.DataFrame(columns=["vehicle_type_code1", "vehicle_type_code2", "vehicle_type_code_3"])
    assert list(normalize_columns(df).columns) == [
        "vehicle_type_code_1", "vehicle_type_code_2", "vehicle_type_code_3",
    ]


def test_output_has_exactly_the_canonical_columns():
    assert list(clean(pd.DataFrame([api_row()])).columns) == CANONICAL_COLUMNS


def test_sparse_and_redundant_columns_are_dropped():
    out = clean(pd.DataFrame([api_row()]))
    assert "vehicle_type_code_5" not in out.columns
    assert "location" not in out.columns


@pytest.mark.parametrize(
    "crash_date,crash_time,expected",
    [
        ("2025-06-14T00:00:00.000", "9:05", "2025-06-14 09:05:00"),   # API
        ("06/14/2025", "9:05", "2025-06-14 09:05:00"),                 # CSV export
        ("2025-06-14T00:00:00.000", "23:59:12", "2025-06-14 23:59:12"),
        ("2025-06-14T00:00:00.000", "00:00", "2025-06-14 00:00:00"),
    ],
)
def test_crash_datetime_parsing(crash_date, crash_time, expected):
    out = clean(pd.DataFrame([api_row(crash_date=crash_date, crash_time=crash_time)]))
    assert str(out["crash_datetime"].iloc[0]) == expected


def test_unparseable_time_falls_back_to_midnight():
    out = clean(pd.DataFrame([api_row(crash_time="not a time")]))
    assert str(out["crash_datetime"].iloc[0]) == "2025-06-14 00:00:00"


@pytest.mark.parametrize("lat,lon", [("0.0", "0.0"), ("0", "0"), ("12.3", "45.6")])
def test_out_of_bounds_coordinates_are_masked(lat, lon):
    out = clean(pd.DataFrame([api_row(latitude=lat, longitude=lon)]))
    assert pd.isna(out["latitude"].iloc[0])
    assert pd.isna(out["longitude"].iloc[0])


def test_valid_coordinates_survive():
    out = clean(pd.DataFrame([api_row()]))
    assert out["latitude"].iloc[0] == pytest.approx(40.6955)


def test_zip_code_is_a_zero_padded_string():
    out = clean(pd.DataFrame([api_row(zip_code="7001")]))
    assert out["zip_code"].iloc[0] == "07001"


def test_duplicate_collision_ids_keep_the_latest_record():
    rows = pd.DataFrame([api_row(borough="BROOKLYN"), api_row(borough="QUEENS")])
    out = clean(rows)
    assert len(out) == 1
    assert out["borough"].iloc[0] == "QUEENS"


def test_rows_without_a_collision_id_are_dropped():
    out = clean(pd.DataFrame([api_row(), api_row(collision_id=None)]))
    assert len(out) == 1


def test_output_is_sorted_by_crash_datetime():
    rows = pd.DataFrame([
        api_row(collision_id="2", crash_date="2025-06-20T00:00:00.000"),
        api_row(collision_id="1", crash_date="2025-06-14T00:00:00.000"),
    ])
    assert clean(rows)["crash_datetime"].is_monotonic_increasing


def test_empty_input_yields_an_empty_canonical_frame():
    out = clean(pd.DataFrame())
    assert out.empty
    assert list(out.columns) == CANONICAL_COLUMNS


def test_missing_required_column_raises():
    with pytest.raises(KeyError):
        clean(pd.DataFrame([{"crash_date": "2025-06-14", "borough": "QUEENS"}]))


def test_missing_optional_columns_are_filled_with_nulls():
    out = clean(pd.DataFrame([api_row()]))
    assert out["off_street_name"].isna().all()
