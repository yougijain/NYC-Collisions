"""Tests for Socrata pagination and the incremental build, without network."""

from unittest.mock import patch

import pandas as pd
import pytest

import build_dataset
import fetch_data
from fetch_data import build_where, fetch_collisions


@pytest.fixture
def fake_api():
    """Serve `total` synthetic rows, honouring $limit and $offset."""
    state = {"total": 0, "calls": []}

    def handler(session, params):
        state["calls"].append(params)
        offset, limit = params["$offset"], params["$limit"]
        count = max(0, min(limit, state["total"] - offset))
        return [{"collision_id": str(offset + i)} for i in range(count)]

    with patch.object(fetch_data, "_get_with_retry", handler):
        yield state


def test_build_where_is_none_without_bounds():
    assert build_where() is None


def test_build_where_expands_dates_to_timestamps():
    assert build_where("2020-01-01") == "crash_date >= '2020-01-01T00:00:00'"


def test_build_where_combines_both_bounds():
    assert build_where("2020-01-01", "2021-01-01") == (
        "crash_date >= '2020-01-01T00:00:00' AND crash_date < '2021-01-01T00:00:00'"
    )


def test_pagination_returns_every_row_exactly_once(fake_api):
    fake_api["total"] = 120_000
    df = fetch_collisions()
    assert len(df) == 120_000
    assert df["collision_id"].nunique() == 120_000


def test_pagination_terminates_on_an_exact_page_multiple(fake_api):
    """A full final page must be followed by one empty request, not a loop."""
    fake_api["total"] = 100_000
    assert len(fetch_collisions()) == 100_000


def test_max_rows_caps_the_result(fake_api):
    fake_api["total"] = 120_000
    assert len(fetch_collisions(max_rows=70_000)) == 70_000


def test_page_size_is_capped_at_the_socrata_limit(fake_api):
    fake_api["total"] = 10
    fetch_collisions(page_size=999_999)
    assert fake_api["calls"][0]["$limit"] == fetch_data.MAX_PAGE_SIZE


def test_requests_carry_a_deterministic_order(fake_api):
    """Offset paging without a total order silently drops and repeats rows."""
    fake_api["total"] = 10
    fetch_collisions()
    assert all(call["$order"] == fetch_data.PAGE_ORDER for call in fake_api["calls"])


def test_empty_result_set_is_an_empty_frame(fake_api):
    fake_api["total"] = 0
    assert fetch_collisions().empty


def test_rejects_a_non_positive_page_size():
    with pytest.raises(ValueError):
        fetch_collisions(page_size=0)


# --- incremental build -------------------------------------------------

@pytest.fixture
def sample_raw():
    return pd.read_csv("data/raw/nyc_collisions_sample.csv")


def test_incremental_run_refetches_the_overlap_window(tmp_path, sample_raw):
    out = tmp_path / "collisions.parquet"
    seen = []

    def fake_fetch(since=None, **kwargs):
        seen.append(since)
        return sample_raw

    with patch.object(build_dataset, "fetch_collisions", fake_fetch):
        build_dataset.build(output=out, full=True)
        watermark = pd.read_parquet(out)["crash_datetime"].max()
        build_dataset.build(output=out, overlap_days=30)

    expected = (watermark - pd.Timedelta(days=30)).strftime("%Y-%m-%d")
    assert seen == ["2020-01-01", expected]


def test_repeated_builds_are_idempotent(tmp_path, sample_raw):
    out = tmp_path / "collisions.parquet"
    with patch.object(build_dataset, "fetch_collisions", lambda **kw: sample_raw):
        first = build_dataset.build(output=out, full=True)
        second = build_dataset.build(output=out)
    assert len(first) == len(second)
    assert second["collision_id"].duplicated().sum() == 0


def test_amended_records_overwrite_held_ones(tmp_path, sample_raw):
    out = tmp_path / "collisions.parquet"
    with patch.object(build_dataset, "fetch_collisions", lambda **kw: sample_raw):
        built = build_dataset.build(output=out, full=True)

    target = int(built["collision_id"].iloc[-1])
    amended = sample_raw.copy()
    amended.loc[amended["COLLISION_ID"] == target, "BOROUGH"] = "STATEN ISLAND"

    with patch.object(build_dataset, "fetch_collisions", lambda **kw: amended):
        updated = build_dataset.build(output=out)

    assert updated.loc[updated["collision_id"] == target, "borough"].iloc[0] == "STATEN ISLAND"
    assert len(updated) == len(built)


def test_an_empty_fetch_leaves_the_existing_dataset_intact(tmp_path, sample_raw):
    out = tmp_path / "collisions.parquet"
    with patch.object(build_dataset, "fetch_collisions", lambda **kw: sample_raw):
        built = build_dataset.build(output=out, full=True)

    with patch.object(build_dataset, "fetch_collisions", lambda **kw: pd.DataFrame()):
        kept = build_dataset.build(output=out)

    assert len(kept) == len(built)


def test_an_empty_first_build_fails_loudly(tmp_path):
    with patch.object(build_dataset, "fetch_collisions", lambda **kw: pd.DataFrame()):
        with pytest.raises(RuntimeError):
            build_dataset.build(output=tmp_path / "collisions.parquet", full=True)
