"""Tests for the generic Socrata client and the traffic volume fetch.

The paging behaviour is exercised in depth through the collisions client
in test_fetch.py. What is here is the part that only matters now there is
a second dataset: that nothing about crashes leaked into the client, and
that the traffic fetch can survive meeting a schema it has never seen.
"""

from unittest.mock import patch

import pandas as pd
import pyarrow.parquet as pq
import pytest

import fetch_traffic_volume as traffic
import socrata


@pytest.fixture
def fake_api():
    """Serve `total` synthetic rows, honouring $limit and $offset."""
    state = {"total": 0, "calls": [], "urls": []}

    def handler(session, params, url):
        state["calls"].append(params)
        state["urls"].append(url)
        offset, limit = params["$offset"], params["$limit"]
        count = max(0, min(limit, state["total"] - offset))
        return [{"row": offset + i} for i in range(count)]

    with patch.object(socrata, "get_with_retry", handler):
        yield state


# --- the client is generic ---------------------------------------------

def test_a_dataset_id_becomes_a_json_endpoint():
    assert socrata.dataset_url("7ym2-wayt").endswith("/7ym2-wayt.json")
    assert socrata.dataset_url("h9gi-nx95") != socrata.dataset_url("7ym2-wayt")


def test_the_client_fetches_whichever_dataset_it_is_given(fake_api):
    fake_api["total"] = 10
    socrata.fetch("7ym2-wayt", ":id")
    assert all("7ym2-wayt" in url for url in fake_api["urls"])


def test_the_order_column_is_sent_on_every_request(fake_api):
    """Offset paging without a total order silently drops and repeats rows."""
    fake_api["total"] = 120_000
    socrata.fetch("7ym2-wayt", ":id")
    assert {call["$order"] for call in fake_api["calls"]} == {":id"}


def test_paging_covers_a_dataset_larger_than_one_page(fake_api):
    fake_api["total"] = 120_000
    fetched = socrata.fetch("7ym2-wayt", ":id")
    assert len(fetched) == 120_000
    assert fetched["row"].nunique() == 120_000


def test_a_non_positive_page_size_is_rejected():
    with pytest.raises(ValueError):
        socrata.fetch("7ym2-wayt", ":id", page_size=0)


def test_an_empty_dataset_is_an_empty_frame(fake_api):
    fake_api["total"] = 0
    assert socrata.fetch("7ym2-wayt", ":id").empty


# --- meeting an unfamiliar schema --------------------------------------

def test_the_fetch_makes_no_assumption_about_the_columns(fake_api):
    """The whole point of the first run is to learn the schema, so a
    surprising one must not break it."""
    fake_api["total"] = 3
    with patch.object(
        socrata, "fetch",
        lambda *a, **k: pd.DataFrame([{"Surprise": "1", "Unexpected": "2"}]),
    ):
        fetched = traffic.run(describe_only=True)
    assert list(fetched.columns) == ["Surprise", "Unexpected"]


def test_an_empty_dataset_is_reported_as_a_wrong_identifier():
    with patch.object(socrata, "fetch", lambda *a, **k: pd.DataFrame()):
        with pytest.raises(RuntimeError, match="dataset identifier"):
            traffic.run()


def test_the_schema_report_names_every_column():
    frame = pd.DataFrame({"Boro": ["Bronx", None], "Vol": ["12", "40"]})
    report = traffic.describe(frame)
    assert "`Boro`" in report and "`Vol`" in report
    assert "2 rows, 2 columns" in report


def test_the_schema_report_survives_an_all_null_column():
    frame = pd.DataFrame({"Vol": ["12"], "Empty": [None]})
    assert "`Empty`" in traffic.describe(frame)


def test_an_empty_frame_reports_rather_than_raises():
    assert "No rows" in traffic.describe(pd.DataFrame())


# --- what gets written -------------------------------------------------

def test_the_written_file_records_where_it_came_from(tmp_path):
    """Six months on, nobody remembers which dataset a Parquet came from."""
    out = traffic.write(pd.DataFrame({"Vol": ["1", "2"]}), tmp_path / "v.parquet")
    metadata = pq.read_schema(out).metadata

    assert metadata[traffic.SOURCE_KEY].decode() == traffic.DATASET_ID
    assert metadata[traffic.FETCHED_AT_KEY]


def test_writing_leaves_the_data_alone(tmp_path):
    frame = pd.DataFrame({"Vol": ["1", "2"], "Boro": ["Bronx", "Queens"]})
    out = traffic.write(frame, tmp_path / "v.parquet")
    assert pd.read_parquet(out).equals(frame)


def test_describe_only_writes_nothing(tmp_path):
    out = tmp_path / "v.parquet"
    with patch.object(socrata, "fetch",
                      lambda *a, **k: pd.DataFrame({"Vol": ["1"]})):
        traffic.run(output=out, describe_only=True)
    assert not out.exists()
