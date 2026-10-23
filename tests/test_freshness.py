"""Tests for the source-freshness check.

The check exists because the pipeline ran green for five weeks over data
that had not moved, so the thing worth testing is that it tells the two
silences apart: a source that has stopped publishing, and a pipeline that
has stopped keeping up. Reporting one as the other sends somebody to debug
an ingest that was working.
"""

import json
from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

import check_freshness as C
import socrata

TODAY = date(2026, 10, 20)


def assess(source, dataset, **kwargs):
    return C.assess(source, dataset, today=TODAY, **kwargs)


# --- telling the two silences apart ------------------------------------

def test_a_source_that_stopped_publishing_is_not_our_bug():
    """Five green weekly runs over a dataset frozen at 2026-06-11, because
    NYC stopped publishing. The pipeline was fine."""
    found = assess(date(2026, 6, 11), date(2026, 6, 11))

    assert found["source_quiet"] is True
    assert found["pipeline_behind"] is False
    assert "gone quiet" in found["headline"]


def test_dates_read_the_same_on_every_platform():
    """%-d is glibc-only: green in CI, ValueError on a Windows checkout."""
    found = assess(date(2026, 6, 5), date(2026, 6, 5))
    assert "since 5 June 2026," in found["headline"]


def test_a_pipeline_trailing_a_live_source_is_our_bug():
    found = assess(date(2026, 10, 18), date(2026, 6, 11))

    assert found["pipeline_behind"] is True
    assert found["source_quiet"] is False
    assert "has not ingested" in found["headline"]


def test_being_behind_outranks_the_source_being_quiet():
    """Both can be true at once. The actionable one has to win, or the
    banner sends nobody to look at the ingest."""
    found = assess(date(2026, 8, 1), date(2026, 1, 1))

    assert found["pipeline_behind"] is True
    assert found["source_quiet"] is True
    assert "behind the source" in found["headline"]


def test_keeping_up_with_a_live_source_raises_nothing():
    found = assess(date(2026, 10, 18), date(2026, 10, 18))

    assert found["source_quiet"] is False
    assert found["pipeline_behind"] is False


# --- the thresholds ----------------------------------------------------

def test_the_ordinary_reporting_lag_is_not_an_alarm():
    """NYC publishes in batches a fortnight behind. Alarming on that would
    fire most weeks and teach everyone to ignore it."""
    assert assess(date(2026, 10, 6), date(2026, 10, 6))["source_quiet"] is False


def test_three_weeks_of_silence_is():
    assert assess(date(2026, 9, 29), date(2026, 9, 29))["source_quiet"] is True


def test_a_build_a_day_behind_a_live_source_is_mid_lag_not_broken():
    assert assess(date(2026, 10, 19), date(2026, 10, 18))["pipeline_behind"] is False


# --- an unreachable source ---------------------------------------------

def test_an_unreachable_source_is_reported_as_unknown_not_as_fine():
    """Absence of evidence must not render as a green tick."""
    found = assess(None, date(2026, 6, 11))

    assert found["source_quiet"] is None
    assert found["pipeline_behind"] is None
    assert "Could not reach" in found["headline"]


def test_a_failing_source_query_does_not_fail_the_run(tmp_path, dataset_path):
    """A quiet or unreachable upstream is not a broken build, and a
    workflow that goes red for it trains people to ignore red."""
    with patch.object(C, "source_latest", side_effect=RuntimeError("502")):
        found = C.run(dataset=dataset_path, output=tmp_path / "f.json")

    assert found["source_quiet"] is None
    assert (tmp_path / "f.json").exists()


# --- what it asks the source -------------------------------------------

def test_the_newest_date_is_one_aggregate_request_not_a_page_of_rows():
    """`max()` is answered from the index, so this can run every refresh
    without costing the API anything."""
    seen = {}

    def handler(http, params, url):
        seen.update(params)
        return [{"newest": "2026-06-11T00:00:00.000"}]

    with patch.object(socrata, "get_with_retry", handler), \
         patch.object(socrata, "session", lambda token=None: None):
        assert C.source_latest() == date(2026, 6, 11)

    assert "max(crash_date)" in seen["$select"]
    assert "$limit" not in seen and "$offset" not in seen


def test_a_source_answering_with_nothing_is_unknown():
    with patch.object(socrata, "get_with_retry", lambda *a, **k: []), \
         patch.object(socrata, "session", lambda token=None: None):
        assert C.source_latest() is None


# --- what gets written -------------------------------------------------

def test_the_record_carries_both_dates(tmp_path, dataset_path):
    with patch.object(C, "source_latest", return_value=date(2026, 6, 11)):
        C.run(dataset=dataset_path, output=tmp_path / "f.json")

    written = json.loads((tmp_path / "f.json").read_text())
    assert written["source_latest"] == "2026-06-11"
    assert written["dataset_latest"]
    assert written["headline"]


def test_the_job_summary_names_which_finding_it_is():
    quiet = C.summarise(assess(date(2026, 6, 11), date(2026, 6, 11)))
    behind = C.summarise(assess(date(2026, 10, 18), date(2026, 6, 11)))
    healthy = C.summarise(assess(date(2026, 10, 18), date(2026, 10, 18)))

    assert "gone quiet" in quiet
    assert "behind the source" in behind
    assert "Current with the source" in healthy


def test_the_dataset_date_comes_from_the_build_being_shipped(dataset_path):
    latest = C.dataset_latest(pd.io.common.stringify_path(dataset_path))
    frame = pd.read_parquet(dataset_path, columns=["crash_datetime"])

    assert latest == pd.to_datetime(frame["crash_datetime"]).max().date()


# --- the banner --------------------------------------------------------

def test_the_banner_is_silent_when_nothing_has_been_checked(monkeypatch):
    """A fresh clone has no freshness record, and that is not a warning."""
    from views import common

    monkeypatch.setattr(common, "freshness_data", dict)
    monkeypatch.setattr(common.st, "warning", _fail("warned"))
    monkeypatch.setattr(common.st, "info", _fail("informed"))
    common.freshness_banner()


def test_the_banner_is_silent_when_the_data_is_current(monkeypatch):
    from views import common

    monkeypatch.setattr(common, "freshness_data",
                        lambda: assess(date(2026, 10, 18), date(2026, 10, 18)))
    monkeypatch.setattr(common.st, "warning", _fail("warned"))
    monkeypatch.setattr(common.st, "info", _fail("informed"))
    common.freshness_banner()


def test_a_quiet_source_reads_as_information_and_a_gap_as_a_warning(monkeypatch):
    """Tone carries the difference a reader acts on: one is nobody's
    fault, the other is a defect."""
    from views import common

    said = {}
    monkeypatch.setattr(common.st, "info", lambda m: said.setdefault("info", m))
    monkeypatch.setattr(common.st, "warning", lambda m: said.setdefault("warn", m))

    monkeypatch.setattr(common, "freshness_data",
                        lambda: assess(date(2026, 6, 11), date(2026, 6, 11)))
    common.freshness_banner()
    assert "info" in said and "warn" not in said

    said.clear()
    monkeypatch.setattr(common, "freshness_data",
                        lambda: assess(date(2026, 10, 18), date(2026, 6, 11)))
    common.freshness_banner()
    assert "warn" in said and "info" not in said


def _fail(what):
    def boom(*args, **kwargs):
        raise AssertionError(f"the banner {what} when it should have stayed quiet")
    return boom
