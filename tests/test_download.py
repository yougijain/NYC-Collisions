"""Tests for fetching the published dataset.

The failure worth guarding is not a download that errors -- requests
raises and the fallbacks take over -- but one that never finishes. A host
that trickles bytes slower than the per-read timeout can hold a deployed
dashboard on "Running" for good, which is what a total deadline is for.
No test here touches the network.
"""

import pytest
import requests

import db


class FakeResponse:
    """Just enough of a streamed requests response."""

    def __init__(self, chunks, status=200):
        self.chunks = chunks
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status}")

    def iter_content(self, chunk_size):
        yield from self.chunks


@pytest.fixture
def clock(monkeypatch):
    """A clock that only moves when a chunk is read, by a set step."""
    state = {"now": 0.0, "step": 0.0}

    def tick():
        return state["now"]

    monkeypatch.setattr(db.time, "monotonic", tick)
    return state


def serve(monkeypatch, chunks, clock=None, status=200):
    def advancing():
        for chunk in chunks:
            if clock is not None:
                clock["now"] += clock["step"]
            yield chunk

    monkeypatch.setattr(
        db.requests, "get",
        lambda *a, **k: FakeResponse(advancing(), status),
    )


def test_a_download_lands_whole(tmp_path, monkeypatch):
    serve(monkeypatch, [b"abc", b"def"])
    target = tmp_path / "collisions.parquet"

    assert db._download("https://example.invalid/x", target)
    assert target.read_bytes() == b"abcdef"


def test_a_trickle_is_abandoned_at_the_deadline(tmp_path, monkeypatch, clock):
    """Each chunk arrives well inside the per-read timeout, but the whole
    transfer would run for ten minutes."""
    clock["step"] = 60.0
    serve(monkeypatch, [b"x"] * 10, clock)
    target = tmp_path / "collisions.parquet"

    assert not db._download("https://example.invalid/x", target, deadline=180)
    assert not target.exists()
    assert not list(tmp_path.glob("*.part"))


def test_an_abandoned_download_leaves_the_old_cache_alone(tmp_path, monkeypatch, clock):
    target = tmp_path / "collisions.parquet"
    target.write_bytes(b"last week's dataset")
    clock["step"] = 60.0
    serve(monkeypatch, [b"x"] * 10, clock)

    db._download("https://example.invalid/x", target, deadline=180)
    assert target.read_bytes() == b"last week's dataset"


def test_an_http_error_is_a_failed_download(tmp_path, monkeypatch):
    serve(monkeypatch, [b"never written"], status=500)
    target = tmp_path / "collisions.parquet"

    assert not db._download("https://example.invalid/x", target)
    assert not target.exists()
