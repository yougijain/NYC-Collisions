"""Tests for the build artifacts: version stamping, the seed, the facts file."""

import pandas as pd
import pytest

import build_dataset
import build_seed
import dataset_facts
from clean import DATASET_VERSION


@pytest.fixture
def dataset():
    """A small slice of the committed seed, as a build would produce it."""
    return pd.read_parquet("data/clean/collisions_seed.parquet").head(500)


@pytest.fixture
def unsampled():
    """Dense, consecutive collision_ids.

    The committed seed is itself a hash sample, so every id in it already
    survives the filter and sampling it again is a no-op. These tests need a
    frame that stands in for a full build.
    """
    return pd.DataFrame({"collision_id": range(1, 2001)})


# --- version stamping --------------------------------------------------

def test_a_build_records_the_cleaning_semantics_it_used(tmp_path, dataset):
    out = tmp_path / "collisions.parquet"
    build_dataset.write(dataset, out)
    assert build_dataset.stamped_version(out) == DATASET_VERSION


def test_stamping_leaves_the_data_unchanged(tmp_path, dataset):
    out = tmp_path / "collisions.parquet"
    build_dataset.write(dataset, out)
    assert pd.read_parquet(out).equals(dataset.reset_index(drop=True))


def test_an_unstamped_file_reads_as_version_zero(tmp_path, dataset):
    """Anything published before the stamp existed predates every version."""
    out = tmp_path / "legacy.parquet"
    dataset.to_parquet(out, index=False)
    assert build_dataset.stamped_version(out) == 0


def test_a_stale_dataset_is_not_built_on(tmp_path, dataset, caplog):
    """Merging new rows into rows cleaned under older semantics would leave
    the two mixed together, so the builder starts over instead."""
    out = tmp_path / "legacy.parquet"
    dataset.to_parquet(out, index=False)
    assert build_dataset.read_existing(out) is None
    assert "rebuilding in full" in caplog.text


def test_a_current_dataset_is_built_on(tmp_path, dataset):
    out = tmp_path / "collisions.parquet"
    build_dataset.write(dataset, out)
    assert len(build_dataset.read_existing(out)) == len(dataset)


def test_a_stale_dataset_triggers_a_full_refetch(tmp_path, dataset, monkeypatch):
    """The rebuild has to go back to --since, not to the overlap window."""
    out = tmp_path / "collisions.parquet"
    dataset.to_parquet(out, index=False)

    requested = []
    raw = pd.read_csv("data/raw/nyc_collisions_sample.csv")

    def fake_fetch(since=None, **kwargs):
        requested.append(since)
        return raw

    monkeypatch.setattr(build_dataset, "fetch_collisions", fake_fetch)
    build_dataset.build(output=out)

    assert requested == [build_dataset.DEFAULT_SINCE]
    assert build_dataset.stamped_version(out) == DATASET_VERSION


# --- seed sampling -----------------------------------------------------

def test_the_seed_is_about_the_requested_fraction(unsampled):
    kept = build_seed.sample(unsampled, keep_every=10)
    assert set(kept["collision_id"]).issubset(set(unsampled["collision_id"]))
    assert len(kept) == pytest.approx(len(unsampled) / 10, rel=0.25)


def test_the_same_crashes_are_sampled_every_time(unsampled):
    """Otherwise rebuilding the seed churns the whole committed file."""
    first = build_seed.sample(unsampled, keep_every=10)
    second = build_seed.sample(unsampled, keep_every=10)
    assert first["collision_id"].tolist() == second["collision_id"].tolist()


def test_sampling_does_not_depend_on_row_order(unsampled):
    shuffled = unsampled.sample(frac=1, random_state=0)
    assert sorted(build_seed.sample(shuffled, keep_every=10)["collision_id"]) == (
        build_seed.sample(unsampled, keep_every=10)["collision_id"].tolist()
    )


def test_a_sample_of_one_in_one_keeps_everything(unsampled):
    assert len(build_seed.sample(unsampled, keep_every=1)) == len(unsampled)


def test_a_non_positive_sampling_rate_is_rejected(unsampled):
    with pytest.raises(ValueError):
        build_seed.sample(unsampled, keep_every=0)


def test_building_a_seed_without_a_source_fails_loudly(tmp_path):
    with pytest.raises(FileNotFoundError):
        build_seed.build_seed(source=tmp_path / "absent.parquet")


# --- published figures -------------------------------------------------

def test_the_facts_describe_the_dataset_they_were_measured_from(dataset_path):
    facts = dataset_facts.collect(dataset_path)
    expected = pd.read_parquet(dataset_path)

    assert facts["rows"] == len(expected)
    assert facts["people_killed"] == int(expected["number_of_persons_killed"].sum())
    assert facts["dataset_version"] == DATASET_VERSION
    assert 0 < facts["injury_crash_rate"] < 1


def test_the_facts_render_as_a_readable_table(dataset_path):
    facts = dataset_facts.collect(dataset_path)
    rendered = dataset_facts.render(facts)
    assert f"{facts['rows']:,}" in rendered
    assert facts["first_crash"][:10] in rendered


# --- documentation that quotes generated figures -----------------------

def test_the_documentation_matches_the_generated_figures():
    """The README and the model card quote about thirty numbers. If one is
    edited by hand instead of regenerated, this is what notices."""
    import sync_docs

    assert sync_docs.sync(check=True) == []


def test_every_marker_in_the_docs_has_a_block_behind_it():
    import sync_docs

    blocks = set(sync_docs.build_blocks())
    for path in sync_docs.TARGETS:
        referenced = set(sync_docs.markers_in(path.read_text(encoding="utf-8")))
        assert referenced, f"{path.name} references no generated blocks"
        assert referenced <= blocks


def test_a_marker_with_no_block_is_an_error(tmp_path):
    import sync_docs

    doc = tmp_path / "stray.md"
    doc.write_text(
        "<!-- generated:not-a-block -->\nx\n<!-- /generated:not-a-block -->\n",
        encoding="utf-8",
    )
    with pytest.raises(KeyError, match="not-a-block"):
        sync_docs.sync(targets=[doc])


def test_rewriting_a_block_leaves_the_prose_around_it_alone(tmp_path):
    import sync_docs

    doc = tmp_path / "page.md"
    doc.write_text(
        "before\n\n<!-- generated:dataset-headline -->\nstale\n"
        "<!-- /generated:dataset-headline -->\n\nafter\n",
        encoding="utf-8",
    )
    sync_docs.sync(targets=[doc])

    written = doc.read_text(encoding="utf-8")
    assert written.startswith("before\n")
    assert written.endswith("after\n")
    assert "stale" not in written
