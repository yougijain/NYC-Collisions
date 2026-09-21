"""Tests for the one-spelling-per-street rule.

The source writes a single junction as many as fourteen ways, and each
spelling keys as a different intersection. These pin the collapse.
"""

import pandas as pd
import pytest

import street_names
from street_names import canonical, canonical_series


@pytest.mark.parametrize(
    "written,expected",
    [
        ("Belt Pkwy", "BELT PARKWAY"),
        ("BELT PARKWAY", "BELT PARKWAY"),
        ("belt parkway", "BELT PARKWAY"),
        ("  Belt   Pkwy  ", "BELT PARKWAY"),
        ("E 165th St", "EAST 165 STREET"),
        ("EAST 165 STREET", "EAST 165 STREET"),
        ("College Point Blvd", "COLLEGE POINT BOULEVARD"),
        ("Cross Bronx Expy", "CROSS BRONX EXPRESSWAY"),
        ("W 42nd St", "WEST 42 STREET"),
        ("Hylan Blvd.", "HYLAN BOULEVARD"),
        ("GOWANUS EXPY (BQE)", "GOWANUS EXPRESSWAY BQE"),
    ],
)
def test_one_junction_gets_one_spelling(written, expected):
    assert canonical(written) == expected


@pytest.mark.parametrize(
    "spellings",
    [
        ("Belt Pkwy", "BELT PARKWAY", "belt parkway", "Belt Parkway"),
        ("E 165th St", "EAST 165 STREET", "e 165 st"),
        ("Jackie Robinson Pkwy", "JACKIE ROBINSON PARKWAY"),
    ],
)
def test_variants_of_one_street_collapse_together(spellings):
    assert len({canonical(name) for name in spellings}) == 1


def test_different_streets_stay_different():
    """Collapsing must not merge streets that are genuinely distinct."""
    assert canonical("3 AVENUE") != canonical("3 STREET")
    assert canonical("EAST 165 STREET") != canonical("WEST 165 STREET")
    assert canonical("OCEAN AVENUE") != canonical("OCEAN PARKWAY")


def test_an_ordinal_is_only_stripped_from_a_number():
    """'1ST' is first; 'FIRST' is a word and stays one."""
    assert canonical("1ST AVENUE") == "1 AVENUE"
    assert canonical("FIRST AVENUE") == "FIRST AVENUE"


def test_nothing_in_becomes_nothing_out():
    assert canonical(None) == ""
    assert canonical(float("nan")) == ""
    assert canonical("   ") == ""
    assert canonical(123) == ""


def test_canonicalising_is_idempotent():
    """Rebuilding a dataset must not keep changing it."""
    for name in ("Belt Pkwy", "E 165th St", "GOWANUS EXPY (BQE)"):
        once = canonical(name)
        assert canonical(once) == once


def test_a_column_keeps_blanks_null():
    column = canonical_series(pd.Series(["Belt Pkwy", None, "  ", "E 9th St"]))
    assert column.tolist()[0] == "BELT PARKWAY"
    assert pd.isna(column.iloc[1]) and pd.isna(column.iloc[2])
    assert column.iloc[3] == "EAST 9 STREET"


def test_every_abbreviation_expands_to_something_longer():
    for short, long in street_names.ABBREVIATIONS.items():
        assert len(long) > len(short), f"{short} -> {long}"
        assert long.isupper()
