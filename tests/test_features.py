"""Tests for the feature layer: the vocabulary, and what it refuses to see."""

import pandas as pd
import pytest

from models import features as F


@pytest.fixture(scope="module")
def crashes(dataset_path):
    return pd.read_parquet(dataset_path)


# --- vehicle vocabulary ------------------------------------------------

@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Sedan", "Car"),
        ("4 dr sedan", "Car"),
        ("Convertible", "Car"),
        ("Station Wagon/Sport Utility Vehicle", "SUV or station wagon"),
        ("Taxi", "Taxi or livery"),
        ("Bus", "Bus"),
        ("School Bus", "Bus"),
        ("Box Truck", "Truck"),
        ("Tractor Truck Diesel", "Truck"),
        ("Garbage or Refuse", "Truck"),
        ("Van", "Van"),
        ("Carry All", "Van"),
        ("Motorcycle", "Motorcycle or moped"),
        ("Moped", "Motorcycle or moped"),
        ("Bike", "Bicycle"),
        ("E-Bike", "E-bike"),
        ("E-Scooter", "E-scooter"),
        ("Ambulance", "Emergency vehicle"),
        ("Horse carriage", "Other"),
    ],
)
def test_vehicle_spellings_land_in_the_right_class(raw, expected):
    assert F.classify_vehicle(pd.Series([raw])).iloc[0] == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Each of these contains a word that a broader rule also matches, so
        # they only come out right if the narrow rules are checked first.
        ("Pick-up Truck", "Pickup truck"),
        ("Fire Truck", "Emergency vehicle"),
        ("Tow Truck / Wrecker", "Truck"),
        ("Motorbike", "Motorcycle or moped"),
        ("E-Bike", "E-bike"),
    ],
)
def test_the_narrow_rule_wins_over_the_broad_one(raw, expected):
    assert F.classify_vehicle(pd.Series([raw])).iloc[0] == expected


def test_a_missing_vehicle_is_a_fact_not_a_gap():
    """No second vehicle means a single-vehicle crash, which the model
    should be able to split on."""
    assert F.classify_vehicle(pd.Series([None])).iloc[0] == F.NONE_RECORDED
    assert F.classify_vehicle(pd.Series([None]), missing=F.UNKNOWN).iloc[0] == F.UNKNOWN


def test_classification_ignores_case_and_padding():
    assert F.classify_vehicle(pd.Series(["  station   WAGON/sport utility  "])).iloc[0] == (
        "SUV or station wagon"
    )


# --- road vocabulary ---------------------------------------------------

@pytest.mark.parametrize(
    "raw,expected",
    [
        ("CROSS BRONX EXPRESSWAY", "Expressway or ramp"),
        ("GOWANUS EXPY (BQE)", "Expressway or ramp"),
        ("BELT PARKWAY", "Parkway"),
        ("GRAND CENTRAL PKWY", "Parkway"),
        ("BROOKLYN BRIDGE", "Bridge or tunnel"),
        ("QUEENS BOULEVARD", "Boulevard"),
        ("ATLANTIC AVENUE", "Avenue"),
        ("HENRY STREET", "Street"),
        ("BOSTON ROAD", "Road"),
        ("GRAND ARMY PLAZA", "Other named road"),
        (None, F.UNKNOWN),
    ],
)
def test_road_type_is_read_out_of_the_street_name(raw, expected):
    assert F.classify_road(pd.Series([raw])).iloc[0] == expected


def test_a_bridge_named_after_an_avenue_is_still_a_bridge():
    assert F.classify_road(pd.Series(["3 AVENUE BRIDGE"])).iloc[0] == "Bridge or tunnel"


# --- the label ---------------------------------------------------------

@pytest.mark.parametrize(
    "injured,killed,expected",
    [
        (0, 0, False),
        (1, 0, True),
        (0, 1, True),
        (3, 2, True),
        (None, None, False),
        (0, None, False),
    ],
)
def test_injury_covers_both_hurt_and_killed(injured, killed, expected):
    df = pd.DataFrame(
        {"number_of_persons_injured": [injured], "number_of_persons_killed": [killed]}
    )
    assert bool(F.label(df).iloc[0]) is expected


# --- the frame ---------------------------------------------------------

def test_the_frame_has_exactly_the_declared_features(crashes):
    assert list(F.build(crashes.head(500)).columns) == F.FEATURES


def test_categoricals_arrive_as_category_dtype(crashes):
    """HistGradientBoostingClassifier reads native categoricals off the dtype."""
    built = F.build(crashes.head(500))
    for column in F.CATEGORICAL_FEATURES:
        assert isinstance(built[column].dtype, pd.CategoricalDtype)


def test_nothing_is_left_null(crashes):
    """Absent values are named categories, so there is no missingness for
    the model to treat as a signal in its own right."""
    assert not F.build(crashes.head(2000)).isna().any().any()


def test_the_casualty_counts_are_not_features(crashes):
    """They are the target. Changing them must not change a single feature."""
    sample = crashes.head(1000).copy()
    before = F.build(sample)

    for column in sample.columns:
        if column.startswith("number_of_"):
            sample[column] = 99

    assert F.build(sample).equals(before)


def test_where_a_crash_happened_is_not_a_feature(crashes):
    """The watchlist ranks sites by how far their observed rate sits from the
    predicted one. Let the model see the location and it learns the site, and
    that difference goes to zero."""
    sample = crashes.head(1000).copy()
    before = F.build(sample)

    sample["latitude"] = 40.7
    sample["longitude"] = -73.9
    sample["zip_code"] = "10001"
    sample["off_street_name"] = "SOMEWHERE ELSE"

    assert F.build(sample).equals(before)


def test_the_vocabulary_stays_small_enough_to_read(crashes):
    """Native categorical splitting caps out at 255 categories, and a reader
    caps out well before that."""
    built = F.build(crashes)
    for column in F.CATEGORICAL_FEATURES:
        assert built[column].nunique() <= 60


def test_almost_nothing_falls_through_to_other(crashes):
    """A taxonomy that dumps a tenth of the data into "Other" is not a
    taxonomy."""
    built = F.build(crashes)
    assert (built["vehicle_1"] == "Other").mean() < 0.02
    assert (built["vehicle_2"] == "Other").mean() < 0.02
