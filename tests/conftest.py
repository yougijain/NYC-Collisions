import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "app"))
sys.path.insert(0, str(ROOT / "scripts"))

import db  # noqa: E402

SEED = ROOT / "data" / "clean" / "collisions_seed.parquet"


@pytest.fixture(scope="session")
def dataset_path() -> str:
    """The dataset the suite runs against.

    The committed seed by default, so a run is hermetic: no network, and the
    same rows on every machine. app/db.py's own resolution order would reach
    for the published Release asset whenever one is downloadable, which would
    quietly turn a code change's CI into a check on live data.

    The refresh workflow sets $NYC_COLLISIONS_DATA to a freshly built dataset
    and reuses this suite as the gate on publishing it.
    """
    return os.getenv("NYC_COLLISIONS_DATA") or str(SEED)


@pytest.fixture(scope="session")
def connection(dataset_path):
    con = db.connect(dataset_path)
    yield con
    con.close()
