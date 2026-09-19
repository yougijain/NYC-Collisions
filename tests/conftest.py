import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "app"))
sys.path.insert(0, str(ROOT / "scripts"))

import db  # noqa: E402


@pytest.fixture(scope="session")
def connection():
    """A DuckDB connection over whichever dataset is resolvable.

    Falls back to the committed seed, so the suite runs offline on a fresh
    clone without first building the dataset.
    """
    con = db.connect()
    yield con
    con.close()
