"""Cut the committed seed dataset down from a full build.

The seed is the dashboard's last-resort fallback and the fixture the test
suite runs against, so it has to live in git. A full build is ~13 MB and
grows every week, which is more binary churn than a repository should carry,
so the seed is a fixed fraction of it.

Sampling is by a hash of collision_id rather than at random: the same crash
lands in or out of the seed on every run, so rebuilding it produces a diff
only where the underlying data actually changed.

Usage:
    python scripts/build_seed.py --source data/clean/collisions.parquet
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from build_dataset import write  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_SOURCE = Path("data/clean/collisions.parquet")
DEFAULT_OUTPUT = Path("data/clean/collisions_seed.parquet")

# 1 row in 20. Keeps the seed near a megabyte while leaving enough rows per
# borough and per month for the query and model tests to mean something.
DEFAULT_KEEP_EVERY = 20


def sample(df: pd.DataFrame, keep_every: int = DEFAULT_KEEP_EVERY) -> pd.DataFrame:
    """Take a deterministic 1-in-`keep_every` sample of the dataset.

    Args:
        df: A full cleaned dataset.
        keep_every: Denominator of the sampling fraction.

    Returns:
        The sampled rows, in the source's order.

    Raises:
        ValueError: If `keep_every` is not a positive integer.
    """
    if keep_every < 1:
        raise ValueError("keep_every must be a positive integer")
    if keep_every == 1:
        return df.reset_index(drop=True)

    # pandas' hash is stable across runs and platforms, unlike hash(), and
    # spreads sequential collision_ids evenly across the buckets.
    buckets = pd.util.hash_array(df["collision_id"].to_numpy(dtype="int64"))
    return df[buckets % keep_every == 0].reset_index(drop=True)


def build_seed(
    source: Path = DEFAULT_SOURCE,
    output: Path = DEFAULT_OUTPUT,
    keep_every: int = DEFAULT_KEEP_EVERY,
) -> pd.DataFrame:
    """Sample a full build down to the committed seed and write it.

    Args:
        source: Path to the full dataset.
        output: Destination for the seed.
        keep_every: Denominator of the sampling fraction.

    Returns:
        The seed that was written.

    Raises:
        FileNotFoundError: If `source` does not exist.
    """
    if not source.exists():
        raise FileNotFoundError(
            f"{source} not found. Run `python scripts/build_dataset.py --full` "
            f"or point --source at a published build."
        )

    full = pd.read_parquet(source)
    seed = sample(full, keep_every)
    logger.info(
        f"Sampled {len(seed):,} of {len(full):,} rows (1 in {keep_every})"
    )
    write(seed, output)
    return seed


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE,
                        help=f"full dataset to sample (default: {DEFAULT_SOURCE})")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT,
                        help=f"seed destination (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--keep-every", type=int, default=DEFAULT_KEEP_EVERY,
                        help=f"keep 1 row in N (default: {DEFAULT_KEEP_EVERY})")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    build_seed(source=args.source, output=args.out, keep_every=args.keep_every)


if __name__ == "__main__":
    main()
