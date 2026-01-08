import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Union

import pandas as pd
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
DATASET_ID: str = "h9gi-nx95"
BASE_URL: str = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"
OUTPUT_DIR: Path = Path("data/raw")
REQUEST_TIMEOUT: int = 60
DEFAULT_LIMIT: int = 1000

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def fetch_collisions_data(
    limit: Optional[int] = None, where_clause: Optional[str] = None
) -> pd.DataFrame:
    params: Dict[str, Union[int, str]] = {}

    if limit is not None:
        if limit <= 0:
            raise ValueError("Limit must be a positive integer")
        params["$limit"] = limit

    if where_clause:
        params["$where"] = where_clause

    logger.info(f"Fetching data from NYC Open Data API: {BASE_URL}")
    if params:
        logger.debug(f"Request parameters: {params}")

    try:
        response = requests.get(
            BASE_URL, params=params, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()

        data = response.json()
        if not data:
            logger.warning("API returned empty dataset")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        logger.info(f"Successfully fetched {len(df)} records")
        return df

    except requests.exceptions.Timeout:
        logger.error(f"Request timed out after {REQUEST_TIMEOUT} seconds")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error occurred: {e.response.status_code} - {e}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        raise
    except (ValueError, KeyError) as e:
        logger.error(f"Error parsing response data: {e}")
        raise ValueError(f"Invalid data format received: {e}") from e


def save_data(df: pd.DataFrame, filename: Optional[str] = None) -> Path:
    """
    Save fetched data to CSV file.

    Args:
        df: DataFrame to save.
        filename: Optional custom filename. If None, generates timestamped filename.

    Returns:
        Path to the saved file.

    Raises:
        ValueError: If DataFrame is empty.
        OSError: If file cannot be written.
    """
    if df.empty:
        raise ValueError("Cannot save empty DataFrame")

    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"nyc_collisions_latest_{timestamp}.csv"

    output_path = OUTPUT_DIR / filename

    try:
        df.to_csv(output_path, index=False)
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(
            f"Data saved successfully: {output_path} ({file_size_mb:.2f} MB)"
        )
        return output_path
    except OSError as e:
        logger.error(f"Failed to save file to {output_path}: {e}")
        raise


def main() -> None:
    """
    Main entry point for fetching and saving NYC collision data.

    Fetches a sample of collision data (default: 1000 records) and saves
    it to a CSV file in the data/raw directory.
    """
    logger.info("Starting NYC Collisions data fetch")

    try:
        logger.info(f"Fetching sample data (limit: {DEFAULT_LIMIT} records)")
        df = fetch_collisions_data(limit=DEFAULT_LIMIT)

        if df.empty:
            logger.warning("No data fetched from API")
            return

        # Display summary statistics
        logger.info("Data Summary:")
        logger.info(f"  Records: {len(df):,}")
        logger.info(f"  Columns: {len(df.columns)}")

        if "crash_date" in df.columns:
            min_date = df["crash_date"].min()
            max_date = df["crash_date"].max()
            logger.info(f"  Date range: {min_date} to {max_date}")

        # Save to file
        output_path = save_data(df, "nyc_collisions_latest.csv")
        logger.info(f"Data fetch completed successfully: {output_path}")

    except (requests.exceptions.RequestException, ValueError, OSError) as e:
        logger.error(f"Data fetch failed: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error occurred: {e}")
        raise


if __name__ == "__main__":
    main()
