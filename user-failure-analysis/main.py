#!/usr/bin/env python3
"""
Main module for user failure analysis.
Handles data fetching and analysis coordination.
"""

import os
import logging
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import json
import argparse
from datetime import datetime

from analyzer import create_visualization

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Azure Storage configuration
AZURE_STORAGE_CONNECTION_STRING = os.getenv(
    "AZURE_STORAGE_CONNECTION_STRING",
)
CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER", "user-failure-reports")


class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder for numpy types."""

    def default(self, obj):
        if isinstance(
            obj,
            (
                np.int_,
                np.intc,
                np.intp,
                np.int8,
                np.int16,
                np.int32,
                np.int64,
                np.uint8,
                np.uint16,
                np.uint32,
                np.uint64,
            ),
        ):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def get_blob_service_client() -> BlobServiceClient:
    """Get Azure Blob Service client."""
    return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)


def save_to_blob_storage(file_path: str, blob_name: str) -> str:
    """
    Upload a file to Azure Blob Storage.

    Args:
        file_path: Local file path
        blob_name: Name for the blob

    Returns:
        Blob URL
    """
    try:
        blob_service_client = get_blob_service_client()
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        with open(file_path, "rb") as data:
            blob_client = container_client.upload_blob(
                name=blob_name, data=data, overwrite=True
            )

        return blob_client.url

    except Exception as e:
        logger.error(f"Error uploading to blob storage: {str(e)}", exc_info=True)
        raise


def fetch_data(from_date: str, to_date: str) -> pd.DataFrame:
    """
    Fetch user session data from the database.

    Args:
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format

    Returns:
        DataFrame with user session data
    """
    try:
        # TODO: Replace with actual database query
        # This is sample data for testing
        data = {
            "user": [
                "Akshay",
                "Akhilesh",
                "Mrinal",
                "Swap",
                "Anushka",
                "Namya",
                "Ravi",
                "Vishnu",
            ],
            "total_sessions": [1, 3, 10, 5, 53, 6, 2, 2],
            "failed_sessions": [1, 2, 6, 2, 15, 1, 0, 0],
            "failure_rate": [100.0, 66.7, 60.0, 40.0, 28.3, 16.7, 0.0, 0.0],
        }
        return pd.DataFrame(data)

    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}", exc_info=True)
        raise


def process_data(df: pd.DataFrame, max_users: int = 15) -> pd.DataFrame:
    """
    Process the raw data and calculate failure rates.

    Args:
        df: Raw DataFrame with user session data
        max_users: Maximum number of users to include in analysis

    Returns:
        Processed DataFrame with failure statistics
    """
    try:
        # Sort by failure rate descending
        df = df.sort_values("failure_rate", ascending=False)

        # Take top N users
        if len(df) > max_users:
            df = df.head(max_users)

        return df

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}", exc_info=True)
        raise


def run_analysis(
    from_date: str,
    to_date: str,
    max_users: Optional[int] = 15,
    output_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run the complete analysis pipeline.

    Args:
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format
        max_users: Maximum number of users to include
        output_dir: Directory to save output files

    Returns:
        Dictionary containing analysis results and file URLs
    """
    try:
        # Set output directory
        if output_dir is None:
            output_dir = os.getenv("OUTPUT_DIR", "/tmp")

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Fetch data
        logger.info(f"Fetching data for period {from_date} to {to_date}")
        df = fetch_data(from_date, to_date)

        # Process data
        logger.info("Processing data...")
        df_processed = process_data(df, max_users)

        # Generate date range string for file names
        date_range = f"from_{from_date.replace('-', '')}_to_{to_date.replace('-', '')}"

        # Create visualization
        logger.info("Creating visualization...")
        html_path = os.path.join(output_dir, f"user_failure_rates_{date_range}.html")
        csv_path = os.path.join(output_dir, f"user_failure_stats_{date_range}.csv")

        create_visualization(df_processed, output_dir, date_range)

        # Upload files to Azure Blob Storage
        logger.info("Uploading files to Azure Blob Storage...")
        html_url = save_to_blob_storage(
            html_path, f"user_failure_rates_{date_range}.html"
        )
        csv_url = save_to_blob_storage(csv_path, f"user_failure_stats_{date_range}.csv")

        # Calculate summary statistics
        total_users = int(len(df))  # Convert to standard Python int
        total_sessions = int(df["total_sessions"].sum())
        failed_sessions = int(df["failed_sessions"].sum())
        overall_failure_rate = float(  # Convert to standard Python float
            (failed_sessions / total_sessions * 100) if total_sessions > 0 else 0
        )

        logger.info("Analysis completed successfully")
        return {
            "success": True,
            "data": {
                "from_date": from_date,
                "to_date": to_date,
                "html_url": html_url,
                "csv_url": csv_url,
                "stats": {
                    "total_users": total_users,
                    "total_sessions": total_sessions,
                    "failed_sessions": failed_sessions,
                    "overall_failure_rate": overall_failure_rate,
                },
            },
        }

    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}", exc_info=True)
        return {"success": False, "error": str(e)}


def validate_date(date_str: str) -> str:
    """Validate date string format."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD"
        )


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="User failure rate analysis")
    parser.add_argument(
        "--from-date", type=validate_date, required=True, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--to-date", type=validate_date, required=True, help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--max-users",
        type=int,
        default=15,
        help="Maximum number of users to include (default: 15)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./output",
        help="Output directory for reports (default: ./output)",
    )

    args = parser.parse_args()

    # Run analysis
    result = run_analysis(
        from_date=args.from_date,
        to_date=args.to_date,
        max_users=args.max_users,
        output_dir=args.output_dir,
    )

    # Print result with custom JSON encoder for numpy types
    print(json.dumps(result, indent=2, cls=NumpyEncoder))
