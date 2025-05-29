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
import psycopg2
from psycopg2 import sql

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
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER", "user-failure-reports")

# Ensure required environment variables are set
if not AZURE_STORAGE_CONNECTION_STRING:
    logger.warning(
        "AZURE_STORAGE_CONNECTION_STRING not set. Azure Blob Storage will not be available."
    )


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


def get_blob_service_client() -> Optional[BlobServiceClient]:
    """Get Azure Blob Service client."""
    if not AZURE_STORAGE_CONNECTION_STRING:
        return None
    return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)


def save_to_blob_storage(file_path: str, blob_name: str) -> Optional[str]:
    """
    Upload a file to Azure Blob Storage.

    Args:
        file_path: Local file path
        blob_name: Name for the blob

    Returns:
        Blob URL or None if Azure Storage is not configured
    """
    try:
        blob_service_client = get_blob_service_client()
        if not blob_service_client:
            logger.warning("Azure Blob Storage not configured. Skipping upload.")
            return None

        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        with open(file_path, "rb") as data:
            blob_client = container_client.upload_blob(
                name=blob_name, data=data, overwrite=True
            )

        return blob_client.url

    except Exception as e:
        logger.error(f"Error uploading to blob storage: {str(e)}", exc_info=True)
        return None


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
        # Database connection string
        db_url = os.getenv(
            "AZURE_POSTGRESQL_CONNECTIONSTRING",  # Azure provided connection string
            os.getenv("DB_URL", "")  # Fallback to custom DB_URL
        )
        
        if not db_url:
            logger.error("No database connection string found in environment variables")
            raise ValueError("Database connection string not configured")
        
        logger.info(f"Fetching data for period {from_date} to {to_date}")
        
        # Format date range for query
        start_datetime = f"{from_date} 00:00:00"
        end_datetime = f"{to_date} 23:59:59"
        
        # Query to get session data
        query = """
        SELECT
            c.session_id,
            u.name,
            c.exotel_call_sid
        FROM
            public.calllog c
        JOIN
            public."user" u
            ON c.user_id = u.user_id
        WHERE
            c.created_on >= %s::timestamp
            AND c.created_on <= %s::timestamp
        ORDER BY c.created_on DESC;
        """
        
        # Execute query
        connection = None
        try:
            logger.info("Connecting to database...")
            connection = psycopg2.connect(db_url)
            
            # Set statement timeout (60 seconds)
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout = 60000;")  # 60 seconds in ms
            
            logger.info("Executing query...")
            df = pd.read_sql_query(query, connection, params=(start_datetime, end_datetime))
            
            logger.info(f"Query executed successfully. Retrieved {len(df)} rows")
            
            # Check if we need to generate test data for development/demo purposes
            if df.empty:
                logger.warning("Query returned no results for the specified date range")
                
                # Check if we should generate test data (for development/demo only)
                generate_test_data = os.getenv("GENERATE_TEST_DATA", "").lower() == "true"
                if generate_test_data:
                    logger.warning("Generating test data for development/demo purposes")
                    df = _generate_test_data()
            
            return df
            
        finally:
            if connection:
                connection.close()
                logger.info("Database connection closed")

    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}", exc_info=True)
        raise


def _generate_test_data(num_records=100):
    """Generate test data with some failures for development/demo purposes."""
    test_data = []
    for i in range(1, num_records + 1):
        user = f"test_user_{i % 5 + 1}"
        # Make some calls have null exotel_call_sid to simulate failures
        call_sid = None if i % 3 == 0 else f"call_{i}"
        test_data.append({
            "session_id": i,
            "name": user,
            "exotel_call_sid": call_sid
        })
    
    df = pd.DataFrame(test_data)
    logger.info(f"Generated {len(df)} test records with {df['exotel_call_sid'].isnull().sum()} failures")
    return df


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
        # Return empty DataFrame with required columns if input is empty
        if df.empty:
            logger.warning("Empty dataset provided to process_data")
            return pd.DataFrame(
                columns=["user", "total_sessions", "failed_sessions", "failure_rate"]
            )

        # Group by user name
        user_stats = (
            df.groupby("name")
            .agg({
                "session_id": "count", 
                "exotel_call_sid": lambda x: x.isnull().sum()
            })
            .rename(
                columns={
                    "session_id": "total_sessions",
                    "exotel_call_sid": "failed_sessions",
                }
            )
        )

        # Calculate failure rates
        user_stats["failure_rate"] = (
            user_stats["failed_sessions"] / user_stats["total_sessions"] * 100
        ).round(1)
        
        # Reset index to make 'name' a column and rename to 'user' for consistency
        user_stats = user_stats.reset_index().rename(columns={"name": "user"})
        
        # Sort by failure rate descending
        user_stats = user_stats.sort_values("failure_rate", ascending=False)

        # Take top N users
        if len(user_stats) > max_users:
            user_stats = user_stats.head(max_users)

        return user_stats

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

        # Upload files to Azure Blob Storage if configured
        html_url = None
        csv_url = None

        if AZURE_STORAGE_CONNECTION_STRING:
            logger.info("Uploading files to Azure Blob Storage...")
            html_url = save_to_blob_storage(
                html_path, f"user_failure_rates_{date_range}.html"
            )
            csv_url = save_to_blob_storage(
                csv_path, f"user_failure_stats_{date_range}.csv"
            )

        # Calculate summary statistics
        total_users = len(df_processed)
        total_sessions = int(df_processed["total_sessions"].sum())
        failed_sessions = int(df_processed["failed_sessions"].sum())
        overall_failure_rate = float(
            (failed_sessions / total_sessions * 100) if total_sessions > 0 else 0
        )

        logger.info("Analysis completed successfully")
        return {
            "success": True,
            "data": {
                "from_date": from_date,
                "to_date": to_date,
                "html_url": html_url
                or html_path,  # Fall back to local path if no Azure URL
                "csv_url": csv_url or csv_path,
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
