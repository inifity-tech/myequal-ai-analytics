#!/usr/bin/env python3
"""
Test script for user failure analysis.
"""

import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

from main import fetch_data, process_data, run_analysis

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def test_fetch_data():
    """Test database connection and data fetching."""
    logger.info("Testing database connection and data fetching...")

    # Default to 7 days ago to today
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    logger.info(f"Fetching data from {start_date} to {end_date}")

    try:
        # Test database connection and query
        df = fetch_data(start_date, end_date)

        # Log results
        logger.info(f"Successfully fetched {len(df)} records")
        logger.info(f"DataFrame columns: {df.columns.tolist()}")

        if not df.empty:
            # Check for null values in exotel_call_sid
            null_count = df["exotel_call_sid"].isnull().sum()
            logger.info(
                f"Number of null exotel_call_sid values: {null_count} out of {len(df)}"
            )

            # Display sample data
            logger.info("Sample data (first 3 rows):")
            for i, row in df.head(3).iterrows():
                logger.info(f"Row {i}: {row.to_dict()}")

            # Test data processing
            logger.info("Testing data processing...")
            df_processed = process_data(df)
            logger.info(f"Processed data has {len(df_processed)} users")
            logger.info(f"Processed data columns: {df_processed.columns.tolist()}")

            if not df_processed.empty:
                logger.info("Sample processed data (first 3 rows):")
                for i, row in df_processed.head(3).iterrows():
                    logger.info(f"Row {i}: {row.to_dict()}")
        else:
            logger.warning("No data returned from database")

        return df

    except Exception as e:
        logger.error(f"Error in test_fetch_data: {str(e)}", exc_info=True)
        raise


def test_run_analysis():
    """Test the complete analysis pipeline."""
    logger.info("Testing the complete analysis pipeline...")

    # Default to 7 days ago to today
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    logger.info(f"Running analysis from {start_date} to {end_date}")

    try:
        # Run the analysis
        result = run_analysis(start_date, end_date)

        # Log results
        if result.get("success", False):
            logger.info("Analysis completed successfully")
            logger.info(f"HTML report: {result['data']['html_url']}")
            logger.info(f"CSV report: {result['data']['csv_url']}")

            # Log summary statistics
            stats = result["data"]["stats"]
            logger.info(f"Total users: {stats['total_users']}")
            logger.info(f"Total sessions: {stats['total_sessions']}")
            logger.info(f"Failed sessions: {stats['failed_sessions']}")
            logger.info(f"Overall failure rate: {stats['overall_failure_rate']:.2f}%")
        else:
            logger.error(f"Analysis failed: {result.get('error', 'Unknown error')}")

        return result

    except Exception as e:
        logger.error(f"Error in test_run_analysis: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    logger.info("Starting tests...")

    # Uncomment the test you want to run
    test_fetch_data()
    # test_run_analysis()

    logger.info("Tests completed")
