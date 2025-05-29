#!/usr/bin/env python3
"""
Main script for user failure rate analysis.
Orchestrates database querying, data analysis, and visualization generation.
"""

import sys
import logging
import traceback
import argparse
from datetime import datetime

from config import Config, DatabaseError
from analyzer import analyze_failure_data, AnalysisError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("failure_analysis.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def run_analysis(from_date=None, to_date=None, max_users=15):
    """
    Run analysis using database query.

    Args:
        from_date: Start date in format 'YYYY-MM-DD'
        to_date: End date in format 'YYYY-MM-DD'
        max_users: Maximum number of users to display in the visualization

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logger.info("Starting user failure rate analysis with database query...")

    start_time = datetime.now()

    try:
        # Initialize configuration
        config = Config()

        # Set custom date range if provided
        if from_date or to_date:
            try:
                config.set_date_range(from_date=from_date, to_date=to_date)
                logger.info(
                    f"Using custom date range: {config.start_date} to {config.end_date}"
                )
            except ValueError as e:
                logger.error(f"Invalid date range: {e}")
                print(f"\n❌ Invalid date range: {e}")
                return 1

        if not config.validate():
            raise ValueError("Configuration validation failed")

        # Get date range description for file naming
        date_range_desc = config.get_date_range_description()

        # Test database connection
        logger.info("Testing database connection...")
        if not config.test_connection():
            raise DatabaseError("Database connection test failed")

        # Execute query and fetch data
        logger.info("Executing failure rate analysis query...")
        data = config.execute_query()

        if data.empty:
            logger.warning("Query returned no results")
            print("❌ No data found for the specified date range")
            return 1

        logger.info(f"Retrieved {len(data)} records from database")

        # Analyze the data
        logger.info("Analyzing failure rate data...")
        analyzer = analyze_failure_data(data, config.output.output_dir, date_range_desc)
        user_stats = analyzer.calculate_failure_rates()
        summary_stats = analyzer.get_summary_statistics()

        # Export to CSV
        logger.info("Exporting results to CSV...")
        csv_filepath = analyzer.export_to_csv()

        # Create visualization
        logger.info("Generating interactive failure rate histogram...")
        plot_filepath = analyzer.create_interactive_bar_plot(top_n=max_users)

        # Print summary to console
        print_summary(user_stats, summary_stats)

        # Print execution summary
        execution_time = (datetime.now() - start_time).total_seconds()
        print("\n✅ Analysis completed successfully!")
        print(f"⏱️  Execution time: {execution_time:.2f} seconds")
        print("📁 Generated files:")
        print(f"  - CSV: {csv_filepath}")
        print(f"  - Interactive plot: {plot_filepath}")

        logger.info(f"Analysis completed successfully in {execution_time:.2f} seconds")
        return 0

    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        print(f"\n❌ Database error: {e}")
        return 1

    except AnalysisError as e:
        logger.error(f"Analysis error: {e}")
        print(f"\n❌ Analysis error: {e}")
        return 1

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        print(f"\n💥 Fatal error: {e}")
        return 1


def print_summary(user_stats, summary_stats):
    """Print a formatted summary to the console."""
    print("\n" + "=" * 80)
    print("USER FAILURE RATE ANALYSIS SUMMARY")
    print("=" * 80)

    print("\nOVERALL STATISTICS:")
    print(f"  Total Users: {summary_stats['total_users']:,}")
    print(f"  Total Sessions: {summary_stats['total_sessions']:,}")
    print(f"  Total Failures: {summary_stats['total_failures']:,}")
    print(f"  Overall Failure Rate: {summary_stats['overall_failure_rate']:.2%}")

    print("\nUSER FAILURE RATE STATISTICS:")
    print(f"  Average: {summary_stats['avg_user_failure_rate']:.2%}")
    print(f"  Median: {summary_stats['median_user_failure_rate']:.2%}")
    print(f"  Maximum: {summary_stats['max_user_failure_rate']:.2%}")
    print(f"  Minimum: {summary_stats['min_user_failure_rate']:.2%}")
    print(f"  Std Deviation: {summary_stats['std_user_failure_rate']:.2%}")

    print("\nTOP 10 USERS BY FAILURE RATE:")
    print(f"{'Rank':<5} {'User':<20} {'Failure Rate':<15} {'Failed/Total':<15}")
    print("-" * 55)

    for i, stat in enumerate(user_stats[:10], 1):
        print(
            f"{i:<5} {stat.name:<20} {stat.failure_rate:.2%}{'':>9} "
            f"{stat.failed_sessions}/{stat.total_sessions}"
        )

    print("\n" + "=" * 80)


def main():
    """Main entry point for the script."""
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(description="User failure rate analysis")
        parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
        parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
        parser.add_argument(
            "--max-users",
            type=int,
            default=15,
            help="Maximum number of users to display",
        )

        args = parser.parse_args()

        return run_analysis(
            from_date=args.from_date, to_date=args.to_date, max_users=args.max_users
        )
    except KeyboardInterrupt:
        logger.info("Analysis interrupted by user")
        print("\n🛑 Analysis interrupted by user")
        return 1


if __name__ == "__main__":
    sys.exit(main())
