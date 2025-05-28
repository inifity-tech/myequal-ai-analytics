#!/usr/bin/env python3
"""
Test script for the user failure rate analysis system.
Tests database connectivity and analysis.
"""

import os
from datetime import datetime
from config import Config, DatabaseError
from analyzer import analyze_failure_data, AnalysisError


def main():
    """Test the failure rate analysis with database connection."""
    print("Starting database connection test...")

    # Define output directory
    output_dir = "./test_output"
    os.makedirs(output_dir, exist_ok=True)

    # Initialize configuration
    try:
        config = Config()
        if not config.validate():
            print("Error: Configuration validation failed!")
            return 1

        # Test database connection
        print("Testing database connection...")
        if not config.test_connection():
            print("Error: Database connection test failed!")
            return 1

        print("Database connection successful!")

        # Execute query
        print("Executing query...")
        data = config.execute_query()

        if data.empty:
            print("Query returned no results. Check your date range in .env file.")
            return 1

        print(f"Retrieved {len(data)} records from database")

        # Initialize analyzer
        analyzer = analyze_failure_data(data, output_dir)

        # Calculate failure rates
        user_stats = analyzer.calculate_failure_rates()
        print(f"Calculated failure rates for {len(user_stats)} users")

        # Get summary statistics
        summary = analyzer.get_summary_statistics()
        print(f"Overall failure rate: {summary['overall_failure_rate']:.2%}")

        # Export to CSV
        csv_file = analyzer.export_to_csv()
        print(f"Exported statistics to {csv_file}")

        # Generate visualization
        html_file = analyzer.create_interactive_bar_plot()
        print(f"Created interactive plot at {html_file}")

        # Show execution time
        execution_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\nTest completed successfully at {execution_time}!")
        return 0

    except DatabaseError as e:
        print(f"Database error: {e}")
        return 1
    except AnalysisError as e:
        print(f"Analysis error: {e}")
        return 1
    except Exception as e:
        print(f"Error during test: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
