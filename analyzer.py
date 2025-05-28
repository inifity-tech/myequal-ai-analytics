"""
Analyzer module for calculating user failure rates and generating visualizations.
Combines data analysis and visualization capabilities in a single module.
"""

import logging
import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class UserFailureStats:
    """Data class to hold user failure statistics."""

    name: str
    total_sessions: int
    failed_sessions: int
    failure_rate: float
    success_sessions: int
    success_rate: float


class AnalysisError(Exception):
    """Custom exception for analysis-related errors."""

    pass


class FailureAnalyzer:
    """Analyzes user failure rates and generates visualizations."""

    def __init__(self, data: pd.DataFrame, output_dir: str = "./output"):
        """
        Initialize analyzer with query results.

        Args:
            data: DataFrame containing session_id, name, and exotel_call_sid columns
            output_dir: Directory to save output files
        """
        self.data = data.copy()
        self.output_dir = output_dir
        self._validate_data()
        self._processed_stats = None

        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

    def _validate_data(self) -> None:
        """Validate that required columns exist in the data."""
        required_columns = ["session_id", "name", "exotel_call_sid"]
        missing_columns = [
            col for col in required_columns if col not in self.data.columns
        ]

        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        logger.info(f"Data validation passed. Total records: {len(self.data)}")

    def calculate_failure_rates(self) -> List[UserFailureStats]:
        """
        Calculate failure rates for each user.

        A failure is defined as having a session_id but NULL exotel_call_sid.

        Returns:
            List of UserFailureStats objects
        """
        logger.info("Calculating failure rates by user...")

        # Clean user names (remove trailing spaces)
        self.data["name"] = self.data["name"].str.strip()

        # Group by user name and calculate statistics
        user_stats = []

        for user_name, user_data in self.data.groupby("name"):
            total_sessions = len(user_data)

            # Failed sessions are those with NULL exotel_call_sid
            failed_sessions = user_data["exotel_call_sid"].isnull().sum()
            success_sessions = total_sessions - failed_sessions

            failure_rate = (
                failed_sessions / total_sessions if total_sessions > 0 else 0.0
            )
            success_rate = (
                success_sessions / total_sessions if total_sessions > 0 else 0.0
            )

            stats = UserFailureStats(
                name=user_name,
                total_sessions=total_sessions,
                failed_sessions=failed_sessions,
                failure_rate=failure_rate,
                success_sessions=success_sessions,
                success_rate=success_rate,
            )

            user_stats.append(stats)

            logger.debug(
                f"User: {user_name}, Total: {total_sessions}, "
                f"Failed: {failed_sessions}, Rate: {failure_rate:.2%}"
            )

        # Sort by failure rate (descending) then by total sessions (descending)
        user_stats.sort(key=lambda x: (-x.failure_rate, -x.total_sessions))

        self._processed_stats = user_stats
        logger.info(f"Calculated failure rates for {len(user_stats)} users")

        return user_stats

    def get_summary_statistics(self) -> Dict[str, float]:
        """
        Get overall summary statistics.

        Returns:
            Dictionary containing summary statistics
        """
        if self._processed_stats is None:
            self.calculate_failure_rates()

        total_sessions = sum(stat.total_sessions for stat in self._processed_stats)
        total_failures = sum(stat.failed_sessions for stat in self._processed_stats)

        overall_failure_rate = (
            total_failures / total_sessions if total_sessions > 0 else 0.0
        )

        failure_rates = [stat.failure_rate for stat in self._processed_stats]

        summary = {
            "total_users": len(self._processed_stats),
            "total_sessions": total_sessions,
            "total_failures": total_failures,
            "overall_failure_rate": overall_failure_rate,
            "avg_user_failure_rate": np.mean(failure_rates),
            "median_user_failure_rate": np.median(failure_rates)
            if failure_rates
            else 0.0,
            "max_user_failure_rate": np.max(failure_rates) if failure_rates else 0.0,
            "min_user_failure_rate": np.min(failure_rates) if failure_rates else 0.0,
            "std_user_failure_rate": np.std(failure_rates) if failure_rates else 0.0,
        }

        logger.info(
            f"Summary - Total Users: {summary['total_users']}, "
            f"Overall Failure Rate: {summary['overall_failure_rate']:.2%}"
        )

        return summary

    def get_top_failing_users(self, top_n: int = 10) -> List[UserFailureStats]:
        """
        Get top N users with highest failure rates.

        Args:
            top_n: Number of top users to return

        Returns:
            List of UserFailureStats for top failing users
        """
        if self._processed_stats is None:
            self.calculate_failure_rates()

        return self._processed_stats[:top_n]

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert processed statistics to DataFrame.

        Returns:
            DataFrame with user statistics
        """
        if self._processed_stats is None:
            self.calculate_failure_rates()

        data = []
        for stat in self._processed_stats:
            data.append(
                {
                    "user_name": stat.name,
                    "total_sessions": stat.total_sessions,
                    "failed_sessions": stat.failed_sessions,
                    "success_sessions": stat.success_sessions,
                    "failure_rate": stat.failure_rate,
                    "success_rate": stat.success_rate,
                    "failure_rate_percent": f"{stat.failure_rate:.2%}",
                    "success_rate_percent": f"{stat.success_rate:.2%}",
                }
            )

        return pd.DataFrame(data)

    def export_to_csv(self, filepath: Optional[str] = None) -> str:
        """
        Export processed statistics to CSV file.

        Args:
            filepath: Optional path to save the CSV file. If not provided, a default path is used.

        Returns:
            Path to the exported CSV file
        """
        if not filepath:
            csv_filename = (
                f"user_failure_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            filepath = os.path.join(self.output_dir, csv_filename)

        df = self.to_dataframe()
        df.to_csv(filepath, index=False)
        logger.info(f"Exported user statistics to {filepath}")
        return filepath

    def create_interactive_bar_plot(self, top_n: int = 15) -> str:
        """
        Create an interactive histogram showing the distribution of failure rates.

        Args:
            top_n: Number of top users to include in the table (doesn't affect histogram)

        Returns:
            Path to the saved HTML file
        """
        try:
            logger.info("Generating interactive failure rate distribution histogram")

            # Ensure we have processed data
            if self._processed_stats is None:
                self.calculate_failure_rates()

            # Create dataframe with all user stats
            df_all = pd.DataFrame(
                [
                    {
                        "User": stat.name,
                        "Failure Rate": stat.failure_rate * 100,
                        "Failed Sessions": stat.failed_sessions,
                        "Total Sessions": stat.total_sessions,
                    }
                    for stat in self._processed_stats
                ]
            )

            # Get top N users for the table
            top_users = self._processed_stats[:top_n]
            df_top = pd.DataFrame(
                [
                    {
                        "User": stat.name,
                        "Failure Rate": stat.failure_rate * 100,
                        "Failed Sessions": stat.failed_sessions,
                        "Total Sessions": stat.total_sessions,
                    }
                    for stat in top_users
                ]
            )

            # Create bins for histogram (0-10%, 10-20%, etc.)
            bins = list(range(0, 101, 10))
            bin_labels = [f"{i}-{i + 10}%" for i in range(0, 100, 10)]

            # Assign each user to a bin
            df_all["Bin"] = pd.cut(
                df_all["Failure Rate"],
                bins=bins,
                labels=bin_labels,
                include_lowest=True,
                right=False,
            )

            # Count users in each bin
            bin_counts = df_all["Bin"].value_counts().sort_index()

            # Calculate percentages
            total_users = len(df_all)
            bin_percentages = (bin_counts / total_users * 100).map(lambda x: round(x, 1) if not pd.isna(x) else 0.0)

            # Create a dictionary mapping bins to lists of users
            users_by_bin = {}
            for bin_label in bin_labels:
                bin_users = df_all[df_all["Bin"] == bin_label]
                users_by_bin[bin_label] = bin_users.sort_values(
                    "Failure Rate", ascending=False
                )

            # Prepare hover text with user lists for each bin
            hover_texts = []
            for bin_label in bin_labels:
                if bin_label in bin_counts.index:
                    bin_users = users_by_bin[bin_label]
                    user_count = len(bin_users)
                    percentage = round((user_count / total_users * 100), 1)

                    hover_text = f"<b>{bin_label}</b><br>"
                    hover_text += f"Users: {user_count} ({percentage}%)<br><br>"

                    user_list = "<br>".join(
                        [
                            f"{row['User']}: {row['Failure Rate']:.1f}% ({row['Failed Sessions']}/{row['Total Sessions']})"
                            for _, row in bin_users.head(
                                10
                            ).iterrows()  # Show top 10 users per bin
                        ]
                    )

                    if len(bin_users) > 10:
                        user_list += f"<br>...and {len(bin_users) - 10} more users"

                    hover_text += user_list
                    hover_texts.append(hover_text)
                else:
                    hover_texts.append(f"<b>{bin_label}</b><br>No users in this range")

            # Create figure with subplots
            fig = make_subplots(
                rows=1,
                cols=2,
                specs=[[{"type": "bar"}, {"type": "table"}]],
                column_widths=[0.7, 0.3],
                subplot_titles=("User Failure Rate Distribution", "Top Failing Users"),
            )

            # Prepare text annotations showing both count and percentage
            text_annotations = []
            for i, (count, percent) in enumerate(zip(bin_counts, bin_percentages)):
                if not pd.isna(count):
                    text_annotations.append(f"{int(count)} ({percent}%)")
                else:
                    text_annotations.append("0 (0.0%)")

            # Add histogram - show counts and percentages
            fig.add_trace(
                go.Bar(
                    x=bin_labels,
                    y=bin_counts.values,
                    text=text_annotations,  # Show both count and percentage
                    textposition="auto",
                    hoverinfo="text",
                    hovertext=hover_texts,
                    marker=dict(
                        color=bins[:-1],
                        colorscale="Greens",
                        colorbar=dict(title="Failure Rate (%)"),
                    ),
                ),
                row=1,
                col=1,
            )

            # Add table with top failing users
            fig.add_trace(
                go.Table(
                    header=dict(
                        values=["User", "Failure Rate", "Failed", "Total"],
                        fill_color="paleturquoise",
                        align="left",
                        font=dict(size=12),
                    ),
                    cells=dict(
                        values=[
                            df_top["User"],
                            df_top["Failure Rate"].apply(lambda x: f"{x:.1f}%"),
                            df_top["Failed Sessions"],
                            df_top["Total Sessions"],
                        ],
                        fill_color="lavender",
                        align="left",
                        font=dict(size=11),
                    ),
                ),
                row=1,
                col=2,
            )

            # Update layout
            summary_stats = self.get_summary_statistics()

            fig.update_layout(
                title_text=f"User Failure Rate Analysis<br><sup>Overall Failure Rate: {summary_stats['overall_failure_rate']:.2%} | Total Users: {summary_stats['total_users']} | Total Sessions: {summary_stats['total_sessions']}</sup>",
                height=600,
                showlegend=False,
                xaxis_title="Failure Rate Brackets",
                yaxis_title="Number of Users",
                hovermode="closest",
            )

            # Create filename and save
            html_filename = (
                f"user_failure_rates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            )
            filepath = os.path.join(self.output_dir, html_filename)

            fig.write_html(
                filepath,
                include_plotlyjs="cdn",
                full_html=True,
                config={"displayModeBar": True, "displaylogo": False},
            )

            logger.info(
                f"Interactive failure rate distribution plot saved to {filepath}"
            )
            return filepath

        except Exception as e:
            error_msg = f"Failed to create interactive bar plot: {e}"
            logger.error(error_msg)
            raise AnalysisError(error_msg)


def analyze_failure_data(
    data: pd.DataFrame, output_dir: str = "./output"
) -> FailureAnalyzer:
    """
    Factory function to create and return a FailureAnalyzer instance.

    Args:
        data: DataFrame containing call log data
        output_dir: Directory to save output files

    Returns:
        FailureAnalyzer instance
    """
    return FailureAnalyzer(data, output_dir)
