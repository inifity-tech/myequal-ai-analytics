"""
User failure analysis visualization module.
Creates interactive bar chart of user failure rates matching the required style.
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.colors as colors
import os
import logging

logger = logging.getLogger(__name__)


def create_failure_rate_bins(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create failure rate distribution bins.

    Args:
        df: DataFrame with user failure statistics

    Returns:
        DataFrame with failure rate distribution
    """
    # Create bins from 0 to 100 with 10% intervals
    bins = list(range(0, 101, 10))
    labels = [f"{bins[i]}-{bins[i + 1]}%" for i in range(len(bins) - 1)]

    # Add a bin column
    df_bins = pd.DataFrame({"failure_rate": df["failure_rate"]})
    df_bins["bin"] = pd.cut(
        df_bins["failure_rate"], bins=bins, labels=labels, include_lowest=True
    )

    # Count users in each bin
    distribution = df_bins["bin"].value_counts().reset_index()
    distribution.columns = ["range", "count"]
    distribution = distribution.sort_values("range")

    return distribution


def create_visualization(df: pd.DataFrame, output_dir: str, date_range: str) -> None:
    """
    Create an interactive visualization of user failure rates distribution.
    Shows the distribution chart with hover details.

    Args:
        df: DataFrame with user failure statistics
        output_dir: Directory to save the visualization
        date_range: String representing the date range of the analysis
    """
    try:
        # Calculate summary statistics
        total_users = len(df)
        total_sessions = df["total_sessions"].sum()
        total_failures = df["failed_sessions"].sum()
        overall_failure_rate = (
            (total_failures / total_sessions * 100) if total_sessions > 0 else 0
        )

        # Create failure rate distribution
        distribution = create_failure_rate_bins(df)

        # Calculate percentages
        distribution["percentage"] = (distribution["count"] / total_users * 100).round(2)

        # Add user details to each range for hover information
        distribution["user_details"] = ""
        for i, row in distribution.iterrows():
            range_min = float(row["range"].split("-")[0].rstrip("%"))
            range_max = float(row["range"].split("-")[1].rstrip("%"))

            # Get users in this range
            users_in_range = df[df["failure_rate"].between(range_min, range_max)]

            # Format user details
            if not users_in_range.empty:
                user_list = []
                for _, user_row in users_in_range.iterrows():
                    user_list.append(
                        f"• {user_row['user']} ({user_row['failure_rate']:.1f}%)"
                    )
                distribution.at[i, "user_details"] = "<br>".join(user_list)

        # Create the main figure
        fig = go.Figure()

        # Create a colorscale for the bars - Use the greens colorscale for consistency
        colorscale = colors.sequential.Greens[1:]  # Skip the first (almost white) color
        max_color_idx = len(colorscale) - 1

        # Add distribution bars with color gradient based on failure rate
        for i, row in distribution.iterrows():
            range_start = float(row["range"].split("-")[0].rstrip("%"))
            color_idx = min(int((range_start / 100) * max_color_idx), max_color_idx)

            fig.add_trace(
                go.Bar(
                    x=[row["range"]],
                    y=[row["percentage"]],
                    name=row["range"],
                    text=[f"{row['count']} ({row['percentage']}%)"],
                    textposition="auto",
                    marker_color=colorscale[color_idx],
                    marker_line_width=0,
                    hovertemplate=(
                        "<b>%{x}</b><br>"
                        + "<b>Users in this range:</b> "
                        + str(row["count"])
                        + "<br>"
                        + "<b>Percentage of total users:</b> %{y:.1f}%<br>"
                        + "<b>Users in this category:</b><br>"
                        + row["user_details"]
                        + "<extra></extra>"
                    ),
                    showlegend=False,
                )
            )

        # Update layout for the main chart
        fig.update_layout(
            yaxis_title="Percentage of Users",
            xaxis_title="Failure Rate Brackets",
            height=600,
            margin=dict(t=20, l=50, r=50, b=50),  # Reduced top margin since we removed the title
            template="plotly_white",
            plot_bgcolor="#f8f9fa",
            paper_bgcolor="#ffffff",
            yaxis=dict(
                gridcolor="rgba(0,0,0,0)",
                tickformat=".1f",
                ticksuffix="%",
                dtick=5,
                range=[
                    0,
                    max(distribution["percentage"].max() + 5, 20)
                    if not distribution.empty
                    else 20,
                ],
            ),
            xaxis=dict(
                gridcolor="rgba(0,0,0,0)",
                tickangle=0,
                categoryorder="array",
                categoryarray=[f"{i}-{i + 10}%" for i in range(0, 100, 10)],
            ),
            hoverlabel=dict(bgcolor="white", font_size=14, font_family="Arial"),
        )

        # Add a color scale reference
        fig.update_layout(
            coloraxis=dict(
                colorscale=colorscale,
                colorbar=dict(
                    title="Failure Rate (%)",
                    thicknessmode="pixels",
                    thickness=15,
                    lenmode="fraction",
                    len=0.6,
                    yanchor="middle",
                    y=0.5,
                    xanchor="right",
                    x=1.05,
                    ticks="outside",
                ),
            ),
        )

        # Additional CSS for better styling
        css_styles = """
        <style>
            body {
                font-family: 'Arial', sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f8f9fa;
            }
            .plotly-graph-div {
                background-color: #ffffff;
                border-radius: 8px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.05);
                margin: 0 auto;
                padding: 20px;
                max-width: 1200px;
            }
            .js-plotly-plot {
                margin: 0 auto;
            }
        </style>
        """

        # Save the visualization
        output_file = os.path.join(output_dir, f"user_failure_rates_{date_range}.html")

        # Write HTML with just the plot
        with open(output_file, "w") as f:
            f.write(f"""
            <html>
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
                {css_styles}
            </head>
            <body>
                <div style="padding: 20px;">
                    {fig.to_html(full_html=False, include_plotlyjs=False, config={"displayModeBar": True, "displaylogo": False, "responsive": True})}
                </div>
            </body>
            </html>
            """)

        # Save the raw data
        csv_file = os.path.join(output_dir, f"user_failure_stats_{date_range}.csv")
        df.to_csv(csv_file, index=False)

        logger.info(f"Visualization saved to {output_file}")
        logger.info(f"Raw data saved to {csv_file}")

    except Exception as e:
        logger.error(f"Error creating visualization: {str(e)}", exc_info=True)
        raise
