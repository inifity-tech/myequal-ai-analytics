"""
User failure analysis visualization module.
Creates interactive bar chart of user failure rates matching the required style.
"""

import pandas as pd
import plotly.graph_objects as go
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
    labels = [f"{i}-{i + 10}%" for i in range(0, 100, 10)]

    # Create a copy of the DataFrame to avoid modifying the original
    df_copy = df.copy()

    # Add a bin column
    df_copy["bin_range"] = pd.cut(
        df_copy["failure_rate"],
        bins=bins,
        labels=labels,
        include_lowest=True,
        right=True,
    )

    # Count users in each bin and get user details
    distribution = (
        df_copy.groupby("bin_range", observed=True)
        .agg({"user": list, "failure_rate": list})
        .reset_index()
    )

    # Add count column
    distribution["count"] = distribution["user"].apply(len)

    # Create user details string for each bin
    distribution["user_details"] = distribution.apply(
        lambda row: "<br>".join(
            [
                f"• {user} ({rate:.1f}%)"
                for user, rate in zip(row["user"], row["failure_rate"])
            ]
        )
        if len(row["user"]) > 0
        else "No users in this category",
        axis=1,
    )

    # Rename columns
    distribution = distribution.rename(columns={"bin_range": "range"})

    # Ensure all ranges are represented
    all_ranges = pd.DataFrame({"range": labels})
    distribution = pd.merge(
        all_ranges,
        distribution[["range", "count", "user_details"]],
        on="range",
        how="left",
    ).fillna({"count": 0, "user_details": "No users in this category"})

    return distribution


def create_visualization(df: pd.DataFrame, output_dir: str, date_range: str) -> None:
    """
    Create an interactive visualization of user failure rates distribution.
    Shows the distribution chart with hover details but without the table.

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
        distribution["percentage"] = (distribution["count"] / total_users * 100).round(
            1
        )

        # Create the figure
        fig = go.Figure()

        # Define the color scale - dark to light green
        colorscale = [
            [0, "rgb(247, 252, 245)"],  # Very light green
            [0.1, "rgb(229, 245, 224)"],
            [0.2, "rgb(199, 233, 192)"],
            [0.3, "rgb(161, 217, 155)"],
            [0.4, "rgb(116, 196, 118)"],
            [0.5, "rgb(65, 171, 93)"],
            [0.6, "rgb(35, 139, 69)"],
            [0.7, "rgb(0, 109, 44)"],
            [0.8, "rgb(0, 90, 50)"],
            [1, "rgb(0, 68, 27)"],  # Very dark green
        ]

        # Add distribution bars with color gradient based on failure rate
        for i, row in distribution.iterrows():
            # Extract the range values for color mapping
            range_start = float(row["range"].split("-")[0].rstrip("%"))
            # Scale the range (0-100) to color value (0-1)
            color_val = range_start / 100  # Higher failure rates get darker colors

            # Get color from colorscale
            color = get_color_from_scale(colorscale, color_val)

            # Add text with value and percentage
            text_label = f"{int(row['count'])} ({row['percentage']}%)"

            fig.add_trace(
                go.Bar(
                    x=[row["range"]],
                    y=[row["count"]],
                    name=row["range"],
                    text=[text_label],
                    textposition="auto",
                    marker_color=color,
                    customdata=[[row["user_details"]]],
                    hovertemplate=(
                        "<b>%{x}</b><br>"
                        + "<b>Users in this range:</b> %{y}<br>"
                        + "<b>Percentage:</b> %{text}<br>"
                        + "<b>Users in this category:</b><br>%{customdata[0]}"
                        + "<extra></extra>"
                    ),
                    showlegend=False,
                )
            )

        # Update layout to match the image
        fig.update_layout(
            title={
                "text": "User Failure Rate Distribution",
                "y": 0.99,
                "x": 0.5,
                "xanchor": "center",
                "yanchor": "top",
                "font": dict(size=20),
            },
            yaxis_title="Number of Users",
            xaxis_title="Failure Rate Brackets",
            height=700,
            margin=dict(t=50, l=50, r=50, b=200),
            template="plotly_white",
            plot_bgcolor="white",
            yaxis=dict(
                gridcolor="lightgray",
                tickformat=".0f",
                dtick=0.5,
                range=[0, max(max(distribution["count"]) * 1.2, 2)],
            ),
            xaxis=dict(
                gridcolor="lightgray",
                tickangle=0,
                categoryorder="array",
                categoryarray=[f"{i}-{i + 10}%" for i in range(0, 100, 10)],
            ),
            hoverlabel=dict(bgcolor="white", font_size=14, font_family="Arial"),
        )

        # Add color scale reference on the right side
        fig.update_layout(
            coloraxis=dict(
                colorscale=colorscale,
                colorbar=dict(
                    title="Failure Rate (%)",
                    thicknessmode="pixels",
                    thickness=20,
                    lenmode="pixels",
                    len=300,
                    yanchor="top",
                    y=0.8,
                    xanchor="right",
                    x=1.05,
                    ticks="outside",
                ),
            ),
        )

        # Create HTML with proper initialization
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>User Failure Rate Analysis</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            margin-bottom: 20px;
        }}
        .header h1 {{
            margin-bottom: 5px;
            color: #333;
        }}
        .date-range {{
            color: #666;
            font-size: 14px;
            margin-bottom: 15px;
        }}
        .chart-container {{
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            padding: 15px;
            margin-bottom: 20px;
        }}
        .summary-header {{
            font-size: 18px;
            font-weight: bold;
            color: #333;
            margin-bottom: 10px;
            text-align: left;
            padding-left: 20px;
        }}
        .stats-container {{
            display: flex;
            justify-content: space-around;
            width: 100%;
            margin-top: 20px;
        }}
        .stat-card {{
            background-color: white;
            border-radius: 8px;
            padding: 20px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            min-width: 200px;
        }}
        .stat-value {{
            font-size: 32px;
            font-weight: bold;
            color: #006D2C;
            margin: 10px 0;
        }}
        .stat-label {{
            font-size: 16px;
            color: #666;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>User Failure Rate Analysis</h1>
        <div class="date-range">Date Range: {date_range.replace("from_", "").replace("to_", " to ").replace("_", "/")}</div>
    </div>
    
    <div class="stats-container">
        <div class="stat-card">
            <div class="stat-value">{total_sessions}</div>
            <div class="stat-label">Total Sessions</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{total_failures}</div>
            <div class="stat-label">Failed Sessions</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{overall_failure_rate:.2f}%</div>
            <div class="stat-label">Overall Failure Rate</div>
        </div>
    </div>
    
    <script>
        var plotData = {fig.to_json()};
        Plotly.newPlot('chart', plotData.data, plotData.layout, {{
            displayModeBar: true,
            displaylogo: false,
            responsive: true
        }});
    </script>
</body>
</html>
"""

        # Save the visualization
        output_file = os.path.join(output_dir, f"user_failure_rates_{date_range}.html")
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)

        # Save the raw data
        csv_file = os.path.join(output_dir, f"user_failure_stats_{date_range}.csv")
        df.to_csv(csv_file, index=False)

        logger.info(f"Visualization saved to {output_file}")
        logger.info(f"Raw data saved to {csv_file}")

    except Exception as e:
        logger.error(f"Error creating visualization: {str(e)}", exc_info=True)
        raise


def get_color_from_scale(colorscale, val):
    """
    Get color from a colorscale based on a value between 0 and 1.

    Args:
        colorscale: List of [position, color] items
        val: Value between 0 and 1

    Returns:
        Color string
    """
    if val <= 0:
        return colorscale[0][1]
    if val >= 1:
        return colorscale[-1][1]

    for i in range(len(colorscale) - 1):
        pos1, color1 = colorscale[i]
        pos2, color2 = colorscale[i + 1]

        if pos1 <= val <= pos2:
            # Interpolate between the two colors
            frac = (val - pos1) / (pos2 - pos1)

            # Parse RGB values
            rgb1 = [
                int(color1.split("(")[1].split(")")[0].split(",")[j]) for j in range(3)
            ]
            rgb2 = [
                int(color2.split("(")[1].split(")")[0].split(",")[j]) for j in range(3)
            ]

            # Interpolate RGB
            rgb_result = [int(rgb1[j] + frac * (rgb2[j] - rgb1[j])) for j in range(3)]

            return f"rgb({rgb_result[0]}, {rgb_result[1]}, {rgb_result[2]})"

    return colorscale[-1][1]
