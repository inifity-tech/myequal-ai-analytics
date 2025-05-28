# User Failure Rate Analysis System

A streamlined Python-based system for analyzing user failure rates from call log data directly from the database. This system can run locally, as a script, or as an Azure Function, and provides detailed statistics and visualizations.

## Overview

This system analyzes call session data to calculate failure rates for different users. A failure is defined as having a `session_id` but a NULL `exotel_call_sid`, indicating that a session was created but the call failed to establish.

## Features

- **Database Integration**: Connects directly to PostgreSQL staging database
- **Comprehensive Analysis**: Calculates failure rates, success rates, and summary statistics
- **Interactive Visualization**: Failure rate distribution histogram with detailed user information on hover
- **Export Capabilities**: CSV export of analysis results
- **Azure Function Support**: Can be deployed as a serverless function
- **Modular Design**: Clean separation of concerns with proper error handling

## Project Structure

```
Azure/user-failure-stats/
├── config.py             # Configuration and database management
├── analyzer.py           # Analysis and visualization logic
├── main.py               # Command-line script
├── function_app.py       # Azure Function implementation
├── requirements.txt      # Python dependencies
├── env_template.txt      # Environment configuration template
└── README.md             # This file
```

## Installation

1. **Clone or download the project files**

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**:
   - Copy `env_template.txt` to `.env`
   - Add your database credentials
   
   ```bash
   cp env_template.txt .env
   nano .env  # Edit to add your database URL
   ```

## Configuration

### Environment Variables

The `.env` file contains important configuration for the system. Create it by copying `env_template.txt`:

```env
# Database Configuration
DB_URL=

# Query Configuration - Adjust these dates as needed for your analysis
START_DATE=2025-05-20 18:30:00
END_DATE=2025-05-22 18:29:59

# Output Configuration
OUTPUT_DIR=./output
```

**Note**: You must add the database URL to connect to your PostgreSQL database. Update the `START_DATE` and `END_DATE` to define the time period for your analysis.

### SQL Query

The system executes this SQL query based on the date range in your `.env` file:

```sql
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
    c.created_on >= '2025-05-20 18:30:00'::timestamp
    AND c.created_on <= '2025-05-22 18:29:59'::timestamp
ORDER BY c.created_on DESC;
```

## Usage

### Running the Analysis

To run the analysis:

```bash
# First copy the env_template.txt to .env
cp env_template.txt .env

# Edit to add your database URL and adjust dates if needed
nano .env

# Run the analysis
python main.py
```

This will:
- Connect to the database
- Execute the configured SQL query
- Analyze failure rates
- Generate interactive visualization
- Export results to CSV

### Testing Database Connectivity

You can test your database connection before running the full analysis:

```bash
python test.py
```

This test script will verify that:
- Your configuration is valid
- The database connection works
- The query returns data
- The analysis can be performed

## Azure Function Deployment

### 1. Prepare Function App

1. Create an Azure Function App with Python runtime
2. Set up application settings with your environment variables (especially DB_URL)
3. Deploy the code

### 2. Deploy

```bash
# Using Azure CLI
func azure functionapp publish <your-function-app-name>

# Or using VS Code Azure Functions extension
```

### 3. Usage

**HTTP Endpoint**: `https://<your-function-app>.azurewebsites.net/api/failure_analysis`

**Query Parameters**:
- `export_csv=true/false` (default: true)
- `include_details=true/false` (default: false)

**Example Request**:
```bash
curl -X POST "https://your-function-app.azurewebsites.net/api/failure_analysis"
```

## Output

### 1. Console Output

The system provides a detailed summary in the console:

```
================================================================================
USER FAILURE RATE ANALYSIS SUMMARY
================================================================================

OVERALL STATISTICS:
  Total Users: 6
  Total Sessions: 341
  Total Failures: 81
  Overall Failure Rate: 23.75%

USER FAILURE RATE STATISTICS:
  Average: 23.75%
  Median: 16.67%
  Maximum: 50.00%
  Minimum: 0.00%
  Std Deviation: 19.69%

TOP 10 USERS BY FAILURE RATE:
Rank  User                 Failure Rate    Failed/Total   
-------------------------------------------------------
1     Anushka              50.00%          4/8
2     Jahanvi              33.33%          1/3
3     Akhilesh             26.19%          11/42
4     Adit Lal             25.00%          4/16
5     Ayush                18.92%          28/148
6     Vishnu Vardhan       27.42%          34/124
```

### 2. Generated Files

The system generates two output files:

- **CSV Export**: `user_failure_stats_YYYYMMDD_HHMMSS.csv`
- **Interactive Plot**: `user_failure_rates_YYYYMMDD_HHMMSS.html`

### 3. Visualization

The interactive visualization includes:

- **Failure Rate Distribution Histogram**: Shows the number of users in each failure rate bracket (0-10%, 10-20%, etc.)
- **Hover Functionality**: When hovering over a bar, displays detailed information about users in that failure rate bracket
- **Top Users Table**: Side panel showing details about top users with highest failure rates
- **Summary Statistics**: Overall metrics about user failure rates

This approach is scalable for analyzing large numbers of users, as it groups them by failure rate instead of displaying individual entries for each user.

## API Reference

### FailureAnalyzer Class

```python
# Initialize
analyzer = analyze_failure_data(data, output_dir="./output")

# Calculate failure rates
user_stats = analyzer.calculate_failure_rates()

# Get summary statistics
summary = analyzer.get_summary_statistics()

# Export to CSV
csv_file = analyzer.export_to_csv()

# Generate visualization
html_file = analyzer.create_interactive_bar_plot()
```

## Data Schema

### Database Query Result

The system expects data with these columns:

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | string | Unique session identifier |
| `name` | string | User name |
| `exotel_call_sid` | string/null | Call SID (null indicates failure) |

### Output Data

The processed CSV contains:

| Column | Type | Description |
|--------|------|-------------|
| `user_name` | string | User name |
| `total_sessions` | int | Total sessions for user |
| `failed_sessions` | int | Failed sessions (null call_sid) |
| `success_sessions` | int | Successful sessions |
| `failure_rate` | float | Failure rate (0.0 to 1.0) |
| `failure_rate_percent` | string | Formatted percentage |

