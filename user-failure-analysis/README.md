# User Failure Analysis System

This system analyzes user call failures by tracking sessions where a `session_id` is present but `exotel_call_sid` is null. It generates interactive visualizations and CSV exports for analysis.

## Features

- **Database Query**: Fetches session data from PostgreSQL database
- **Failure Detection**: Identifies sessions where `exotel_call_sid` is null
- **User Statistics**: Calculates failure rates per user
- **Interactive Visualization**: Creates an interactive HTML bar chart
- **CSV Export**: Exports raw statistics for further analysis
- **API Server**: Provides a REST API for integration with other systems
- **Azure Blob Storage**: Optional upload of reports to Azure Blob Storage

## Getting Started

### Prerequisites

- Python 3.9+
- PostgreSQL database
- Azure Blob Storage (optional)

### Installation

1. Clone this repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

### Configuration

Create a `.env` file with the following variables:

```
# Database connection
DB_URL=postgresql://username:password@hostname:port/database?sslmode=require

# Azure Blob Storage (optional)
AZURE_STORAGE_CONNECTION_STRING=your_connection_string
AZURE_STORAGE_CONTAINER=user-failure-reports

# Logging
LOG_LEVEL=INFO

# Analysis settings
GENERATE_TEST_DATA=false
OUTPUT_DIR=./output
```

## Usage

### Run Tests

To test database connectivity and the analysis pipeline:

```bash
python test.py
```

### Run API Server

To start the API server:

```bash
python server.py
```

The server will be available at http://localhost:8000

### API Endpoints

- `GET /health` - Health check endpoint
- `POST /api/analyze` - Run analysis with parameters:
  ```json
  {
    "from_date": "2023-01-01",
    "to_date": "2023-01-31",
    "max_users": 15,
    "force_refresh": false
  }
  ```

### Example API Call

```bash
curl -X POST http://localhost:8000/api/analyze \
  -H "Content-Type: application/json" \
  -d '{"from_date": "2023-01-01", "to_date": "2023-01-31", "max_users": 15, "force_refresh": true}'
```

## Database Query

The system uses the following SQL query to fetch session data:

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
    c.created_on >= 'yyyy-mm-dd 00:00:00'
    AND c.created_on <= 'yyyy-mm-dd 23:59:59'
ORDER BY c.created_on DESC;
```

The system identifies failures where `exotel_call_sid` is null, indicating a session that failed to establish a proper call.

## Docker Support

Build and run with Docker:

```bash
docker-compose up -d
``` 