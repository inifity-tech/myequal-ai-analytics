FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set environment variables
ENV PORT=8000
ENV LOG_LEVEL=info
ENV OUTPUT_DIR=/app/output

# Create output directory
RUN mkdir -p /app/output

# Expose port
EXPOSE 8000

# Run the FastAPI server
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"] 