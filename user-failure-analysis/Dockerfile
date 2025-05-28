FROM ghcr.io/astral-sh/uv:python3.12-alpine
WORKDIR /app

# Install system dependencies
RUN apk add --no-cache gcc musl-dev postgresql-dev

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN uv pip install --system -r requirements.txt

# Copy application code
COPY *.py .
COPY env_template.txt .env

# Create output directory
RUN mkdir -p ./output

# Set environment variables
ENV PYTHONPATH=/app
ENV OUTPUT_DIR=/app/output

# Expose the port for Azure Functions (if needed)
EXPOSE 80

# Run the application
CMD ["python", "main.py"] 