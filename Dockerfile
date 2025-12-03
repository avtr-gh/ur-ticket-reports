# Use slim Python base
FROM python:3.12-slim

WORKDIR /app

# Install system deps for pip if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Use PORT env var set by Cloud Run
ENV PORT 8080
EXPOSE 8080

# Use gunicorn for production
CMD ["gunicorn", "service:app", "-b", "0.0.0.0:8080", "--timeout", "300"]
