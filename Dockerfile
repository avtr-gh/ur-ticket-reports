# Use slim Python base
FROM python:3.12-slim

WORKDIR /app

# Install system deps for pip
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# PORT loud Run
ENV PORT 8080
EXPOSE 8080

CMD ["gunicorn", "service:app", "-b", "0.0.0.0:8080", "--timeout", "300"]
