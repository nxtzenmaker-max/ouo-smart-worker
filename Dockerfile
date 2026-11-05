FROM python:3.11-slim

# Install minimal system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Chromium + system deps for Playwright
RUN playwright install --with-deps chromium

# Copy app
COPY . .

# Railway free plan memory-safe workers
ENV PYTHONUNBUFFERED=1
ENV OUO_WORKERS=25

# Railway injects PORT automatically
EXPOSE 8080

CMD ["python", "main.py"]
