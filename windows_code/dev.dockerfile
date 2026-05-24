FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/lists/*

# Copy requirements and install
COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt

# Copy proto and regenerate stubs (ensures compatibility with grpcio 1.78.0)
COPY crawler.proto .
RUN python -m grpc_tools.protoc \
    -I. \
    --python_out=. \
    --grpc_python_out=. \
    crawler.proto

# Copy source — file_server.py removed, Shivam is crawler-only now
COPY crawler.py .

# Create output directory (used by mounted volume if needed)
RUN mkdir -p /app/output

# Expose nothing — Shivam's machine runs no servers
CMD ["python", "crawler.py"]
