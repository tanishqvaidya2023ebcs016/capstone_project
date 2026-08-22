FROM python:3.11-slim

WORKDIR /app


RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/lists/*


COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt


COPY crawler.proto .
RUN python -m grpc_tools.protoc \
    -I. \
    --python_out=. \
    --grpc_python_out=. \
    crawler.proto

COPY crawler.py .

RUN mkdir -p /app/output

CMD ["python", "crawler.py"]
