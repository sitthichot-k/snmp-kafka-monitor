FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends snmp \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir confluent-kafka
WORKDIR /app
COPY *.py index.html ./
