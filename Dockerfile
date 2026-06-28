# ============================================================
# Stage 1: Build zdns from source
# ============================================================
FROM golang:1.25-bookworm AS zdns-builder

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 https://github.com/zmap/zdns.git /build/zdns
WORKDIR /build/zdns
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /usr/local/bin/zdns

# ============================================================
# Stage 2: Python runtime with pipeline code
# ============================================================
FROM python:3.11-slim-bookworm

# System dependencies for enrichment downloaders
RUN apt-get update &&     apt-get install -y --no-install-recommends         wget         bzip2         curl     && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install the hermes package (provides deps + console scripts; ships SQL as package data)
COPY pyproject.toml README.md ./
COPY src/ /app/src/
RUN pip install --no-cache-dir .

# Copy zdns binary from build stage
COPY --from=zdns-builder /usr/local/bin/zdns /usr/local/bin/zdns

# Copy static data files needed at runtime
COPY src/hermes/enrichment/ripe_ipmap/cache/iso_code_2 /app/data/iso_code_2

# Copy entrypoint
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Set environment variables
ENV HERMES_CACHE_DIR=/app/cache
ENV ZDNS_PATH=/usr/local/bin/zdns
ENV PYTHONUNBUFFERED=1

# The cache directory is a required mount point
VOLUME /app/cache

ENTRYPOINT ["/app/docker-entrypoint.sh"]
