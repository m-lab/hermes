# HERMES Pipeline — Docker Deployment

Run the HERMES union pipeline (IPv4+IPv6 anomaly detection) in a Docker container.

## Prerequisites

- Docker Engine 20.10+
- Google Cloud service account with BigQuery access to `mlab-collaboration`
- IPInfo API token

## Quick Start

### 1. Build the image

```bash
docker build -t hermes-pipeline:latest .
```

### 2. One-time VM setup

```bash
# Create persistent cache directory
sudo mkdir -p /data/hermes-cache

# Create config directory
sudo mkdir -p /etc/hermes

# Add your service account key
cp /path/to/service-account-key.json /etc/hermes/

# Create environment file with API tokens
cat > /etc/hermes/hermes.env <<EOF
IPINFO_TOKEN=your-token-here
HOIHO_TOKEN=your-token-here
EOF

chmod 600 /etc/hermes/hermes.env
```

### 3. Run the pipeline

**Default mode** (processes last 2 days — use this for daily cron):

```bash
docker run --rm \
  --env-file /etc/hermes/hermes.env \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
  -v /etc/hermes/service-account-key.json:/app/credentials.json:ro \
  -v /data/hermes-cache:/app/cache \
  hermes-pipeline
```

**On GCE with attached service account** (no key file needed):

```bash
docker run --rm \
  --env-file /etc/hermes/hermes.env \
  -v /data/hermes-cache:/app/cache \
  hermes-pipeline
```

**Explicit date range:**

```bash
docker run --rm \
  --env-file /etc/hermes/hermes.env \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
  -v /etc/hermes/service-account-key.json:/app/credentials.json:ro \
  -v /data/hermes-cache:/app/cache \
  hermes-pipeline --start-date 2026-06-01 --end-date 2026-06-03
```

**Dry run** (show what would execute without running queries):

```bash
docker run --rm \
  --env-file /etc/hermes/hermes.env \
  -v /data/hermes-cache:/app/cache \
  hermes-pipeline --dry-run
```

### 4. Set up daily cron

```bash
# /etc/cron.d/hermes-pipeline
0 6 * * * root docker run --rm \
  --env-file /etc/hermes/hermes.env \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
  -v /etc/hermes/service-account-key.json:/app/credentials.json:ro \
  -v /data/hermes-cache:/app/cache \
  hermes-pipeline >> /var/log/hermes-pipeline.log 2>&1
```

## CLI Options

All options from `hermes_pipeline_union.py` are passed through:

| Flag | Description |
|------|-------------|
| `--start-date YYYY-MM-DD` | Start of date range (default: 2 days ago) |
| `--end-date YYYY-MM-DD` | End of date range (default: yesterday) |
| `--interval N` | Days between processed dates (default: 1) |
| `--max-workers N` | Parallel workers (default: CPU count) |
| `--force-rerun` | Re-process dates even if output exists |
| `--rerun-dates DATE [DATE ...]` | Specific dates to re-process |
| `--delete-first` | Delete existing output before processing |
| `--skip-data-check` | Skip input data availability check |
| `--dry-run` | Show plan without executing |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `IPINFO_TOKEN` | Yes | IPInfo API authentication token |
| `HOIHO_TOKEN` | No | HOIHO geolocation API token |
| `GOOGLE_APPLICATION_CREDENTIALS` | No | Path to service account JSON inside container. Not needed on GCE with attached SA. |

## Volume Mount

| Container path | Purpose |
|---|---|
| `/app/cache` | **Required.** Persistent cache for IPInfo MMDB snapshots, RIPE IPMap CSVs, RouteViews BGP data, rDNS results, and HOIHO cache. Grows to several GB over time. |

## Troubleshooting

**"ERROR: /app/cache directory does not exist"**
You forgot the volume mount. Add `-v /data/hermes-cache:/app/cache`.

**"ERROR: IPINFO_TOKEN environment variable is not set"**
Pass it via `--env-file` or `-e IPINFO_TOKEN=xxx`.

**BigQuery authentication errors**
Either mount a service account key or ensure the VM has an attached service account with BigQuery access to `mlab-collaboration`.

**"WARNING: iso_code_2 not found"**
The RIPE IPMap country codes file is missing from the cache. It should be copied automatically on first run. If not, check that the Docker image built correctly.

**Cache directory growing too large**
Old RIPE IPMap CSV files accumulate. Periodically clean files older than 30 days:
```bash
find /data/hermes-cache/ripe_ipmap -name "geolocations_*.csv*" -mtime +30 -delete
```
