#!/bin/bash
set -e

# --- Validate required cache volume ---
if [ ! -d "/app/cache" ]; then
    echo "ERROR: /app/cache directory does not exist."
    echo "Mount a host directory: docker run -v /data/hermes-cache:/app/cache ..."
    exit 1
fi

# First-run marker
if [ -z "$(ls -A /app/cache 2>/dev/null)" ] && [ ! -f /app/cache/.initialized ]; then
    echo "WARNING: /app/cache is empty. This looks like a first run."
    echo "Cache data (IPInfo, RIPE IPMap, RouteViews, rDNS) will be downloaded."
    touch /app/cache/.initialized
fi

# Create cache subdirectories
mkdir -p /app/cache/zdns          /app/cache/zdns_ipv6          /app/cache/routeviews          /app/cache/routeviews_ipv6          /app/cache/ripe_ipmap          /app/cache/as_org_inetintel          /app/cache/hoiho

# --- Validate required environment variables ---
if [ -z "$IPINFO_TOKEN" ]; then
    echo "ERROR: IPINFO_TOKEN environment variable is not set."
    echo "Pass it via: docker run -e IPINFO_TOKEN=xxx ..."
    exit 1
fi

# --- Validate Google Cloud credentials ---
if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    if curl -s -f -m 2 "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"          -H "Metadata-Flavor: Google" > /dev/null 2>&1; then
        echo "Using GCE metadata service account."
    else
        echo "WARNING: No GOOGLE_APPLICATION_CREDENTIALS set and no GCE metadata server found."
        echo "BigQuery authentication may fail."
    fi
else
    if [ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
        echo "ERROR: GOOGLE_APPLICATION_CREDENTIALS points to '$GOOGLE_APPLICATION_CREDENTIALS' but file does not exist."
        exit 1
    fi
    echo "Using service account key: $GOOGLE_APPLICATION_CREDENTIALS"
fi

# --- Copy iso_code_2 to RIPE cache if not present ---
if [ ! -f /app/cache/ripe_ipmap/iso_code_2 ]; then
    if [ -f /app/data/iso_code_2 ]; then
        cp /app/data/iso_code_2 /app/cache/ripe_ipmap/iso_code_2
        echo "Copied iso_code_2 to RIPE cache."
    else
        echo "WARNING: iso_code_2 not found. RIPE IPMap enrichment may fail."
    fi
fi

echo "=== HERMES Pipeline starting ==="
echo "Cache dir: /app/cache"
echo "Arguments: $@"
echo "================================"

exec hermes-pipeline "$@"
