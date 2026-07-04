# Step 04 — Mapping: Hop Enrichment & Geolocation

## Overview

Step 04 is the **mapping and enrichment** stage of the HERMES union pipeline. It takes the raw transient events produced by Step 03 (which contain forward and reverse traceroute paths with bare IP addresses) and enriches every hop with:

- **ASN ownership** (via longest-prefix matching against BGP prefix tables and IXP membership data)
- **Geolocation** (latitude/longitude/city/country, using a priority cascade: HOIHO rDNS-based > IPinfo > RIPE IPmap > M-Lab server metadata)
- **Distance and speed-of-light checks** (cumulative geographic distance along the path vs. observed RTT)
- **Latency anomaly flags** on the reverse path (above-baseline, increasing-latency, baseline-consistency)

The step is a single self-contained SQL script:

| Script | Purpose |
|--------|---------|
| `04_mapping_union.sql` | Builds four reusable **lookup temp tables** (closest AS metadata, rDNS, geolocation, extracted IP prefixes) scoped to the target `${DAY}`, then uses them to enrich forward and reverse hops, compute distance/RTT metrics, and produce the **final output table** — all in one multi-statement script |

## Input Tables

| Table | Description |
|-------|-------------|
| `hermes_union.transient_events_union` | Output of Step 03. Contains one row per (scamper trace, anomaly group) with `node_details` (forward path) and `reverse_node_details` (reverse path) arrays |
| `hermes.as_metadata` | ASN-level metadata (org name, PeeringDB name), partitioned by date |
| `hermes.unified_ip_to_rdns` / `unified_ip_to_rdns_ipv6` | IP-to-rDNS hostname mappings |
| `hermes.unified_ip_to_geoloc` / `unified_ip_to_geoloc_ipv6` | IP-to-geolocation (lat, lon, city, country) from IPinfo and RIPE IPmap |
| `hermes.unified_ip_to_as` / `unified_ip_to_as_ipv6` | IP prefix-to-ASN mappings |
| `hermes.geolocation` | HOIHO rDNS-based geolocation (hostname to lat/lon/place/CLLI) |
| `ix_data.ixp_members` | IXP membership: maps IXP peering IPs to ASN and IXP name |
| `measurement-lab.ndt_raw.hopannotation2` | M-Lab hop annotation data (CIDR, ASN from traceroute annotations) |
| `hermes.site_to_state` | Maps M-Lab server site codes to US state names |

## Output Table

**`mlab-collaboration.hermes_union.transient_events_with_as_and_geoloc`**

Partitioned by `partition_date`. One row per transient event, carrying all the original event metadata plus two enriched hop arrays (forward and reverse).

---

## Output Fields

### Event-level fields (inherited from Step 03)

| Field | Type | Description |
|-------|------|-------------|
| `id` | `STRING` | Scamper trace UUID — unique identifier for the traceroute measurement |
| `ip_version` | `STRING` | `"v4"` or `"v6"` — IP version of the measurement |
| `src` | `STRING` | Source (client) IP address of the traceroute |
| `dst` | `STRING` | Destination (server) IP address of the traceroute |
| `src_city` | `STRING` | Client city in format `"City-State-CountryCode"` (e.g., `"New York-NY-US"`) |
| `src_asn` | `INT64` | Client's Autonomous System Number |
| `src_asn_name` | `STRING` | Human-readable name of the client's AS |
| `src_country` | `STRING` | Client's country code (ISO 3166-1 alpha-2) |
| `src_state` | `STRING` | Client's state/subdivision code |
| `src_lat` | `FLOAT64` | Client latitude |
| `src_lon` | `FLOAT64` | Client longitude |
| `client_name` | `STRING` | NDT client software name |
| `dst_site` | `STRING` | M-Lab server site code (e.g., `"lga07"`) |
| `dst_city` | `STRING` | Server city |
| `dst_country` | `STRING` | Server country code |
| `dst_asn` | `INT64` | Server's ASN |
| `dst_lat` | `FLOAT64` | Server latitude |
| `dst_lon` | `FLOAT64` | Server longitude |
| `start` | `INT64` | Traceroute start time (Unix epoch seconds) |
| `window_start` | `TIMESTAMP` | Hour-aligned window start of the traceroute |
| `reach_dest` | `BOOL` | Whether the traceroute reached the destination |
| `ndt_rtt` | `FLOAT64` | NDT download minimum RTT (ms) |
| `ndt_throughput` | `FLOAT64` | NDT download throughput (Mbps) |
| `traceroute_rtt` | `FLOAT64` | RTT from the last responding hop in the traceroute (ms) |

### Anomaly detection fields (from Step 02)

| Field | Type | Description |
|-------|------|-------------|
| `total_windows` | `INT64` | Total number of time windows in the analysis group |
| `is_consistent` | `INT64` | Whether the client IP was consistently geolocated (1 = yes) |
| `anomaly_ratio_rtt` | `FLOAT64` | Fraction of windows flagged as RTT anomalies |
| `anomaly_ratio_throughput` | `FLOAT64` | Fraction of windows flagged as download throughput anomalies |
| `anomaly_ratio_upload_throughput` | `FLOAT64` | Fraction of windows flagged as upload throughput anomalies |
| `anomaly_loss_ratio` | `FLOAT64` | Fraction of windows flagged as packet loss anomalies |
| `anomaly_rtt_count` | `INT64` | Count of RTT anomaly windows |
| `anomaly_throughput_count` | `INT64` | Count of download throughput anomaly windows |
| `anomaly_upload_throughput_count` | `INT64` | Count of upload throughput anomaly windows |
| `difference_latency` | `FLOAT64` | Observed latency difference from baseline (ms) |
| `difference_throughput` | `FLOAT64` | Observed download throughput difference from baseline (Mbps) |
| `difference_upload_throughput` | `FLOAT64` | Observed upload throughput difference from baseline (Mbps) |
| `wasserstein_throughput_result` | `FLOAT64` | Wasserstein distance for download throughput distribution shift |
| `wasserstein_upload_throughput_result` | `FLOAT64` | Wasserstein distance for upload throughput distribution shift |
| `mann_whitney_latency` | `FLOAT64` | Mann-Whitney U-test p-value for latency |
| `mann_whitney_throughput` | `FLOAT64` | Mann-Whitney U-test p-value for download throughput |
| `mann_whitney_upload_throughput` | `FLOAT64` | Mann-Whitney U-test p-value for upload throughput |
| `t_test_latency` | `FLOAT64` | t-test p-value for latency |
| `baseline_median_rtt` | `FLOAT64` | Baseline median RTT (ms) |
| `baseline_median_throughput` | `FLOAT64` | Baseline median download throughput (Mbps) |
| `baseline_median_upload_throughput` | `FLOAT64` | Baseline median upload throughput (Mbps) |
| `baseline_median_loss` | `FLOAT64` | Baseline median packet loss rate |
| `number_of_measurements_baseline` | `FLOAT64` | Number of NDT measurements in the baseline period |
| `number_of_unique_src_ips_baseline` | `FLOAT64` | Number of unique source IPs in the baseline period |
| `unique_ip_count_per_site` | `FLOAT64` | Unique client IPs per server site |
| `measurement_count_per_site` | `FLOAT64` | Total measurements per server site |

### City-level percentile fields

| Field | Type | Description |
|-------|------|-------------|
| `city_median_rtt` | `FLOAT64` | 50th percentile RTT for this (city, ASN, site) group (ms) |
| `city_ninetyth_percentile_rtt` | `FLOAT64` | 90th percentile RTT for this group (ms) |
| `city_oneth_percentile_rtt` | `FLOAT64` | 1st percentile RTT for this group (ms) |
| `city_tenth_percentile_rtt` | `FLOAT64` | 10th percentile RTT for this group (ms) |
| `city_median_throughput` | `FLOAT64` | 50th percentile download throughput for this group (Mbps) |
| `city_ninetyth_percentile_throughput` | `FLOAT64` | 90th percentile download throughput for this group (Mbps) |

### Reverse traceroute metadata fields

| Field | Type | Description |
|-------|------|-------------|
| `revtr_system_label` | `STRING` | Label assigned by the reverse traceroute system |
| `revtr_stop_reason` | `STRING` | Why the reverse trace stopped (e.g., `"REACHES"`, `"FAILED"`) |
| `revtr_fail_reason` | `STRING` | Failure reason if the reverse trace did not complete |
| `is_try_from_destination_AS` | `BOOL` | Whether the reverse trace attempted from the destination's AS |
| `revtr_id` | `INT64` | Reverse traceroute measurement ID |

### `forward_updated_node_details` (enriched forward path)

`ARRAY<STRUCT>` — one element per TTL hop, ordered by TTL ascending (client to server direction).

| Sub-field | Type | Description |
|-----------|------|-------------|
| `ttl` | `INT64` | Time-to-live / hop number (1 = first hop from client) |
| `addr` | `STRING` | IP address of the router at this hop (`"*"` if non-responsive) |
| `rdns_name` | `STRING` | Reverse DNS hostname of the hop IP |
| `rtts` | `FLOAT64` | Round-trip time to this hop (ms); `-1` if no response |
| `associated_asn` | `INT64` | ASN that owns this hop's IP (NULL for private/RFC1918 addresses) |
| `associated_org` | `STRING` | Organization name of the ASN (from PeeringDB/WHOIS) |
| `associated_peeringdb_name` | `STRING` | PeeringDB network name of the ASN |
| `associated_ixp` | `STRING` | IXP name if this hop is at an Internet Exchange Point; `"None"` otherwise |
| `latitude` | `FLOAT64` | Hop geolocation latitude (NULL for private IPs) |
| `longitude` | `FLOAT64` | Hop geolocation longitude (NULL for private IPs) |
| `place` | `STRING` | City/place name of the hop |
| `clli` | `STRING` | CLLI code (Common Language Location Identifier) from HOIHO |
| `cc` | `STRING` | Country code of the hop |
| `metro` | `STRING` | Metro area of the hop |
| `score` | `INT64` | Geolocation confidence score (`-1` if unknown) |
| `geo_source` | `STRING` | Provenance of geolocation: `"server_metadata"`, `"hoiho"`, `"ipinfo"`, or `"ripe_ipmap"` |
| `geo_partition_date` | `DATE` | Date of the geolocation data used (NULL for HOIHO/server metadata) |
| `ixp_partition_date` | `DATE` | Date of the IXP membership data used |
| `facilities_info` | `ARRAY<STRUCT>` | Reserved for colocation facility data (currently NULL) |
| `cumulative_distance_km` | `FLOAT64` | Cumulative geographic distance from client to this hop (km) |
| `distance_to_destination_km` | `FLOAT64` | Great-circle distance from this hop to the server (km) |
| `speed_of_internet_fiber` | `FLOAT64` | Minimum possible RTT based on fiber distance at 200 km/ms (ms) |
| `distance_rtt_check` | `STRING` | `"Above threshold"` if fiber-speed RTT > observed RTT (potential geolocation error); `"Below threshold"` otherwise |
| `above_baseline_flag` | `STRING` | Always NULL on forward path (computed only for reverse) |
| `increasing_latency_flag` | `STRING` | Always NULL on forward path |
| `baseline_consistency_flag` | `STRING` | Always NULL on forward path |

### `reverse_updated_node_details` (enriched reverse path)

`ARRAY<STRUCT>` — one element per hop on the reverse traceroute (server to client direction), ordered by hop number.

Same sub-fields as the forward path, plus:

| Sub-field | Type | Description |
|-----------|------|-------------|
| `hop_type` | `STRING` | Type of reverse-traceroute hop (e.g., `"TR"` for traceroute, `"RR"` for record-route, `"TS"` for timestamp, `"SPOOF"` for spoofed) |
| `above_baseline_flag` | `STRING` | `"Above baseline"` if hop RTT exceeds the event's baseline median RTT; `"Within baseline"` otherwise; `"Not responsive"` if no RTT |
| `increasing_latency_flag` | `STRING` | `"Increasing"` if RTT increased by >3ms vs. previous hop; `"Stable/Decreasing"` otherwise; `"Not responsive"` if no RTT |

Note: reverse-path RTTs are converted from microseconds to seconds (divided by 1000) during enrichment.

### Partition field

| Field | Type | Description |
|-------|------|-------------|
| `partition_date` | `DATE` | The processing date (`${DAY}` parameter), used as partition key |

---

## Geolocation Priority Cascade

For each hop IP, geolocation is resolved in this order (first match wins):

1. **Server metadata** (TTL = 1 only): uses the known lat/lon of the M-Lab server
2. **HOIHO** (rDNS-based): matches the hop's rDNS hostname against HOIHO's hostname-to-location table
3. **IPinfo**: uses the `lat_ip_info`/`lon_ip_info` fields from the geolocation lookup table
4. **RIPE IPmap**: uses community-sourced `lat`/`lon` fields

Private IPs (RFC 1918, RFC 6598 CGNAT, IPv6 link-local/ULA) are assigned NULL for all geolocation fields.

## ASN Mapping via Longest-Prefix Match

Each hop IP is matched to an ASN using longest-prefix matching:

1. The IP is masked against all prefix lengths (8-32 for IPv4, 8-128 for IPv6)
2. The masked result is joined against the `extracted_prefixes` table (which merges M-Lab's `hopannotation2` data with `unified_ip_to_as`)
3. The longest matching prefix (highest mask) is selected
4. IXP member IPs are given special treatment: they receive a `/32` prefix and are mapped to the IXP member's ASN

---

## Example Output Row (simplified)

```json
{
  "id": "2026/04/08/mlab4-lga07_ndt-abcde-12345",
  "ip_version": "v4",
  "src": "198.51.100.1",
  "dst": "64.86.132.100",
  "src_city": "New York-NY-US",
  "src_asn": 7922,
  "src_asn_name": "COMCAST-7922",
  "src_country": "US",
  "dst_site": "lga07",
  "dst_city": "New York",
  "dst_country": "US",
  "dst_asn": 396982,
  "ndt_rtt": 12.5,
  "ndt_throughput": 85.3,
  "baseline_median_rtt": 14.2,
  "anomaly_ratio_rtt": 0.75,
  "city_median_rtt": 13.8,
  "partition_date": "2026-04-08",
  "forward_updated_node_details": [
    {
      "ttl": 1,
      "addr": "64.86.132.100",
      "rdns_name": "lga07.measurement-lab.org",
      "rtts": 0.0,
      "associated_asn": 396982,
      "associated_org": "Google LLC",
      "associated_ixp": "None",
      "latitude": 40.7128,
      "longitude": -74.0060,
      "place": "New York-NY-US",
      "cc": "US",
      "geo_source": "server_metadata",
      "cumulative_distance_km": 0.0,
      "distance_rtt_check": "Below threshold"
    },
    {
      "ttl": 2,
      "addr": "162.251.163.113",
      "rdns_name": "ae-3-3512.ear2.NewYork2.Level3.net.",
      "rtts": 3.2,
      "associated_asn": 3356,
      "associated_org": "Lumen Technologies",
      "associated_ixp": "None",
      "latitude": 40.7306,
      "longitude": -73.9866,
      "place": "New York",
      "cc": "US",
      "geo_source": "hoiho",
      "cumulative_distance_km": 2.8,
      "distance_rtt_check": "Below threshold"
    },
    {
      "ttl": 3,
      "addr": "*",
      "rdns_name": "*",
      "rtts": -1,
      "associated_asn": null,
      "associated_ixp": "None",
      "latitude": null,
      "longitude": null,
      "cumulative_distance_km": 2.8,
      "distance_rtt_check": null
    },
    {
      "ttl": 4,
      "addr": "68.86.103.1",
      "rdns_name": "po-303-1511-rtr.nyc.rr.com.",
      "rtts": 10.8,
      "associated_asn": 7922,
      "associated_org": "Comcast Cable Communications, LLC",
      "associated_ixp": "None",
      "latitude": 40.7580,
      "longitude": -73.9855,
      "place": "New York",
      "cc": "US",
      "geo_source": "ipinfo",
      "cumulative_distance_km": 5.9,
      "distance_to_destination_km": 5.1,
      "speed_of_internet_fiber": 0.055,
      "distance_rtt_check": "Below threshold"
    }
  ],
  "reverse_updated_node_details": [
    {
      "ttl": 1,
      "addr": "64.86.132.100",
      "rtts": 0.001,
      "associated_asn": 396982,
      "hop_type": "TR",
      "above_baseline_flag": "Within baseline",
      "increasing_latency_flag": "Stable/Decreasing"
    },
    {
      "ttl": 2,
      "addr": "162.251.163.114",
      "rtts": 0.004,
      "associated_asn": 3356,
      "hop_type": "TR",
      "above_baseline_flag": "Within baseline",
      "increasing_latency_flag": "Increasing"
    }
  ]
}
```

*Note: The example is illustrative with representative field values. Actual IPs, ASNs, and measurements will vary. Some fields omitted for brevity.*

---

## Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `${DAY}` | The target processing date | `2026-04-08` |

## Execution Order

Run `04_mapping_union.sql` as a single multi-statement script: it builds its lookup temp tables, enriches the forward and reverse paths, and INSERTs into the output table in one pass. (The hop-annotation date range in `extracted_prefixes` uses a hardcoded lower bound, `WHERE date BETWEEN '2025-05-01' AND '${DAY}'`.)
