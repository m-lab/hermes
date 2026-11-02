--------------------------------------------------------------------------------
-- HERMES (union): attach topology (scamper + revtr) to anomalies
--
-- Input:
-- - `mlab-collaboration.${DS}.anomaly_counts_union` (partition_date = 2026-04-08)
-- - `measurement-lab.ndt.scamper1`            (MDA / multipath traceroutes)
-- - `measurement-lab.autojoin_autoload_v2_ndt.scamper2_union` (BYOS standard traceroutes)
-- - `measurement-lab.revtr_raw.revtr1`
-- - `mlab-collaboration.${DS}.merged_download_upload` (for ndt_rtt/throughput + city percentiles)
--
-- Output: `mlab-collaboration.${DS}.transient_events_union`
--
-- Processes IPv4 and IPv6 via ip_version column.
--------------------------------------------------------------------------------
-- CREATE OR REPLACE TABLE `mlab-collaboration.${DS}.transient_events_union`
-- PARTITION BY partition_date
-- AS
DECLARE _detection_granularity STRING DEFAULT '${DETECTION_GRANULARITY}';
ASSERT _detection_granularity IN ('city', 'metro')
  AS 'DETECTION_GRANULARITY must be city or metro';

-- This resolver intentionally mirrors step 02. Traceroutes and day-of
-- percentiles must attach to the exact population whose anomaly was tested.
-- _source_metro_lookup is gone: Phase A0 already resolved every client IP
-- to a metro in unified_src_ip_to_geoloc, so detection no longer does a spatial
-- join at all. That also removes the ST_COVERS/ST_DWITHIN work from this step.


-- Step 03 joins anomaly_counts_union on the detection group only. More than one
-- row per group means step 02 split a group's current-day sample, and the
-- ANY_VALUE(a.*) reads below would mix columns from different splits.
ASSERT NOT EXISTS (
  SELECT 1
  FROM `mlab-collaboration.${DS}.anomaly_counts_union`
  WHERE partition_date = '${DAY}'
  GROUP BY src_group_label, src_asn, dst_site, ip_version
  HAVING COUNT(*) > 1
) AS 'anomaly_counts_union has more than one row per detection group; re-run step 02 for this date';

INSERT INTO `mlab-collaboration.${DS}.transient_events_union`
  (id, dst, src, ndt_rtt, ndt_throughput, ndt_loss_rate, traceroute_rtt,
   total_windows, is_consistent, src_city, src_lat, src_state, src_lon,
   dst_lat, dst_lon, src_asn, dst_site, dst_city, dst_asn, dst_country,
   src_country, src_asn_name, client_name, start, window_start, reach_dest,
   node_details, number_of_measurements_baseline,
   number_of_unique_src_ips_baseline, unique_ip_count_per_site,
   measurement_count_per_site, baseline_median_rtt,
   baseline_median_throughput, baseline_median_upload_throughput,
   baseline_median_loss, ip_version, reverse_node_details,
   revtr_system_label, revtr_stop_reason, revtr_fail_reason,
   is_try_from_destination_AS, revtr_id, city_median_rtt,
   city_ninetyth_percentile_rtt, city_oneth_percentile_rtt,
   city_tenth_percentile_rtt, city_median_throughput,
   city_ninetyth_percentile_throughput, partition_date, anomaly_ratio_rtt,
   anomaly_rtt_count, anomaly_ratio_throughput, anomaly_throughput_count,
   anomaly_ratio_upload_throughput, anomaly_upload_throughput_count,
   anomaly_loss_ratio, difference_latency, difference_throughput,
   difference_upload_throughput, wasserstein_throughput_result,
   wasserstein_upload_throughput_result, mann_whitney_latency,
   mann_whitney_throughput, mann_whitney_upload_throughput, t_test_latency,
   median_upload_throughput, detection_granularity, src_group_label,
   client_geo_source)

WITH
MeasurementsWithGroup AS (
  -- Client geography comes from IPInfo (unified_src_ip_to_geoloc, written by
  -- Phase A0), NOT MaxMind. Rewriting the client.Geo struct in place means every
  -- downstream reference keeps working unchanged, and none of them can
  -- accidentally still be reading MaxMind.
  --
  -- Why replace rather than blend: MaxMind emits a single designated point per
  -- country when it cannot place a client below country level -- 547,105
  -- measurements on 864 coordinates across 185 countries on 2026-08-07, sometimes
  -- the geographic centre (India 21.997,79.001), often the capital (Tokyo, London,
  -- Paris, Jakarta). Metro grouping piled those into whichever metro contains the
  -- point, leaving Tokyo 53.3% and Paris 55.9% synthetic. IPInfo resolves them
  -- properly: 8,066 distinct cities at a median /24.5.
  --
  -- Where IPInfo has nothing the geography is NULL, deliberately. Coverage is
  -- 99.995% of client IPs / 99.05% of measurements (measured against the 2-month
  -- stale BigQuery snapshot, so a lower bound on the daily MMDB). A NULL city
  -- yields a NULL group label, which is exactly how MaxMind's unplaceable clients
  -- already behaved -- excluded rather than guessed at.
  --
  -- NOTE: Subdivision1ISOCode now carries IPInfo's full region NAME, not an ISO
  -- code, so city-mode labels become `Paris-Ile-de-France-FR` rather than
  -- `Paris-IDF-FR`. That matches the metro label shape.
  SELECT
    ndt.* REPLACE (
      (SELECT AS STRUCT ndt.client.* REPLACE (
          (SELECT AS STRUCT ndt.client.Geo.* REPLACE (
              g.city_ip_info        AS City,
              g.region_ip_info      AS Subdivision1ISOCode,
              g.country_ip_info     AS CountryCode,
              g.lat_ip_info         AS Latitude,
              g.lon_ip_info         AS Longitude
          )) AS Geo
      )) AS client
    ),
    IF(
      _detection_granularity = 'metro',
      g.metro,
      -- city_ip_info already holds 'City-Region-CC' for 99.25% of rows (2025-08..2026-10),
      -- so appending region and country again produced 'Paris-Ile-de-France-FR-Ile-de-France-FR'.
      -- Append only when the suffix is absent.
      IF(ENDS_WITH(g.city_ip_info, CONCAT('-', g.region_ip_info, '-', g.country_ip_info)),
        g.city_ip_info,
        CONCAT(g.city_ip_info, '-', g.region_ip_info, '-', g.country_ip_info))
    ) AS detection_src_city,
    -- The client's region either way: in metro mode the metro was resolved from
    -- this same coordinate, so IPInfo's region is the consistent answer and
    -- avoids re-parsing it out of the metro string.
    g.region_ip_info AS detection_src_state
  FROM `mlab-collaboration.${DS}.merged_download_upload` ndt
  LEFT JOIN (
    -- One row per IP: the geolocation closest in time to the day being detected,
    -- mirroring _closest_geo in 04.
    SELECT * EXCEPT(rn) FROM (
      SELECT ip_address, city_ip_info, region_ip_info, country_ip_info,
             lat_ip_info, lon_ip_info, metro,
             -- Match on the IPInfo dump that produced the row, NOT on partition_date.
             -- partition_date is the RUN's target date, so a batched run stamps every
             -- IP in its lookback with one value: 3,592,678 rows landed on 2025-07-31
             -- carrying traffic from 07-11 onward. Ordering by that picks rows by how
             -- the runs happened to be batched. The dump date is a property of the
             -- geolocation itself. COALESCE covers rows written before the column
             -- existed, where partition_date is a fair proxy because a nightly used
             -- that day's dump.
             ROW_NUMBER() OVER (
               PARTITION BY ip_address
               ORDER BY ABS(DATE_DIFF(COALESCE(geoloc_snapshot_date, partition_date),
                                      DATE '${DAY}', DAY)) ASC,
                        COALESCE(geoloc_snapshot_date, partition_date) DESC
             ) AS rn
      -- BOTH address families. The enrichment writes IPv4 and IPv6 client
      -- geolocation to separate tables (the enricher is instantiated once per
      -- family, each with its own `tables` dict), so reading only the IPv4 table
      -- silently drops every IPv6 client: 1,169,928 of 2,564,893 distinct client
      -- IPs on 2026-08-07 (45.6%), carrying 1,786,805 of 4,855,404 measurements
      -- (36.8%). Those measurements got a NULL group label and left detection
      -- entirely -- which is why the first staging run showed transient events
      -- down 70% rather than the expected small change.
      --
      -- UNION ALL, not a join: an address belongs to exactly one family, so the
      -- two tables are disjoint on ip_address and the ROW_NUMBER below still
      -- yields one row per IP.
      FROM (
        SELECT ip_address, partition_date, geoloc_snapshot_date, city_ip_info,
               region_ip_info, country_ip_info, lat_ip_info, lon_ip_info, metro
        FROM `mlab-collaboration.hermes.unified_src_ip_to_geoloc`
        UNION ALL
        SELECT ip_address, partition_date, geoloc_snapshot_date, city_ip_info,
               region_ip_info, country_ip_info, lat_ip_info, lon_ip_info, metro
        FROM `mlab-collaboration.hermes.unified_src_ip_to_geoloc_ipv6`
      )
    ) WHERE rn = 1
  ) g
    ON ndt.client_ip = g.ip_address
  WHERE ndt.partition_date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    -- An unresolved coordinate is not a metro. Do not silently mix a city
    -- fallback into a partition labelled as metro-derived.
    AND (_detection_granularity != 'metro' OR g.metro IS NOT NULL)
  -- A test id carried by more than one test in the window is ambiguous: its
  -- traces and revTrs cannot be attributed to either test, and the per-id
  -- ANY_VALUEs below then mixed fields from both (non-deterministically). Rare:
  -- 90 ids / ~340 tests in 2026-08-05..12, mostly mlab-e2e-monitoring
  -- 'Speedtest_*' ids reused across days. Drop them rather than guess.
  QUALIFY COUNT(*) OVER (PARTITION BY ndt.id) = 1
),
AnomalyCounts AS (
  SELECT *
  FROM `mlab-collaboration.${DS}.anomaly_counts_union`
  WHERE partition_date = '${DAY}'
    AND detection_granularity = _detection_granularity
),

--------------------------------------------------------------------------------
-- D) City-level stats, scamper expansions, final merges
--------------------------------------------------------------------------------
ScamperDataIntermediary AS (
  SELECT
    scamper.id,
    ANY_VALUE(a.total_group_rows) AS total_windows,

    -- Granularity src_city is grouped at; NULL when the join misses (an
    -- unmatched giga trace was never in a tested group).
    -- See docs/proposals/2026-08-group-granularity.md.
    ANY_VALUE(a.detection_granularity) AS detection_granularity,
    ANY_VALUE(a.src_group_label)       AS src_group_label,
    'ipinfo'                           AS client_geo_source,
    ANY_VALUE(a.anomaly_ratio_rtt)        AS anomaly_ratio_rtt,
    ANY_VALUE(a.anomaly_ratio_throughput) AS anomaly_ratio_throughput,
    ANY_VALUE(a.anomaly_ratio_upload_throughput) AS anomaly_ratio_upload_throughput,
    ANY_VALUE(a.anomaly_loss_ratio)       AS anomaly_loss_ratio,
    ANY_VALUE(a.anomaly_throughput_count) AS anomaly_throughput_count,
    ANY_VALUE(a.anomaly_upload_throughput_count) AS anomaly_upload_throughput_count,
    ANY_VALUE(a.anomaly_rtt_count)        AS anomaly_rtt_count,
    ANY_VALUE(a.difference_latency)       AS difference_latency,
    ANY_VALUE(a.difference_throughput)    AS difference_throughput,
    ANY_VALUE(a.difference_upload_throughput) AS difference_upload_throughput,
    ANY_VALUE(a.wasserstein_throughput_result) AS wasserstein_throughput_result,
    ANY_VALUE(a.wasserstein_upload_throughput_result) AS wasserstein_upload_throughput_result,
    ANY_VALUE(a.mann_whitney_latency)     AS mann_whitney_latency,
    ANY_VALUE(a.mann_whitney_throughput)  AS mann_whitney_throughput,
    ANY_VALUE(a.mann_whitney_upload_throughput)  AS mann_whitney_upload_throughput,
    ANY_VALUE(a.t_test_latency)           AS t_test_latency,
    AVG(a.baseline_median_rtt)           AS baseline_median_rtt,
    AVG(a.baseline_median_throughput)     AS baseline_median_throughput,
    AVG(a.baseline_median_upload_throughput)     AS baseline_median_upload_throughput,
    AVG(a.baseline_median_loss)          AS baseline_median_loss,

    -- Fall back to the scamper record when NO AnomalyCounts row matched.
    -- Keyed on a.src_asn IS NULL (did the join match at all?) and NOT on each
    -- column being NULL: COALESCE(a.src_lat, ...) would also fire for a MATCHED
    -- row whose src_lat happens to be NULL, silently changing existing output.
    IF(a.src_asn IS NULL, ndt.detection_src_city, a.src_city) AS src_city,
    IF(a.src_asn IS NULL, scamper.client.Network.ASNumber, a.src_asn) AS src_asn,
    AVG(IF(a.src_asn IS NULL, ndt.client.Geo.Latitude, a.src_lat)) AS src_lat,
    ANY_VALUE(IF(a.src_asn IS NULL, ndt.detection_src_state, a.src_state)) AS src_state,
    AVG(IF(a.src_asn IS NULL, ndt.client.Geo.Longitude, a.src_lon)) AS src_lon,
    AVG(IF(a.src_asn IS NULL, scamper.server.Geo.Latitude, a.dst_lat)) AS dst_lat,
    AVG(IF(a.src_asn IS NULL, scamper.server.Geo.Longitude, a.dst_lon)) AS dst_lon,
    IF(a.src_asn IS NULL, scamper.server.Site, a.dst_site) AS dst_site,
    ANY_VALUE(IF(a.src_asn IS NULL, scamper.server.Geo.City, a.dst_city)) AS dst_city,
    ANY_VALUE(IF(a.src_asn IS NULL, scamper.server.Geo.CountryCode, a.dst_country)) AS dst_country,
    ANY_VALUE(IF(a.src_asn IS NULL, scamper.server.Network.ASNumber, a.dst_asn)) AS dst_asn,
    ANY_VALUE(IF(a.src_asn IS NULL, scamper.client.Geo.CountryCode, a.src_country)) AS src_country,
    ANY_VALUE(IF(a.src_asn IS NULL, scamper.client.Network.ASName, a.src_asn_name)) AS src_asn_name,
    -- client_name describes ONE test: keep the test's own, or NULL if it sent none.
    -- Never the group's. Step 02's group value is MIN(client_name), the
    -- alphabetically first name in the group, and copying it onto every matched
    -- test mislabelled 58% of test rows on 2026-09-10 (staging, all countries) in
    -- both directions: 5 'bwp-client' tests renamed 52,600 'giga-meter' tests in
    -- one Tashkent group (AS8193/sxr138800) so step 04 dropped them, while groups
    -- whose first name was 'giga-meter' renamed their 'ist', 'ndt7-android', ...
    -- tests, which then made up 68% of giga_meter_measurements. A NULL own name
    -- also inherited 'giga-meter' that way (7,550 tests), so no fallback.
    ANY_VALUE(ndt.client_name) AS client_name,
    IF(a.src_asn IS NULL,
       IF(REGEXP_CONTAINS(scamper.raw.Tracelb.src, ':'), 'v6', 'v4'),
       a.ip_version) AS ip_version,

    TIMESTAMP_TRUNC(TIMESTAMP_SECONDS(scamper.raw.Tracelb.start.sec), HOUR) AS window_start,
    MAX(a.is_consistent) AS is_consistent,
    AVG(a.number_of_measurements_baseline) AS number_of_measurements_baseline,
    AVG(a.number_of_unique_src_ips_baseline) AS number_of_unique_src_ips_baseline,
    AVG(a.unique_ip_count_per_site) AS unique_ip_count_per_site,
    AVG(a.measurement_count_per_site) AS measurement_count_per_site
  FROM `measurement-lab.ndt.scamper1` scamper
  -- Needed only to identify giga clients (and supply client_name) when the
  -- AnomalyCounts join does not match. LEFT so it can never drop a row that
  -- reaches this CTE today.
  LEFT JOIN MeasurementsWithGroup ndt
    ON ndt.id = scamper.id
    AND ndt.partition_date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
  LEFT JOIN AnomalyCounts a
    ON ndt.detection_src_city = a.src_group_label
    AND scamper.client.Network.ASNumber = a.src_asn
    AND scamper.server.Site = a.dst_site
    AND a.ip_version = IF(REGEXP_CONTAINS(scamper.raw.Tracelb.src, ':'), 'v6', 'v4')
  WHERE scamper.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    -- Keep a trace if its group was detected that day (unchanged behaviour) OR
    -- if it is a giga client. 03 previously INNER JOINed AnomalyCounts, so giga
    -- traces from groups absent that day were discarded before 04 ever saw them
    -- (measured 2026-08-05: 17.6% of giga measurements never reached 04, 99.6%
    -- of which DID have a trace available).
    AND (a.src_asn IS NOT NULL OR ndt.client_name = 'giga-meter'
         -- A listed Giga school IP, whatever client it ran: step 04 writes those
         -- rows to the giga subset too, so dropping them here (ordinary client,
         -- group with no anomaly-count row) lost them before 04 could see them.
         -- Same list and window as 04's giga INSERT (any month DAY-8..DAY touches).
         OR ndt.client_ip IN (
              SELECT ip_address
              FROM `mlab-collaboration.${DS}.giga_school_ips`
              WHERE month_start BETWEEN DATE_TRUNC(DATE_SUB(CAST('${DAY}' AS DATE), INTERVAL 8 DAY), MONTH)
                                    AND DATE_TRUNC(CAST('${DAY}' AS DATE), MONTH)
                AND ip_address IS NOT NULL
            ))
  GROUP BY
    scamper.id, src_city, src_asn, dst_site, window_start,
    ip_version
),

ScamperData AS (
  SELECT
    s.raw,
    s.raw.Tracelb.src           AS src,
    s.raw.Tracelb.dst           AS dst,
    s.raw.Tracelb.start.Sec     AS start_sec,
    s.raw.Tracelb.nodes         AS nodes,
    sc.*
  FROM `measurement-lab.ndt.scamper1` s
  JOIN ScamperDataIntermediary sc
    ON sc.id = s.id
  WHERE s.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
),

FilteredData AS (
  SELECT
    sd.*,
    ndt.download_min_rtt             AS ndt_rtt,
    ndt.download_throughput_mbps     AS ndt_throughput,
    ndt.download_loss_rate           AS ndt_loss_rate
  FROM ScamperData sd
  JOIN MeasurementsWithGroup ndt
    ON ndt.id = sd.id
  WHERE
    ndt.partition_date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    AND sd.src_city IS NOT NULL
    AND sd.src_asn  IS NOT NULL
),

--------------------------------------------------------------------------------
-- D.2) scamper2_union: BYOS standard traceroutes (parallel to D for scamper1)
--------------------------------------------------------------------------------
Scamper2DataIntermediary AS (
  SELECT
    scamper.raw.Metadata.UUID AS id,
    ANY_VALUE(a.total_group_rows) AS total_windows,

    -- Granularity src_city is grouped at; NULL when the join misses (an
    -- unmatched giga trace was never in a tested group).
    -- See docs/proposals/2026-08-group-granularity.md.
    ANY_VALUE(a.detection_granularity) AS detection_granularity,
    ANY_VALUE(a.src_group_label)       AS src_group_label,
    'ipinfo'                           AS client_geo_source,
    ANY_VALUE(a.anomaly_ratio_rtt)        AS anomaly_ratio_rtt,
    ANY_VALUE(a.anomaly_ratio_throughput) AS anomaly_ratio_throughput,
    ANY_VALUE(a.anomaly_ratio_upload_throughput) AS anomaly_ratio_upload_throughput,
    ANY_VALUE(a.anomaly_loss_ratio)       AS anomaly_loss_ratio,
    ANY_VALUE(a.anomaly_throughput_count) AS anomaly_throughput_count,
    ANY_VALUE(a.anomaly_upload_throughput_count) AS anomaly_upload_throughput_count,
    ANY_VALUE(a.anomaly_rtt_count)        AS anomaly_rtt_count,
    ANY_VALUE(a.difference_latency)       AS difference_latency,
    ANY_VALUE(a.difference_throughput)    AS difference_throughput,
    ANY_VALUE(a.difference_upload_throughput) AS difference_upload_throughput,
    ANY_VALUE(a.wasserstein_throughput_result) AS wasserstein_throughput_result,
    ANY_VALUE(a.wasserstein_upload_throughput_result) AS wasserstein_upload_throughput_result,
    ANY_VALUE(a.mann_whitney_latency)     AS mann_whitney_latency,
    ANY_VALUE(a.mann_whitney_throughput)  AS mann_whitney_throughput,
    ANY_VALUE(a.mann_whitney_upload_throughput)  AS mann_whitney_upload_throughput,
    ANY_VALUE(a.t_test_latency)           AS t_test_latency,
    AVG(a.baseline_median_rtt)           AS baseline_median_rtt,
    AVG(a.baseline_median_throughput)     AS baseline_median_throughput,
    AVG(a.baseline_median_upload_throughput)     AS baseline_median_upload_throughput,
    AVG(a.baseline_median_loss)          AS baseline_median_loss,

    -- Same fallback rule as the scamper1 branch above; see the note there on
    -- why this is IF(a.src_asn IS NULL, ...) and not COALESCE. Fallback values
    -- come from ndt, which this branch already joins.
    IF(a.src_asn IS NULL, ndt.detection_src_city, a.src_city) AS src_city,
    IF(a.src_asn IS NULL, ndt.client.Network.ASNumber, a.src_asn) AS src_asn,
    AVG(IF(a.src_asn IS NULL, ndt.client.Geo.Latitude, a.src_lat)) AS src_lat,
    ANY_VALUE(IF(a.src_asn IS NULL, ndt.detection_src_state, a.src_state)) AS src_state,
    AVG(IF(a.src_asn IS NULL, ndt.client.Geo.Longitude, a.src_lon)) AS src_lon,
    AVG(IF(a.src_asn IS NULL, ndt.server.Geo.Latitude, a.dst_lat)) AS dst_lat,
    AVG(IF(a.src_asn IS NULL, ndt.server.Geo.Longitude, a.dst_lon)) AS dst_lon,
    IF(a.src_asn IS NULL, ndt.server.Site, a.dst_site) AS dst_site,
    ANY_VALUE(IF(a.src_asn IS NULL, ndt.server.Geo.City, a.dst_city)) AS dst_city,
    ANY_VALUE(IF(a.src_asn IS NULL, ndt.server.Geo.CountryCode, a.dst_country)) AS dst_country,
    ANY_VALUE(IF(a.src_asn IS NULL, ndt.server.Network.ASNumber, a.dst_asn)) AS dst_asn,
    ANY_VALUE(IF(a.src_asn IS NULL, ndt.client.Geo.CountryCode, a.src_country)) AS src_country,
    ANY_VALUE(IF(a.src_asn IS NULL, ndt.client.Network.ASName, a.src_asn_name)) AS src_asn_name,
    -- The test's own name, never the group's; see the scamper1 branch above.
    ANY_VALUE(ndt.client_name) AS client_name,
    IF(a.src_asn IS NULL,
       IF(REGEXP_CONTAINS(scamper.raw.Trace.src, ':'), 'v6', 'v4'),
       a.ip_version) AS ip_version,

    TIMESTAMP_TRUNC(TIMESTAMP_SECONDS(SAFE_CAST(scamper.raw.Trace.start.sec AS INT64)), HOUR) AS window_start,
    MAX(a.is_consistent) AS is_consistent,
    AVG(a.number_of_measurements_baseline) AS number_of_measurements_baseline,
    AVG(a.number_of_unique_src_ips_baseline) AS number_of_unique_src_ips_baseline,
    AVG(a.unique_ip_count_per_site) AS unique_ip_count_per_site,
    AVG(a.measurement_count_per_site) AS measurement_count_per_site
  FROM `measurement-lab.autojoin_autoload_v2_ndt.scamper2_union` scamper
  JOIN MeasurementsWithGroup ndt
    ON ndt.id = scamper.raw.Metadata.UUID
    AND ndt.client_ip = scamper.raw.Trace.dst
    AND ndt.partition_date = CAST(scamper.date AS DATE)
    AND ndt.partition_date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
  LEFT JOIN AnomalyCounts a
    ON ndt.client.Network.ASNumber = a.src_asn
    AND ndt.server.Site = a.dst_site
    AND ndt.detection_src_city = a.src_group_label
    AND a.ip_version = IF(REGEXP_CONTAINS(scamper.raw.Trace.src, ':'), 'v6', 'v4')
  WHERE scamper.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    -- Same rule as the scamper1 branch. BYOS is where most of the loss sits:
    -- measured capture rate 68.6% for BYOS-only traces vs 90.9% for scamper1,
    -- because BYOS deployments are sparser and their groups more often absent
    -- from anomaly_counts_union on a given day.
    AND (a.src_asn IS NOT NULL OR ndt.client_name = 'giga-meter'
         -- A listed Giga school IP, whatever client it ran: step 04 writes those
         -- rows to the giga subset too, so dropping them here (ordinary client,
         -- group with no anomaly-count row) lost them before 04 could see them.
         -- Same list and window as 04's giga INSERT (any month DAY-8..DAY touches).
         OR ndt.client_ip IN (
              SELECT ip_address
              FROM `mlab-collaboration.${DS}.giga_school_ips`
              WHERE month_start BETWEEN DATE_TRUNC(DATE_SUB(CAST('${DAY}' AS DATE), INTERVAL 8 DAY), MONTH)
                                    AND DATE_TRUNC(CAST('${DAY}' AS DATE), MONTH)
                AND ip_address IS NOT NULL
            ))
  GROUP BY
    scamper.raw.Metadata.UUID, src_city, src_asn, dst_site, window_start,
    ip_version
),

Scamper2Data AS (
  SELECT
    s.raw.Trace.src           AS src,
    s.raw.Trace.dst           AS dst,
    SAFE_CAST(s.raw.Trace.start.sec AS INT64) AS start_sec,
    s.raw.Trace.hops          AS hops,
    sc.*
  FROM `measurement-lab.autojoin_autoload_v2_ndt.scamper2_union` s
  JOIN Scamper2DataIntermediary sc
    ON sc.id = s.raw.Metadata.UUID
  WHERE s.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
),

Scamper2FilteredData AS (
  SELECT
    sd.*,
    ndt.download_min_rtt             AS ndt_rtt,
    ndt.download_throughput_mbps     AS ndt_throughput,
    ndt.download_loss_rate           AS ndt_loss_rate
  FROM Scamper2Data sd
  JOIN MeasurementsWithGroup ndt
    ON ndt.id = sd.id
    AND ndt.client_ip = sd.dst
  WHERE
    ndt.partition_date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
    AND sd.src_city IS NOT NULL
    AND sd.src_asn  IS NOT NULL
),

--------------------------------------------------------------------------------
-- E) Expand traceroute nodes — scamper1 (MDA) + scamper2 (BYOS), merged
--------------------------------------------------------------------------------
-- scamper1 (MDA) is multipath: one trace is a DAG of nodes×links×Links×Probes×
-- Replies across several flows. The previous version CROSS JOIN UNNEST-ed all five
-- levels into one global table and picked, per (id, TTL), the lowest-RTT node via a
-- global ROW_NUMBER. That (a) materialized a ~1B-row intermediate + full sort that
-- exhausts query memory on large days, and (b) chose nodes independently per TTL,
-- so it could stitch hops from different flows into one incoherent "path".
--
-- Instead: collapse each trace's nested arrays *inside the row* (per-trace ARRAY
-- subquery — no global flatten) into per-(flow, ttl) hops with the min reply RTT,
-- then keep only the single MOST-RESPONSIVE flow's path (most responding hops, then
-- furthest TTL, then lowest flow id). That removes the explosion and yields one
-- coherent real path per trace.
--
-- WHERE EACH HOP SITS, AND WHOSE RTT IS WHOSE. In M-Lab's scamper1 records
-- node.links[] is the sequence of HOPS along the node's outgoing link(s), one
-- entry per TTL step, and each inner Links[] holds the addresses that answered at
-- that step ('*' for unanswered probes, which never carry replies). A reply to a
-- probe sent with TTL t comes from that Links[] entry's address -- the far end --
-- so the address sits at distance t, i.e. position t + 1, and the reply's RTT is
-- its OWN RTT. Every hop row below is built from exactly that: one row per
-- (flow, position, responder), RTT = that responder's fastest reply on that flow
-- at that TTL. Nothing is copied from the node that owns the link.
--
-- Until 2026-10 a hop row was the link's NEAR end (node.addr) at position =
-- probe TTL. That is right only when the link spans one TTL. A "compressed" link
-- spans several (it crosses an unresponsive or per-packet load-balanced hop:
-- node.links has more entries than node.linkc). There the near end was copied onto
-- every TTL of the span, its real position became '*', and the routers answering
-- inside the span were lost: 18.5% of step-03 rows on 2026-10-14 repeated an
-- address and 3.7% held a manufactured A..B..A loop. E.g. 195.22.209.41 -> '*' ->
-- 154.54.73.169 -> 154.54.61.46 at positions 3..6 came out as
-- '*', 195.22.209.41, 195.22.209.41, 154.54.61.46.
--
-- The FIRST router (nodes[0], reached at TTL 1) is the one address with no reply:
-- scamper does not store TTL-1 probes. It still answered, so it is placed at
-- position 2 on every flow, with a NULL RTT (unknown, not 0, not the next hop's).
-- nodes[0] is '*' when the first hop did not answer; that row is then a gap.
-- Verified on 5,654 traces of 2026-10-14: nodes[0] is the only node that never
-- appears as a responder, and '*' entries never carry a reply.
--
-- far_rtt equals rtts for scamper1 rows: the responder's own reply is also "the
-- reply at that position". It is kept as a separate column because the hop chosen
-- per position (FinalNodeExtraction) and traceroute_rtt (LastNonStarRTT) are
-- defined on it for both sources.
ScamperFlowHops AS (
  SELECT
    fd.* EXCEPT(nodes, raw),
    ARRAY(
      -- Derived tables, not a WITH: a CTE inside this per-row subquery cannot
      -- reference fd, while FROM-clause subqueries over UNNEST(fd.nodes) can.
      -- ONE pass over the replies: an earlier draft unnested them a second time to
      -- list each flow for the first-router row, and with the UNION ALL that ran out
      -- of memory on a full day (2026-10-31, staging 2026-10-14).
      SELECT AS STRUCT
        r.flow_id,
        IF(first_row, 2, r.probe_ttl)                       AS probe_ttl,
        IF(first_row, fd.nodes[SAFE_OFFSET(0)].addr, r.addr) AS addr,
        IF(first_row, fd.nodes[SAFE_OFFSET(0)].name, nm.rdns_name) AS rdns_name,
        IF(first_row, fd.nodes[SAFE_OFFSET(0)].addr, r.addr) AS link_addr,
        IF(first_row, CAST(NULL AS FLOAT64), r.rtt)         AS rtts,
        IF(first_row, CAST(NULL AS FLOAT64), r.rtt)         AS far_rtt,
        IF(first_row, CAST(NULL AS INT64), r.probe_rttl)    AS probe_rttl
      FROM (
        SELECT
          r0.*,
          ROW_NUMBER() OVER (PARTITION BY r0.flow_id ORDER BY r0.probe_ttl, r0.addr) AS rn_in_flow,
          MIN(r0.probe_ttl) OVER (PARTITION BY r0.flow_id)                            AS min_ttl
        FROM (
          -- every reply: its responder, at probe TTL + 1, with its own RTT
          SELECT
            probe.Flowid         AS flow_id,
            probe.TTL + 1        AS probe_ttl,  -- position = distance + 1
            link_item.addr       AS addr,
            MIN(replies.RTT)     AS rtt,
            MIN(replies.TTL)     AS probe_rttl
          FROM UNNEST(fd.nodes)              AS node,
               UNNEST(node.links)            AS link,
               UNNEST(link.Links)            AS link_item,
               UNNEST(link_item.Probes)      AS probe,
               UNNEST(probe.Replies)         AS replies
          -- A reply without an RTT is ignored (none of 138K replies sampled on
          -- 2026-10-14 had one). Referencing replies.RTT also matters for cost:
          -- an UNNEST(probe.Replies) that names no field of it reads every field.
          WHERE link_item.addr != '*' AND replies.RTT IS NOT NULL
          GROUP BY flow_id, probe_ttl, addr
        ) AS r0
      ) AS r
      -- rDNS lives on the node entry; a responder inside a compressed link has
      -- none (it is not in nodes[]), so NULL
      LEFT JOIN (
        SELECT n.addr, MIN(n.name) AS rdns_name
        FROM UNNEST(fd.nodes) AS n
        GROUP BY n.addr
      ) AS nm
        ON nm.addr = r.addr
      -- The first router (nodes[0], reached at TTL 1, whose reply scamper does not
      -- store) gets one extra row at position 2 on every flow that has a reply --
      -- unless the flow has a stored TTL-1 reply (min_ttl = 2, never seen in this
      -- data), whose own responder already holds position 2, or nodes[0] is '*'.
      CROSS JOIN UNNEST(
        IF(r.rn_in_flow = 1 AND r.min_ttl > 2 AND fd.nodes[SAFE_OFFSET(0)].addr != '*',
           [FALSE, TRUE], [FALSE])
      ) AS first_row
    ) AS hops
  FROM FilteredData fd
),

ScamperBestFlow AS (
  SELECT
    sfh.* EXCEPT(hops),
    hops,
    (
      SELECT t.flow_id
      FROM (
        SELECT
          flow_id,
          -- prefer a flow on which the destination answers, so the multipath
          -- collapse never drops a dst-reaching path in favour of a merely-longer one
          MAX(CASE WHEN addr = sfh.dst THEN 1 ELSE 0 END) AS reaches_dst,
          COUNT(DISTINCT IF(addr != '*', probe_ttl, NULL)) AS responsive_hops,
          MAX(probe_ttl)                                    AS max_ttl
        FROM UNNEST(hops)
        GROUP BY flow_id
      ) t
      ORDER BY t.reaches_dst DESC, t.responsive_hops DESC, t.max_ttl DESC, t.flow_id ASC
      LIMIT 1
    ) AS best_flow_id
  FROM ScamperFlowHops sfh
),

-- The chosen flow's path. Every responder already has its own row, including the
-- deepest router of a trace that stops short of the destination and the
-- destination itself (which NodeExtraction then does not append again).
--
-- NO-REPLY TRACES. A trace with no stored reply at all has no hops and no best
-- flow (7.1% of scamper1 traces on 2026-10-14). Dropping it removed its NDT test
-- from transient_events_union and from step 07's day-of counts. It gets a single
-- placeholder at position 1, which ExpandedRows turns into the server row, so its
-- path is just the server. flow_id NULL marks it: it is not scamper1 data for the
-- merge below, so a scamper2 trace for the same test still fills the path.
WithoutDstNodeExtraction AS (
  SELECT
    bf.* EXCEPT(hops, best_flow_id),
    h.addr,
    h.rdns_name,
    h.rtts,
    h.far_rtt,
    h.probe_ttl,
    h.probe_rttl,
    h.flow_id,
    h.link_addr,
    1 AS rtt_order
  FROM ScamperBestFlow bf
  CROSS JOIN UNNEST(ARRAY(
    SELECT AS STRUCT * FROM (
      SELECT addr, rdns_name, rtts, far_rtt, probe_ttl, probe_rttl, flow_id, link_addr
      FROM UNNEST(bf.hops)
      WHERE flow_id = bf.best_flow_id

      UNION ALL

      -- no-reply placeholder (becomes the server row)
      SELECT
        bf.src, CAST(NULL AS STRING), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        1, CAST(NULL AS INT64), CAST(NULL AS INT64), bf.src
      FROM UNNEST([1])
      WHERE bf.best_flow_id IS NULL
    )
  )) AS h
),

-- scamper2: flat hops (no multipath — no Flowid / link_item nesting)
--
-- POSITION. A router sits at its distance + 1 (see ScamperFlowHops), so position
-- 1 is left for the synthetic server row (ExpandedRows). scamper2 lists each hop
-- at its real TTL (the first router at 1), so it is shifted by one to the same
-- convention. Without the shift the first router (present in 97% of scamper2
-- traces on 2026-10-14) was overwritten by the server row, and every other
-- scamper2 router sat one position lower than the same router in scamper1.
--
-- A trace with no usable hop (hops = [], ~1.7K tests/day) gets the same
-- server-only placeholder as a no-reply scamper1 trace (position 1, flow_id
-- NULL). Before, the CROSS JOIN UNNEST produced no row and the test vanished.
Scamper2WithoutDstNodeExtraction AS (
  -- ONE reference to Scamper2FilteredData. BigQuery inlines a CTE at every
  -- reference, so a second branch for the placeholder (UNION ALL ... WHERE NOT
  -- EXISTS) duplicated the whole scamper2 pipeline in the plan: 1,911 -> 4,563
  -- stages, and with the school-IP predicate step 03 ran out of memory
  -- (bisected on 2026-10-31). The LEFT JOIN yields one row per usable hop, or a
  -- single row with hop NULL when the trace has none -- the server-only
  -- placeholder (position 1, flow_id NULL) that becomes the server row.
  SELECT * EXCEPT(no_hop)
  FROM (
    SELECT
      fd.* EXCEPT(hops),
      hop IS NULL                                            AS no_hop,
      IF(hop IS NULL, fd.src, hop.addr)                      AS addr,
      NULLIF(hop.name, '')                                   AS rdns_name,
      SAFE_CAST(hop.rtt AS FLOAT64)                          AS rtts,
      -- scamper2 hops carry their own address and RTT, so nothing is shifted here.
      SAFE_CAST(hop.rtt AS FLOAT64)                          AS far_rtt,
      IF(hop IS NULL, 1, SAFE_CAST(hop.probe_ttl AS INT64) + 1) AS probe_ttl,
      CAST(NULL AS INT64)                                    AS probe_rttl,
      IF(hop IS NULL, CAST(NULL AS INT64), 1)                AS flow_id,
      IF(hop IS NULL, fd.src, hop.addr)                      AS link_addr,
      ROW_NUMBER() OVER (
        PARTITION BY fd.id, SAFE_CAST(hop.probe_ttl AS INT64), hop.addr
        ORDER BY SAFE_CAST(hop.rtt AS FLOAT64) ASC
      ) AS rtt_order
    FROM Scamper2FilteredData fd
    LEFT JOIN UNNEST(fd.hops) AS hop
      ON SAFE_CAST(hop.probe_ttl AS INT64) IS NOT NULL
  )
  WHERE rtt_order = 1
),

-- Merge: prefer scamper1; scamper2 only fills gaps where scamper1 has no data.
-- A placeholder (flow_id NULL, either source) is not data: real scamper2 hops may
-- still fill a scamper1 placeholder, but a scamper2 placeholder is only used when
-- scamper1 has no row at all for the test, so a test never gets two placeholders.
Scamper1IDs AS (
  SELECT id, LOGICAL_OR(flow_id IS NOT NULL) AS has_data
  FROM WithoutDstNodeExtraction
  GROUP BY id
),

MergedNodeExpansion AS (
  SELECT * FROM WithoutDstNodeExtraction

  UNION ALL

  SELECT s2.*
  FROM Scamper2WithoutDstNodeExtraction s2
  LEFT JOIN Scamper1IDs s1 ON s1.id = s2.id
  WHERE s1.id IS NULL
     OR (NOT s1.has_data AND s2.flow_id IS NOT NULL)
),

LastNonStarRTT AS (
  SELECT
    w.*,
    -- The RTT of the furthest reply on the path (normally the destination's); for
    -- scamper1 that is the deepest responder's own reply (far_rtt = rtts there).
    FIRST_VALUE(far_rtt) OVER (PARTITION BY w.id ORDER BY probe_ttl DESC, far_rtt ASC) AS traceroute_rtt
  FROM MergedNodeExpansion w
  WHERE addr != '*'
),

DstCheck AS (
  SELECT id, ARRAY_AGG(DISTINCT link_addr) AS all_link_addrs
  FROM LastNonStarRTT
  GROUP BY id
),

MaxTTLPerID AS (
  SELECT id, MAX(probe_ttl) AS max_probe_ttl
  FROM LastNonStarRTT
  GROUP BY id
),

-- ttl_rn picks the hop kept per position (NodeExtraction). dst_is_hop: the
-- destination already is the KEPT hop at some position of this path. Then it
-- must not be appended again after the path: link_addr = addr for both sources,
-- so a responding destination is a hop row and was also appended (listed twice
-- at the end of 820K scamper2 traces, 2.4%, on 2026-10-14). Computed on the kept hops, not on
-- every row, so a destination that loses its position to another address is
-- still appended rather than lost.
FinalNodeExtraction AS (
  SELECT
    r.*,
    IFNULL(LOGICAL_OR(r.ttl_rn = 1 AND r.addr = r.dst) OVER (PARTITION BY r.id), FALSE) AS dst_is_hop
  FROM (
    SELECT
      ne.*,
      CASE
        WHEN EXISTS(
          SELECT 1
          FROM UNNEST(dc.all_link_addrs) AS addr
          WHERE addr = ne.dst
        ) THEN TRUE
        ELSE FALSE
      END AS add_dst,
      -- Several addresses at one position (per-packet load balancing on the chosen
      -- flow): keep the fastest reply, then the lowest address, so the choice is
      -- deterministic. far_rtt = rtts for scamper1 (each responder's own reply).
      ROW_NUMBER() OVER (
        PARTITION BY ne.id, ne.probe_ttl
        ORDER BY ne.far_rtt ASC, ne.addr ASC
      ) AS ttl_rn
    FROM LastNonStarRTT ne
    LEFT JOIN DstCheck dc USING(id)
  ) AS r
),

-- Deduplicate to one row per (id, TTL).  link_addr / probe_rttl / flow_id
-- were only needed for DstCheck (destination-reach test) and are dropped here
-- to avoid multiplying hops when MDA discovers multiple next-hops or multiple
-- routers at the same TTL.
NodeExtraction AS (
  SELECT
    id, src, dst, src_city, src_asn, dst_site, dst_city, dst_asn, dst_country,
    src_country, src_asn_name, client_name, src_lat, src_state, src_lon, dst_lat, dst_lon,
    is_consistent, window_start, total_windows, ndt_rtt, ndt_throughput, ndt_loss_rate, traceroute_rtt,
    detection_granularity, src_group_label, client_geo_source,
    start_sec, addr, rdns_name, rtts, probe_ttl,
    number_of_measurements_baseline, number_of_unique_src_ips_baseline,
    unique_ip_count_per_site, measurement_count_per_site,
    baseline_median_rtt, baseline_median_throughput, baseline_median_upload_throughput,
    baseline_median_loss,
    anomaly_ratio_rtt, anomaly_rtt_count,
    anomaly_ratio_throughput, anomaly_throughput_count,
    anomaly_ratio_upload_throughput, anomaly_upload_throughput_count,
    anomaly_loss_ratio,
    difference_latency, difference_throughput, difference_upload_throughput,
    wasserstein_throughput_result, wasserstein_upload_throughput_result,
    mann_whitney_latency, mann_whitney_throughput, mann_whitney_upload_throughput,
    t_test_latency,
    ip_version
  FROM FinalNodeExtraction
  WHERE ttl_rn = 1

  UNION ALL

  SELECT
    FinalNodeExtraction.id,
    src, dst, src_city, src_asn, dst_site, dst_city, dst_asn, dst_country,
    src_country, src_asn_name, client_name, src_lat, src_state, src_lon, dst_lat, dst_lon,
    is_consistent, window_start, total_windows, ndt_rtt, ndt_throughput, ndt_loss_rate, traceroute_rtt,
    detection_granularity, src_group_label, client_geo_source,
    start_sec,
    CAST(dst AS STRING)          AS addr,
    CAST(NULL AS STRING)         AS rdns_name,
    traceroute_rtt               AS rtts,
    mt.max_probe_ttl + 1         AS probe_ttl,
    number_of_measurements_baseline, number_of_unique_src_ips_baseline,
    unique_ip_count_per_site, measurement_count_per_site,
    baseline_median_rtt, baseline_median_throughput, baseline_median_upload_throughput,
    baseline_median_loss,
    anomaly_ratio_rtt, anomaly_rtt_count,
    anomaly_ratio_throughput, anomaly_throughput_count,
    anomaly_ratio_upload_throughput, anomaly_upload_throughput_count,
    anomaly_loss_ratio,
    difference_latency, difference_throughput, difference_upload_throughput,
    wasserstein_throughput_result, wasserstein_upload_throughput_result,
    mann_whitney_latency, mann_whitney_throughput, mann_whitney_upload_throughput,
    t_test_latency,
    ip_version
  FROM FinalNodeExtraction
  LEFT JOIN MaxTTLPerID mt USING(id)
  WHERE add_dst = TRUE
    AND NOT dst_is_hop  -- already on the path; see FinalNodeExtraction
),

SequenceGenerator AS (
  SELECT
    ne.id, ne.src, ne.dst,
    ANY_VALUE(ne.window_start) AS window_start,
    ANY_VALUE(ne.ndt_rtt) AS ndt_rtt,
    ANY_VALUE(ne.ndt_throughput) AS ndt_throughput,
    ANY_VALUE(ne.ndt_loss_rate) AS ndt_loss_rate,
    ANY_VALUE(ne.traceroute_rtt) AS traceroute_rtt,
    ANY_VALUE(ne.total_windows) AS total_windows,
    MAX(ne.is_consistent) AS is_consistent,
    ANY_VALUE(ne.src_city) AS src_city,
    ANY_VALUE(ne.src_asn) AS src_asn,
    ANY_VALUE(ne.dst_site) AS dst_site,
    ANY_VALUE(ne.dst_city) AS dst_city,
    ANY_VALUE(ne.dst_asn) AS dst_asn,
    ANY_VALUE(ne.dst_country) AS dst_country,
    ANY_VALUE(ne.src_lat) AS src_lat,
    ANY_VALUE(ne.src_state) AS src_state,
    ANY_VALUE(ne.src_lon) AS src_lon,
    ANY_VALUE(ne.dst_lat) AS dst_lat,
    ANY_VALUE(ne.dst_lon) AS dst_lon,
    ANY_VALUE(ne.src_country) AS src_country,
    ANY_VALUE(ne.src_asn_name) AS src_asn_name,
    ANY_VALUE(ne.client_name) AS client_name,
    ANY_VALUE(ne.start_sec) AS start_sec,
    ne.ip_version AS ip_version,
    ANY_VALUE(ne.detection_granularity) AS detection_granularity,
    ANY_VALUE(ne.src_group_label)       AS src_group_label,
    ANY_VALUE(ne.client_geo_source)     AS client_geo_source,
    GENERATE_ARRAY(1, MAX(ne.probe_ttl)) AS ttl_sequence,
    ANY_VALUE(ne.number_of_measurements_baseline)   AS number_of_measurements_baseline,
    ANY_VALUE(ne.number_of_unique_src_ips_baseline) AS number_of_unique_src_ips_baseline,
    ANY_VALUE(ne.unique_ip_count_per_site)          AS unique_ip_count_per_site,
    ANY_VALUE(ne.measurement_count_per_site)        AS measurement_count_per_site,
    ANY_VALUE(ne.baseline_median_rtt)               AS baseline_median_rtt,
    ANY_VALUE(ne.baseline_median_throughput)         AS baseline_median_throughput,
    ANY_VALUE(ne.baseline_median_upload_throughput)  AS baseline_median_upload_throughput,
    ANY_VALUE(ne.baseline_median_loss)              AS baseline_median_loss,
    ANY_VALUE(ne.anomaly_ratio_rtt)                 AS anomaly_ratio_rtt,
    ANY_VALUE(ne.anomaly_rtt_count)                 AS anomaly_rtt_count,
    ANY_VALUE(ne.anomaly_ratio_throughput)           AS anomaly_ratio_throughput,
    ANY_VALUE(ne.anomaly_throughput_count)           AS anomaly_throughput_count,
    ANY_VALUE(ne.anomaly_ratio_upload_throughput)    AS anomaly_ratio_upload_throughput,
    ANY_VALUE(ne.anomaly_upload_throughput_count)    AS anomaly_upload_throughput_count,
    ANY_VALUE(ne.anomaly_loss_ratio)                AS anomaly_loss_ratio,
    ANY_VALUE(ne.difference_latency)                AS difference_latency,
    ANY_VALUE(ne.difference_throughput)              AS difference_throughput,
    ANY_VALUE(ne.difference_upload_throughput)       AS difference_upload_throughput,
    ANY_VALUE(ne.wasserstein_throughput_result)      AS wasserstein_throughput_result,
    ANY_VALUE(ne.wasserstein_upload_throughput_result) AS wasserstein_upload_throughput_result,
    ANY_VALUE(ne.mann_whitney_latency)              AS mann_whitney_latency,
    ANY_VALUE(ne.mann_whitney_throughput)            AS mann_whitney_throughput,
    ANY_VALUE(ne.mann_whitney_upload_throughput)     AS mann_whitney_upload_throughput,
    ANY_VALUE(ne.t_test_latency)                    AS t_test_latency
  FROM NodeExtraction ne
  GROUP BY ne.id, ne.src, ne.dst, ne.ip_version
),

FlattenedSequence AS (
  SELECT
    sg.* EXCEPT(ttl_sequence),
    ttl
  FROM SequenceGenerator sg
  CROSS JOIN UNNEST(sg.ttl_sequence) AS ttl
  ORDER BY ttl
),

NodeExtractionOnePerTTL AS (
  SELECT * EXCEPT(_dedup)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY id, src, dst, probe_ttl
        ORDER BY
          CASE WHEN addr = '*' THEN 1 ELSE 0 END,
          rtts ASC,
          addr ASC
      ) AS _dedup
    FROM NodeExtraction
  )
  WHERE _dedup = 1
),

ExpandedRows AS (
  SELECT
    fs.*,

    CASE
      WHEN fs.ttl = 1 THEN fs.src
      ELSE COALESCE(n.addr, '*')
    END AS hop_addr,

    CASE
      WHEN fs.ttl = 1 THEN fs.dst_site
      ELSE COALESCE(n.rdns_name, '*')
    END AS hop_rdns_name,

    -- -1 marks an unanswered hop ('*'). A hop that answered but has no recorded
    -- RTT of its own (the first router: TTL-1 probes are not stored) stays NULL;
    -- -1 there would read as "faster than light" to step 04's distance check.
    CASE
      WHEN fs.ttl = 1 THEN 0.0
      WHEN n.addr IS NULL OR n.addr = '*' THEN -1
      ELSE n.rtts
    END AS hop_rtts

  FROM FlattenedSequence fs
  LEFT JOIN NodeExtractionOnePerTTL n
    ON  fs.id  = n.id
    AND fs.src = n.src
    AND fs.dst = n.dst
    AND fs.ttl = n.probe_ttl
),

ExpandedRowsDeduped AS (
  SELECT * EXCEPT(_rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY id, src, dst, ttl
        ORDER BY
          CASE WHEN hop_addr = '*' THEN 1 ELSE 0 END,
          hop_rtts ASC,
          hop_addr ASC
      ) AS _rn
    FROM ExpandedRows
  )
  WHERE _rn = 1
),

ExpandedNodeDetails AS (
  SELECT
    er.id,
    er.dst,
    er.src,

    ANY_VALUE(er.ndt_rtt)         AS ndt_rtt,
    ANY_VALUE(er.window_start)    AS window_start,
    ANY_VALUE(er.ndt_throughput)  AS ndt_throughput,
    ANY_VALUE(er.ndt_loss_rate)  AS ndt_loss_rate,
    ANY_VALUE(er.traceroute_rtt)  AS traceroute_rtt,
    ANY_VALUE(er.total_windows)   AS total_windows,
    ANY_VALUE(er.detection_granularity) AS detection_granularity,
    ANY_VALUE(er.src_group_label)       AS src_group_label,
    ANY_VALUE(er.client_geo_source)     AS client_geo_source,
    ANY_VALUE(er.is_consistent)   AS is_consistent,

    ANY_VALUE(er.src_city)        AS src_city,
    ANY_VALUE(er.src_lat)         AS src_lat,
    ANY_VALUE(er.src_state)       AS src_state,
    ANY_VALUE(er.src_lon)         AS src_lon,
    ANY_VALUE(er.src_asn)         AS src_asn,

    ANY_VALUE(er.dst_lat)         AS dst_lat,
    ANY_VALUE(er.dst_lon)         AS dst_lon,
    ANY_VALUE(er.dst_site)        AS dst_site,
    ANY_VALUE(er.dst_city)        AS dst_city,
    ANY_VALUE(er.dst_asn)         AS dst_asn,
    ANY_VALUE(er.dst_country)     AS dst_country,

    ANY_VALUE(er.src_country)     AS src_country,
    ANY_VALUE(er.src_asn_name)    AS src_asn_name,
    ANY_VALUE(er.client_name)     AS client_name,
    ANY_VALUE(er.start_sec)       AS start_sec,
    er.ip_version,

    ANY_VALUE(er.baseline_median_rtt)              AS baseline_median_rtt,
    ANY_VALUE(er.baseline_median_throughput)        AS baseline_median_throughput,
    ANY_VALUE(er.baseline_median_upload_throughput) AS baseline_median_upload_throughput,
    ANY_VALUE(er.baseline_median_loss)             AS baseline_median_loss,

    ANY_VALUE(er.anomaly_ratio_rtt)                AS anomaly_ratio_rtt,
    ANY_VALUE(er.anomaly_rtt_count)                AS anomaly_rtt_count,
    ANY_VALUE(er.anomaly_ratio_throughput)          AS anomaly_ratio_throughput,
    ANY_VALUE(er.anomaly_throughput_count)          AS anomaly_throughput_count,
    ANY_VALUE(er.anomaly_ratio_upload_throughput)   AS anomaly_ratio_upload_throughput,
    ANY_VALUE(er.anomaly_upload_throughput_count)   AS anomaly_upload_throughput_count,
    ANY_VALUE(er.anomaly_loss_ratio)               AS anomaly_loss_ratio,
    ANY_VALUE(er.difference_latency)               AS difference_latency,
    ANY_VALUE(er.difference_throughput)             AS difference_throughput,
    ANY_VALUE(er.difference_upload_throughput)      AS difference_upload_throughput,
    ANY_VALUE(er.wasserstein_throughput_result)     AS wasserstein_throughput_result,
    ANY_VALUE(er.wasserstein_upload_throughput_result) AS wasserstein_upload_throughput_result,
    ANY_VALUE(er.mann_whitney_latency)             AS mann_whitney_latency,
    ANY_VALUE(er.mann_whitney_throughput)           AS mann_whitney_throughput,
    ANY_VALUE(er.mann_whitney_upload_throughput)    AS mann_whitney_upload_throughput,
    ANY_VALUE(er.t_test_latency)                   AS t_test_latency,

    ARRAY_AGG(
      STRUCT(
        er.ttl AS ttl,
        er.hop_addr AS addr,
        er.hop_rdns_name AS rdns_name,
        er.hop_rtts AS rtts
      )
      ORDER BY er.ttl
    ) AS expanded_details,

    ARRAY_AGG(er.hop_addr ORDER BY er.ttl DESC LIMIT 1)[SAFE_OFFSET(0)] = er.dst AS reach_dest,

    AVG(er.number_of_measurements_baseline)   AS number_of_measurements_baseline,
    AVG(er.number_of_unique_src_ips_baseline) AS number_of_unique_src_ips_baseline,
    AVG(er.unique_ip_count_per_site)          AS unique_ip_count_per_site,
    AVG(er.measurement_count_per_site)        AS measurement_count_per_site

  FROM ExpandedRowsDeduped er
  GROUP BY er.id, er.dst, er.src, er.ip_version
),

AggregatedData AS (
  SELECT
    id,
    src  AS dst,
    dst  AS src,
    ndt_rtt,
    ndt_throughput,
    ndt_loss_rate,
    traceroute_rtt,
    total_windows,
    detection_granularity,
    src_group_label,
    client_geo_source,
    is_consistent,
    src_city,
    src_lat,
    src_state,
    src_lon,
    dst_lat,
    dst_lon,
    src_asn,
    dst_site,
    dst_city,
    dst_asn,
    dst_country,
    src_country,
    src_asn_name,
    client_name,
    start_sec AS start,
    window_start,
    reach_dest,
    expanded_details AS node_details,
    number_of_measurements_baseline,
    number_of_unique_src_ips_baseline,
    unique_ip_count_per_site,
    measurement_count_per_site,
    baseline_median_rtt,
    baseline_median_throughput,
    baseline_median_upload_throughput,
    baseline_median_loss,
    ip_version,
    anomaly_ratio_rtt,
    anomaly_rtt_count,
    anomaly_ratio_throughput,
    anomaly_throughput_count,
    anomaly_ratio_upload_throughput,
    anomaly_upload_throughput_count,
    anomaly_loss_ratio,
    difference_latency,
    difference_throughput,
    difference_upload_throughput,
    wasserstein_throughput_result,
    wasserstein_upload_throughput_result,
    mann_whitney_latency,
    mann_whitney_throughput,
    mann_whitney_upload_throughput,
    t_test_latency,
    (baseline_median_upload_throughput + difference_upload_throughput) AS median_upload_throughput
  FROM ExpandedNodeDetails
),

--------------------------------------------------------------------------------
-- 2) Match the reverse-path data (revtr)
--------------------------------------------------------------------------------
-- A test is two ndt7 connections (download, upload) with different ids, and
-- revTr can be keyed by either. Joining on the download id alone discarded every
-- revTr keyed by the upload: on 2026-10-06, 194,746 tests had a revTr only on
-- their upload connection (464,780 had one on the download). Each test's
-- connections are unpivoted so revtr1 is scanned once.
TestConnections AS (
  SELECT DISTINCT id AS test_id, id AS connection_id, 0 AS connection_pref
  FROM MeasurementsWithGroup
  UNION ALL
  SELECT DISTINCT id, upload_id, 1
  FROM MeasurementsWithGroup
  WHERE upload_id IS NOT NULL
),

matched_reverse_path_on_date AS (
  SELECT
    c.test_id,
    c.connection_pref,
    t.*
  FROM `measurement-lab.revtr_raw.revtr1` t
  JOIN TestConnections c
    ON t.raw.uuid = c.connection_id
  WHERE t.date BETWEEN '${ONE_WEEK_EARLIER}' AND '${DAY}'
),

filtered_reverse_path_on_date AS (
  -- One revTr per test: a complete one first, then the download connection's
  -- (so tests that already had a reaching revTr keep the same one), then the
  -- most recent. Which connection a revTr came from is recoverable by joining
  -- revtr_id back to revtr1 and comparing raw.uuid with id.
  SELECT *
  FROM matched_reverse_path_on_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY test_id
    ORDER BY
      CASE WHEN raw.stop_reason = 'REACHES' THEN 1 ELSE 2 END,
      connection_pref,
      raw.date DESC,
      raw.id  -- total order: same stop reason, connection and date otherwise tie
  ) = 1
),

WithReversePathData AS (
  SELECT
    ag.*,
    t2.raw.revtr_hops AS reverse_node_details,
    t2.raw.label      AS revtr_system_label,
    t2.raw.stop_reason AS revtr_stop_reason,
    t2.raw.fail_reason AS revtr_fail_reason,
    t2.raw.is_try_from_destination_AS,
    t2.raw.id AS revtr_id
  FROM filtered_reverse_path_on_date t2
  FULL OUTER JOIN AggregatedData ag
    ON t2.test_id = ag.id
),

--------------------------------------------------------------------------------
-- 3) City-level percentile summary, then final join
--------------------------------------------------------------------------------
-- Exact percentiles. The approximate quantile aggregate is not reproducible across
-- runs (step 02 dropped it for the same reason). PERCENTILE_DISC keeps the old
-- meaning: an actual sample value at that rank.
CitySamples AS (
  SELECT
    ndt.detection_src_city AS src_city,
    ndt.client.Network.ASNumber AS src_asn,
    ndt.server.Site AS dst_site,
    ndt.ip_version,
    ARRAY_AGG(ndt.download_min_rtt IGNORE NULLS ORDER BY ndt.download_min_rtt) AS rtts,
    ARRAY_AGG(ndt.download_throughput_mbps IGNORE NULLS ORDER BY ndt.download_throughput_mbps)
      AS throughputs
  FROM MeasurementsWithGroup ndt
  WHERE ndt.partition_date = '${DAY}'
  GROUP BY src_city, ndt.client.Network.ASNumber, ndt.server.Site, ndt.ip_version
),

CityServerLatencyThroughputSummary AS (
  SELECT
    src_city,
    src_asn,
    dst_site,
    ip_version,
    (SELECT PERCENTILE_DISC(x, 0.01) OVER() FROM UNNEST(rtts) x LIMIT 1) AS oneth_percentile_rtt,
    (SELECT PERCENTILE_DISC(x, 0.10) OVER() FROM UNNEST(rtts) x LIMIT 1) AS tenth_percentile_rtt,
    (SELECT PERCENTILE_DISC(x, 0.50) OVER() FROM UNNEST(rtts) x LIMIT 1) AS median_rtt,
    (SELECT PERCENTILE_DISC(x, 0.90) OVER() FROM UNNEST(rtts) x LIMIT 1) AS ninetyth_percentile_rtt,
    (SELECT PERCENTILE_DISC(x, 0.50) OVER() FROM UNNEST(throughputs) x LIMIT 1)
      AS median_throughput,
    (SELECT PERCENTILE_DISC(x, 0.90) OVER() FROM UNNEST(throughputs) x LIMIT 1)
      AS ninetyth_percentile_throughput
  FROM CitySamples
),

with_median_lat_throughput AS (
  SELECT
    aga.* EXCEPT(
      anomaly_ratio_rtt, anomaly_rtt_count,
      anomaly_ratio_throughput, anomaly_throughput_count,
      anomaly_ratio_upload_throughput, anomaly_upload_throughput_count,
      anomaly_loss_ratio,
      difference_latency, difference_throughput, difference_upload_throughput,
      wasserstein_throughput_result, wasserstein_upload_throughput_result,
      mann_whitney_latency, mann_whitney_throughput, mann_whitney_upload_throughput,
      t_test_latency,
      median_upload_throughput,
      -- re-emitted last to keep the final projection easy to compare with the
      -- explicit INSERT list and the appended ALTER TABLE positions
      detection_granularity, src_group_label, client_geo_source
    ),

    cslts.median_rtt                       AS city_median_rtt,
    cslts.ninetyth_percentile_rtt          AS city_ninetyth_percentile_rtt,
    cslts.oneth_percentile_rtt             AS city_oneth_percentile_rtt,
    cslts.tenth_percentile_rtt             AS city_tenth_percentile_rtt,
    cslts.median_throughput                AS city_median_throughput,
    cslts.ninetyth_percentile_throughput   AS city_ninetyth_percentile_throughput,

    CAST('${DAY}' AS DATE) AS partition_date,

    -- Anomaly fields last (matches ALTER TABLE append order in transient_events_union)
    aga.anomaly_ratio_rtt,
    aga.anomaly_rtt_count,
    aga.anomaly_ratio_throughput,
    aga.anomaly_throughput_count,
    aga.anomaly_ratio_upload_throughput,
    aga.anomaly_upload_throughput_count,
    aga.anomaly_loss_ratio,
    aga.difference_latency,
    aga.difference_throughput,
    aga.difference_upload_throughput,
    aga.wasserstein_throughput_result,
    aga.wasserstein_upload_throughput_result,
    aga.mann_whitney_latency,
    aga.mann_whitney_throughput,
    aga.mann_whitney_upload_throughput,
    aga.t_test_latency,
    aga.median_upload_throughput,

    -- Detection-group identity, last (appended columns)
    aga.detection_granularity,
    aga.src_group_label,
    aga.client_geo_source
  FROM WithReversePathData aga
  INNER JOIN CityServerLatencyThroughputSummary cslts
    ON cslts.src_city = aga.src_city
    AND cslts.src_asn = aga.src_asn
    AND cslts.dst_site = aga.dst_site
    AND cslts.ip_version = aga.ip_version
)

SELECT * FROM with_median_lat_throughput;
