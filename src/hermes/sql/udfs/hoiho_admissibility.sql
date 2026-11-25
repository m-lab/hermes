-- When may step 04 use a HOIHO (hostname-rule) location instead of IPinfo's?
--
-- Calibrated on the 2026-11-21 HOIHO A/B in hermes_staging (2026-11-24). HOIHO
-- fails by (a) ambiguous codes -- "bgl" read as Baglung NP (meant Bangalore),
-- "slc" as Shelter Cove, MTS "nur"/"tum" as Nuristan AF / Tumba SE -- and (b)
-- hostnames naming the place an address is SOLD in, not where the router is:
-- VSNL "<ip>.static-mumbai" customer pools answered 0.9 ms from Chennai. IPinfo
-- fails by reporting the registrant for backbone ports (Level3 Milan -> Djibouti,
-- Cogent Buenos Aires -> Washington). Hence: HOIHO is used when it is the only
-- location, agrees with IPinfo, or is corroborated by a PeeringDB facility of the
-- hop's own AS in HOIHO's city. RTT feasibility is checked separately, in
-- _hop_geo_validation, on whichever candidate this admits.

-- Accent-, case- and punctuation-insensitive form of a place name.
CREATE TEMP FUNCTION hoiho_norm(s STRING) AS (
  TRIM(REGEXP_REPLACE(
    REGEXP_REPLACE(LOWER(NORMALIZE(s, NFD)), r'\p{M}', ''),
    r'[^a-z0-9]+', ' '))
);

-- A hostname that names an address allocation rather than a router: it embeds
-- the IPv4 address (either byte order, any separator) or carries a customer-pool
-- token. "bb" (backbone) and "cust" (customer-facing interface at a PoP) are
-- deliberately NOT pool tokens.
CREATE TEMP FUNCTION hoiho_pool_hostname(hostname STRING, addr STRING) AS (
  hostname IS NOT NULL AND (
    REGEXP_CONTAINS(LOWER(hostname),
      r'(^|[.\-_])(static|dynamic|dyn|dhcp|pool|dsl|adsl|vdsl|ppp|pppoe)([.\-_0-9]|$)')
    OR (
      REGEXP_CONTAINS(IFNULL(addr, ''), r'^\d+\.\d+\.\d+\.\d+$')
      AND (
        REGEXP_CONTAINS(LOWER(hostname),
          CONCAT(r'(^|\D)', ARRAY_TO_STRING(SPLIT(addr, '.'), r'[.\-_]'), r'(\D|$)'))
        OR REGEXP_CONTAINS(LOWER(hostname),
          CONCAT(r'(^|\D)', ARRAY_TO_STRING(ARRAY_REVERSE(SPLIT(addr, '.')), r'[.\-_]'), r'(\D|$)'))
      )
    )
  )
);

-- Does the AS have a PeeringDB facility in HOIHO's "City-CC" place? Same country,
-- and one normalized city name equal to or a word-prefix of the other ("Milan" /
-- "Milano", "Frankfurt" / "Frankfurt am Main"), at least 4 characters.
CREATE TEMP FUNCTION hoiho_facility_match(place STRING, facilities ARRAY<STRING>) AS (
  IFNULL((
    SELECT LOGICAL_OR(
      UPPER(REGEXP_EXTRACT(f, r'-([A-Za-z]{2})$')) = UPPER(REGEXP_EXTRACT(place, r'-([A-Za-z]{2})$'))
      AND LENGTH(hoiho_norm(REGEXP_REPLACE(place, r'-[A-Za-z]{2}$', ''))) >= 4
      AND LENGTH(hoiho_norm(REGEXP_REPLACE(f, r'-[A-Za-z]{2}$', ''))) >= 4
      AND (
        STARTS_WITH(
          hoiho_norm(REGEXP_REPLACE(f, r'-[A-Za-z]{2}$', '')),
          hoiho_norm(REGEXP_REPLACE(place, r'-[A-Za-z]{2}$', '')))
        OR STARTS_WITH(
          hoiho_norm(REGEXP_REPLACE(place, r'-[A-Za-z]{2}$', '')),
          hoiho_norm(REGEXP_REPLACE(f, r'-[A-Za-z]{2}$', '')))
      )
    )
    FROM UNNEST(facilities) AS f
    WHERE REGEXP_CONTAINS(IFNULL(place, ''), r'-[A-Za-z]{2}$')
  ), FALSE)
);

-- Step 04's refutation rule, shared by _hop_geo_validation and the facility branch
-- below: a location is impossible if, from an M-Lab site, the address's 10th-
-- percentile RTT cannot cover the distance and back at 200 km/ms, allowing 100 km
-- for metro-centroid and site-metadata error. FALSE when anything is missing.
CREATE TEMP FUNCTION rtt_refutes_location(
  rtt_q10 FLOAT64, lat FLOAT64, lon FLOAT64, site_lat FLOAT64, site_lon FLOAT64
) AS (
  IFNULL(
    rtt_q10 * 100
      < ST_DISTANCE(ST_GEOGPOINT(lon, lat), ST_GEOGPOINT(site_lon, site_lat)) / 1000 - 100,
    FALSE)
);

-- Why a coordinate-bearing HOIHO row may be used, or NULL if it may not.
-- Agreement is 100 km: the slack step 04 already allows for metro centroids.
-- A facility match is the weakest basis -- large backbones list facilities almost
-- everywhere -- so it must also survive the RTT check (rtt_refuted: refuted from
-- any site). _hop_geo_validation exempts addresses seen at mutually-inconsistent
-- sites, and through that gap a facility-admitted he.net "fmt1" router 34.5 ms
-- from Stavanger was placed in San Mateo (2026-11-21 arm C).
CREATE TEMP FUNCTION hoiho_basis(
  hoiho_lat FLOAT64, hoiho_lon FLOAT64, ip_lat FLOAT64, ip_lon FLOAT64,
  pool BOOL, facility BOOL, rtt_refuted BOOL
) AS (
  CASE
    WHEN hoiho_lat IS NULL OR hoiho_lon IS NULL OR IFNULL(pool, FALSE) THEN NULL
    WHEN ip_lat IS NULL OR ip_lon IS NULL THEN 'hoiho_only'
    WHEN ST_DISTANCE(ST_GEOGPOINT(hoiho_lon, hoiho_lat), ST_GEOGPOINT(ip_lon, ip_lat)) <= 100000
      THEN 'hoiho_agree'
    WHEN IFNULL(facility, FALSE) AND NOT IFNULL(rtt_refuted, FALSE) THEN 'hoiho_facility'
    ELSE NULL
  END
);
