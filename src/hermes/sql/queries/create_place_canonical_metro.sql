-- One-time bootstrap for the optional place→canonical-metro lookup used by the
-- temporal-v2 and path-local attribution queries (LEFT JOIN … COALESCE fallback).
-- Empty is safe: joins degrade to n.metro/n.place. Populate later if desired.
CREATE TABLE IF NOT EXISTS `mlab-collaboration.${DS}.place_canonical_metro`
(
  place STRING,
  canon_metro STRING
);
