# Step 0: geolocate source IPs with IPInfo, reusing the existing enrichment

**Status:** scoping. Nothing implemented.
**Written:** 2026-08-11. Figures measured on 2026-08-07 / the VM unless stated.
**Decision taken:** reuse the existing enrichment code as far as possible, and run
it for source IPs earlier in the run, before anomaly detection.

---

## 1. Why

MaxMind emits a **single designated point per country** when it cannot resolve a
client below country level — sometimes the geographic centre (India: 21.997,
79.001), often the capital (Tokyo, London, Paris, Jakarta, Madrid). On 2026-08-07
that was **547,105 measurements on 864 coordinates across 185 countries**.

City-mode grouping discards them (the label `CONCAT(NULL, …)` is NULL). Metro-mode
grouping assigns them to whichever metro contains the point:

| metro | measurements | synthetic | ASNs |
|---|---|---|---|
| `Jabalpur-Madhya Pradesh-IN` | 153,690 | **97.8 %** | 1,451 |
| `Paris-Paris-FR` | 33,521 | **55.9 %** | 247 |
| `Tokyo-Tokyo-JP` | 73,540 | **53.3 %** | 280 |
| `London-Westminster-GB` | 81,343 | 25.7 % | 438 |

IPInfo resolves **100 %** of those clients (136,210 of 136,210 IPv4 tier-2 IPs)
into **8,066 distinct cities** at a median /24.5.

---

## 2. What already exists — reuse target

`IPInfoEnricher` does **not** query BigQuery. It downloads IPInfo's
`standard_location.mmdb` with `IPINFO_TOKEN`, caches it as
`$HERMES_CACHE_DIR/ipinfo_<date>.snapshot`, and looks up via
`maxminddb.open_database()` → `reader.get(ip)`.

That is better than any BigQuery design for this job:

* an MMDB is a radix trie, so longest-prefix matching is exact and free — no
  candidate-prefix join, no prefix table;
* **same-day fresh.** The VM holds 42 snapshots, `2026-06-12 → 2026-08-11`, and
  today's nightly downloaded `ipinfo_2026-08-11.snapshot` at 15:12:42;
* IPv6 handled by the sibling `enricher_ipv6.py`;
* **already proven at this scale** — the same path resolves **3,806,808 hop
  IPs/day** through `_get_geolocation_data` on a `ThreadPoolExecutor`.

> A prefix-keyed table was considered and rejected. IPInfo resolves to /32
> (7,863,732 leaves at /32, 6,362,855 at /31), and **2,285,725 of 2,308,678 /24
> blocks that have finer leaves span multiple cities** — one sampled /24 held 256
> leaves in 255 different cities. Geolocation is a property of the leaf, and
> leaves reach individual addresses, so prefix-level storage loses real
> resolution. Per-IP via MMDB is both more accurate and simpler.

> Note `hermes.ipinfo_location` (BigQuery, 112 GiB) is a *different* artifact:
> newest `snapshot_date` **2026-06-11**, last modified 2026-07-27. It is two
> months stale while the live path is same-day. Worth confirming nothing reads it,
> then refreshing or dropping it — it is a trap for anyone who assumes it is
> current.

---

## 3. Where step 0 goes

Client IPs come from `merged_download_upload`, which step 01 produces. So the new
phase sits **between 01 and 02**, not before 01 — collecting client IPs from the
raw NDT tables instead would duplicate step 01's scan (**122.66 GiB**).

```
  today            proposed
  ─────            ────────
  Phase A: 01,02,03    Phase A1: 01
  Phase B: enrichment  Phase A0: enrichment(source="clients")   ← new
  Phase C: 04, 05      Phase A2: 02, 03
  Phase D: tomography  Phase B:  enrichment(source="topology")
  Phase E: 07          Phase C/D/E unchanged
```

Phase A currently runs 01–03 together per date; it has to split so client
enrichment lands in the middle.

---

## 4. Code changes

### 4a. `enrichment/main.py` — one parameter

`process_geolocation`'s candidate query is already the right shape:

```
latest_geoloc   (staleness join)      reused unchanged
unique_ips      (source-specific)  ←  the ONLY part that differs
public_ips      (RFC1918 filter)      reused unchanged
final SELECT    (staleness filter)    reused unchanged
```

Add `source: Literal["topology","clients"] = "topology"` and swap the `unique_ips`
CTE:

```sql
-- source="clients"
unique_ips AS (
  SELECT DISTINCT client_ip AS addr
  FROM `mlab-collaboration.{DS}.merged_download_upload`
  WHERE partition_date BETWEEN '{start_date}' AND '{date}'
    AND client_ip IS NOT NULL
    AND NOT REGEXP_CONTAINS(client_ip, ':')      -- IPv6 via the ipv6 enricher
)
```

Everything downstream — MMDB lookup, `_get_geolocation_data`,
`_upload_geolocation_data`, `unified_ip_to_geoloc`, the 30-day staleness rule,
`_update_metro_for_geolocation_table()` — is reused verbatim.

### 4b. `pipeline/union.py` — split Phase A, add the call

`run_dates` gains an `A0` step calling
`enricher.process_geolocation(date, lookback_days=0, source="clients")` between
the 01 and 02/03 groups.

### 4c. `02` and `03` — replace `client.Geo.*`

The whole MaxMind surface is here: `CountryCode` (30 refs), `Subdivision1ISOCode`
(19), `Latitude`/`Longitude` (16 each), `City` (9), across 02 (24) and 03 (18).
Join `unified_ip_to_geoloc` on `client_ip` and read `*_ip_info` instead.

Bonus: `_source_metro_lookup` disappears from both files — enrichment already
resolves `metro`, so 02/03 stop doing spatial joins entirely.

### 4d. Not changed

* **04** already reads `ip_geo.*` from IPInfo.
* **`server.Geo.*`** is *not* MaxMind: 215 sites produce exactly 215 site+coordinate
  pairs across 68 machines — that is M-Lab's own site registry. Replacing it with
  IPInfo would substitute an inferred value for a known one.

---

## 5. The actual blocker: table growth, not lookup

```
unified_ip_to_geoloc   99,561,656 rows   12.8 GiB   UNPARTITIONED, UNCLUSTERED
enrich_ip_geoloc_add_metro.sql:  CREATE OR REPLACE TABLE … AS SELECT … FROM itself
```

Every enrichment run **rewrites the entire table** through a spatial join. That is
tolerable at 99.5 M rows. Client IPs break it:

```
distinct client IPs / day     2,564,893
seen the previous day too       357,578   (13.9 % carry-over)
new per day                   2,207,315   →  ~806 M rows/year
```

86 % of client IPs are never seen again, so under the current 30-day staleness
rule the table would grow by ~806 M rows/year **and be fully rewritten nightly**.
Within a year the nightly metro update goes from 12.8 GiB to >100 GiB of rewrite.

This is the engineering work. Options:

1. **Partition `unified_ip_to_geoloc` by `partition_date`** and make the metro
   update touch only new partitions instead of `CREATE OR REPLACE`. Fixes the
   rewrite for hops too, and is worth doing regardless.
2. **Separate client geolocation into its own partitioned table** with short
   retention (say 30–90 days), leaving the topology table alone. Cleanest
   isolation; costs a second table and a second join in 02/03.
3. **Do not persist client IPs at all** — resolve them in-run and write only the
   metro onto the measurement rows. No growth, no per-client-IP storage (also the
   simplest answer to the privacy question), but loses reuse across dates and
   breaks the "reuse existing code" goal.

(1) + (2) together is my recommendation: partition the table regardless, and keep
clients separate so their retention is independent of topology.

---

## 6. Open questions

1. **Retention for client IPs.** 30-day staleness is right for infrastructure and
   wasteful for consumer IPs seen once. Needs an explicit policy.
2. **Privacy.** Persisting per-client-IP geolocation is a policy decision, not a
   technical one. Option 3 above avoids it entirely.
3. **The 1.4 % country disagreement.** 1,912 of 136,210 country-only clients got a
   *different country* from IPInfo. For a country-level MaxMind answer that is a
   conflict, not a refinement. Needs a rule: trust IPInfo, trust MaxMind, or drop.
4. **IPv6 clients.** 100 % of clients were IPv4 on the sampled day; confirm that
   holds generally before assuming the IPv4 path suffices.
5. **Comparability.** Changing client geolocation moves group membership for
   *every* measurement, not just the 547 k — a larger discontinuity than
   city→metro, on the same tables the dashboard reads. History stays
   MaxMind-derived unless reprocessed (04 alone is 324 GB/day).

---

## 7. Sequencing

1. **Now — unblock metro mode** by excluding MaxMind precision tiers 1–3 from
   metro grouping. One predicate; independent of all of the above.
2. **Partition `unified_ip_to_geoloc`** and stop the full-table rewrite. Useful on
   its own merits.
3. **Step 0 in shadow mode** — run client enrichment, do not consume it, and
   compare: agreement with MaxMind, distance between placements, how many groups
   change.
4. **Switch 02/03**, and drop the tier filter from (1) since IPInfo now places
   those clients properly.
5. **Decide backfill.**

---

## 8. Measured vs assumed

**Measured (2026-08-07 / VM, 2026-08-11):** MaxMind precision tiers (41,868 /
404,061 / 143,044 / 4,266,431 measurements); 864 fallback coordinates over 185
countries; contamination percentages above; IPInfo resolving 136,210/136,210
country-only client IPs into 8,066 cities at /24.5; 2,564,893 client IPs/day at
13.9 % carry-over; 3,806,808 forward-hop IPs/day; 215 site+coordinate pairs for
215 sites; 42 MMDB snapshots through today; `unified_ip_to_geoloc` unpartitioned
at 99,561,656 rows; the metro update being a full `CREATE OR REPLACE`.

**Assumed / unmeasured:** IPv6 client share over time; hop-IP coverage under
IPInfo (presumed fine, since the path already runs); how many *groups* change when
client geo switches; whether the 1.4 % country conflicts are IPInfo or MaxMind
errors; the cost of the client candidate-IP scan at 30-day lookback.
