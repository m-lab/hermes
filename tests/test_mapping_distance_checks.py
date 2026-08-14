"""Regression tests for endpoint roles in Step 04 distance/RTT calculations."""

from pathlib import Path

QUERY = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "hermes"
    / "sql"
    / "queries"
    / "04_mapping_union.sql"
)


def _forward_distance_checks() -> str:
    sql = QUERY.read_text()
    start = sql.index("\ndistance_rtt_checks AS (")
    end = sql.index("\ndistance_rtt_checks_with_metas_info AS (", start)
    return sql[start:end]


def test_forward_remaining_distance_uses_the_client_endpoint():
    checks = _forward_distance_checks()

    assert "ST_GEOGPOINT(src_lon, src_lat)) / 1000" in checks


def test_forward_fiber_rtt_closes_the_return_leg_to_the_server():
    checks = _forward_distance_checks()
    server_guard = (
        "cumulative_distance_km IS NULL OR latitude IS NULL OR longitude IS NULL "
        "OR dst_lat IS NULL OR dst_lon IS NULL"
    )

    assert checks.count(server_guard) == 2
    assert checks.count("ST_GEOGPOINT(dst_lon, dst_lat)) / (200 * 1000)") == 2
