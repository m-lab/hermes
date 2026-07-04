from hermes.sql import loader, paths

STEP = "05_temporal_edge_prevalences_union.sql"


def test_file_exists():
    assert paths.query_path(STEP).is_file()


def test_templates_and_selects_expected_columns():
    sql = loader.load_query(STEP, {"DAY": "2026-06-19"})
    assert "${DAY}" not in sql and "2026-06-19" in sql
    assert "events_with_as_and_geoloc" in sql
    for col in (
        "prev_u",
        "prev_d",
        "direction",
        "edge",
        "healthy_n",
        "dayof_n",
        "base_hop_rtt",
        "day_hop_rtt",
    ):
        assert col in sql
    # both path directions are covered
    assert "forward_updated_node_details" in sql and "reverse_updated_node_details" in sql
    # usual route is healthy (is_anomaly = FALSE), not "before"
    assert "is_anomaly" in sql
