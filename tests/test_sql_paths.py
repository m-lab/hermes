from hermes.sql import paths


def test_sql_root_default_contains_queries():
    root = paths.sql_root()
    assert (root / "queries" / "01_merge_upload_download_union.sql").is_file()


def test_query_path_resolves():
    p = paths.query_path("02_detect_anomalies_union.sql")
    assert p.is_file()
    assert p.name == "02_detect_anomalies_union.sql"


def test_env_override(tmp_path, monkeypatch):
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "x.sql").write_text("SELECT 1")
    monkeypatch.setenv("HERMES_SQL_DIR", str(tmp_path))
    assert paths.query_path("x.sql").read_text() == "SELECT 1"


def test_metro_sql_packaged():
    # The enrichment metro step loads these via paths.query_path(); they must ship.
    for name in (
        "enrich_geolocation_add_metro.sql",
        "enrich_ip_geoloc_add_metro.sql",
    ):
        assert paths.query_path(name).is_file(), name


def test_new_pipeline_sql_files_packaged():
    from hermes.sql import paths

    for name in (
        "05_temporal_edge_prevalences_union.sql",
        "06_correlation_tomography_unexplained_hops_union.sql",
        "07_translating_to_public_format_union.sql",
    ):
        assert paths.query_path(name).is_file(), name
