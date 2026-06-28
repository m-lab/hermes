import pytest

from hermes.sql import loader


@pytest.fixture()
def sql_tree(tmp_path, monkeypatch):
    (tmp_path / "queries").mkdir()
    (tmp_path / "udfs").mkdir()
    (tmp_path / "udfs" / "has_loop.sql").write_text(
        "CREATE TEMP FUNCTION has_loop(xs ARRAY<STRING>) RETURNS BOOL AS (\n"
        "  ARRAY_LENGTH(xs) <> ARRAY_LENGTH(ARRAY(SELECT DISTINCT x FROM UNNEST(xs) x))\n"
        ");"
    )
    (tmp_path / "queries" / "q.sql").write_text(
        "-- @requires-udf: has_loop\nSELECT has_loop(${col}) FROM t;"
    )
    monkeypatch.setenv("HERMES_SQL_DIR", str(tmp_path))
    return tmp_path


def test_load_query_prepends_udf_and_substitutes(sql_tree):
    out = loader.load_query("q.sql", {"col": "path"})
    assert "CREATE TEMP FUNCTION has_loop" in out
    assert out.index("CREATE TEMP FUNCTION") < out.index("SELECT has_loop")
    assert "has_loop(path)" in out
    assert "${col}" not in out


def test_missing_udf_raises(sql_tree):
    (sql_tree / "queries" / "bad.sql").write_text("-- @requires-udf: nope\nSELECT 1;")
    with pytest.raises(FileNotFoundError):
        loader.load_query("bad.sql", {})


def test_no_directive_passthrough(sql_tree):
    (sql_tree / "queries" / "plain.sql").write_text("SELECT 1;")
    assert loader.load_query("plain.sql", {}).strip() == "SELECT 1;"
