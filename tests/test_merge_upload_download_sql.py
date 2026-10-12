from hermes.sql import loader

STEP = "01_merge_upload_download_union.sql"


def _sql() -> str:
    return loader.load_query(STEP, {"DAY": "2026-10-06"})


def test_uploads_and_downloads_share_the_ndt7_union_source():
    sql = _sql()
    # ndt_raw.ndt7 lacks every upload at the <metro><ASN> sites.
    assert "FROM `measurement-lab.ndt_raw.ndt7`" not in sql
    assert sql.count("FROM `measurement-lab.ndt.ndt7_union`") == 2


def test_keeps_fastest_upload_per_token():
    sql = _sql()
    # one upload per token: the fastest (was the slowest, a low bias)
    assert "ORDER BY upload_throughput_mbps DESC" in sql
    assert "GROUP BY date, access_token" in sql


def test_output_columns_unchanged():
    sql = _sql()
    for col in (
        "upload_throughput_mbps",
        "upload_min_rtt",
        "upload_loss_rate",
        "download_throughput_mbps",
        "partition_date",
    ):
        assert col in sql


def test_upload_id_is_the_last_column():
    """Step 01's INSERT is positional; add_upload_id_column.sql appends upload_id."""
    sql = _sql().rstrip().rstrip(";")
    final_select = sql[sql.rindex("SELECT\n  d.id,") :]
    assert (
        final_select.split("FROM Downloads d")[0].rstrip().endswith("u.selected_upload.upload_id")
    )
