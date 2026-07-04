from hermes.sql import paths

STEP = "create_temporal_path_verdicts.sql"


def test_ddl_has_verdict_columns():
    assert paths.query_path(STEP).is_file()
    ddl = paths.query_path(STEP).read_text()
    for col in (
        "partition_date",
        "src_dst_pair",
        "ip_version",
        "verdict",
        "change_dir",
        "div_forward",
        "div_reverse",
        "changed_segment",
        "congested_segment",
        "agrees_with_culprit",
    ):
        assert col in ddl
    assert "PARTITION BY partition_date" in ddl
