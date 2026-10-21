"""A test's client_name must be its own, never its group's.

Step 02 keeps MIN(client_name) per group, the alphabetically first name among the
group's tests. Step 03 used to copy that onto every matched test, and step 04 keeps
only client_name = 'giga-meter', so one 'bwp-client' or 'fing-mlab-ndt7' test in a
group dropped all of its Giga Meter tests from giga_meter_measurements, and a
group whose first name was 'giga-meter' pulled every other client into it.
"""

from hermes.sql import loader

PARAMS = {"DAY": "2026-09-10", "ONE_WEEK_EARLIER": "2026-09-03",
          "DETECTION_GRANULARITY": "metro"}


def _step03():
    return loader.load_query("03_build_transient_events_union.sql", PARAMS)


def test_both_trace_branches_keep_the_tests_own_client_name():
    sql = _step03()
    assert sql.count("ANY_VALUE(ndt.client_name) AS client_name") == 2


def test_no_branch_ever_falls_back_to_the_group_name():
    # A fallback would label a test that sent no name with the group's MIN,
    # which is how 7,550 nameless tests became 'giga-meter' on 2026-09-10.
    assert "a.client_name" not in _step03()
