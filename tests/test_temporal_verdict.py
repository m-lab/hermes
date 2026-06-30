from hermes.pipeline.temporal_verdict import classify, divergence, label_edges


def test_divergence_sums_positive_shift_onto_new_edges():
    u = {"a-b": 1.0, "b-c": 1.0}
    d = {"a-b": 1.0, "b-x": 1.0}  # b-c abandoned, b-x diverted onto
    assert divergence(u, d) == 1.0  # only the +1.0 on b-x counts


def test_label_edges():
    u = {"a-b": 1.0, "b-c": 1.0}
    d = {"a-b": 1.0, "b-x": 1.0}
    lab = label_edges(u, d, delta=0.5)
    assert lab["b-x"] == "diverted"
    assert lab["b-c"] == "abandoned"
    assert lab["a-b"] == "stable"


def test_classify_reroute_congestion_indeterminate():
    # high divergence -> reroute, on the direction with the larger divergence
    assert classify(0.8, 0.1, healthy_n=20, dayof_n=20) == ("reroute", "forward")
    assert classify(0.1, 0.7, healthy_n=20, dayof_n=20) == ("reroute", "reverse")
    # low divergence but enough data -> congestion in place
    assert classify(0.05, 0.0, healthy_n=20, dayof_n=20) == ("congestion_in_place", "forward")
    # thin support -> indeterminate
    assert classify(0.9, 0.9, healthy_n=2, dayof_n=20) == ("indeterminate", None)
    assert classify(0.9, 0.9, healthy_n=20, dayof_n=1) == ("indeterminate", None)
