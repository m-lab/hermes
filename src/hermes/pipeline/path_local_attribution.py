"""Single-path (path-local) culprit attribution for anomalies the correlation
set-cover could not explain. Localizes to the first hop on a group's own path
where a per-hop latency flag fires."""

# Confirmed BigQuery flag string values (not booleans).
_FLAGGED = {
    "above_baseline_flag": "Above baseline",
    "increasing_latency_flag": "Increasing",
    "distance_rtt_check": "Above threshold",
}


def _flag_reason(hop: dict) -> str | None:
    if hop.get("above_baseline_flag") == _FLAGGED["above_baseline_flag"]:
        return "RTT above baseline at this hop"
    if hop.get("increasing_latency_flag") == _FLAGGED["increasing_latency_flag"]:
        return "increasing latency from this hop"
    if hop.get("distance_rtt_check") == _FLAGGED["distance_rtt_check"]:
        return "RTT exceeds distance-implied minimum at this hop"
    return None


def localize_on_path(hops: list[dict]) -> dict | None:
    """Return {'from_node','to_node','reason'} for the first flagged segment, else None."""
    ordered = sorted(hops, key=lambda h: h.get("ttl", 0))
    prev = None
    for hop in ordered:
        reason = _flag_reason(hop)
        if reason is not None:
            node = hop.get("asn_metro", "")
            if node.startswith("*"):
                return None
            return {
                "from_node": (prev or hop).get("asn_metro", node),
                "to_node": node,
                "reason": reason,
            }
        if not hop.get("asn_metro", "").startswith("*"):
            prev = hop
    return None
