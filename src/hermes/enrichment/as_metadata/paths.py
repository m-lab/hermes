"""Where the ``hermes.as_metadata`` input files live.

Two places computed these independently and both hardcoded one laptop's layout
(``~/Documents/GitHub/missing-peering-links``): ``ASMetadataEnricher`` when
reading them, and ``refresh_topology_tables`` when checking they exist. Same
failure as the IXP collectors -- the refresh could only ever run on that machine.

The directory layout is kept as-is rather than flattened, because the two
consumers agree on it and changing it would be churn for no gain. Only the base
moves, resolved as: explicit argument, then ``HERMES_AS_METADATA_DIR``, then the
historical default -- so existing laptop runs are unchanged.
"""

import os
from pathlib import Path

#: Historical location, kept for backwards compatibility.
DEFAULT_AS_METADATA_DIR = os.path.join(
    os.path.expanduser("~"), "Documents", "GitHub", "missing-peering-links"
)


def resolve_as_metadata_dir(base: str | None = None) -> Path:
    """Base directory holding the as_metadata inputs."""
    return Path(base or os.environ.get("HERMES_AS_METADATA_DIR", DEFAULT_AS_METADATA_DIR))


def as_metadata_input_paths(date: str, base: str | None = None) -> dict[str, Path]:
    """The three files ``hermes.as_metadata`` is built from, for `date`.

    Naming must match what ``ASMetadataEnricher.update_as_metadata`` reads; the
    CAIDA file is keyed by day and the two PeeringDB CSVs by month, which is why
    as_metadata belongs on a monthly cadence -- the PeeringDB inputs simply do
    not change within a month.

    Args:
        date: Target date, ``YYYY-MM-DD``.
        base: Override for the base directory.

    Returns:
        ``{"caida": ..., "footprint": ..., "as_type": ...}``.
    """
    root = resolve_as_metadata_dir(base)
    yyyymmdd = date.replace("-", "")
    year, month, _ = date.split("-")
    return {
        "caida": root / "data" / "BGP_data" / f"ASNS-{yyyymmdd}.json",
        "footprint": root
        / "scripts"
        / "data"
        / "PeeringDB"
        / f"AS_footprint_info_{year}-{month}.csv",
        "as_type": root / "scripts" / "data" / "PeeringDB" / f"AS_Type{year}-{month}.csv",
    }
