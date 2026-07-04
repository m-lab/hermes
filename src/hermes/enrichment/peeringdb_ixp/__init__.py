"""
IXP Collector module for HERMES enrichment pipeline.

This module provides functionality to collect and process IXP data from PeeringDB
and integrate it into the HERMES enrichment pipeline.
"""

from .ixp_collector import IXPCollector, update_ixp_data

__all__ = ["IXPCollector", "update_ixp_data"]
