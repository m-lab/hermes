"""
AS Metadata module for HERMES enrichment pipeline.

This module provides functionality to collect and process AS metadata from various sources
and integrate it into the HERMES enrichment pipeline.
"""

from .enricher import ASMetadataEnricher, update_as_metadata

__all__ = ["ASMetadataEnricher", "update_as_metadata"]
