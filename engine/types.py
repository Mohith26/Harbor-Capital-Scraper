"""Pipeline stage data contracts — typed dataclasses used by all engine stages."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd


@dataclass
class LoadedSegment:
    df: pd.DataFrame
    source_path: str
    sheet_name: Optional[str]
    segment_title: Optional[str]
    segment_index: int
    loader: str  # "xlsx" | "csv" | "pdf_vision"
    raw_headers: list[str]


@dataclass
class Fingerprint:
    """Content-addressable identifier for a (file_type, header-layout) pair.

    - raw_hash: order-sensitive hash, used for Tier 1 exact match.
    - header_set_hash: order-agnostic hash for debugging / inspection.
    - headers: original raw headers as strings, preserved for UI display.
    - normalized_headers: clean_header()-processed form, used for Jaccard.
    """
    raw_hash: str
    header_set_hash: str
    headers: list[str]
    normalized_headers: list[str]
    file_type: str  # "lease" | "sale"
    filename: str
    sheet_name: Optional[str]


@dataclass
class FingerprintMatch:
    """Result of a tiered lookup against a LearningStore."""
    source: str  # "exact" | "fuzzy" | "broker"
    similarity: float  # 1.0 for exact, Jaccard score otherwise
    fingerprint: Fingerprint
    mappings: dict[str, str]
    confidence: float
    hit_count: int


@dataclass
class MappingResult:
    """Output of the mapping stage for one segment."""
    fingerprint: Fingerprint
    mappings: dict[str, str]  # raw_header -> target_column
    confidence: dict[str, float]  # raw_header -> similarity score
    source: str  # "exact" | "fuzzy" | "broker" | "embedding" | "embedding+corrections" | "vision_pdf"
    similarity: float  # Jaccard score when source in {fuzzy, broker}, else 0
    cleaned_df: pd.DataFrame


@dataclass
class CleanedRows:
    df: pd.DataFrame
    rate_basis: Optional[str]
    warnings: list[str] = field(default_factory=list)


@dataclass
class GeocodedRows:
    df: pd.DataFrame
    geocode_sources: list[str]
    warnings: list[str] = field(default_factory=list)


@dataclass
class SegmentResult:
    """One processed segment. segment_key format: '<sheet_name_or_root>::<segment_index>'."""
    segment_key: str
    fingerprint: Fingerprint
    mapping_result: MappingResult
    cleaned_df: pd.DataFrame


@dataclass
class PipelineResult:
    segments: list[SegmentResult]
    combined_df: pd.DataFrame
    confidence_by_segment: dict[str, dict[str, float]]
    mappings_by_segment: dict[str, dict[str, str]]
    warnings: list[str] = field(default_factory=list)
