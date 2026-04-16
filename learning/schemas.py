"""SQLAlchemy models for the learning store tables."""
from __future__ import annotations
from sqlalchemy import Column, Integer, String, Float, Text, Boolean, DateTime, UniqueConstraint, Index, func
from sqlalchemy.types import JSON as GenericJSON
from sqlalchemy.orm import declarative_base

LearningBase = declarative_base()


class TemplateFingerprint(LearningBase):
    __tablename__ = "template_fingerprints"
    id = Column(Integer, primary_key=True)
    raw_hash = Column(String, unique=True, nullable=False, index=True)
    broker_id = Column(Integer, nullable=True, index=True)
    file_type = Column(String, nullable=False, index=True)
    normalized_headers = Column(GenericJSON,nullable=False, default=list)
    mappings = Column(GenericJSON,nullable=False, default=dict)
    confidence = Column(Float, nullable=False, default=0.5)
    hit_count = Column(Integer, nullable=False, default=1)
    confirmed_by = Column(String, nullable=False, default="seed")
    last_seen_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class ColumnMappingCorrection(LearningBase):
    __tablename__ = "column_mapping_corrections"
    id = Column(Integer, primary_key=True)
    file_type = Column(String, nullable=False)
    raw_header = Column(String, nullable=False)
    target_column = Column(String, nullable=False)
    hit_count = Column(Integer, nullable=False, default=1)
    confirmed_by = Column(String, nullable=False, default="unknown")
    last_confirmed = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("file_type", "raw_header", "target_column", name="uq_correction"),
        Index("ix_correction_lookup", "file_type", "raw_header"),
    )


class GeocodeAlias(LearningBase):
    __tablename__ = "geocode_aliases"
    raw_text = Column(String, primary_key=True)  # normalized Texas-biased form
    canonical_address = Column(Text, nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    hit_count = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime, server_default=func.now())


class GeocodeOverride(LearningBase):
    __tablename__ = "geocode_overrides"
    raw_text = Column(String, primary_key=True)  # normalized Texas-biased form
    override_address = Column(Text, nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    confirmed_by = Column(String, nullable=False)
    confirmed_at = Column(DateTime, server_default=func.now())


class Broker(LearningBase):
    __tablename__ = "brokers"
    id = Column(Integer, primary_key=True)
    canonical_name = Column(String, unique=True, nullable=False, index=True)
    aliases = Column(GenericJSON,nullable=False, default=list)
    confirmed_by = Column(String, nullable=False, default="llm_auto")
    created_at = Column(DateTime, server_default=func.now())


class PdfExtractionCorrection(LearningBase):
    __tablename__ = "pdf_extraction_corrections"
    id = Column(Integer, primary_key=True)
    pdf_hash = Column(String, nullable=False, index=True)
    page_num = Column(Integer, nullable=False)
    row_index = Column(Integer, nullable=False)
    field = Column(String, nullable=False)
    original_value = Column(Text, nullable=True)
    corrected_value = Column(Text, nullable=True)
    confirmed_by = Column(String, nullable=False)
    confirmed_at = Column(DateTime, server_default=func.now())
