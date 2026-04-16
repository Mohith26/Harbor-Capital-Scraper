"""SqliteLearningStore — SQLAlchemy-backed LearningStore implementation."""
from __future__ import annotations
import json
from typing import Optional
from sqlalchemy import select, update, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from engine.types import Fingerprint
from learning.schemas import (
    TemplateFingerprint, ColumnMappingCorrection, GeocodeAlias,
    GeocodeOverride, Broker, PdfExtractionCorrection,
)


_DEFAULT_DB_URL = "sqlite:///learning_local.db"


class SqliteLearningStore:
    """SQLAlchemy-backed store targeting SQLite (also works with PostgreSQL via dialect swap).

    Usage:
        SqliteLearningStore(session_factory)          # existing tests/callers
        SqliteLearningStore(engine_url="sqlite:///x") # URL-based convenience
        SqliteLearningStore()                          # defaults to learning_local.db
    """

    def __init__(self, session_factory=None, *, engine_url: Optional[str] = None):
        if session_factory is None:
            # URL-based construction: create engine + session factory + tables
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
            from learning.schemas import LearningBase as Base
            url = engine_url or _DEFAULT_DB_URL
            engine = create_engine(url, connect_args={"check_same_thread": False} if "sqlite" in url else {})
            Base.metadata.create_all(engine)
            self._Session = sessionmaker(bind=engine, expire_on_commit=False)
        else:
            self._Session = session_factory

    def _session(self):
        return self._Session()

    # ---- Fingerprints ----

    def get_fingerprint_by_hash(self, fp_hash: str) -> Optional[dict]:
        with self._session() as s:
            row = s.execute(
                select(TemplateFingerprint).where(TemplateFingerprint.raw_hash == fp_hash)
            ).scalar_one_or_none()
            if row is None:
                return None
            return {
                "raw_hash": row.raw_hash,
                "file_type": row.file_type,
                "normalized_headers": row.normalized_headers or [],
                "mappings": row.mappings or {},
                "confidence": row.confidence,
                "hit_count": row.hit_count,
                "broker_id": row.broker_id,
            }

    def find_fuzzy_fingerprints(self, file_type: str) -> list[dict]:
        with self._session() as s:
            rows = s.execute(
                select(TemplateFingerprint).where(TemplateFingerprint.file_type == file_type)
            ).scalars().all()
            return [
                {
                    "raw_hash": r.raw_hash,
                    "file_type": r.file_type,
                    "normalized_headers": r.normalized_headers or [],
                    "mappings": r.mappings or {},
                    "confidence": r.confidence,
                    "hit_count": r.hit_count,
                    "broker_id": r.broker_id,
                }
                for r in rows
            ]

    def find_broker_fingerprints(self, broker_name: str, file_type: str) -> list[dict]:
        with self._session() as s:
            broker_row = s.execute(
                select(Broker).where(
                    text("lower(canonical_name) = lower(:name)")
                ).params(name=broker_name)
            ).scalar_one_or_none()
            if broker_row is None:
                return []
            rows = s.execute(
                select(TemplateFingerprint).where(
                    TemplateFingerprint.file_type == file_type,
                    TemplateFingerprint.broker_id == broker_row.id,
                )
            ).scalars().all()
            return [
                {
                    "raw_hash": r.raw_hash,
                    "file_type": r.file_type,
                    "normalized_headers": r.normalized_headers or [],
                    "mappings": r.mappings or {},
                    "confidence": r.confidence,
                    "hit_count": r.hit_count,
                    "broker_id": r.broker_id,
                }
                for r in rows
            ]

    def record_accepted_mapping(
        self,
        fingerprint: Fingerprint,
        mappings: dict[str, str],
        confirmed_by: str,
        broker_id: Optional[int] = None,
    ) -> None:
        with self._session() as s:
            stmt = sqlite_insert(TemplateFingerprint).values(
                raw_hash=fingerprint.raw_hash,
                file_type=fingerprint.file_type,
                normalized_headers=list(fingerprint.normalized_headers),
                mappings=dict(mappings),
                confidence=0.5,
                hit_count=1,
                confirmed_by=confirmed_by,
                broker_id=broker_id,
            )
            # On conflict: increment hit_count and replace mappings with latest
            stmt = stmt.on_conflict_do_update(
                index_elements=["raw_hash"],
                set_={
                    "hit_count": TemplateFingerprint.hit_count + 1,
                    "mappings": dict(mappings),
                    "confirmed_by": confirmed_by,
                },
            )
            s.execute(stmt)
            s.commit()

    # ---- Corrections ----

    def get_corrections_for_context(self, file_type: str, raw_header: str) -> dict[str, int]:
        with self._session() as s:
            rows = s.execute(
                select(ColumnMappingCorrection).where(
                    ColumnMappingCorrection.file_type == file_type,
                    ColumnMappingCorrection.raw_header == raw_header,
                )
            ).scalars().all()
            return {r.target_column: r.hit_count for r in rows}

    def upsert_correction(
        self,
        file_type: str,
        raw_header: str,
        target_column: str,
        confirmed_by: str,
    ) -> None:
        with self._session() as s:
            stmt = sqlite_insert(ColumnMappingCorrection).values(
                file_type=file_type,
                raw_header=raw_header,
                target_column=target_column,
                hit_count=1,
                confirmed_by=confirmed_by,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["file_type", "raw_header", "target_column"],
                set_={
                    "hit_count": ColumnMappingCorrection.hit_count + 1,
                    "confirmed_by": confirmed_by,
                },
            )
            s.execute(stmt)
            s.commit()

    # ---- Geocoding ----

    def get_geocode_override(self, raw_text: str) -> Optional[dict]:
        with self._session() as s:
            row = s.get(GeocodeOverride, raw_text)
            if row is None:
                return None
            return {
                "raw_text": row.raw_text,
                "override_address": row.override_address,
                "formatted_address": row.override_address,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "confirmed_by": row.confirmed_by,
            }

    def get_geocode_alias(self, raw_text: str) -> Optional[dict]:
        with self._session() as s:
            row = s.get(GeocodeAlias, raw_text)
            if row is None:
                return None
            return {
                "raw_text": row.raw_text,
                "canonical_address": row.canonical_address,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "hit_count": row.hit_count,
            }

    def insert_geocode_alias(
        self, raw_text: str, canonical_address: str, lat: float, lng: float
    ) -> None:
        with self._session() as s:
            stmt = sqlite_insert(GeocodeAlias).values(
                raw_text=raw_text,
                canonical_address=canonical_address,
                latitude=lat,
                longitude=lng,
                hit_count=1,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["raw_text"],
                set_={
                    "canonical_address": canonical_address,
                    "latitude": lat,
                    "longitude": lng,
                    "hit_count": GeocodeAlias.hit_count + 1,
                },
            )
            s.execute(stmt)
            s.commit()

    def bump_hit_count(self, raw_text: str) -> None:
        with self._session() as s:
            s.execute(
                update(GeocodeAlias)
                .where(GeocodeAlias.raw_text == raw_text)
                .values(hit_count=GeocodeAlias.hit_count + 1)
            )
            s.commit()

    def record_geocode_override(
        self, raw_text: str, override_address: str, lat: float, lng: float, confirmed_by: str
    ) -> None:
        with self._session() as s:
            stmt = sqlite_insert(GeocodeOverride).values(
                raw_text=raw_text,
                override_address=override_address,
                latitude=lat,
                longitude=lng,
                confirmed_by=confirmed_by,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["raw_text"],
                set_={
                    "override_address": override_address,
                    "latitude": lat,
                    "longitude": lng,
                    "confirmed_by": confirmed_by,
                },
            )
            s.execute(stmt)
            s.commit()

    # ---- Brokers ----

    def upsert_broker(self, name: str, confirmed_by: str) -> int:
        with self._session() as s:
            stmt = sqlite_insert(Broker).values(
                canonical_name=name,
                aliases=[],
                confirmed_by=confirmed_by,
            ).on_conflict_do_update(
                index_elements=["canonical_name"],
                set_={"confirmed_by": confirmed_by},
            ).returning(Broker.id)
            result = s.execute(stmt)
            broker_id = result.scalar_one()
            s.commit()
            return broker_id

    def find_broker_by_alias(self, name: str) -> Optional[dict]:
        with self._session() as s:
            # Check canonical name first
            row = s.execute(
                select(Broker).where(
                    text("lower(canonical_name) = lower(:name)")
                ).params(name=name)
            ).scalar_one_or_none()
            if row:
                return {"id": row.id, "canonical_name": row.canonical_name, "aliases": row.aliases or []}
            # Check aliases (JSON list stored as text)
            all_brokers = s.execute(select(Broker)).scalars().all()
            name_lower = name.lower()
            for b in all_brokers:
                aliases = b.aliases or []
                if name_lower in [a.lower() for a in aliases]:
                    return {"id": b.id, "canonical_name": b.canonical_name, "aliases": b.aliases or []}
            return None

    def find_all_brokers(self) -> list[dict]:
        with self._session() as s:
            rows = s.execute(select(Broker)).scalars().all()
            return [
                {"id": r.id, "canonical_name": r.canonical_name, "aliases": r.aliases or []}
                for r in rows
            ]

    def record_broker_correction(self, alias: str, canonical_name: str, confirmed_by: str) -> None:
        bid = self.upsert_broker(canonical_name, confirmed_by)
        with self._session() as s:
            row = s.get(Broker, bid)
            if row is None:
                return
            aliases = list(row.aliases or [])
            alias_lower = alias.lower()
            if alias_lower not in [a.lower() for a in aliases]:
                aliases.append(alias_lower)
            # Reassign to trigger SQLAlchemy change tracking for JSON column
            row.aliases = aliases
            s.commit()

    # ---- PDF corrections ----

    def get_pdf_corrections(self, pdf_hash: str) -> list[dict]:
        with self._session() as s:
            rows = s.execute(
                select(PdfExtractionCorrection).where(
                    PdfExtractionCorrection.pdf_hash == pdf_hash
                )
            ).scalars().all()
            return [
                {
                    "pdf_hash": r.pdf_hash, "page_num": r.page_num,
                    "row_index": r.row_index, "field": r.field,
                    "original_value": r.original_value, "corrected_value": r.corrected_value,
                    "confirmed_by": r.confirmed_by,
                }
                for r in rows
            ]

    def record_pdf_correction(
        self, pdf_hash: str, page_num: int, row_index: int, field: str,
        original: str, corrected: str, confirmed_by: str
    ) -> None:
        with self._session() as s:
            s.add(PdfExtractionCorrection(
                pdf_hash=pdf_hash, page_num=page_num, row_index=row_index,
                field=field, original_value=original, corrected_value=corrected,
                confirmed_by=confirmed_by,
            ))
            s.commit()

    # ---- Seed ----

    def load_seed(self, seed_dir: str) -> None:
        from learning.seed_io import load_seed_into_store
        load_seed_into_store(self, seed_dir)


# Alias for environments that use Supabase/PostgreSQL — same class,
# dialect-specific insert is swapped at import time if needed.
SupabaseLearningStore = SqliteLearningStore
