import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, Text, DateTime, CheckConstraint, func
from sqlalchemy.orm import declarative_base, sessionmaker, validates

load_dotenv()

def _get_db_url():
    """Get database URL from environment variable. No Streamlit dependency."""
    return os.environ.get("SUPABASE_DB_URL", "sqlite:///comps.db")

Base = declarative_base()
from learning.schemas import LearningBase


def _validate_address(value):
    """Reject NULL / empty / whitespace-only addresses at the ORM layer.

    Address is required for every comp — without it the row cannot be
    geocoded, mapped, or matched in Comp Finder. This is the schema-level
    half of the defense-in-depth check; the upload save path also performs
    its own check and reports skipped rows to the user.
    """
    if value is None or not str(value).strip():
        raise ValueError("address is required and must not be empty or whitespace-only")
    return value


class SaleComp(Base):
    __tablename__ = 'sale_comps'
    __table_args__ = (
        CheckConstraint("length(trim(address)) > 0", name='ck_sale_comps_address_not_blank'),
    )
    id = Column(Integer, primary_key=True)
    address = Column(String, nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)
    sale_price = Column(Float)
    building_size = Column(Float)
    price_per_sf = Column(Float)
    closing_date = Column(String)
    year_built = Column(Float)
    cap_rate = Column(Float)
    buyer = Column(String)
    seller = Column(String)
    notes = Column(Text)
    raw_address_data = Column(Text)
    source_file = Column(String)
    source_file_url = Column(String)
    city = Column(String)
    zip_code = Column(String)
    created_at = Column(DateTime, server_default=func.now())

    @validates('address')
    def _check_address(self, key, value):
        return _validate_address(value)


class LeaseComp(Base):
    __tablename__ = 'lease_comps'
    __table_args__ = (
        CheckConstraint("length(trim(address)) > 0", name='ck_lease_comps_address_not_blank'),
    )
    id = Column(Integer, primary_key=True)
    address = Column(String, nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)
    tenant_name = Column(String)
    leased_sf = Column(Float)
    rate_monthly = Column(Float)
    rate_annually = Column(Float)
    lease_type = Column(String)
    term_months = Column(Float)
    commencement_date = Column(String)
    ti_allowance = Column(Float)
    free_rent = Column(String)
    escalations = Column(String)
    building_type = Column(String)
    clear_height = Column(Float)
    notes = Column(Text)
    raw_address_data = Column(Text)
    source_file = Column(String)
    source_file_url = Column(String)
    city = Column(String)
    zip_code = Column(String)
    created_at = Column(DateTime, server_default=func.now())

    @validates('address')
    def _check_address(self, key, value):
        return _validate_address(value)

DB_URL = _get_db_url()

engine_kwargs = {}
if DB_URL.startswith("postgresql"):
    engine_kwargs["pool_pre_ping"] = True
    engine_kwargs["pool_size"] = 5
    engine_kwargs["max_overflow"] = 10
    if "sslmode" not in DB_URL:
        separator = "&" if "?" in DB_URL else "?"
        DB_URL = f"{DB_URL}{separator}sslmode=require"

engine = create_engine(DB_URL, **engine_kwargs)

# Create tables — defer errors so the app can still show a useful message
_tables_created = False
def ensure_tables():
    global _tables_created
    if not _tables_created:
        try:
            Base.metadata.create_all(engine)
            LearningBase.metadata.create_all(engine)
            _tables_created = True
        except Exception as e:
            print(f"Warning: Could not create tables: {e}")

try:
    ensure_tables()
except Exception:
    pass

Session = sessionmaker(bind=engine)
