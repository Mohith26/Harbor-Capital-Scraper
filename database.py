import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, Text, DateTime, func
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

# Try st.secrets first (Streamlit Cloud), then env vars, then SQLite fallback
def _get_db_url():
    try:
        import streamlit as st
        return st.secrets["SUPABASE_DB_URL"]
    except Exception:
        return os.environ.get("SUPABASE_DB_URL", "sqlite:///comps.db")

Base = declarative_base()

class SaleComp(Base):
    __tablename__ = 'sale_comps'
    id = Column(Integer, primary_key=True)
    address = Column(String)
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
    created_at = Column(DateTime, server_default=func.now())

class LeaseComp(Base):
    __tablename__ = 'lease_comps'
    id = Column(Integer, primary_key=True)
    address = Column(String)
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
    created_at = Column(DateTime, server_default=func.now())

DB_URL = _get_db_url()

engine_kwargs = {}
if DB_URL.startswith("postgresql"):
    engine_kwargs["pool_pre_ping"] = True
    # Append sslmode to the URL itself (more reliable than connect_args)
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
            _tables_created = True
        except Exception as e:
            print(f"Warning: Could not create tables: {e}")

try:
    ensure_tables()
except Exception:
    pass

Session = sessionmaker(bind=engine)
