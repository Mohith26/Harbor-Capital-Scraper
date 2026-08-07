import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, Text, DateTime, CheckConstraint, func
from sqlalchemy.orm import declarative_base, sessionmaker, validates

load_dotenv()

def _get_db_url():
    """Resolve the database URL from the environment.

    Resolution order:
      1. ``DATABASE_URL`` / ``SUPABASE_DB_URL`` if set explicitly.
      2. Cloudflare D1 (``sqlite+d1://``) when ``D1_DATABASE_ID`` is present.
      3. A local SQLite file, for development and tests.
    """
    explicit = os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL")
    if explicit:
        return explicit
    if os.environ.get("D1_DATABASE_ID"):
        return "sqlite+d1://"
    return "sqlite:///comps.db"

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


class UserSession(Base):
    """Persistent login sessions.

    These used to live in a module-level dict, which meant every process
    restart silently logged everyone out.  That is fatal on Cloudflare
    Containers, which sleep after a few minutes idle, so sessions are stored
    in the database instead.
    """
    __tablename__ = 'user_sessions'
    token = Column(String, primary_key=True)
    username = Column(String, nullable=False)
    name = Column(String)
    role = Column(String)
    login_time = Column(Float, nullable=False)
    expires_at = Column(Float, nullable=False)


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
    costar_property_id = Column(String)
    costar_url = Column(String)
    costar_specs = Column(Text)            # JSON blob of CoStar-derived specs
    costar_status = Column(String, default='pending', server_default='pending')
    costar_candidates = Column(Text)       # JSON list of candidate matches (when ambiguous)
    costar_enriched_at = Column(DateTime)
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
    costar_property_id = Column(String)
    costar_url = Column(String)
    costar_specs = Column(Text)            # JSON blob of CoStar-derived specs
    costar_status = Column(String, default='pending', server_default='pending')
    costar_candidates = Column(Text)       # JSON list of candidate matches (when ambiguous)
    costar_enriched_at = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())

    @validates('address')
    def _check_address(self, key, value):
        return _validate_address(value)

DB_URL = _get_db_url()

USING_D1 = DB_URL.startswith("sqlite+d1")

engine_kwargs = {}
if DB_URL.startswith("postgresql"):
    engine_kwargs["pool_pre_ping"] = True
    engine_kwargs["pool_size"] = 5
    engine_kwargs["max_overflow"] = 10
    if "sslmode" not in DB_URL:
        separator = "&" if "?" in DB_URL else "?"
        DB_URL = f"{DB_URL}{separator}sslmode=require"
elif USING_D1:
    # Importing the package registers the ``sqlite+d1`` dialect.
    import d1  # noqa: F401

    # Each statement is an independent HTTPS call, so pooling buys nothing and
    # a stale pooled "connection" would just hide errors.
    from sqlalchemy.pool import NullPool

    engine_kwargs["poolclass"] = NullPool

engine = create_engine(DB_URL, **engine_kwargs)

# Create tables — defer errors so the app can still show a useful message
_tables_created = False
def ensure_tables():
    global _tables_created
    if _tables_created:
        return
    if USING_D1:
        # D1's schema is managed out-of-band by `migrations/` + `wrangler d1
        # execute`, and SQLAlchemy's reflection round-trips are expensive over
        # HTTP, so skip create_all entirely.
        _tables_created = True
        return
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
