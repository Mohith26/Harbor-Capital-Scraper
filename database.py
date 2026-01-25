from sqlalchemy import create_engine, Column, Integer, String, Float, Text
from sqlalchemy.orm import declarative_base, sessionmaker
import datetime

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
    upload_date = Column(String, default=datetime.datetime.now().strftime("%Y-%m-%d"))

class LeaseComp(Base):
    __tablename__ = 'lease_comps'
    id = Column(Integer, primary_key=True)
    address = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    tenant_name = Column(String)
    leased_sf = Column(Float)
    
    # NEW: Split Rates
    rate_monthly = Column(Float)   # <--- NEW
    rate_annually = Column(Float)  # <--- NEW
    
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
    upload_date = Column(String, default=datetime.datetime.now().strftime("%Y-%m-%d"))

engine = create_engine('sqlite:///comps.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)