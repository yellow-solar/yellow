""" Create mobile payments table """

# Standard Libraries
import json, os

# 3rd Party Libraries
import sqlalchemy as db
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric, text
from sqlalchemy.dialects.mysql import TIMESTAMP

# Database config
with open('config/config.json', 'r') as f:
    db_cfg = json.load(f)[f"yellowdb{os.environ['env']}"]

# Create engine
engine = db.create_engine(
    f"{db_cfg['driver']}{db_cfg['user']}:{db_cfg['passwd']}@{db_cfg['host']}/Finance", 
    # echo = True,
    )

# Declare base
Base = declarative_base()

class MobileRecon(Base):
    __tablename__ = 'mobile_manual_recon_trns'
    provider_id = Column(String(50), primary_key=True)
    note = Column(String(255))

    # System fields
    changed_user = Column(String(50),nullable=False)
    changed_timestamp = Column(
        TIMESTAMP,
        server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    added_user = Column(String(50), nullable=False)
    added_timestamp = Column(TIMESTAMP, server_default=func.now())
    
    # Schema name, in which the table is to be created
    schema = 'Finance'

# Drop mobile table if exists
try:
    MobileRecon.__table__.drop(engine)
except:
    pass

# Create new table
Base.metadata.create_all(engine)
