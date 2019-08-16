""" Create mobile payments table """

# Standard Libraries
import json, os

# 3rd Party Libraries
import sqlalchemy as db
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric, text
from sqlalchemy.dialects.mysql import TIMESTAMP

# Import
import model

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

class Mobile(Base):
    __tablename__ = 'mobile'
    trn_id = Column(Integer, primary_key=True,nullable=False)
    provider_id = Column(String(50),nullable=False, unique=True)
    sender_number = Column(Integer, nullable=False)
    
    trn_timestamp = Column(TIMESTAMP, nullable=False)
    trn_ref_number = Column(String(50))
    trn_currency = Column(String(3), nullable=False)
    trn_amount = Column(Numeric(15,2), nullable=False)
    trn_status = Column(String(50))
    trn_type = Column(String(100))
    service_name = Column(String(100))
    trn_status = Column(String(100))

    provider = Column(String(50), nullable=False)
    receiver_number = Column(Integer, nullable=False)
    provider_acc_no = Column(String(50))
    provider_pre_bal = Column(Numeric(15,2))
    provider_post_bal = Column(Numeric(15,2))
    gsmipn_id = Column(String(50))

    # System fields
    changed_user = Column(String(50),nullable=False)
    changed_timestamp = Column(
        TIMESTAMP,
        server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    added_user = Column(String(50), nullable=False)
    added_timestamp = Column(TIMESTAMP, server_default=func.now())
    
    # Schema name, in which the table is to be created
    schema = 'Finance'

    def __repr__(self):
        return f"<User(provider_id='{self.provider_id}, amount={self.trn_amount})>"

# Drop mobile table if exists
try:
    Mobile.__table__.drop(engine)
except:
    pass

# Create new table
Base.metadata.create_all(engine)
