""" Do a cash recon for all inflow and outflow 

        - Mobile money transactions to Angaza
        - Total Mobile money to sweeps in accounts
        - Standard bank inflows and outflows relative to balance

"""

# System
import os
import sys
from datetime import datetime
import json

# Third party
import sqlalchemy as db
from sqlalchemy import exists, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine import reflection
import pandas as pd

# Local
from yellowsync.API.yellowDB import yellowDBSync
from yellowsync.API.zohoAPI import ZohoAPI
from yellowsync.API.angazaAPI import AngazaAPI

# Log timestamp
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Sync the relevant data into DB
# Angaza payments
# yellowDBSync(
#     table = "payments",
#     schema = 'Angaza',
#     index_label='angaza_id',
# )
# # Cashflows
# yellowDBSync(
#     table = "Historic_Cashflows_Report",
#     schema = 'Zoho',
#     form_link = "Add_Historic_Cashflow",
#     insert_cols_rename = {'ID':'zoho_ID','COMMENTS':'Comments', 'Date1':'Trn_Date'}
# )
# # Account balances
# yellowDBSync(
#     table = "All_Account_Balances",
#     schema = 'Zoho',
#     form_link = "Account_Balance",
#     insert_cols_rename={'Date_field':'Date'}
# )

# Create engine for connections
os.environ['env'] = 'd'
with open('config/config.json', 'r') as f:
    db_cfg = json.load(f)[f"yellowdb{os.environ['env']}"]

engine = db.create_engine(
    f"{db_cfg['driver']}{db_cfg['user']}:{db_cfg['passwd']}@{db_cfg['host']}/"
    , echo=True
    )

# Open connection to DB to execute recon
with engine.connect() as conn:
    # create staging table in sandbox
    staging_query = open("db/recon_stage.sql", 'r').read()
    resultProxy = conn.execute(staging_query)

    # pull recon table    
    query = open("db/recon.sql", 'r').read()
    df = pd.read_sql_query(query, con=conn)
    

print(df.head())