""" Do a cash recon for all inflow and outflow 

        - Mobile money transactions to Angaza
        - Total Mobile money to sweeps in accounts
        - Standard bank inflows and outflows relative to balance
"""

# System
import os, sys, json
from datetime import datetime

# Third party
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
import pandas as pd

# Local
from yellowsync.API.yellowDB import yellowDBSync
from yellowsync.API.zohoAPI import ZohoAPI
from yellowsync.API.angazaAPI import AngazaAPI

# Sync the relevant data into DB
# Angaza payments
def cashflowDataSync():
    """ run data syncs for cashflow recon"""
    # Angaza tables
    yellowDBSync(
            table = "receipts",
            schema = 'Angaza',
            index_label='angaza_id',
            )
    yellowDBSync(
        table = "payments",
        schema = 'Angaza',
        index_label='angaza_id',
    )
    # Cashflows
    yellowDBSync(
        table = "Historic_Cashflows_Report",
        schema = 'Zoho',
        form_link = "Add_Historic_Cashflow",
        insert_cols_rename = {'ID':'zoho_ID','COMMENTS':'Comments', 'Date1':'Trn_Date'}
    )
    # Account balances
    yellowDBSync(
        table = "All_Account_Balances",
        schema = 'Zoho',
        form_link = "Account_Balance",
        insert_cols_rename={'Date_field':'Date'}
    )
    return

if __name__ == "__main__":
    # Log timestamp
    print("Start: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")) 
    
    #  Run sync if flag found
    if len(sys.argv) > 1:
        sync = sys.argv[1]
        if sync=="live":
            cashflowDataSync()

    # Create engine for connections
    os.environ['env'] = 'd'
    with open('config/config.json', 'r') as f:
        db_cfg = json.load(f)[f"yellowdb{os.environ['env']}"]

    engine = db.create_engine(
        f"{db_cfg['driver']}{db_cfg['user']}:{db_cfg['passwd']}@{db_cfg['host']}/"
        # , echo=True
        )
    
    # Open connection to DB to execute recon
    with engine.connect() as conn:
        # create staging table in sandbox
        print("Transactions query...")
        staging_query = open("cashflow/sql/recon_transactions.sql", 'r').read()
        resultProxy = conn.execute(staging_query.replace('%','%%'))     
        
    # Log timestamp
    print("End: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))