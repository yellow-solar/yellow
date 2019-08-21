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
from sqlalchemy import exists, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine import reflection
import pandas as pd

# Local
from yellowsync.API.yellowDB import yellowDBSync
from yellowsync.API.zohoAPI import ZohoAPI
from yellowsync.API.angazaAPI import AngazaAPI
from googleapi.gmail import Gmail
from tools.html import htmlTableBody

USER = 'system@yellow.africa'
TO = 'ben@yellow.africa'

# Sync the relevant data into DB
# Angaza payments
def cashflowDataSync():
    """ run data syncs """
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

    # Create email service
    gmail = Gmail('config/mail-93851bb46b8d.json', USER)

    # Open connection to DB to execute recon
    with engine.connect() as conn:
        # create staging table in sandbox
        print("Staging query...")
        staging_query = open("cashflow/recon_stage.sql", 'r').read()
        resultProxy = conn.execute(staging_query)

        # pull recon table    
        query = open("cashflow/recon.sql", 'r').read()
        print("Recon query...")
        df = pd.read_sql_query(query, con=conn)
        df = df.fillna('')

        # pull missing statements 
        # pull recon table    
        query = open("cashflow/missing_statements.sql", 'r').read()
        df_missing = pd.read_sql_query(query, con=conn)
        df_missing = df_missing.fillna('')

    # Recon email
    # Create HTML body
    html = htmlTableBody("Cashflow Recon", df, style='blueTable')
    # Create multimessage to send
    recon_msg = gmail.create_message(
        sender = USER,
        to = TO,
        subject = 'test message',
        message_text = 'please open as HTML email',
        html=html,
    )
    recon_send = gmail.send_message(recon_msg)

    # If other statements need to be downloaded:
    if len(df_missing)>0:
        # Html table
        html = htmlTableBody("Download the following statements:", 
                                df_missing, style='blueTable')
        # Create multimessage to send
        msg = gmail.create_message(
            sender = USER,
            to = TO,
            subject = 'Task: Statement Downloads',
            message_text = 'Please open as HTML email',
            html=html,
        )
        task_send = gmail.send_message(msg)

    # Log timestamp
    print("End: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))