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
from googleapi.gmail import Gmail
from tools.html import htmlTxtandTable
from tools.exporters import exportCSV

USER = 'system@yellow.africa'
TASK_TO = 'tasks@yellow.africa'

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
    # Agent payments
    yellowDBSync(
        table = "All_Agent_Payments",
        schema = 'Zoho',
        form_link = "Agent_Payments",
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
        with open("cashflow/sql/recon_transactions.sql", 'r') as sql:
            query = sql.read().replace('%','%%')
            resultProxy = conn.execute(query)

        # update the views
        print("Summary query...")
        with open("cashflow/sql/standard_bank_account_trns.sql", 'r') as sql:
            query = sql.read().replace('%','%%')
            resultProxy = conn.execute(query)

        # update the views
        print("Summary query...")
        with open("cashflow/sql/all_accounts_balance_daily.sql", 'r') as sql:
            query = sql.read().replace('%','%%')
            resultProxy = conn.execute(query) 

        # pull missing statements 
        print("Statement check...")
        with open("cashflow/sql/missing_statements.sql", 'r') as sql:
            query = sql.read().replace('%','%%')
            df_missing = (pd
                .read_sql_query(query, con=conn)
                .fillna('')
            )

    # Create email service
    gmail = Gmail('config/mail-93851bb46b8d.json', USER)

    # Check if statements are missing
    if len(df_missing) > 0:
        # Html table
        html = htmlTxtandTable("Download the following statements:", 
                                df_missing, style='blueTable')
        # Create multimessage to send
        msg = gmail.create_message(
            sender = USER,
            to = TASK_TO,
            subject = 'Task: Statement Downloads',
            message_text = 'Please open as HTML email',
            html=html,
        )
        task_send = gmail.send_message(msg)
        print("Missing statement. Task email sent")

    # Log timestamp
    print("End: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))