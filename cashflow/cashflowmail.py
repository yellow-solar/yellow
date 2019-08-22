""" Product daily update mail of cashflow recons 

        will notify if statements not yet downloaded
        
        requires data sync to first run 
"""

# standard
import os, sys, json
from datetime import datetime

# third party
import sqlalchemy as db
import pandas as pd
pd.set_option('display.float_format', lambda x: '%.2f' % x)

# local
from googleapi.gmail import Gmail
from tools.html import htmlTxtandTable
from tools.exporters import exportCSV

USER = 'system@yellow.africa'
TASK_TO = 'tasks@yellow.africa'
REPORT_TO = 'tech-support@yellow.africa'
# REPORT_TO = 'ben@yellow.africa'

RECON_EMAIL_COLS = ['TrnDate', 
    'MatchedPmt', 
    'SweepRecon', 
    'TrnRecon', 
    'AngazaPaymentsToProcess', 
    'HasMissingMobileMoneyTrns', 
    'HasUnmatchedPayments',  
    'HasDuplicate',
]

if __name__ == "__main__":
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
        # pull summary recon table    
        print("Summary query...")
        with open("cashflow/sql/recon_trn_summary.sql", 'r') as sql:
            query = sql.read().replace('%','%%')
            recon_summary = (pd.read_sql_query(query, con=conn)
                .fillna('')
                .round(0)
            )

        # pull unprocessed transactions for manual action
        print("Unprocessed trns...")
        with open("cashflow/sql/trn_to_process.sql", 'r') as sql:
            query = sql.read().replace('%','%%')
            df_toprocess = (pd.read_sql_query(query, con=conn)
                .fillna('')
            )

        # pull unreconciled
        print("Unreconciled trns...")
        with open("cashflow/sql/unreconciled.sql", 'r') as sql:
            query = sql.read().replace('%','%%')
            df_unreconciled = (pd
                .read_sql_query(query, con=conn)
                .fillna('')
            )

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

    # Logic to define table heading and paragraphs
    # Check what sentences need to go into email  
    # Only heading for now
    table_heading = ("Yesterdays Cashflow Recon Status: "
        + recon_summary.loc[0,'TrnRecon'])
    
    # Create recon email to be sent morning
    html = htmlTxtandTable(
        table_heading, 
        recon_summary.loc[0:14, RECON_EMAIL_COLS], 
        style='blueTable')
    # Create multimessage to send
    recon_msg = gmail.create_message(
        sender = USER,
        to = REPORT_TO,
        subject = 'Cashflow: recon',
        message_text = 'Please open as HTML email',
        html=html,
        # send recon table as csv attachment
        files = {'cashflowrecon.csv':exportCSV(recon_summary),
                'unreconciled.csv':exportCSV(df_unreconciled),
                'paymentstoprocess.csv':exportCSV(df_toprocess),
                },
    )
    recon_send = gmail.send_message(recon_msg)
