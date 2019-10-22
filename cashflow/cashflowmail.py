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

# local
from googleapi.gmail import Gmail
from tools.html import htmlTxtandTable
from tools.exporters import exportCSV

# Set options for emails
pd.options.display.float_format = '{:,.2f}'.format
USER = 'system@yellow.africa'
REPORT_TO = 'tech-support@yellow.africa'
PAYMENTS_EMAIL = 'payments@yellow.africa'
TEST_EMAIL = 'ben@yellow.africa'

RECON_EMAIL_COLS = ['TrnDate', 
    'MatchedPmt', 
    'MonthPmtAmnt',
    'SweepRecon', 
    'MobileFinalBalance',
    'TrnRecon', 
    'AngazaPaymentsToProcess', 
    'HasMissingMobileMoneyTrns', 
    'HasUnmatchedPayments',  
    'HasDuplicate',
]

# Log timestamp
print("Start: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Create engine for connections
os.environ['env'] = 'd'
with open('config/config.json', 'r') as f:
    db_cfg = json.load(f)[f"yellowdb{os.environ['env']}"]

engine = db.create_engine(
    f"{db_cfg['driver']}{db_cfg['user']}:{db_cfg['passwd']}@{db_cfg['host']}/Finance?charset=utf8mb4"
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

    # pull balance recon table    
    print("Balance recon queries...")
    with open("cashflow/sql/recent_daily_balances.sql", 'r') as sql:
        queries = sql.read().replace('%','%%').split(';')
        del queries[-1]
        balance_summaries = (
            [pd.read_sql_query(query, con=conn).round(0) for query in queries]
            )
    balance_14day = balance_summaries[0]
    balance_compare_actuals = balance_summaries[1]
    balance_compare_actuals_60day = balance_summaries[2]

    # email all transactions from account   
    # TODO: might be better to send a number of days rather
    print("Std bnk trn query...")
    query = 'select * from Finance.standard_bank_account_trns'
    std_bnk_trns = (pd.read_sql_query(query, con=conn)
        .fillna('')
        .round(0)
    )

# Create email service
gmail = Gmail('config/mail-93851bb46b8d.json', USER)

# Trn Recon Email
# Logic to define table heading and paragraphs
table_heading = ("Yesterdays Cashflow Recon Status: "
    + recon_summary.loc[0,'TrnRecon'])
# Create recon email to be sent morning
html = htmlTxtandTable(
    table_heading, 
    recon_summary.loc[0:14, RECON_EMAIL_COLS], 
    style='blueTable')
# Create multimessage to send
trn_recon_msg = gmail.create_message(
    sender = USER,
    to = ", ".join([REPORT_TO,PAYMENTS_EMAIL]),
    subject = 'Cashflow: Mobile Money Recon',
    message_text = 'Please open as HTML email',
    html=html,
    # send recon table as csv attachment
    files = {'cashflowrecon.csv':exportCSV(recon_summary),
            'unreconciled.csv':exportCSV(df_unreconciled),
            'paymentstoprocess.csv':exportCSV(df_toprocess),
            },
)

### Balance Recon Email

# Create tables in HTML
html_14day = htmlTxtandTable(
    "Balances past 14 days - calculated and actual where valuable", 
    balance_14day, 
    style='blueTable')
html_actual_60day = htmlTxtandTable(
    "Comparison of the days where actual balances are available (past 60 days)", 
    balance_compare_actuals_60day, 
    style='blueTable')

# Create multimessage to send
recon_msg = gmail.create_message(
    sender = USER,
    to = REPORT_TO,
    subject = 'Cashflow: STD BNK balance',
    message_text = 'Please open as HTML email',
    html=(html_14day+html_actual_60day),
    # send recon table as csv attachment
    files = {'balancerecon.csv':exportCSV(balance_14day),
            'actualbalances.csv':exportCSV(balance_compare_actuals),
            'stdbnktrns.csv':exportCSV(std_bnk_trns),
            },
)

# Send emails
try:
    trn_recon_send = gmail.send_message(trn_recon_msg)
    balance_send = gmail.send_message(recon_msg)
except: 
    raise

