""" Check if statements have been downloaded for previous days """

# System
import os, sys, json
from datetime import datetime

# Third party
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
import pandas as pd

# local
from googleapi.gmail import Gmail
from tools.html import htmlTxtandTable

# email users
USER = 'system@yellow.africa'
TASK_TO = 'tasks@yellow.africa'

if __name__ == "__main__":    
    # Log timestamp
    print("Start: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")) 
    
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
        # pull missing statements 
        query = open("cashflow/sql/missing_statements.sql", 'r').read()
        df_missing = pd.read_sql_query(query, con=conn)
        df_missing = df_missing.fillna('')

    # If other statements need to be downloaded:
    if len(df_missing)>0:
        print("Missing statement")
        # Create email service
        gmail = Gmail('config/mail-93851bb46b8d.json', USER)
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
    else:
        print("No missing statements")
