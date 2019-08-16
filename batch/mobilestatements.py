""" Sync all mobile money statements in gdrive
        Poll the gdrive folder for any unprocessed files
        Download and df
        Upload to mysql database
"""
# Standard libraries
import io
import json
import os
from datetime import datetime

# Third party
import pandas as pd

from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from httplib2 import Http
from oauth2client import file, client, tools
from google.oauth2 import service_account

import sqlalchemy as db
from sqlalchemy import exists
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.automap import automap_base

# Local application imports
from batch.batch_modules.statementsProcess import statementDF, uploadStatement
from batch.batch_modules.yellowDB import yellowDBSync
from batch.batch_modules.gdrive import (getFolderID, getFileIDs, 
                                  getCSV, setArchive)
from batch.batch_modules import gdrive
from batch.batch_modules.zohoAPI import ZohoAPI

# Log timestamp
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

### Setup connections
# Google drive API config
SCOPES = ['https://www.googleapis.com/auth/drive',]
SERVICE_ACCOUNT_FILE = 'config/drive-api-249008-db31cdfca28b.json'
# Credentials object creation with config file
credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)    
# Initiate gdrive service
gdrive_service = discovery.build('drive', 'v3',
          credentials=credentials)

# Yellow DB
# Access the config for DB
os.environ['env'] = 'd'
with open('config/config.json', 'r') as f:
    db_cfg = json.load(f)[f"yellowdb{os.environ['env']}"]

# Create session object
Session = sessionmaker()
# Create engine for connections pool
engine = db.create_engine(
    f"{db_cfg['driver']}{db_cfg['user']}:{db_cfg['passwd']}@{db_cfg['host']}/Finance", 
    # echo = True,
    )
# Configure "Session" class
Session.configure(bind=engine)
# Create automap base and reflect tables into base
Base = automap_base()
Base.prepare(engine, reflect=True)


### ACTION
# Search for specific folder name - e.g. airtel list
FOLDER_NAME = '1. Airtel Malawi Statements'
FOLDER_ID = getFolderID(FOLDER_NAME, gdrive_service)

# Get dictionary list of file ids
FILE_IDS = getFileIDs(FOLDER_ID, gdrive_service)

# Return archive from file listing, or None for exception handling
ARCHIVE_ID = setArchive(FILE_IDS)

# Check if archive exists
if ARCHIVE_ID is None:
    raise Exception("Create archive in google drive folder before processing...")
else:
    FILE_IDS = [file_ for file_ in FILE_IDS if file_['id']!=ARCHIVE_ID]

# Create a db Session
session = Session()
# For each file, upload the statement and archive if successful
for gdrive_file in FILE_IDS:
    if gdrive_file['id'] == ARCHIVE_ID:
        continue
    print("Processing file: " + gdrive_file.get("name") 
        + " at " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    uploadStatement(
        gdrive_file, 
        gdrive_service, 
        ARCHIVE_ID,
        FOLDER_ID,
        session,
        mobile = Base.classes.mobile,
        add_table_args = dict(
            provider = 'Airtel Malawi',
            trn_currency = 'MWK',
            added_user = 'admin',
            changed_user = 'admin'
        ),
        header_row = 4,
        dt_columns = ['Transaction Date and Time'],
        col_mapping = {
            'provider_id':'Transaction ID',
            'sender_number':'Sender Msisdn',
            'trn_timestamp':'Transaction Date and Time',
            'trn_ref_number':'Reference Number',
            'trn_amount':'Transaction Amount',
            'receiver_number':'Receiver Msisdn',
            'trn_status':'Transaction Status',
            'trn_type':'Transaction Type',
            'service_name':'Service Name',
            'provider_pre_bal':'Previous Balance',
            'provider_post_bal':'Post Balance',           
        },
    )
    print("Finished processing file: " + gdrive_file.get("name") 
        + " at " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

if len(FILE_IDS) == 0:
    print("No files at " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
