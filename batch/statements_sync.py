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
# google api
from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from httplib2 import Http
from oauth2client import file, client, tools
from google.oauth2 import service_account
# sqlalchemy
import sqlalchemy as db
from sqlalchemy import exists
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.automap import automap_base

# Local application imports
from batch_modules.statementsProcess import statementDF
from batch_modules.yellowDB import yellowDBSync
from batch_modules.gdrive import (getFolderID, getFileIDs, 
                                  getCSV, setArchive)
from batch_modules import gdrive

### Setup connections
# Google drive API config
SCOPES = ['https://www.googleapis.com/auth/drive',
    ]
SERVICE_ACCOUNT_FILE = 'config/drive-api-c8ced979962d.json'
# Credentials object creation with config file
credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)    
# Initiate gdrive service
gdrive_service = discovery.build('drive', 'v3',
          credentials=credentials)

# Yellow DB
# Access the config for DB
with open('config/config.json', 'r') as f:
    db_cfg = json.load(f)[f"yellowdb{os.environ['env']}"]

# Create session object
Session = sessionmaker()
# Create engine for connections pool
engine = db.create_engine(
    f"{db_cfg['driver']}{db_cfg['user']}:{db_cfg['passwd']}@{db_cfg['host']}/Finance", 
    echo = True)
# Configure "Session" class
Session.configure(bind=engine)
# Create automap base and reflect tables into base
Base = automap_base()
Base.prepare(engine, reflect=True)

def uploadStatement(gdrive_file, gdrive_service, archive_id, mobile, **kwargs):
    """ Upload statement based on given file, gdrive service and 
        csv format arguments 
            - datetime columns
            - header row for statement csv (0 is first row)
            - column mapping
    """
    
    # Additional columns to set values for in the colun (statement level)
    # e.g. Current, Provider name, User etc.
    add_table_args = kwargs.get('add_table_args')

    # Get csv in dataframe
    df = getCSV(gdrive_file, gdrive_service, dt_columns=kwargs['dt_columns'], header=kwargs['header_row'])
    # If not a df, continue
    if df is None:
        # use this if statement to delete/move non-csv files
        if gdrive_file.get("id") != archive_id:
            print(f"file {gdrive_file.get('name')} is not a readable csv file")
        return

    # Process df
    processed_df = statementDF(
        df,
        col_mapping = kwargs.get('col_mapping')
    )
    
    # Create the dict to add to 
    rows_dict_list = processed_df.to_dict(orient="records")
    
    # Create a db Session
    session = Session()
    for row_dict in rows_dict_list:
        # Create transaction objects trn with statement data plus other
        trn = mobile(
            **row_dict,
            **add_table_args
            )
        # Check whether mobile provider trn id exists 
        trn_exists = (session
            .query(exists()
                .where(mobile.provider_id==row_dict['provider_id']))
            .scalar())
        # If it does not exist, add it
        if not trn_exists:
            session.add(trn)
        # If it already exists, get info for it, check if ref or status has changed 
        # and only then update
        else:
            print(row_dict['provider_id'] + ' already exists.')
            trn = session.query(mobile).filter_by(provider_id=row_dict['provider_id'])
            # Update if ref or status has changed
            if (trn.first().trn_ref_number!=row_dict['trn_ref_number'] 
                or trn.first().trn_status!=row_dict['trn_status']):
                trn.update({**row_dict,**{'changed_user':add_table_args['changed_user']}})
                session.flush()
                print('Updated.')
        
    # try commit and move file. If it failes, rollback and raise exception
    try: 
        session.commit()
        #if successful, move file to archive
        _ = gdrive_service.files().update(
                                        fileId=gdrive_file.get("id"),
                                        addParents=archive_id,
                                        removeParents=folder_id,
                                        fields='id, parents').execute()
    except:
        raise
    finally:
        session.close()

    # print(processed_df.loc[0,'json'])
    return


### ACTION
# Search for specific folder name - e.g. airtel list
folder_name = '1. Airtel Malawi Statements'
folder_id = getFolderID(folder_name, gdrive_service)

# Get dictionary list of file ids
file_ids = getFileIDs(folder_id, gdrive_service)

# Return archive from file listing, or None for exception handling
archive_id = setArchive(file_ids)
if archive_id is None:
    raise Exception("Create gdrive archive folder before processing...")

# For each file, upload the statement and archive if successful
for gdrive_file in file_ids:
    uploadStatement(
        gdrive_file, 
        gdrive_service, 
        archive_id,
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


# df = airtelHistory(file_path = '../data/airtel/')

# yellowDBSync(table = 'airtel_trns', schema = 'Mobile', df = df)