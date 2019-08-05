"""Get the standard bank cashflows from Zoho

Download via ZohoAPI and then add new records to relevant finance Database

Note: 
    # Config for zoho sync calls - the report view name is what is shown on the dashboar, 
    # while the link name is the API name, and seen on the Zoho API reference page

"""

# Standard library imports
from datetime import datetime 
import json
import os

# Third party imports
import pandas as pd
import sqlalchemy as db
from sqlalchemy import MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from pandas.io.json import json_normalize

# Local application imports
import zohoAPI
from zohoAPI import ZohoAPI

def yellowDBSync(table, schema, insert_cols=None, insert_cols_rename=None, form_link=None, if_exists='replace'):
    """ Download or import and sync table in Yellow DB """
    # if there is a csv in the filename, import rather than fetch:
    if schema == 'Angaza':
        insert_df = pd.read_csv(f"../data/{table}.csv")

    # else need to fetch from Zoho
    else:
        # If form_link is not given, set to form name
        if form_link is None:
            form_link = table
        
        # Fetch zoho cfg and setup API connection
        with open('config/config.json', 'r') as f:
            zoho_cfg = json.load(f)['zoho']
        zoho = ZohoAPI(zoho_cfg['zc_ownername'], zoho_cfg['authtoken'], zoho_cfg['app'])

        # Get the records from the cashflow form
        form_view = zoho.get(table, payload={'raw':'true',}) # via the trigger table
        form_json = json.loads(form_view.text)

        # Convert JSON to pandas dataframe
        insert_df = json_normalize(form_json[form_link])

        # Save current in csv
        insert_df.to_csv(f'../data/{table}.csv')

        # Assert len > 1
        assert len(insert_df)>0

    ### Reformat header if requested:
    if insert_cols is not None:
        insert_df = insert_df[insert_cols]
    if insert_cols_rename is not None:
        insert_df = insert_df.rename(columns=insert_cols_rename)

    ### Upload to database
    # Database config
    # Access the config for DB
    with open('config/config.json', 'r') as f:
        db_cfg = json.load(f)[f"yellowdb{os.environ['env']}"]

    # Create engine for connections pool
    engine = db.create_engine(
        f"{db_cfg['driver']}{db_cfg['user']}:{db_cfg['passwd']}@{db_cfg['host']}/{schema}", 
        echo = True)
    
    # Insert full table
    with engine.connect() as connection:
        insert_df.to_sql(con=connection, name=table, 
            if_exists=if_exists, index=False)



    