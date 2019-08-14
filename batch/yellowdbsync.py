""" Call Zoho syncs """

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
from batch_modules.yellowDB import yellowDBSync
# from batch_modules.zohoAPI import ZohoAPI, dfUploadSync, formDelete

# Zoho tables sync
yellowDBSync(
    table = "Historic_Cashflows_Report",
    schema = 'Zoho',
    form_link = "Add_Historic_Cashflow",
    insert_cols = [
        'Account_Paid_From',
        'Account_Paid_Into',    
        'Cashflow_Category',
        'Amount_in_MK',
        'Amount_in_ZAR',
        'Amount_in_USD',
        'Description',
        'COMMENTS',
        ],
    insert_cols_rename = {'ID':'zoho_ID','COMMENTS':'Comments'}
)
#     zoho_cols_rename = {'angaza_id':'client_external_id'}
# )

### Angaza tables
# clients table
yellowDBSync(
    table = "clients",
    schema = 'Angaza',
    # form_link = "Add_Historic_Cashflow",
    insert_cols = ['angaza_id', 'organization','client_name',
    'phone_number','account_numbers','recorder','date_created_utc',	
    'archived',	
    ],
    insert_cols_rename = {'angaza_id':'client_external_id'}
)

#payments table
yellowDBSync(
    table = "payments",
    schema = 'Angaza',
    # form_link = "Add_Historic_Cashflow",
    # insert_cols = ['angaza_id', 'organization','client_name',
    # 'phone_number','account_numbers','recorder','date_created_utc',	
    # 'archived',	
    # ],
    # insert_cols_rename = {'angaza_id':'client_external_id'}
)

