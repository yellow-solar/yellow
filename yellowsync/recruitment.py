"""Agement Recruitment data sync from sheets to Zoho 

    Function to call , from sheet, 

"""

# Standard library
from datetime import datetime 
import os, json, re, sys

# Import 3rd party libaries
import pandas as pd

# Import Yellow libaries
from yellowsync.API import zohoAPI
from yellowsync.API.zohoAPI import ZohoAPI, dfUploadSync, formDelete, zohoToDF
from yellowsync.API.angazaAPI import AngazaAPI
from yellowsync.API.yellowDB import yellowDBSync
from googleapi.sheets import GSheet, processBatchDF

# CONFIGURATION OF TESTS REQUIRED TO POPULATE ZOHO
AGENT_TESTS_GSHEET_ID = '1l0rxY-SR0e9vW8n4Ox4-EwRC-H3JvdoAL6fDb3Z86FY'
SHEETS_CONFIG = {
    'Agent_Recruitment_Test1':{
        'report':'Agent_Recruitment_Test1_Report',
        'timestamps': ['Timestamp'],
        'index_col':'Timestamp',
    },
    'Agent_Recruitment_Test2':{
        'report':'Agent_Recruitment_Test2_Report',
        'columns': ['Timestamp','Score','Input your 4-digit Yellow Agent ID'],
        'timestamps': ['Timestamp'],
        'index_col':'Timestamp',
    },
    'Agent_Recruitment_Test3':{
        'report':'Agent_Recruitment_Test3_Report',
        'timestamps': ['Timestamp'],
        'index_col':'Timestamp',
    }
}

# Initialise objects required for integration
# Create google sheets objects
gsheet = GSheet('config/agent-recruitment-sync-c26ca6015141.json')

# Fetch zoho cfg and setup API connection
with open('config/config.json', 'r') as f:
    zoho_cfg = json.load(f)['zoho']
zoho = ZohoAPI(zoho_cfg['zc_ownername'], zoho_cfg['authtoken'], zoho_cfg['app'])

# Log timestamp
print("Start: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Get dictionary of recruitment test results - all ranges
# more efficient to one query to google and process on this side after
# NB: ASSUMES GOOGLE SHEET FORM IS NAMED THE SAME AS ZOHO
print(f"Downloading sheets tables into dataframe...")
print([form for form in SHEETS_CONFIG.keys()]       )
batch_result = gsheet.readBatchDFs(
    sheetID = AGENT_TESTS_GSHEET_ID, 
    sheet_names = [form for form in SHEETS_CONFIG.keys()]                        
    )
df_dict = processBatchDF(batch_result, SHEETS_CONFIG)

# Loop through batch, compare shee to zoho and upload 
# any difference in rows to Zoho 
# NB ZOHO SHEET COLUMNS SHOULD BE SHORTER THAN 98
for form in df_dict.keys():
    # set dataframe from dict returned from google api
    # filter out Zoho unwanted characters 
    # also filter out keyword characters in zoho
    google_df = (df_dict[form]
        .replace("[&<>]","and",regex=True)
        .fillna('')
    )

    # Download current zoho form
    report = SHEETS_CONFIG[form]['report']
    index_col = SHEETS_CONFIG[form]['index_col']
    print(f"Downloading zoho table {report}...")
    report_request = zoho.get(report, 
                        payload={'raw':'true',}) 
    zoho_df = zohoToDF(report_request, form)

    # compare dfs in index column and add new ones
    # TODO: make this a comparison of any row thats changed as well 
    insert_df = google_df[~google_df[index_col].isin(zoho_df[index_col])]
    # upload insertion table
    if len(insert_df)>0:
        upload = dfUploadSync(df = insert_df, form=form, zoho=zoho, 
                              slice_length=1000)
    else:
        print("No new entries to upload")

# TODO: Place agent recruitment data into Yellow db table
# ISSUE: zoho names are too long for mysql, need to filter down
# yellowDBSync(
    # table = "Agent_Recruitment_Test1_Report",
    # schema = 'Zoho',
    # form_link = "Agent_Recruitment_Test1",
# )


# Log timestamp
print("End: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))