"""Data sync from Google sheets to Zoho 
    Configuration script:
    GoogleSheetID:{
        SheetName (= Form Name): {
            report: Report Name linked to form to view
            timestamps: timestamp columns to be converted
            index_col: the unique identifier/index in the Google sheet
            update: True if you want to update records that may change, 
                    False otherwise i.e. record changes in Gooogle don't occur
        }
    }
"""

# Standard library
from datetime import datetime 
import os, json, re, sys
import traceback

# Import 3rd party libaries
import pandas as pd

# Import Yellow libaries
from yellowsync.API import zohoAPI
from yellowsync.API.zohoAPI import ZohoAPI, dfUploadSync, formDelete, zohoToDF
from yellowsync.API.angazaAPI import AngazaAPI
from yellowsync.API.yellowDB import yellowDBSync
from googleapi.sheets import GSheet, processBatchDF
from googleapi.gmail import Gmail


# CONFIGURATION OF TESTS REQUIRED TO POPULATE ZOHO
GSHEETS = {
    # Recruitment tests ID
    '1l0rxY-SR0e9vW8n4Ox4-EwRC-H3JvdoAL6fDb3Z86FY':{
        'Agent_Recruitment_Test2':{
            'report':'Agent_Recruitment_Test2_Report',
            'columns': ['Timestamp','Score','Input your Yellow Agent ID'],
            'timestamps': ['Timestamp'],
            'index_col':'Timestamp',
            'update':False,
            }
    },
    # Agent test scores - 
    '1UuuMq00xo0WWGGXdOtaqq-TPaVjn0l6YLrv4h5U6nq4':{
        'Learning_Management_Test_Scores':{
        'report':'All_Learning_Management_Test_Scores',
        'index_col':'Response_ID',
        'update':True,
        },
    },
    # Exchange rates  
    '1FOsYC-eWEPmvGlPypua59GmbY_oBXSKRbKmf8J-zlVg':{
        'FX_Rates':{
        'report':'FX_Rates_Report',
        'index_col':'Date1',
        'update':True,
        },
    },
}
# Initialise objects required for integration
# Create google sheets objects
gsheet = GSheet('config/agent-recruitment-sync-c26ca6015141.json')
gmail = Gmail('config/mail-93851bb46b8d.json', 'system@yellow.africa')

# Fetch zoho cfg and setup API connection
with open('config/config.json', 'r') as f:
    zoho_cfg = json.load(f)['zoho']
zoho = ZohoAPI(zoho_cfg['zc_ownername'], zoho_cfg['authtoken'], zoho_cfg['app'])

# Log timestamp
print("Start: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Get dictionary of recruitment test results - all ranges
# more efficient to one query to google and process on this side after
# NB: ASSUMES GOOGLE SHEET FORM IS NAMED THE SAME AS ZOHO
try:
    print(f"Downloading sheets tables into dataframe...")
    # Loop through all sheets in the Config table
    for sheetID in GSHEETS.keys():
        # Set config variable
        SHEETS_CONFIG = GSHEETS.get(sheetID)
        # Print sheet/form names configured in the Google Sheet
        print([form for form in SHEETS_CONFIG.keys()])
        # Download batch result from google using the API function
        batch_result = gsheet.readBatchDFs(
            sheetID = sheetID, 
            sheet_names = [form for form in SHEETS_CONFIG.keys()]                        
            )
        # Process the batch results into dataframes
        df_dict = processBatchDF(batch_result, SHEETS_CONFIG)

        # Loop through batch, compare shee to zoho and upload 
        # any difference in rows to Zoho 
        # NB ZOHO SHEET COLUMNS SHOULD BE SHORTER THAN 98
        for form in df_dict.keys():
            # set values from config
            report = SHEETS_CONFIG[form]['report']
            index_col = SHEETS_CONFIG[form]['index_col']
            timestamp_cols = SHEETS_CONFIG[form].get('timestamps',[])
            update = SHEETS_CONFIG[form].get('update',False)
            # set dataframe from dict returned from google api
            # also filter out keyword characters in Zoho
            google_df = (df_dict[form]
                .replace("[&<>]","and",regex=True)
                .fillna('')
            )
            # Set index if values, else Continue past 
            if len(google_df) > 0:
                google_df.index = google_df[index_col]
            else:
                print(f"{form} has NO values in google sheet")
                continue

            # Download current zoho form
            print(f"Downloading zoho table {report}...")
            report_request = zoho.get(report, 
                                payload={'raw':'true',}) 
            zoho_df = zohoToDF(report_request, form)
            
            # Prepare data for update vs. insert
            # Create df for new rows - entire google sheet if none exist already
            if len(zoho_df) > 0:
                # convert timestamp cols
                for col in timestamp_cols:
                    zoho_df[col] = pd.to_datetime(zoho_df[col])
                # set index of zoho dataframe to provided index
                zoho_df.index = zoho_df[index_col]
                # filter zoho into google columns
                zoho_df = zoho_df[google_df.columns]
                # Create intersect of existing index in both google and zoho
                intersect = google_df.index.intersection(zoho_df.index) 
                # Create insert dataframe
                insert_df = google_df[~google_df.index.isin(zoho_df.index)]
                # TODO: Create update dataframe by comparing the intersection 
                # to pick up any changed rows 
                # compare_df = ~(google_df.loc[intersect] == 
                #                 zoho_df.loc[intersect, google_df.columns])*1
                # # compare_df.to_csv('compare.csv')
                update_df = google_df[google_df.index.isin(zoho_df.index)]
            else:
                insert_df = google_df
                update_df = pd.DataFrame()
            
            # ZOHO INSERTS AND UPDATES
            #TODO: better way of managing zoho values and their ID in zoho
            if (len(insert_df)==0) and (len(update_df) == 0):
                print("No new entries to update/insert")
            else:
                # INSERTING NEW VALUES
                if len(insert_df)>0:
                    print("Inserting new records...")
                    upload = dfUploadSync(df = insert_df, form=form, zoho=zoho, 
                                        slice_length=500)
                # UPDATING VALUES THAT HAVE CHANGED IN GOOGLE 
                # ONLY if update = True in the config
                if (len(update_df) > 0) and update:
                    print("Updating new records...")
                    upload = dfUploadSync(df = update_df, form=form, zoho=zoho, update_id=index_col)
                elif not update:
                    print("No new rows to insert, config set to not update changes")
        # TODO: Place these tables into Yellow db table
        # ISSUE: zoho names are too long for mysql, need to filter down
        # yellowDBSync(
            # table = "Agent_Recruitment_Test1_Report",
            # schema = 'Zoho',
            # form_link = "Agent_Recruitment_Test1",
        # )
except Exception as e: 
    # send an email notifying failure
    traceback.print_exc()
    gmail.quick_send(
        to = 'ben@yellow.africa, ross@yellow.africa',
        subject = f"Google Sheet sync failed",
        text = f"""See AWS log sheetsync.log for details
            
            {e}
            """,
    )  

# Log timestamp
print("End: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))