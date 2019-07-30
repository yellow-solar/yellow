import zohoAPI
import pandas as pd
from datetime import datetime 
from zohoAPI import ZohoAPI, dfUploadSync, formDelete

# Create a function to return
def returnErrorCheck(sync,sync_response):
    if sync_response.status_code == 200:
        print(sync_response.text)
        if 'error' in sync_response:
            raise Exception(sync + ": Received error in REST API response from Zoho")

# Config for scipt update
trigger_command = "execute"

# Print timestamp for log
print("Run Zoho sync scripts:", datetime.now().strftime("%H:%M:%S"))

# Create connection
zoho = ZohoAPI('yellow679', 'bdbda4796c376c1fb955a749d47a17e7', 'collections-management')

# Time this whole thing
t1 = datetime.now()

# List of all syncs to run consecutively
sync_list =['SYNC_A','SYNC_B','SYNC_C','SYNC_D','SYNC_E']

# Execute sync A
sync = "SYNC_A"
sync_a_response = zoho.add("API_Triggers", payload = {"trigger_command":trigger_command,"command_string":sync}) # via the trigger table
returnErrorCheck(sync, sync_a_response)

# sync = "TEST"
# response = zoho.add("API_Triggers", payload = {"trigger_command":trigger_command,"command_string":sync}) # via the trigger table
# returnErrorCheck(sync, response)

# End timing
t2 = datetime.now()
print((t2-t1).total_seconds())



