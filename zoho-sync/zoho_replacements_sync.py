import zohoAPI
import pandas as pd
from datetime import datetime 
from zohoAPI import ZohoAPI, dfUploadSync, formDelete

# Config for zoho sync calls and log
form = "Replaced_Units_Record"

# Print timestamp for log
print(form +" upload sync:", datetime.now().strftime("%H:%M:%S"))

# Create connection
zoho = ZohoAPI('yellow679', 'bdbda4796c376c1fb955a749d47a17e7', 'collections-management')

# Angaza table import
data = pd.read_csv('../data/replacements.csv').replace("&","and",regex=True)
# Header tables
header = pd.read_csv('headers/replacements_header.csv')
# Stock table headers
data = data[header.columns.values].fillna('')

# Time this whole thing
t1 = datetime.now()

# Delete all records
# delete = formDelete(form=form, zoho=zoho)
delete = zoho.add("API_Triggers", payload = {"trigger_command":"delete","form":form}) # via the trigger table

# Upload table
if delete.status_code==200:
    upload = dfUploadSync(df = data, form=form, zoho=zoho, slice_length=2000)
else:
    raise ValueError(form + " delete request failed with status code:" + str(delete.status_code))
    
# End timing
t2 = datetime.now()
print((t2-t1).total_seconds())



