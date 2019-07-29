import zohoAPI
import pandas as pd
from datetime import datetime 
from zohoAPI import ZohoAPI, dfUploadSync, formDelete

# Config for zoho sync calls and log
form = "Agents_Data_Import"

# Print timestamp for log
print(form + " upload sync:", datetime.now().strftime("%H:%M:%S"))

# Create connection
zoho = ZohoAPI('yellow679', 'bdbda4796c376c1fb955a749d47a17e7', 'collections-management')

# Angaza table import
agents_data = pd.read_csv('../data/agents.csv').replace("&","and",regex=True)
# Header tables
header = pd.read_csv('headers/agents_header.csv')

# Drop the non-agent lines
agents_data = agents_data[~agents_data.role.isin(['Administrator','Operator', 'Viewer'])]
# Convert the customer age floats to integer
agents_data.limit_amount = agents_data.limit_amount.apply(lambda x: int(x) if x != '' else x)
# Sync table filter on headers to upload
agents_data = agents_data[header.columns.values].fillna('')


# Time this whole thing
t1 = datetime.now()

# Delete all personal details records
# delete = formDelete(form, zoho)
delete = zoho.add("API_Triggers", payload = {"trigger_command":"delete","form":form}) # via the trigger table

# Upload personal details table
if delete.status_code==200:
    upload = dfUploadSync(df = agents_data, form=form, zoho=zoho, slice_length=1000)
else:
    raise ValueError(form + " delete request failed with status code:" + str(delete.status_code))
    
# End timing
t2 = datetime.now()
print((t2-t1).total_seconds())



