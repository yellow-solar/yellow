import zohoAPI
import pandas as pd
from datetime import datetime 
from zohoAPI import ZohoAPI, dfUploadSync, formDelete

# Config for zoho sync calls and log
form = "Applications_Personal_Details"
int_columns = [
'phone',
]

# Print timestamp for log
print(form + " upload sync:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Create connection
zoho = ZohoAPI('yellow679', 'bdbda4796c376c1fb955a749d47a17e7', 'collections-management')

### Prospects
# Angaza table import
prospects = pd.read_csv('../data/prospects.csv')
# Header tables
app_personal_details_header = pd.read_csv('headers/applications_personal_details_header.csv')
# Personal details table tables
personal_details = prospects[app_personal_details_header.columns.values].fillna('')
# Convert integer columns from strings to integer
for col in int_columns:
    personal_details[col] = personal_details[col].replace('[^0-9]','',regex=True).apply(lambda x: int(x) if x != '' else x)


# Time this whole thing
t1 = datetime.now()

# Delete all personal details records
# delete = formDelete(form, zoho)
delete = zoho.add("API_Triggers", payload = {"trigger_command":"delete","form":form}) # via the trigger table

# Upload personal details table
if delete.status_code==200:
    app_personal_upload = dfUploadSync(df = personal_details, form=form, zoho=zoho, slice_length=1000)
else:
    raise ValueError(form + " delete request failed with status code:" + str(delete.status_code))
    
# End timing
t2 = datetime.now()
print((t2-t1).total_seconds())


