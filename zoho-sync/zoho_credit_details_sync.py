import zohoAPI
import pandas as pd
from datetime import datetime 
import time
from zohoAPI import ZohoAPI, dfUploadSync, formDelete

# Config for zoho sync calls and log
form = "Applications_Credit_Details"

# Print timestamp for log
print(form + " upload sync:", datetime.now().strftime("%H:%M:%S"))

# Create connection
zoho = ZohoAPI('yellow679', 'bdbda4796c376c1fb955a749d47a17e7', 'collections-management')

### Prospects
# Angaza table import
prospects = pd.read_csv('../data/prospects.csv').replace("&","and",regex=True)
# Header tables
header = pd.read_csv('headers/applications_credit_details_header.csv')

# Personal details table tables
credit_details = prospects[header.columns.values].fillna('')
# Replace the '-' with '_' to match the Zoho field index
credit_details.columns = [x.replace('-','_').replace('/','_').rstrip() for x in credit_details.columns.values]
# Convert to integer - total_annual_income_from_other_sources_not_listed_above_in_mwk 
credit_details.total_annual_income_from_other_sources_not_listed_above_in_mwk  = credit_details.total_annual_income_from_other_sources_not_listed_above_in_mwk .apply(lambda x: int(x) if x != '' else x)


# Time this whole thing
t1 = datetime.now()

# Delete all credit details records
# delete = formDelete(form=form, zoho=zoho)
delete = zoho.add("API_Triggers", payload = {"trigger_command":"delete","form":form}) # via the trigger table

# Upload credit details table
if delete.status_code==200:
    app_credit_upload = dfUploadSync(df = credit_details, form=form, zoho=zoho, slice_length=350)
else:
    raise ValueError(form + " delete request failed with status code:" + str(delete.status_code))

# End timing
t2 = datetime.now()
print((t2-t1).total_seconds())



