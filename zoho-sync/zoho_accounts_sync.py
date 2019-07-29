import zohoAPI
import pandas as pd
from datetime import datetime 
from zohoAPI import ZohoAPI, dfUploadSync, formDelete

# Config for zoho sync calls and log
form = "Accounts_Data_Import"

# Print timestamp for log
print(form + " upload sync:", datetime.now().strftime("%H:%M:%S"))

# Create connection
zoho = ZohoAPI('yellow679', 'bdbda4796c376c1fb955a749d47a17e7', 'collections-management')

### Accounts table
# Import column headers for accounts
accounts_header = pd.read_csv('headers/account_headers.csv').replace("&","and",regex=True)
# Import accounts and reformat write-off heading
accounts = pd.read_csv('../data/accounts.csv')
# Rename angaza col name to Zoho col name 
accounts = accounts.rename(columns={'date_of_write-off':'date_of_write_off'})
# Fill NULL values with blank strings
accounts = accounts[accounts_header.columns.values].fillna('')
# Covnert the customer age floats to integer
accounts.customer_age = accounts.customer_age.apply(lambda x: int(x) if x != '' else x)
# Remove detached and written-off acccounts
accounts = accounts[~accounts.account_status.isin(['DETACHED', 'WRITTEN_OFF'])]

# Time the API queries
t1 = datetime.now()

# Delete accounts data
# delete = formDelete(form, zoho)
delete = zoho.add("API_Triggers", payload = {"trigger_command":"delete","form":form}) # via the trigger table

# Run the synchronous XML upload with slide length of 500 rows
if delete.status_code==200:
    accounts_upload = dfUploadSync(df = accounts, form=form, zoho=zoho, slice_length=500)
else:
    raise ValueError(form + " delete request failed with status code:" + str(delete.status_code))

t2 = datetime.now()
print((t2-t1).total_seconds())



