import zoho_api
import pandas as pd
from datetime import datetime 
from zoho_api import ZohoAPI, dfUploadSync, formDelete

# Create connection
zoho = ZohoAPI('yellow679', 'bdbda4796c376c1fb955a749d47a17e7', 'collections-management')

# Time this whole thing
t1 = datetime.now()

### Accounts
# Import column headers for accounts
accounts_header = pd.read_csv('accounts_headers.csv')
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

# Delete accounts data
formDelete("Applications_Personal_Details", zoho)

# Run the synchronous XML upload with slide length of 500 rows
accounts_upload = dfUploadSync(df = accounts, form="Accounts_Data_Import", zoho=zoho, slice_length=500)

t2 = datetime.now()
print((t2-t1).total_seconds())



