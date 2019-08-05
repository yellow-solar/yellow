import zohoAPI
import pandas as pd
from datetime import datetime 
from zohoAPI import ZohoAPI, dfUploadSync, formDelete

# Config for zoho sync calls and log
form = "Payments_Data_Import"
int_columns = [
'account_number',
'phone',
]

# Print timestamp for log
print(form + " upload sync:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Create connection
zoho = ZohoAPI('yellow679', 'bdbda4796c376c1fb955a749d47a17e7', 'collections-management')


# Angaza table import
data = pd.read_csv('../data/payments.csv')
# Header tables
header = pd.read_csv('headers/payments_header.csv')

# Stock table headers
data = data[header.columns.values].fillna('')
#shorten length of payment notes
data.payment_note = data.payment_note.apply(lambda x:x[0:250] if len(x) > 250 else x)
# Convert the customer age, accounts, etc. strings to integer
for col in int_columns:
    data[col] = data[col].replace('[^0-9]','',regex=True).apply(lambda x: int(x) if x != '' else x)

# Time this whole thing
t1 = datetime.now()

# Delete all records
# delete = formDelete(form=form, zoho=zoho)
delete = zoho.add("API_Triggers", payload = {"trigger_command":"delete","form":form}) # via the trigger table

# Upload table
if delete.status_code==200:
    upload = dfUploadSync(df = data, form=form, zoho=zoho, slice_length=1200)
else:
    raise ValueError(form + " delete request failed with status code:" + str(delete.status_code))
    
# End timing
t2 = datetime.now()
print((t2-t1).total_seconds())



