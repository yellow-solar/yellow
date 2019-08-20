import zohoAPI
import pandas as pd
import time
from datetime import datetime 
from zohoAPI import ZohoAPI, dfUploadSync, formDelete

# Config for zoho sync calls and log
form = "Stock_Data_Import"
int_columns = [
'account_number',
'unit_number',
]

# Print timestamp for log
print(form + " upload sync:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Create connection
zoho = ZohoAPI('yellow679', 'bdbda4796c376c1fb955a749d47a17e7', 'collections-management')


# Angaza table import
stock_data = pd.read_csv('../data/stock_unit_statuses.csv')
# Header tables
header = pd.read_csv('headers/stock_header.csv')
# Stock table headers
stock_data = stock_data[header.columns.values].fillna('')
# Convert integer columns from strings to integer
for col in int_columns:
    stock_data[col] = stock_data[col].replace('[^0-9]','',regex=True).apply(lambda x: int(x) if x != '' else x)


# Time this whole thing
t1 = datetime.now()

# Delete all personal details records
# delete = formDelete(form=form, zoho=zoho)
delete = zoho.add("API_Triggers", payload = {"trigger_command":"delete","form":form}) # via the trigger table

time.sleep(5)

# Upload personal details table
if delete.status_code==200:
    upload = dfUploadSync(df = stock_data, form=form, zoho=zoho, slice_length=2000)
else:
    raise ValueError(form + " delete request failed with status code:" + str(delete.status_code))
    
# End timing
t2 = datetime.now()
print((t2-t1).total_seconds())



