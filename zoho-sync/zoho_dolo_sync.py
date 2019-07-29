import zohoAPI
import pandas as pd
from datetime import datetime
from zohoAPI import ZohoAPI, dfUploadSync, formDelete

# Config for zoho sync calls and log
form = "Dolo_Scores"

# Print timestamp for log
print(form + " upload sync:", datetime.now().strftime("%H:%M:%S"))

# Create connection
zoho = ZohoAPI('yellow679', 'bdbda4796c376c1fb955a749d47a17e7', 'collections-management')

# Import dolo scores and format for upload
accounts_enriched = pd.read_csv('../data/accounts_enriched.csv').replace("<","and",regex=True).replace(">","and",regex=True).replace("&","and",regex=True)
dolo_update_data = accounts_enriched[['AngazaID','DoloTier']].rename(columns = {'AngazaID':'Angaza_ID','DoloTier':'Dolo_Tier'}).replace("&","and",regex=True)


# Time this whole thing
t1 = datetime.now()

# Delete dolo data
# delete = formDelete(form, zoho)
delete = zoho.add("API_Triggers", payload = {"trigger_command":"delete","form":form}) # via the trigger table

# Upload in chunks and save responses from zoho RPC API
if delete.status_code==200:
    dolo_upload = dfUploadSync(df = dolo_update_data,form=form, zoho=zoho, slice_length=5000)
else:
    raise ValueError(form + "delete request failed with status code:" + str(delete.status_code))

t2 = datetime.now()
print((t2-t1).total_seconds())