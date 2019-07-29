import zoho_api
import pandas as pd
from datetime import datetime 
from zoho_api import ZohoAPI, dfUploadSync, formDelete

# Create connection
zoho = ZohoAPI('yellow679', 'bdbda4796c376c1fb955a749d47a17e7', 'collections-management')

# Time this whole thing
t1 = datetime.now()

# Import dolo scores and format for upload 
accounts_enriched = pd.read_csv('../data/accounts_enriched.csv')
dolo_update_data = accounts_enriched[['AngazaID','DoloTier']].rename(columns = {'AngazaID':'Angaza_ID','DoloTier':'Dolo_Tier'})

# Delete dolo data
formDelete("Dolo_Scores", zoho)

# Upload in chunks and save responses from zoho RPC API
dolo_upload = dfUploadSync(df = dolo_update_data,form="Dolo_Scores", zoho=zoho, slice_length=5000)

t2 = datetime.now()
print((t2-t1).total_seconds())