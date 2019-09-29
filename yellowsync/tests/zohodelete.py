import json, io
from datetime import datetime

from yellowsync.API.zohoAPI import ZohoAPI

# Fetch zoho cfg and setup API connection
with open('config/config.json', 'r') as f:
    zoho_cfg = json.load(f)['zoho']
zoho = ZohoAPI(zoho_cfg['zc_ownername'], zoho_cfg['authtoken'], zoho_cfg['app'])


print(datetime.now())
x = zoho.delete('Payments_Data_Import', 'ID!=null')
print(x.text)

print(datetime.now())