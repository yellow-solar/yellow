import json, io
from datetime import datetime

from yellowsync.API.zohoAPI import ZohoAPI

# Fetch zoho cfg and setup API connection
with open('config/config.json', 'r') as f:
    zoho_cfg = json.load(f)['zoho']
zoho = ZohoAPI(zoho_cfg['zc_ownername'], zoho_cfg['authtoken'], zoho_cfg['app'])

print(datetime.now())
delete = zoho.add("API_Triggers", payload = {"trigger_command":"delete","form":"Replaced_Units_Record"})
deleted_report = zoho.get(form = "Replaced_Units_Record_Report", payload={'raw':'true',})
# x = zoho.delete('Payments_Data_Import', 'ID!=null')
report_json = json.loads(deleted_report.text)
print(deleted_report.text)

print(datetime.now())