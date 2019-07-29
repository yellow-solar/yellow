import zoho_api
import pandas as pd
from datetime import datetime 
from zoho_api import ZohoAPI, dfUploadSync, formDelete

# Create connection
zoho = ZohoAPI('yellow679', 'bdbda4796c376c1fb955a749d47a17e7', 'collections-management')

# Time this whole thing
t1 = datetime.now()


### Prospects
# Angaza table import
prospects = pd.read_csv('../data/prospects.csv')

# Header tables
app_personal_details_header = pd.read_csv('applications_personal_details_header.csv')
app_credit_details_header = pd.read_csv('applications_credit_details_header.csv')

# Personal details table tables
personal_details = prospects[app_personal_details_header.columns.values].fillna('')
credit_details = prospects[app_credit_details_header.columns.values].fillna('')

# Delete all personal details records
formDelete("Applications_Personal_Details", zoho)
# Upload personal details table
app_personal_upload = dfUploadSync(df = personal_details, form="Applications_Personal_Details", zoho=zoho, slice_length=1000)

# Delete all credit details records
formDelete("Applications_Credit_Details", zoho)
# credit_xml = zoho.createXml(form='Applications_Credit_Details',data=prospects[app_credit_details_header.columns.values])
# Upload credit details table
app_credit_upload = dfUploadSync(df = personal_details, form="Applications_Personal_Details", zoho=zoho, slice_length=500)

# End timing
t2 = datetime.now()
print((t2-t1).total_seconds())



