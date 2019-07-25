import os
import pandas as pd
import numpy as np
import re
from pandas.tseries.offsets import MonthEnd
from datetime import timedelta
from datetime import datetime
import time
import itertools
import sqlite3
import requests
from requests.auth import HTTPBasicAuth
from io import StringIO

# Pandas options
pd.set_option('display.max_rows', 150)
pd.set_option('display.max_columns', 50)
# pd.set_option('display.float_format', lambda x: "%.2" % x)
# pd.set_option('display.width', 1000)

today_ts = pd.to_datetime('today').round('1s')
today_string = pd.to_datetime('today').strftime('%Y-%m-%d')
current_month_key = pd.to_datetime('today').strftime('%Y%m')

# Path to data
data_path = 'data'

# Angaza user and password
username = 'mike@pacafrica.co.za'
password = 'Yellow#32'

### Snapshots
# Configure urls for snapshots
# requests.get(base_url+accounts, auth=HTTPBasicAuth(username, password))
base_url = 'https://payg.angazadesign.com/api/snapshots'
APIs = {'clients':'/clients',
        'accounts':'/accounts',
        'payments':'/payments',
        'replacements':'/replacements',
        'prospects':'/prospects',
        'stock_unit_statuses':'/stock_unit_statuses',
        'receipts':'/receipts',
        'agents':'/agents'}

for APIname in APIs.keys():
    print('Requesting from: ' + APIname)
    snapshot = requests.get(base_url+APIs[APIname], auth=HTTPBasicAuth(username, password))
    if snapshot.status_code == 200:
        print("Request successful. Storing csv...")
        snapshot_df = pd.read_csv(StringIO(snapshot.content.decode('utf-8'))).replace('None',np.NaN) 
        snapshot_df.to_csv(data_path+APIs[APIname]+'.csv', index=False)
    else:
        raise ValueError()
    if APIname == 'accounts':
        snapshot_df.to_csv(data_path + '/'+'accounts '+ current_month_key +'.csv', index=False)

print("Finished.")

### Account snapshots
print('Running account snapshots check...')

# Create list of currently downloaded months
month_keys_downloaded = []
for f in os.listdir(data_path):
     if re.search("[0-9]{6}", f):
        month_key = re.findall("[0-9]{6}", f)[0]
        month_keys_downloaded.append(month_key)

# Url for end of month accounts
url_eom = "https://payg.angazadesign.com/api/end_of_month_reports?factory_name=accounts&month={0}&year={1}"

# Month keys for the period up until last month from the first month we have angaza data
month_keys = pd.date_range('2018-09-01',today_string, freq='MS').strftime("%Y%m").tolist()

# Check you have all accounts snapshots
for month in month_keys:
    # check if it is already downloaded, then just import
    if (month not in month_keys_downloaded):
        print("Fetching: " + month)
        # request the data from Angaza for the missing month
        accounts = requests.get(url_eom.format(int(month[4:6]),int(month[0:4])), auth=HTTPBasicAuth(username, password))
        
        # if request raised successfully
        if accounts.status_code == 200:
            account_data = pd.read_csv(StringIO(accounts.content.decode('utf-8'))).replace('None',np.NaN)
            account_data.to_csv(data_path+'/accounts '+month+'.csv', index=False)
            print("Downloaded: " + month)
        # if return status not 200, raise exception on value
        else:
            raise ValueError("Status not 200: Request to Angaza for " + str(month) + " failed")
    else:
        print("Already downloaded: " + month)