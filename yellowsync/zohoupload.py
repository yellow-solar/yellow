"""Take the downloaded csv and upload in chunks to Zoho

    Program deletes the current records first
    Program uses an argument with the call 

    - need to push to zoho from Yellow DB rather
"""

# Standard library
from datetime import datetime 
import os
import json
import sys

# Import 3rd party libaries
import pandas as pd

# Import Yellow libaries
from yellowsync.API import zohoAPI
from yellowsync.API.zohoAPI import ZohoAPI, dfUploadSync, formDelete
from yellowsync.API.angazaAPI import AngazaAPI
from yellowsync.API.yellowDB import yellowDBSync

def uploadForm(form, file, header_name=None, int_cols=[], 
                slice_length=100, col_rename=None, row_filters={}, 
                field_cutoff=[], round_dict=None, fresh_data=False):
    """ Upload an Angaza table to zoho given a form """

    # Fetch zoho cfg and setup API connection
    with open('config/config.json', 'r') as f:
        zoho_cfg = json.load(f)['zoho']
    zoho = ZohoAPI(zoho_cfg['zc_ownername'], zoho_cfg['authtoken'], zoho_cfg['app'])

    # Print timestamp for log
    print(form + " upload sync:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    ### Data table
    # Import data from file
    if fresh_data:
        angaza = AngazaAPI()
        data = angaza.pullSnapshot(tablename = file)
    else:
        data = pd.read_csv(f'../data/{file}.csv')
    
    # Fill NA with blank string and correct strings 
    # (NB Blank strings do not send to Zoho to increaser performance)
    data = data.fillna('')
    # Round floats to necessary decimal places
    if round_dict is not None:
        data = data.round(round_dict)

    # Replace the '-' with '_' to match the Zoho field index
    data.columns = [x.replace('-','_').replace('/','_').rstrip() for x in data.columns.values]

    ### Optional parts
    # Import column headers if required
    if header_name is not None:
        data_header = pd.read_csv(f'headers/{header_name}_header.csv')
        data = data[data_header.columns.values]
    # Rename angaza col name to Zoho col name 
    if col_rename is not None:
        data = data.rename(columns=col_rename)
    # Filter rows
    for col in row_filters.keys():
        data = data[~data[col].isin(row_filters[col])]
    #shorten length of payment notes
    for col in field_cutoff:
        data[col] = data[col].apply(lambda x: x[0:250] if len(x) > 250 else x)
    # Convert the float type which are now strings (due to fillna in previous step) to integer
    for col in int_cols:
        data[col] = data[col].replace('[^0-9]','',regex=True).apply(lambda x: int(x) if x != '' else x)
    

    # Time the API queries
    t1 = datetime.now()

    # Delete zoho data
    # delete = formDelete(form, zoho)
    delete = zoho.add("API_Triggers", payload = {"trigger_command":"delete","form":form}) # via the trigger table

    # Run the synchronous XML upload with slide length
    if delete.status_code==200:
        upload = dfUploadSync(df = data, form=form, zoho=zoho, slice_length=slice_length)
    else:
        raise ValueError(form + " delete request failed with status code:" + str(delete.status_code))

    t2 = datetime.now()
    print((t2-t1).total_seconds())

    return(upload)

# Run based on input
def uploadSwitch(form):
    """ 
    the dictionary based dispatch table does not work in this case 
    I actually want to run the SAME function with different
    parameters depending on the switch value, rather than a different
    function depending on the switch value
    """
    pass
# upload_switch(form)

if __name__ == "__main__":
    """
    when Cron calls the zoho script, it must call with the form 
    name input 
    """
    # assign form from sys.args (1st is the )
    if len(sys.argv) > 1:
        form = sys.argv[1]
    else:
        raise Exception("Expecting form as argument to call upload")

    # Accounts import
    if form == "Accounts_Data_Import":
        uploadForm(
            form="Accounts_Data_Import",
            file="accounts",
            header_name="accounts",
            col_rename={'date_of_write-off':'date_of_write_off'},
            int_cols = [
                'account_number',
                'previous_account_number',
                'owner_msisdn',
                'next_of_kin_contact_number',
                'customer_age',
                'minimum_payment_amount',
                ],
            slice_length = 500,
            round_dict = {'hour_price':8,'minimum_payment_amount':0},
            )

    # Agents import - old
    elif form == "Agents_Data_Import":
        uploadForm(
            form="Agents_Data_Import",
            file="agents",
            header_name="agents",
            int_cols = [
                'limit_amount',
                'phone',],
            slice_length = 1000,
            row_filters={'role':['Administrator','Operator', 'Viewer']}
            )

    # Agents/user import - new
    elif form == "Users_Data_Import":
        uploadForm(
            form="Agents_Data_Import",
            file="users",
            header_name="users",
            int_cols = [
                'limit_amount',
                'phone',],
            slice_length = 1000,
            # row_filters={'role':['Administrator','Operator', 'Viewer']}
            )

    # Applications_Credit_Details
    elif form == "Applications_Credit_Details":
        uploadForm(
            form="Applications_Credit_Details",
            file="prospects",
            header_name="applications_credit_details",
            int_cols = [
                'total_annual_income_from_other_sources_not_listed_above_in_mwk',],
            slice_length = 350,
            )
        
    # Applications_Personal_Details
    elif form == "Applications_Personal_Details":
        uploadForm(
            form="Applications_Personal_Details",
            file="prospects",
            header_name="applications_personal_details",
            int_cols = ['phone',],
            slice_length = 1000,
            )

    # Payments
    elif form == "Payments_Data_Import":
        uploadForm(
            form="Payments_Data_Import",
            file="payments",
            header_name="payments",
            int_cols = [
                'account_number',
                'phone',
                ],
            slice_length = 1200,
            field_cutoff=['payment_note']
            )

    # Replaced_Units_Record
    elif form == "Replaced_Units_Record":
        uploadForm(
            form="Replaced_Units_Record",
            file="replacements",
            header_name="replacements",
            int_cols = [
                'account_number',
                'new_unit_number',
                'old_unit_number',
                ],
            slice_length = 2000,
            # fresh_data=True,
            )

    # Stock_Data_Import
    elif form == "Stock_Data_Import":
        uploadForm(
            form="Stock_Data_Import",
            file="stock_unit_statuses",
            header_name="stock",
            int_cols = [
                'account_number',
                'unit_number',
                ],
            slice_length = 2000,
            )

    # Dolo import
    elif form == "Dolo_Scores":
        uploadForm(
            form="Dolo_Scores",
            file="accounts_enriched",
            header_name="dolo",
            slice_length = 5000,
            )

    else:
        raise KeyError(f"There is no file sync upload called {form}")




