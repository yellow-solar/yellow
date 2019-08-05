
# clients table
# yellowDBSync(
#     table = "clients",
#     schema = 'Angaza',
#     # form_link = "Add_Historic_Cashflow",
#     insert_cols = ['angaza_id', 'organization','client_name',
#     'phone_number','account_numbers','recorder','date_created_utc',	
#     'archived',	
#     ],
#     insert_cols_rename = {'angaza_id':'client_external_id'}
# )
from batch_modules.yellow_db_sync import yellowDBSync

yellowDBSync(
    table = "Historic_Cashflows_Report",
    schema = 'Zoho',
    form_link = "Add_Historic_Cashflow",
    insert_cols = [
        'Account_Paid_From',
        'Account_Paid_Into',
        'Cashflow_Category',
        'Amount_in_MK',
        'Amount_in_ZAR',
        'Amount_in_USD',
        'Description',
        'COMMENTS',
        ],
    insert_cols_rename = {'ID':'zoho_ID','COMMENTS':'Comments'}
)
#     zoho_cols_rename = {'angaza_id':'client_external_id'}
# )
