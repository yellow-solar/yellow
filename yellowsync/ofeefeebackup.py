"""
Pull key tables off zoho and append to tables in Yellow DB (AWS RDB service)

Tables include:
 - commisions
 - finance tables
 - employee points
 - agent dream scores
 - performance measures
"""

# Standard libraries
import io, os, json

# Third party libraries
import sqlalchemy as db

# Local appication imports
from yellowsync.API.yellowDB import yellowDBSync

# List of tables to backup
TABLES_AND_FORM_LINKS = {
    'Historic_Cashflows_Report':'Add_Historic_Cashflow',
    'All_Agent_Payments':'Agent_Payments',
    'Commission_Weekly_Earnings_per_Agent_Report':'Commission_Weekly_Earnings_per_Agent',
    'Commissions_Per_Customer_Report':'Commissions_Per_Customer',
    'Agent_Score_Report':'Agent_Score',
    'Performance_Records_Report':'Performance_Records',
    'Daily_Employee_Performance_per_Task1':'Daily_Employee_Performance_per_Task',
    'Daily_Employee_Points1':'Daily_Employee_Points',
    'Monthly_Employee_Performances_per_Task':'Monthly_Employee_Performance_per_Task',
    'Monthly_Employee_Points2':'Monthly_Employee_Points',
    'Monthly_Team_Performances_per_Task':'Monthly_Team_Performance_per_Task',
    'Commissions_Transactions_Report':'Commissions_Transactions',
    'FS_Account_Transactions_Report':'FS_Account_Transactions',
    'FS_Accounts_Report':'FS_Accounts',
    'Payslip_Report':'Payslip',
    'All_Agents':'Add_Agent',
}

# Yellow DB
# Access the config for DB
os.environ['env'] = 'd'
with open('config/config.json', 'r') as f:
    db_cfg = json.load(f)[f"yellowdb{os.environ['env']}"]

# Create engine for connections pool
engine = db.create_engine(
    f"{db_cfg['driver']}{db_cfg['user']}:{db_cfg['passwd']}@{db_cfg['host']}/Zoho", 
    echo = True,
    )
# Creat a connection for executing the delete queries
connection = engine.connect()

"""
Loop through tables in dictionary and sync their tables/forms to the database 
 - tables will be called tablename+'_Backup'
 - append tables if they already exist
"""
for table in TABLES_AND_FORM_LINKS.keys():
    try:
        yellowDBSync(
            table = table,
            schema = 'Zoho',
            # form link for table in zoho - if no link provided use table name as form link
            form_link = TABLES_AND_FORM_LINKS.get(table,table),
            # insert_cols_rename = {'ID':'zoho_ID','COMMENTS':'Comments', 'Date1':'Trn_Date'},
            if_exists='append',
        )
    except:
        print("Failed to append, deleteing and re-creating")
        yellowDBSync(
            table = table,
            schema = 'Zoho',
            # form link for table in zoho - if no link provided use table name as form link
            form_link = TABLES_AND_FORM_LINKS.get(table,table),
            # insert_cols_rename = {'ID':'zoho_ID','COMMENTS':'Comments', 'Date1':'Trn_Date'},
        )

    #Clear out the data added to the backup tables from longer than 15 days
    query = f"""
    delete from {table}_Backup
    where added_datetime < DATE_SUB(NOW(), INTERVAL 15 DAY)
    """
    delete_request = connection.execute(query)




