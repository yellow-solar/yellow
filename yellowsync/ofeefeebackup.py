"""
Pull key tables off zoho and append to tables in Yellow DB (AWS RDB service)

Tables include:
 - commisions
 - finance tables
 - employee points
 - agent dream scores
 - performance measures
"""

# Local appication imports
from yellowsync.API.yellowDB import yellowDBSync

# List of tables to backup
# Historics cashflows table - test if append works, and the table name is updated to ***_Backup
TABLES_AND_FORM_LINKS = {
    'Historic_Cashflows_Report':'Add_Historic_Cashflow',
    'Weekly_Commissions_Per_Agent':'Weekly_Commissions_Per_Agent',
}

"""
Loop through tables in dictionary and sync their tables/forms to the database 
 - tables will be called tablename+'_Backup'
 - append tables if they already exist
"""
for table in TABLES_AND_FORM_LINKS.keys():
    yellowDBSync(
        table = table,
        schema = 'Zoho',
        # form link for table in zoho - if no link provided use table name as form link
        form_link = TABLES_AND_FORM_LINKS.get(table,table),
        # insert_cols_rename = {'ID':'zoho_ID','COMMENTS':'Comments', 'Date1':'Trn_Date'},
        if_exists='append',
    )


#TODO: clear out the data added to the backup tables from longer than 1 month, but keep the last value
"""Can do it with a sql statement I think
"""
#sql
f"""
delete from {table}_Backup
where added_time < current_time()-30days
"""

