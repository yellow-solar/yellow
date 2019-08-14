""" Functions for importing statements  
        Called from statements_sync.py
        Fetches statements from relevant folders and uploads to yellowDB
"""

# Standard libaries
import os
import re

# Third party
import pandas as pd
from sqlalchemy import exists

# Local libraries
from batch.batch_modules.gdrive import (getFolderID, getFileIDs, 
                                  getCSV, setArchive)

def statementDF(df, col_mapping):
    """ Process a csv and return a standard pandas dataframe for 
        upload to mobile statements table """    

    # Format headings
    # df.columns = [re.sub(r'\s', '_', x) for x in df.columns.values]

    # Return 
    cols_found = [(x in df.columns) for x in col_mapping.values()]
    if all(cols_found):
        # if personalised have to check more:
        col_mapping = {v: k for k, v in col_mapping.items()}
        df = df.rename(columns = col_mapping)
        df = df[col_mapping.values()]
    else:
        raise KeyError(f"Column mappings found {cols_found}, {col_mapping.values()}")

    # Drop duplicates
    df = df.drop_duplicates()

    return(df)



def uploadStatement(gdrive_file, gdrive_service, archive_id, folder_id,  
                        session, mobile, **kwargs):
    """ Upload statement based on given file, gdrive service and 
            csv format arguments 
            - datetime columns
            - header row for statement csv (0 is first row)
            - column mapping
    """
    
    # Additional columns to set values for in the colun (statement level)
    # e.g. Current, Provider name, User etc.
    add_table_args = kwargs.get('add_table_args')

    # Get csv in dataframe
    df = getCSV(gdrive_file, gdrive_service, dt_columns=kwargs['dt_columns'], header=kwargs['header_row'])
    # If not a df, continue
    if df is None:
        # use this if statement to delete/move non-csv files
        if gdrive_file.get("id") != archive_id:
            print(f"file {gdrive_file.get('name')} is not a readable csv file")
        return

    # Process df
    processed_df = statementDF(
        df,
        col_mapping = kwargs.get('col_mapping')
    )
    
    # Create the dict to add to 
    rows_dict_list = processed_df.to_dict(orient="records")
    
    # Upload row by row
    for row_dict in rows_dict_list:
        # Create transaction objects trn with statement data plus other
        trn = mobile(
            **row_dict,
            **add_table_args
            )
        # Check whether mobile provider trn id exists 
        trn_exists = (session
            .query(exists()
                .where(mobile.provider_id==row_dict['provider_id']))
            .scalar())
        # If it does not exist, add it
        if not trn_exists:
            session.add(trn)
        # If it already exists, get info for it, check if ref or status has changed 
        # and only then update
        else:
            print(row_dict['provider_id'] + ' already exists.')
            trn = session.query(mobile).filter_by(provider_id=row_dict['provider_id'])
            # Update if ref or status has changed
            if (trn.first().trn_ref_number!=row_dict['trn_ref_number'] 
                or trn.first().trn_status!=row_dict['trn_status']):
                trn.update({**row_dict,**{'changed_user':add_table_args['changed_user']}})
                session.flush()
                print('Updated.')
        
    # try commit and move file. If it failes, rollback and raise exception
    try: 
        session.commit()
        #if successful, move file to archive
        _ = gdrive_service.files().update(
                                        fileId=gdrive_file.get("id"),
                                        addParents=archive_id,
                                        removeParents=folder_id,
                                        fields='id, parents').execute()
    except:
        raise
    finally:
        session.close()

    # print(processed_df.loc[0,'json'])
    return


def airtelDFFromFile(file_path):
    """ Process a csv and return a pandas dataframe for upload """    
    airtel_history = pd.DataFrame()
    for file_name in os.listdir(file_path):
        # print(file_path + file_name)
        data = pd.read_csv(file_path + file_name, header=4)

        # Check if file is a header
        if "Transaction ID" in data.columns.values:
            airtel_history = airtel_history.append(data)

    # Format headings
    airtel_history.columns = [re.sub(r'\s', '_', x) for x in airtel_history.columns.values]
    # Drop duplicates
    airtel_history = airtel_history.drop_duplicates()
    # Drop S. No
    airtel_history = airtel_history.drop(['S._No.'], axis=1)

    return(airtel_history)
    
    