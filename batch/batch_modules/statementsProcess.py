""" Functions for importing statements  
        Called from statements_sync.py
        Fetches statements from relevant folders and uploads to yellowDB
"""

# Standard libaries
import os
import re

# Third party
import pandas as pd

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

### run airtel pickup:
# call function to get dataframe from latest files in google drive
# - need to update
# df = pd.read_csv('2019-07-24.csv', header = 4)
# statementDF(df,
# 			col_mapping = {
# 				'provider_id':'Transaction ID',
# 				# 'Sender Msisdn':'',
# 				'trn_timestamp':'Date',
# 				'trn_ref_number':'Reference Number',
# 				'trn_amount':'Amount',
# 			}
# 	)