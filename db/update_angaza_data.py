import sqlalchemy as db
from sqlalchemy import MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

# Format for a db engine
# mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>
# NB WARNING::: NEED TO FIGURE OUT HOW TO PULL PASSWORD FILE
user = 'admin'
password = 'Op3nS3sam3'
mysql_driver = 'mysql+mysqldb://'
db_host='dbyellow1.cfxeszy05os0.eu-west-1.rds.amazonaws.com:3306'
schema = 'Angaza'

# Create engine for connections pool
engine = db.create_engine(mysql_driver 
    + user + ':' + password 
    + '@' + db_host + '/' + schema,
    echo = True)

# Create metadata for the engine
metadata = MetaData()
metadata.reflect(bind=engine)

# Create clients table SQLALCHEMY table from the existing connection
clients = db.Table('clients', metadata, autoload=True, 
    autoload_with=engine)

# Procure a connection resource
# db_session = sessionmaker(bind=engine)
connection = engine.connect()

# Read new clients csv
df_csv = pd.read_csv('../data/clients.csv')
client_cols = ['client_external_id', 'organization','client_name',
    'phone_number','account_numbers','recorder','date_created_utc',	
    'archived',	
    ]
df_csv = df_csv.rename(columns={'angaza_id':'client_external_id'})[client_cols]

# Session = sessionmaker(engine)
# session = Session()
with connection.begin() as trans:
    resultProxy = connection.execute(''' 
        create temporary table TempTable
        select *
        from clients 
        where client_id is null
    ''')
    trans.commit()
    df_csv.to_sql(con=connection, name='TempTable', 
        if_exists='replace', index=False)

# Pull all values froma  table and save to pandas df, then csv
# results = connection.execute(db.select([clients])).fetchall()
# df_sql = pd.DataFrame(results)
# df_sql.columns = results[0].keys()
# df_sql = df_sql[client_cols]


# df.to_csv('test_output.csv')

# Insert full client table
# df.to_sql(con=connection, name='clients', 
#     if_exists='append', index=False)

# Adding single/multiple values via connection
# query = db.insert(clients) 
# values_list = [
#     {
#     # 'client_id':100, 
#     # 'external_source_id':'AC1',
#     'organization':'yellow', 
#     'client_name':'DD MABUZA',
#     'phone_number': 265999576414,
#     'account_numbers': '265999576414, 2659995764',
#     'recorder':'Some Other Guy',
#     'added_user':user
#     }
#     ]
# ResultProxy = (connection
#     .execute(query,values_list))

