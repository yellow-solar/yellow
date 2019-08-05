""" Script to input data in JSON """

# import standard libaries
import json

# import 3rd party libaries
import pandas as pd
import sqlalchemy as db
from sqlalchemy import MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Access the config for DB
with open('config.json', 'r') as f:
    db_cfg = json.load(f)['mysql']
schema = 'Angaza'

# Create engine for connections pool
engine = db.create_engine(
    f"""{db_cfg['driver']}{db_cfg['user']}:{db_cfg['passwd']}@{db_cfg['host']}/{schema}""", 
    echo = True)

# Create metadata for the engine
metadata = MetaData()
metadata.reflect(bind=engine)

# Create clients table Class table from the existing connection
clients = db.Table('clients', metadata, autoload=True, 
    autoload_with=engine)

# Procure a connection resource
# db_session = sessionmaker(bind=engine)
connection = engine.connect()

# Select columns to input to DB, rename if necessary
df = pd.read_csv('../data/clients.csv')
client_cols = ['angaza_id', 'organization','client_name',
    'phone_number','account_numbers','recorder','date_created_utc',	
    'archived',	
    ]
df = df.rename(columns={'angaza_id':'client_external_id'})[client_cols]

# Insert full client table
df.to_sql(con=connection, name='clients', 
    if_exists='append', index=False)

