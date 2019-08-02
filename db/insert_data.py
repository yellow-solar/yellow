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
db_host = 'dbyellow1.cfxeszy05os0.eu-west-1.rds.amazonaws.com:3306'
schema = 'Angaza'

engine = db.create_engine(mysql_driver + 
    user + ':' + password +
    '@' + db_host + '/' + schema,
    echo = True)

# db_session = sessionmaker(bind=engine)
connection = engine.connect()

# Create metadata for the engine
metadata = MetaData()
metadata.reflect(bind=engine)

clients = db.Table('clients', metadata, autoload=True, autoload_with=engine)

query = db.insert(clients) 
values_list = [
    {
    # 'client_id':100, 
    'external_source_id':'AC1',
    'organization':'yellow', 
    'client_name':'DD MABUZA',
    'phone_number': 265999576414,
    'account_numbers': '265999576414, 2659995764',
    'recorder':'Some Other Guy',
    'added_user':user
    }
    ]
ResultProxy = (connection
    .execute(query,values_list))

results = connection.execute(db.select([clients])).fetchall()
df = pd.DataFrame(results)
df.to_csv('test_output.csv')
