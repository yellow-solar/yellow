import sqlalchemy as db
import pandas as pd
# Format for a db engine
# mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>
# NB WARNING::: NEED TO FIGURE OUT HOW TO PULL PASSWORD FILE
user = 'admin'
engine = db.create_engine('mysql+mysqldb://admin:Op3nS3sam3@dbyellow1.cfxeszy05os0.eu-west-1.rds.amazonaws.com:3306/db?charset=utf8mb4')

connection = engine.connect()
metadata = db.MetaData()

clients = db.Table('accounts', metadata, autoload=True, autoload_with=engine)

query = db.insert(clients) 
values_list = [{
    'client_id':101, 
    'organization':'yellow', 
    'client_name':'DD MABUZA',
    'phone_number': 265999576414,
    'account_numbers': '265999576414, 2659995764',
    'recorder':'Some Other Guy',
    'added_user':user
    }]
ResultProxy = (connection
    .execute(query,values_list))

results = connection.execute(db.select([clients])).fetchall()
df = pd.DataFrame(results)
