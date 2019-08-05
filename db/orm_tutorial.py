from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
import sqlalchemy as db
from sqlalchemy import MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd


""" 
Use the ORM to do stuff 
"""

# Create the classes
Base = declarative_base(cls=DeferredReflection)

class Client(Base):
    __tablename__ = 'clients'
    
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

Base.prepare(engine)

Session = sessionmaker(bind=engine)
session = Session()
ed = Client(
    organization='yellow', country='Malawi',
    client_name='Ed Jones', 
    phone_number=265999576414, 
    account_numbers ='265999576414, 2659995764',
    recorder = 'Some Other Guy',
    added_user = user
    )
# One client and print the returned ID
session.add(ed)
print(ed.client_id) 
session.commit()

# Add another client and print returned ID
dd = Client(
    organization='yellow', country='Malawi',
    client_name='DD Mabuza', 
    phone_number=265999576414, 
    account_numbers ='265999576414, 2659995764',
    recorder = 'Some Other Guy',
    added_user = user
    )
session.add(dd)
print(dd.client_id) 
session.commit()


