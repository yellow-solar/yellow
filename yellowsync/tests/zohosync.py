""" Test sync of a zoho table to the yelow database
"""

# System
import os, sys, json
from datetime import datetime

# Third party
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
import pandas as pd

# Local
from yellowsync.API.yellowDB import yellowDBSync
from yellowsync.API.zohoAPI import ZohoAPI

TABLENAME = 'All_Agent_Payments' 
FORM_LINK = 'Agent_Payments'
# RENAME_DICT = {'Date_field':'Date'}
RENAME_DICT=None

# Account balances
upload_ = yellowDBSync(
    table = TABLENAME,
    schema = 'Zoho',
    form_link = FORM_LINK,
)
# Agent payments
yellowDBSync(
    table = "All_Agent_Payments",
    schema = 'Zoho',
    form_link = "Agent_Payments",
)
