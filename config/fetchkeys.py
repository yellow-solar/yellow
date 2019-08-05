import json

# Access the config for DB
with open('config.json', 'r') as f:
    cfg = json.load(f)['mysql']

user = cfg['user']
password = cfg['passwd']
mysql_driver = cfg['driver']
db_host= cfg['host']
schema = cfg['db']
