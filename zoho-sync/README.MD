### Improvements:

RPC XML API:
You can open a requests session within a class rather, and then hit the same session with multiple calls in the loop
I.e. you will initialise the connection with authtoken and scope already, and it will open the connection to the api.


### Run all manually
`~/.venv/data-jobs/bin/python ~/prod-jobs/zoho-sync/zoho_manual_run1.py`
`~/.venv/data-jobs/bin/python ~/prod-jobs/zoho-sync/zoho_manual_run2.py`