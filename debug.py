# standard
import os,sys,json

# third party

# local
from yellowsync.API.yellowDB import yellowDBSync

# import batch.mobilestatements
# import batch.yellowdbsync
# import cashflow.cashflowrecon
# import batch.batch_modules.angazaAPI
# import googleapi.gmail

yellowDBSync(
    table = "receipts",
    schema = 'Angaza',
    index_label='angaza_id',
    )