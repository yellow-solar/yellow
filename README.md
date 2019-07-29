# prod-jobs
Set of scripts for product data jobs - download, sync, Dolo etc.


### The data folder MUST be in the previous directory else all will fail

I.e. You have 

home -> data
home -> prod-jobs -> angaza/zoho/yellow jobs

and

home -> prod-jobs -> header


ERRORS
Zoho sync - maximum url length breach looks like:

Error: 400 - see rpc request text for more detail
{"code":2945,"message":"PATTERN_NOT_MATCHED"


<response><errorlist><error><code>2830</code><message><![CDATA[The reference to entity "sweet" must end with the \';\' delimiter.]]></message></error><errorlist></response>'