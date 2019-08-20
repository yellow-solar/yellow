"""" Test gmail tools """

# standard
from datetime import datetime

# third party
import pandas as pd

# local
from googleapi.gmail import Gmail

# Create email service
gmail = Gmail('config/mail-93851bb46b8d.json','ben@yellow.africa')

### Test hybrid html
# Read test table
df = pd.read_csv('data/test_table.csv')
df = df[df['trn_date'] > '2019-08-16']
df = df.sort_values('trn_date',ascending=False)

# Remove index 
df_html = df.to_html(index=False,classes='table table-striped')

# Create html
html = f"""
    <html><head>Hello, Friend.</head>
    <p>Here is your data:</p>
    {df_html}
    <p>Regards,</p>
    <p>Me</p>
    </body></html>
"""

# Create multimessage to send
msg = gmail.create_message(
    sender = "ben@yellow.africa",
    to = 'ben@yellow.africa',
    subject = 'test message',
    message_text = 'please open as HTML email',
    html=df_html,
)

# Send test
sent = gmail.send_message(msg, 
    user_id='ben@yellow.africa')
print(sent) 