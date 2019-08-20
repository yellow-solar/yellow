""" Set of functions to interact with gmail API
        - alerts
        - reports etc.
 """

#  standard
import os, json, base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# third party
from httplib2 import Http
from oauth2client import file, client, tools
from google.oauth2 import service_account
from googleapiclient import discovery, errors

# Google drive API config
class Gmail():
    """gmail service creator with input account file"""
    def __init__(self, filename, user):
        self.filename = filename
        self.user = user
        self.scopes = ['https://mail.google.com/',
                        # 'https://www.googleapis.com/auth/gmail.compose',
                        # 'https://www.googleapis.com/auth/gmail.send',
                        ]
        self.credentials = (service_account
            .Credentials.from_service_account_file(
                self.filename, 
                scopes=self.scopes,
                subject=self.user,
                )
        )    
        # Initiate gdrive service
        self.service = discovery.build('gmail', 'v1',
            credentials=self.credentials)

    def create_message(self, sender, to, subject, message_text, html=None):
        """Create a message for an email.

        Args:
            sender: Email address of the sender.
            to: Email address of the receiver.
            subject: The subject of the email message.
            message_text: The text of the email message.

        Returns:
            An object containing a base64url encoded email object.
        """
        if html is None:
            message = MIMEText(message_text)
        else :
            message = MIMEMultipart(
                "alternative", None, [MIMEText(message_text), MIMEText(html,'html')])
        message['to'] = to
        message['from'] = sender
        message['subject'] = subject
        return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
        # msg.set_payload(contents)
        # Encode the payload using Base64.  This line is from here:
        # https://docs.python.org/3/library/email-examples.html
        # encoders.encode_base64(msg)

    def send_message(self, message, user_id="me"):
        """Send an email message.

        Args:
            service: Authorized Gmail API service instance.
            user_id: User's email address. The special value "me"
            can be used to indicate the authenticated user.
            message: Message to be sent.

        Returns:
            Sent Message.
        """
        try:
            message = (self.service.users().messages()
                    .send(userId=user_id, body=message)
                    .execute())
            print(f"Message Id: {message['id']}")
            return message
        except errors.HttpError as error:
            print(f"An error occurred: {error}")
            raise


# Credentials object creation with config file





