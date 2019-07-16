import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging
logger = logging.getLogger(__name__+' module')


class Email:
    def __init__(self, sender_email, password):
        self.port = 465  # For SSL
        self.smtp_server = "smtp.gmail.com"
        self.context = ssl.create_default_context()
        self.sender = sender_email
        self.password = password

    def __iter__(self):
        for a,v in self.__dict__.items():
            yield a,v


    def _payload_to_msg(self,payload,recipient):
        today = datetime.now().strftime('%Y-%m-%d')

        message = MIMEMultipart("alternative")
        message["Subject"] = "OEM to Algo Report {}".format(today)
        message["From"] = self.sender
        message["To"] = recipient

        text = """\
        OEM to Algolia report on {}.
        
        Records to be create:
        
        {}
        
        Records to be update:
        
        {}
        
        Records to be delete:
        
        {}
        
        """.format(today, payload.to_create, payload.to_update, payload.to_delete)

        text = MIMEText(text,"plain")
        message.attach(text)
        return message

    def send_result(self, recipient_emails: list, payload):
        logger.info('Sending Email')
        with smtplib.SMTP_SSL(self.smtp_server, self.port, context=self.context) as server:
            server.login(self.sender, self.password)
            for recipient in recipient_emails.split(','):
                msg = self._payload_to_msg(payload, recipient)
                server.sendmail(self.sender, recipient, msg.as_string())
                logger.info('Email sent to {}'.format(recipient))



