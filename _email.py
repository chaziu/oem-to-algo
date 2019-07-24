import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging
from etl import Compare
from typing import Generator, Tuple
logger = logging.getLogger(__name__+' module')


class Email:
    def __init__(self, sender_email: str, password: str) -> None:
        self.port = 465  # For SSL
        self.smtp_server = "smtp.gmail.com"
        self.context = ssl.create_default_context()
        self.sender = sender_email
        self.password = password

    # loop through attribute when enumerate
    def __iter__(self) -> Generator[Tuple[str, str], None, None]:  # Generator[YieldType,SendType,ReturnType]
        for attr, value in self.__dict__.items():
            yield attr, value

    def _payload_to_msg(self, payload: Compare, recipient: str) -> MIMEMultipart:
        date_stamp = datetime.now().strftime('%Y-%m-%d')

        message = MIMEMultipart("alternative")
        message["Subject"] = "OEM to Algo Report {}".format(date_stamp)
        message["From"] = self.sender
        message["To"] = recipient

        text = """\
        OEM to Algolia report on {}.
        
        Records to be create:
        
            * {}
        
        Records to be update:
        
            * {}
        
        Records to be delete:
        
            * {}
        
        """\
        .split.format(date_stamp,
                payload.to_create if payload.to_create else 'No records to be created',
                payload.to_update if payload.to_update else 'No records to be updated',
                payload.to_delete if payload.to_delete else 'No records to be deleted')

        text = MIMEText(text, "plain")
        message.attach(text)
        return message

    def send_result(self, recipient_emails: str, payload: Compare) -> None:
        logger.info('Sending Email')
        with smtplib.SMTP_SSL(self.smtp_server, self.port, context=self.context) as server:
            server.login(self.sender, self.password)
            for recipient in recipient_emailt(','):
                msg = self._payload_to_msg(payload, recipient)
                server.sendmail(self.sender, recipient, msg.as_string())
                logger.info('Email sent to {}'.format(recipient))



