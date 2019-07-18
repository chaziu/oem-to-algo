import configparser
import json


class Config:
    def __init__(self, env: str, config_path: str) ->None:
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.exhibitor_url = self.config[env]['exhibitor_url']
        self.custom_field_url = self.config[env]['custom_field_url']
        self.app_id = self.config[env]['algolia_app_id']
        self.admin_id = self.config[env]['algolia_admin_id']
        self.index_name = self.config[env]['algolia_index_name']
        self.log_file = self.config[env]['log_file']
        self.sender_email = self.config[env]['sender_email']
        self.email_password = self.config[env]['email_password']
        self.receiver_emails = self.config[env]['receiver_email']

