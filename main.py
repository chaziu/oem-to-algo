# Main
from api import Oem
from etl import Compare
from algolia import Algolia, db_logger
from _email import Email
import sys
import logging
from logs.logging import load_log_setting
from datetime import datetime
from config.configuration import Config
import typing


try:
    env = 'live' if sys.argv[1] == 'live' else 'test'
except IndexError:
    print('No environment argument was passed')
    env = 'test'

cfg = Config(env, './config/config.ini')  # Credentials & Endpoints
email = Email(cfg.sender_email, cfg.email_password)  # Setup emails
load_log_setting(cfg.log_file)  # Setup logging
logger = logging.getLogger(__name__)


def main(exhibitors_url: str = cfg.exhibitor_url, custom_fields_url: str = cfg.custom_field_url) -> None:
    logger.info('App in {} environment'.format(env))
    start = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    logger.info('Process Start - {}'.format(start))
    oem = Oem(exhibitors_url, custom_fields_url)
    db = Algolia(cfg.app_id, cfg.admin_id, cfg.index_name)
    delta_records = Compare(oem.exhibitor, db.current_records, oem.custom_fields)

    if delta_records.to_create:
        db.create(delta_records.to_create)
    if delta_records.to_update:
        db.update(delta_records.to_update)
    if delta_records.to_delete:
        db.delete(delta_records.to_delete)

    # if any([delta_records.to_create, delta_records.to_create, delta_records.to_create]):
    #     email.send_result(cfg.receiver_emails, payload=delta_records)

    email.send_result(cfg.receiver_emails, payload=delta_records)

    end = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    logger.info('process end - {}'.format(end))


if __name__ == '__main__':
    main()



