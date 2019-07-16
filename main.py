# Main
from api import Oem
from etl import Compare
from algolia import Algolia
import sys
import logging
from logs.logging import load_log_setting
from datetime import datetime
from config.configuration import Config

try:
    env = 'live' if sys.argv[1] == 'live' else 'test'
except IndexError:
    print('No environment argument was passed')
    env = 'test'

cfg = Config(env, './config/config.ini')  # Credentials & Endpoints
load_log_setting(cfg.log_file)  # logging
logger = logging.getLogger(__name__)


def main(exhibitors_url, custom_fields_url=None):
    logger.info('App in {} environment'.format(env))
    logger.info('Process Start - {}'.format(datetime.now().strftime('%Y-%m-%d_%H:%M:%S')))
    oem = Oem(exhibitors_url, custom_fields_url)
    db = Algolia(cfg.app_id, cfg.admin_id, cfg.index_name)
    recs = Compare(oem.exhibitor, db.current_records, oem.custom_fields)
    if recs.to_create:
        db.create(recs.to_create)
    if recs.to_update:
        db.update(recs.to_update)
    if recs.to_delete:
        db.delete(recs.to_delete)
    logger.info('process end - {}'.format(datetime.now().strftime('%Y-%m-%d_%H:%M:%S')))


if __name__ == '__main__':
    main(cfg.exhibitor_url, cfg.custom_field_url)



