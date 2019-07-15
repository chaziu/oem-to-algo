# Main

from api import Oem
from etl import Compare
from algolia import Algolia
import sys
import logging
import configparser
from datetime import datetime

try:
    env = 'live' if sys.argv[1] == 'live' else 'test'
except IndexError:
    print('No environment argument was passed')
    env = 'test'

# Credentials & Endpoints
config = configparser.ConfigParser()
config.read('config.ini')
exhibitor_url = config[env]['exhibitor_url']
custom_field_url = config[env]['custom_field_url']
app_id = config[env]['algolia_app_id']
admin_id = config[env]['algolia_admin_id']
index_name = config[env]['algolia_index_name']
log_file = config[env]['log_file']

# logging
logging.basicConfig(level=logging.INFO)
fh = logging.FileHandler(filename='./logs/'+log_file)
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger = logging.getLogger(__name__)
logger.addHandler(fh)
logger.addHandler(ch)

logger.info('App in {} environment'.format(env))


def main(exhibitors_url, custom_fields_url=None):
    logger.info('Process Start - {}'.format(datetime.now().strftime('%Y-%m-%d_%H:%M:%S')))

    oem = Oem(exhibitors_url, custom_fields_url)
    db = Algolia(app_id, admin_id, index_name)
    recs = Compare(oem.exhibitor, db.current_records, oem.custom_fields)
    if recs.to_create:
        db.create(recs.to_create)
    if recs.to_update:
        db.update(recs.to_update)
    if recs.to_delete:
        db.delete(recs.to_delete)
    logger.info('process end - {}'.format(datetime.now().strftime('%Y-%m-%d_%H:%M:%S')))


if __name__ == '__main__':
    main(exhibitor_url, custom_field_url)



