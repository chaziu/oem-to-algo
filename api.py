## All API Getting

import requests
import time
import logging
import json

logger = logging.getLogger(__name__+' module')


class Oem:

    def __init__(self, exhibitor_url, custom_field_url):
        self.exhibitor = self.call_api(exhibitor_url)
        if custom_field_url is not None:
            self.custom_fields = self.call_api(custom_field_url)

    def call_api(self, url, attempt=1, max_retries=3):
        if attempt > max_retries:
            logger.error('Connection failed')
            raise ConnectionError('Failed to get table from url after {} retries: {}'.format(max_retries,url))
        try:
            res = requests.get(url)
            if res.status_code != 200:
                raise ConnectionError(url + 'Returned server status code ' + str(res.status_code))
            logger.info('Successfully connected to {} '.format(url))
            return res.json()
        except Exception as e:
            print(e)
            time.sleep(3)
            logger.info('Retrying attempt {}'.format(attempt))
            self.call_api(url, attempt=attempt + 1)

    # Turn custom field into dict to update DF column name
    @ staticmethod
    def get_custom_field_pairs(data):
        if type(data) != list:
            return data
        pairs = {}
        for k, v in enumerate(data):
            pairs[k] = v
        logger.info('There are {} custom field(s)'.format(len(pairs)))
        if pairs:
            logger.info(json.dumps(pairs))
        return pairs

# def save_json_to_txt (table,name,updated=False):
#     timestamp = dt.datetime.now().strftime("%Y-%m-%d")
#     with open('./data_log/{}_{}.txt'.format(name, timestamp), 'w') as fh:
#         fh.write(json.dumps(table))
#     if updated:
#         with open('./existing.txt','w') as fh:
#             fh.write(json.dumps(table))
#
#
# def get_json(path='./existing.txt'):
#     with open(path, 'r') as fh:
#         data = json.load(fh)
#     return data

