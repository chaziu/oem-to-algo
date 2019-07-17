## All API

import requests
import time
import logging
# import json

logger = logging.getLogger(__name__+' module')


class Oem:

    def __init__(self, exhibitor_url, custom_field_url):
        self.exhibitor = None
        self.custom_fields = None

        self.get_exhibitors(exhibitor_url)
        if custom_field_url is not None:
            self.get_custom_field_pairs(custom_field_url)

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

    def get_exhibitors(self, exhibitor_url):
        """ Get exhibitors records from OEM
        :param exhibitor_url: url string
        :return: [{ka1:va1,kb1:vb1},{ka2:va2,kb2:vb2}]
        """
        self.exhibitor = self.call_api(exhibitor_url)
        return self

    def get_custom_field_pairs(self, custom_field_url):
        """ Get custom fields into dict to update DF column name
        :param custom_field_url: url string
        :return: dict {k1:v1, k2:v2}
        """
        self.custom_fields = self.call_api(custom_field_url)
        logger.info('Custom fields: {}'.format(self.custom_fields))
        return self

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

