## All API

import requests
import time
import logging
from typing import Union, List, Dict, Any, Optional

logger = logging.getLogger(__name__+' module')


class Oem:

    def __init__(self, exhibitor_url: str, custom_field_url: str) -> None:
        self.exhibitor = None
        self.custom_fields = None

        self.get_exhibitors(exhibitor_url)
        if custom_field_url is not '': # if custom field is not empty on config.ini
            self.get_custom_field_pairs(custom_field_url)

    def _call_api(self, url: str,
                  attempt: int = 1,
                  max_retries: int = 3) -> requests.models.Response:

        if attempt > max_retries:  # Throw error when attempt more than max_retries setting
            logger.error('Connection failed')
            raise ConnectionError('Failed to get table from url after {} retries: {}'.format(max_retries,url))
        try:
            res = requests.get(url)
            if res.status_code != 200:  # Only consider api call successful when status code is 200
                raise ConnectionError(url + 'Returned server status code ' + str(res.status_code))
            logger.info('Successfully connected to {} '.format(url))
            return res
        except Exception as e:
            print(e)
            time.sleep(3)
            logger.info('Retrying attempt {}'.format(attempt))
            self._call_api(url, attempt=attempt + 1)  # Recursive next attempt

    def get_exhibitors(self, exhibitor_url: str) -> "Oem":
        """ Get exhibitors records from OEM
        :param exhibitor_url: url string
        :return: [{ka1:va1,kb1:vb1},{ka2:va2,kb2:vb2}]
        """
        self.exhibitor = Oem._call_api(self,exhibitor_url).json()
        return self

    def get_custom_field_pairs(self, custom_field_url: str) -> "Oem":
        """ Get custom fields into dict to update DF column name
        :param custom_field_url: url string
        :return: dict {k1:v1, k2:v2}
        """
        self.custom_fields = Oem._call_api(self,custom_field_url).json()
        logger.info('Custom fields: {}'.format(self.custom_fields))
        return self


