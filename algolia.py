# Updating Alogolia index

from algoliasearch.search_client import SearchClient
import json
import logging
logger = logging.getLogger(__name__+' module')


def _db_action_log(func):
    def inner(*arg, **kwarg):
        logger.info('{}ing records ...'.format(func.__name__))
        return_value = func(*arg, **kwarg)
        logger.info('{} records {}d'.format(len(arg[1]), func.__name__))
        logger.info('{}: {}'.format(func.__name__, json.dumps(arg[1])))
        return return_value
    return inner


class Algolia:

    def __init__(self, app_id, admin_id, index_name):
        self.client = SearchClient.create(app_id, admin_id)
        self.index = self.client.init_index(index_name)
        self.current_records = [i for i in self.index.browse_objects({'query': ''})]

    @_db_action_log
    def create(self, records):
        self.index.save_objects(records, {'autoGenerateObjectIDIfNotExist': True})

    @_db_action_log
    def delete(self, records):
        obj_ids = [rec['objectID'] for rec in records]
        self.index.delete_objects(obj_ids)

    @_db_action_log
    def update(self, records):
        self.index.partial_update_objects(records)

