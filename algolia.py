# Updating Alogolia index

from algoliasearch.search_client import SearchClient
import json
import logging
logger = logging.getLogger(__name__+' module')


class Deco_db_log:

    actions_dic = {'create': ['creating', 'created'],
                   'update': ['updating', 'updated'],
                   'delete': ['deleting', 'deleted']}

    def __init__(self):
        self.make_wrappers(self.actions_dic)

    @staticmethod
    def _wrapper_template(action_ing, action_ed):
        def middle(func):
            def inner(*arg, **kwarg):
                logger.info('{} records ...'.format(action_ing))
                return_value = func(*arg, **kwarg)
                logger.info('{} records {}'.format(len(arg[1]), action_ed))  # arg[0] is algo.self
                logger.info('{}: {}'.format(action_ed, json.dumps(arg[1])))
                return return_value
            return inner
        return middle

    def make_wrappers(self, actions: dict):
        for i, v in actions.items():
            setattr(self, i, self._wrapper_template(v[0], v[1]))
        return self


db_logger = Deco_db_log()


class Algolia:

    def __init__(self, app_id, admin_id, index_name):
        self.client = SearchClient.create(app_id, admin_id)
        self.index = self.client.init_index(index_name)
        self.current_records = [i for i in self.index.browse_objects({'query': ''})]

    @db_logger.create
    def create(self, records):
        self.index.save_objects(records, {'autoGenerateObjectIDIfNotExist': True})

    @db_logger.delete
    def delete(self, records):
        obj_ids = [rec['objectID'] for rec in records]
        self.index.delete_objects(obj_ids)

    @db_logger.update
    def update(self, records):
        self.index.partial_update_objects(records)

