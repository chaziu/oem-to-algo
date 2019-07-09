# All the ETL & Data works
import pandas as pd
import logging
import json
logger = logging.getLogger(__name__+' module')


class Compare:

    def __init__(self, new_ex_json, old_ex_json, pairs=None):
        if old_ex_json is None:
            df = Data_process(new_ex_json, exhibitor=True, pairs=pairs).df
            self.to_create = Data_process.df_to_algolia(df)[0]
        else:
            self.old_ex_df = Data_process(old_ex_json, exhibitor=True, pairs=pairs).df
            self.new_ex_df = Data_process(new_ex_json, exhibitor=True, pairs=pairs, latest=True).df
            self.to_create, self.to_update, self.to_delete = self.find_delta_records(self.old_ex_df, self.new_ex_df)
            self.to_create, self.to_update, self.to_delete = Data_process.df_to_algolia(self.to_create, self.to_update, self.to_delete)

    @staticmethod
    def find_delta_records(old_df, new_df):
        """ Compare DataFrames to find newly created, deleted & updated records
        :param old_df: Last Version of DataFrame
        :param new_df: New Version of DataFrame
        :return: Tuple of DataFrame
        """
        # No update needed if new_df = old_df
        if old_df.drop('objectID', axis=1).equals(new_df):
            logger.info('No Updates Needed')
            return [], [], []

        new_df = Data_process.map_objectID_for_comparsion(old_df, new_df)
        # Getting new_df records that is different from the old_df
        to_delete = old_df[~old_df.index.isin(new_df.index)].drop('objectID', axis=1)
        to_create = new_df[~new_df.index.isin(old_df.index)].drop('objectID', axis=1)
        # Get identical index from both df to check if any records has updated

        old_df_common = old_df[old_df.index.isin(new_df.index)].sort_index()
        new_df_common = new_df[new_df.index.isin(old_df.index)].sort_index()

        to_update = new_df_common[(old_df_common != new_df_common).any(axis=1)]
        to_be_overwritten = old_df_common[(old_df_common != new_df_common).any(axis=1)]

        logger.info('{} record(s) to be create; {} record(s) to be update; {} record(s) to be delete'.format(len(to_create), len(to_update), len(to_delete)))
        if len(to_be_overwritten) > 0: logger.info('Records to be overwritten: {}'.format(to_be_overwritten.to_json(orient='records')))
        return to_create, to_update, to_delete


class Data_process:

    def __init__(self, json, exhibitor=False, pairs=None, latest=False):
        self._table = self.data_to_df(json)
        if latest:
            self._table.drop_df_cols()
        if exhibitor:
            self._table.indexing_exhibitor_df()
            if pairs:
                self._table.rename_df_custom_field_column(pairs)
        self.df = self._table.df

    @staticmethod
    def data_to_df(json):
        return Df(pd.DataFrame(json))

    @staticmethod
    def df_to_algolia(*dfs):
        """ Convert DataFrame into JSON for use in Algolia CUD operation
        :param dfs: *DataFrames
        :return: DataFrames converted to JSON by rows for pushing to Algolia
        :rtype: tuple
        """
        res = ()
        for df in dfs:
            res = (*res, df.reset_index().to_dict(orient='records'))
        return res

    @staticmethod
    def map_objectID_for_comparsion(old_df, new_df):
        old_df = old_df[old_df.index.isin(new_df.index)].sort_index()
        cols = old_df.columns.tolist()
        # DataFrame index & columns order has to be identical for comparison
        new_df_with_oid = new_df[new_df.index.isin(old_df.index)].merge(old_df['objectID'], on='ref_no').sort_index()[cols]
        return new_df_with_oid


class Df:

    def __init__(self, df):
        self.df = df

    def indexing_exhibitor_df(self):
        self.df = self.df.set_index('ref_no')
        return self

    def rename_df_custom_field_column(self, pairs):
        self.df = self.df.rename(pairs, axis='columns')
        return self

    def drop_df_cols(self, cols_to_drop=['contact_no', 'email', 'full_name', 'position']):
        try:
            self.df = self.df.drop(cols_to_drop, axis=1)
        except KeyError as e:
            logger.warning(e)
            pass
        return self



