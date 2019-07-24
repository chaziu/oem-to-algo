# All the ETL & Data works
import pandas as pd
import logging
from typing import List, Dict, Union, Any, Optional, Tuple, Callable

logger = logging.getLogger(__name__ + ' module')


class Compare:
    def __init__(self, new_ex_json: List[Dict],
                 old_ex_json: Optional[List[Dict]],
                 pairs: Optional[Dict] = None) -> None:

        self.old_ex_df = None
        self.new_ex_df = None
        self.new_ex_df_wID = None
        self.to_create = []
        self.to_update = []
        self.to_delete = []

        if old_ex_json is None:
            df = Data_process(new_ex_json, exhibitor=True, pairs=pairs).df
            self.to_create = df
        else:
            self.old_ex_df = Data_process(old_ex_json, exhibitor=True, pairs=pairs).df
            self.new_ex_df = Data_process(new_ex_json, exhibitor=True, pairs=pairs, latest=True).df
            self.map_objectID_for_comparsion()
            self.find_delta_records()

        self.df_to_algolia_format()

    def map_objectID_for_comparsion(self) -> None:
        # DataFrame index & columns order has to be identical for comparison
        old_df_sorted = self.old_ex_df[self.old_ex_df.index.isin(self.new_ex_df.index)].sort_index()
        cols = old_df_sorted.columns.tolist()
        # Map objectID to new_df
        self.new_ex_df_wID = self.new_ex_df[self.new_ex_df.index.isin(old_df_sorted.index)]\
            .merge(old_df_sorted['objectID'], on='ref_no').sort_index()[cols]

    def df_to_algolia_format(self) -> None:
        """ Convert DataFrame into JSON for use in Algolia CUD operation
        :return: DataFrames converted to JSON by rows for pushing to Algolia
        :rtype: tuple
        """
        self.to_create = self.to_create.reset_index().to_dict(orient='records') if len(self.to_create) > 0 else []
        self.to_update = self.to_update.reset_index().to_dict(orient='records') if len(self.to_update) > 0 else []
        self.to_delete = self.to_delete.reset_index().to_dict(orient='records') if len(self.to_delete) > 0 else []

    def find_delta_records(self) -> None:
        """ Compare DataFrames to find newly created, deleted & updated records
        :return: Tuple of DataFrame or empty list
        """
        # No update needed if new_df = old_df
        if self.old_ex_df.drop('objectID', axis=1).equals(self.new_ex_df):  # Identical, no updates
            logger.info('No Updates Needed')
            self.to_create, self.to_update, self.to_delete = [], [], []
            return None

        # Getting new_df records that is different from the old_df
        to_delete = self.old_ex_df[~self.old_ex_df.index.isin(self.new_ex_df.index)]
        to_create = self.new_ex_df[~self.new_ex_df.index.isin(self.old_ex_df.index)]
        # Get identical index from both df to check if any records has updated

        old_df_common = self.old_ex_df[self.old_ex_df.index.isin(self.new_ex_df_wID.index)].sort_index()
        new_df_common = self.new_ex_df_wID[self.new_ex_df_wID.index.isin(self.old_ex_df.index)].sort_index()

        to_update = new_df_common[(old_df_common != new_df_common).any(axis=1)]
        to_be_overwritten = old_df_common[(old_df_common != new_df_common).any(axis=1)]

        self.to_create = to_create
        self.to_update = to_update
        self.to_delete = to_delete

        logger.info(
            '{} record(s) to be create; {} record(s) to be update;'
            ' {} record(s) to be delete'.format(len(to_create), len(to_update), len(to_delete)))
        if len(to_be_overwritten) > 0:
            logger.info('Records to be overwritten: {}'.format(to_be_overwritten.to_json(orient='records')))


class Data_process:

    def __init__(self, json_data: List[dict],
                 exhibitor: bool = False,
                 pairs: Dict = None,
                 latest: bool = False) -> None:

        self.df = pd.DataFrame(json_data)

        if latest:
            self.drop_df_cols()  # Drop irrelevant cols (e.g. password, contact person)
        if exhibitor:
            self.indexing_exhibitor_df()  # Set 'ref_no' as index
            if pairs:
                self.rename_df_custom_field_column(pairs)  # Map columns name for comparison
        if latest and exhibitor and pairs:
            self.normalize_country_field()


    # @staticmethod
    # def map_objectID_for_comparsion(old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    #     old_df = old_df[old_df.index.isin(new_df.index)].sort_index()
    #     cols = old_df.columns.tolist()
    #     # DataFrame index & columns order has to be identical for comparison
    #     # Map objectID to new_df
    #     new_df_with_oid =\
    #         new_df[new_df.index.isin(old_df.index)].merge(old_df['objectID'], on='ref_no').sort_index()[cols]
    #     return new_df_with_oid

    def normalize_country_field(self) -> 'Data_process':
        """ Removing trail ; in country field
        :return: self
        """
        self.df['Country'] = self.df['Country'].apply(lambda x: x.replace(';', ''))
        return self

    def indexing_exhibitor_df(self) -> "Data_process":
        self.df = self.df.set_index('ref_no')
        return self

    def rename_df_custom_field_column(self, pairs: Dict[str, str]) -> "Data_process":
        self.df = self.df.rename(pairs, axis='columns')
        return self

    def drop_df_cols(self, cols_to_drop: Optional[List[str]] = None) -> 'Data_process':
        if cols_to_drop is None:
            cols_to_drop = ['contact_no', 'email', 'full_name', 'position']  # Avoid mutable default args
        try:
            self.df = self.df.drop(cols_to_drop, axis=1)
        except KeyError as e:
            logger.warning(e)
            pass
        return self

    def modify_col(self, col: Union[str, List[str]], func: Callable[[Any], Any]) -> "Data_process":
        self.df[col] = self.df[col].apply(func)
        return self




# class Df:
#     """ Contains a handful of dataFrame method
#     : param df: dataFrame
#     """
#
#     def __init__(self, json_data: List[dict]) -> None:
#         self.df = pd.DataFrame(json_data)
#
#     def indexing_exhibitor_df(self) -> "Df":
#         self.df = self.df.set_index('ref_no')
#         return self
#
#     def rename_df_custom_field_column(self, pairs: Dict[str, str]) -> "Df":
#         self.df = self.df.rename(pairs, axis='columns')
#         return self
#
#     def drop_df_cols(self, cols_to_drop: Optional[List[str]] = None) -> 'Df':
#         if cols_to_drop is None:
#             cols_to_drop = ['contact_no', 'email', 'full_name', 'position']  # Avoid mutable default args
#         try:
#             self.df = self.df.drop(cols_to_drop, axis=1)
#         except KeyError as e:
#             logger.warning(e)
#             pass
#         return self
#
#     def modify_col(self, col: Union[str, List[str]], func: Callable[[Any], Any]) -> "Df":
#         self.df[col] = self.df[col].apply(func)
#         return self
