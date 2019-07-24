import unittest
import pandas as pd
from unittest import mock
from etl import Compare, Data_process


class TestCompare(unittest.TestCase):
    """ Test For ETL Functions """

    def test_find_delta_records(self):
        old = mock.Mock()
        new = mock.Mock()
        res = mock.Mock()

        old.df = pd.DataFrame([
            {'ref_no': '2019CPHIK-0069', 'Country': 'Taiwan', 'Company': 'Taiwan No.1 ltd', 'objectID': 1001},
            {'ref_no': '2019CPHIK-0070', 'Country': 'USA', 'Company': 'USA No.1 ltd', 'objectID': 1002},
            {'ref_no': '2019CPHIK-0071', 'Country': 'KOREA', 'Company': 'KOREA No.1 ltd', 'objectID': 1003},
            {'ref_no': '2019CPHIK-0072', 'Country': 'JAPAN', 'Company': 'JAPAN No.1 ltd', 'objectID': 1004}
        ])

        new.df = pd.DataFrame([
            {'ref_no': '2019CPHIK-0070', 'Country': 'USA', 'Company': 'USA No.1 ltd'},
            {'ref_no': '2019CPHIK-0071', 'Country': 'KOREA', 'Company': 'SEOUL No.1 ltd'},
            {'ref_no': '2019CPHIK-0072', 'Country': 'JAPAN', 'Company': 'JAPAN No.1 ltd'},
            {'ref_no': '2019CPHIK-0073', 'Country': 'JAPAN', 'Company': 'JAPAN No.2 ltd'},
        ])

        Data_process.indexing_exhibitor_df(old)
        Data_process.indexing_exhibitor_df(new)

        res.old_ex_df = old.df
        res.new_ex_df = new.df

        Compare.map_objectID_for_comparsion(res)
        Compare.find_delta_records(res)
        Compare.df_to_algolia_format(res)

        with self.subTest():
            self.assertTrue(res.to_create == [{'ref_no': '2019CPHIK-0073', 'Company': 'JAPAN No.2 ltd', 'Country': 'JAPAN'}], 'Records to create has error')

        with self.subTest():
            self.assertTrue(res.to_update == [{'ref_no': '2019CPHIK-0071', 'Company': 'SEOUL No.1 ltd', 'Country': 'KOREA', 'objectID':1003}], 'Records to update has error')

        with self.subTest():
            self.assertTrue(res.to_delete == [{'ref_no': '2019CPHIK-0069', 'Company': 'Taiwan No.1 ltd', 'Country': 'Taiwan', 'objectID':1001}], 'Records to delete has error')

    def test_compare_disorder_equal_records(self):
        old = mock.Mock()
        new = mock.Mock()
        res = mock.Mock()

        old.df = pd.DataFrame([
            {'ref_no': '2019CPHIK-0069', 'Country': 'Taiwan', 'Company': 'Taiwan No.1 ltd', 'objectID': 1001},
            {'ref_no': '2019CPHIK-0070', 'Country': 'USA', 'Company': 'USA No.1 ltd', 'objectID': 1002},
            {'ref_no': '2019CPHIK-0071', 'Country': 'KOREA', 'Company': 'KOREA No.1 ltd', 'objectID': 1003},
            {'ref_no': '2019CPHIK-0072', 'Country': 'JAPAN', 'Company': 'JAPAN No.1 ltd', 'objectID': 1004}
        ])

        new.df = pd.DataFrame([
            {'ref_no': '2019CPHIK-0069', 'Country': 'Taiwan', 'Company': 'Taiwan No.1 ltd'},
            {'ref_no': '2019CPHIK-0070', 'Country': 'USA', 'Company': 'USA No.1 ltd'},
            {'ref_no': '2019CPHIK-0072', 'Country': 'JAPAN', 'Company': 'JAPAN No.1 ltd'},
            {'ref_no': '2019CPHIK-0071', 'Country': 'KOREA', 'Company': 'KOREA No.1 ltd'},
        ])

        Data_process.indexing_exhibitor_df(old)
        Data_process.indexing_exhibitor_df(new)

        res.old_ex_df = old.df
        res.new_ex_df = new.df

        Compare.map_objectID_for_comparsion(res)
        Compare.find_delta_records(res)
        Compare.df_to_algolia_format(res)

        self.assertTrue(res.to_create == res.to_update == res.to_delete == [])

    def test_compare_only_req_update(self):
        old = mock.Mock()
        new = mock.Mock()
        res = mock.Mock()

        old.df = pd.DataFrame([
            {'ref_no': '2019CPHIK-0069', 'Country': 'Taiwan', 'Company': 'Taiwan No.1 ltd', 'objectID': 1001},
            {'ref_no': '2019CPHIK-0070', 'Country': 'USA', 'Company': 'USA No.1 ltd', 'objectID': 1002},
            {'ref_no': '2019CPHIK-0071', 'Country': 'KOREA', 'Company': 'KOREA No.1 ltd', 'objectID': 1003},
            {'ref_no': '2019CPHIK-0072', 'Country': 'JAPAN', 'Company': 'JAPAN No.1 ltd', 'objectID': 1004}
        ])

        new.df = pd.DataFrame([
            {'ref_no': '2019CPHIK-0069', 'Country': 'ROC Taiwan', 'Company': 'Taiwan No.1 ltd'},
            {'ref_no': '2019CPHIK-0070', 'Country': 'America', 'Company': 'USA No.1 ltd'},
            {'ref_no': '2019CPHIK-0072', 'Country': 'JAPAN', 'Company': 'JAPAN No.1 ltd'},
            {'ref_no': '2019CPHIK-0071', 'Country': 'KOREA', 'Company': 'KOREA No.1 ltd'},
        ])

        Data_process.indexing_exhibitor_df(old)
        Data_process.indexing_exhibitor_df(new)

        res.old_ex_df = old.df
        res.new_ex_df = new.df

        Compare.map_objectID_for_comparsion(res)
        Compare.find_delta_records(res)
        Compare.df_to_algolia_format(res)

        with self.subTest():
            self.assertTrue(res.to_create == res.to_delete == [])

        with self.subTest():
            self.assertTrue(res.to_update == [{'ref_no': '2019CPHIK-0069', 'Company': 'Taiwan No.1 ltd', 'Country': 'ROC Taiwan', 'objectID':1001}, {'ref_no': '2019CPHIK-0070', 'Company': 'USA No.1 ltd', 'Country': 'America', 'objectID':1002}])

    #TODO Test whole pairs

    def test_compare_same_tables_unordered(self):
        old = mock.Mock()
        new = mock.Mock()
        res = mock.Mock()

        old.df = pd.DataFrame([
            {'ref_no': '2019CPHIK-0069', 'Country': 'Taiwan', 'Company': 'Taiwan No.1 ltd', 'objectID': 1001},
            {'ref_no': '2019CPHIK-0070', 'Country': 'USA', 'Company': 'USA No.1 ltd', 'objectID': 1002},
            {'ref_no': '2019CPHIK-0071', 'Country': 'KOREA', 'Company': 'KOREA No.1 ltd', 'objectID': 1003},
            {'ref_no': '2019CPHIK-0072', 'Country': 'JAPAN', 'Company': 'JAPAN No.1 ltd', 'objectID': 1004}
        ])

        new.df = pd.DataFrame([
            {'ref_no': '2019CPHIK-0071', 'Country': 'KOREA', 'Company': 'KOREA New ltd'},
            {'ref_no': '2019CPHIK-0072', 'Country': 'JAPAN', 'Company': 'JAPAN No.1 ltd'},
            {'ref_no': '2019CPHIK-0070', 'Country': 'USA', 'Company': 'USA No.1 ltd'},
            {'ref_no': '2019CPHIK-0069', 'Country': 'Taiwan', 'Company': 'Taiwan No.1 ltd'},
        ])

        Data_process.indexing_exhibitor_df(old)
        Data_process.indexing_exhibitor_df(new)

        res.old_ex_df = old.df
        res.new_ex_df = new.df

        Compare.map_objectID_for_comparsion(res)
        Compare.find_delta_records(res)
        Compare.df_to_algolia_format(res)

        with self.subTest():
            self.assertTrue(res.to_create == res.to_delete == [])

        with self.subTest():
            self.assertTrue(res.to_update == [{'ref_no': '2019CPHIK-0071', 'Country': 'KOREA', 'Company': 'KOREA New ltd', 'objectID': 1003}])


if __name__ == '__main__':
    unittest.main()
