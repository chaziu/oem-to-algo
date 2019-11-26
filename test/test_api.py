from unittest import TestCase, mock
from api import Oem


class TestCompare(TestCase):

    def test_connection(self):
        m = mock.Mock()
        with self.subTest():
            res = Oem._call_api(m, url='http://google.com')
            self.assertTrue(res.status_code == 200)
        with self.subTest():
            res = Oem._call_api(m, url='http://googlesdf.com')
            self.assertRaises(ConnectionError)

    def test_get_exhibitors(self):
        m = mock.Mock()
        Oem.get_exhibitors(m, 'http://jublia.ubmasia.com/Exhibitor/api/getexhibitorsInfo/2019CPHIK/1000/1/?p=jublia3762')
        self.assertTrue(type(m.exhibitors[0]) == dict and len(m.exhibitors) > 0)

    def test_get_custom_field_pairs(self):
        m = mock.Mock()
        Oem.get_custom_field_pairs(m, 'http://jublia.ubmasia.com/Exhibitor/api/getattributesinfo/2019CPHIK/1000/1/?p=jublia3762')
        self.assertTrue(type(m.custom_fields) == dict and len(m.custom_fields) > 0)

