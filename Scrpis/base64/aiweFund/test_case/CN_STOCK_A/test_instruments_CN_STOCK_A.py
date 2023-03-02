import datetime
import unittest
from bigdatasource.api import DataSource
from bigdata.api.datareader import D


class instruments_CN_STOCK_A(unittest.TestCase):

    def setUp(self):
        self.table = "instruments_CN_STOCK_A"
        self.start_date = datetime.date.today().isoformat()
        self.end_date = datetime.datetime.today().isoformat()
        self.data_df = DataSource(self.table).read(start_date=self.start_date, end_date=self.end_date)
        self.instruments = D.instruments(start_date=self.start_date, end_date=self.end_date)

    def test_check_base_count(self):
        """检查基本数量"""
        self.assertTrue(abs(len(self.data_df) - len(self.instruments)) < 10)
        self.assertTrue(len(self.data_df) > 4000)

    def test_check_fields_nan(self):
        """检查空值"""
        for col in self.data_df.columns:
            self.assertEqual(0, self.data_df[col].isna().sum(), col)

    def test_check_st(self):
        """检查股票ST状态"""
        st_df = self.data_df[self.data_df['name'].str.contains('ST')& ~self.data_df['name'].str.contains('SST')]
        status_df = DataSource('stock_status_CN_STOCK_A').read(start_date=self.start_date, end_date=self.end_date, fields=['st_status'])
        status_df = status_df[status_df['st_status'].isin([1, 2])]
        self.assertEqual(len(st_df), len(status_df), 'instruments_CN_STOCK_A:%s, stock_status_CN_STOCK_A:%s' % (len(st_df), len(status_df)))


if __name__ == '__main__':
    unittest.main()
