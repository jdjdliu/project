import datetime
import unittest
from sdk.datasource import DataSource


class stock_status_CN_STOCK_A(unittest.TestCase):

    def setUp(self):
        self.table = "stock_status_CN_STOCK_A"
        self.start_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        self.end_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        self.data_df = DataSource(self.table).read(start_date=self.start_date, end_date=self.end_date)

    def test_check_base_count(self):
        """检查基本数量"""
        daily_count = 4500
        count_df = self.data_df.groupby(['date'], as_index=False).instrument.count()
        count_df = count_df.query(f"instrument < {daily_count}")
        self.assertTrue(count_df.empty, msg=f"交易日数量少于{daily_count}的交易日：{count_df.date.tolist()}")

    def test_check_fields_nan(self):
        """检查空值"""
        for col in self.data_df.columns:
            self.assertEqual(0, self.data_df[col].isna().sum(), msg=f"字段{col}含有空值")

    def test_check_fields_minus(self):
        """检查负数"""
        for col in self.data_df.columns:
            if 'int' in str(self.data_df[col].dtype) or 'float' in str(self.data_df[col].dtype):
                self.assertGreaterEqual(0, len(self.data_df[self.data_df[col] < 0]), msg=f"字段{col}含有负数")


if __name__ == '__main__':
    unittest.main()
