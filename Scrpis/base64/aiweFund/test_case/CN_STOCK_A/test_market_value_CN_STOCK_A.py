import datetime
import unittest
from sdk.datasource import DataSource


class market_value_CN_STOCK_A(unittest.TestCase):
    def setUp(self):
        self.table = "market_value_CN_STOCK_A"
        self.start_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        self.end_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        self.data_df = DataSource(self.table).read(start_date=self.start_date,
                                                   end_date=self.end_date)

    def check_base_count(self):
        """检查基本数量"""
        daily_count = 4500
        count_df = self.data_df.groupby(['date'], as_index=False).instrument.count()
        count_df = count_df.query(f"instrument < {daily_count}")
        self.assertTrue(count_df.empty, msg=f"交易日数量少于{daily_count}的交易日：{count_df.date.tolist()}")

    def check_fields_nan(self):
        """检查market_cap不能有0和负数"""
        error_df = self.data_df.query("market_cap <= 0")
        self.assertEqual(len(error_df), 0, msg="market_cap 含有小于等于0的数据")


if __name__ == '__main__':
    unittest.main()
