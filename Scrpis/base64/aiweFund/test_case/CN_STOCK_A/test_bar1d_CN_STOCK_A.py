import datetime
import unittest
from sdk.datasource import DataSource, D


class bar1d_CN_STOCK_A(unittest.TestCase):

    def setUp(self):
        self.table = "bar1d_CN_STOCK_A"
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
            self.assertEqual(0, self.data_df[col].isna().sum(), msg=f"{col} 含有空值")

    def test_check_fields_minus(self):
        """检查负数"""
        for col in self.data_df.columns:
            if 'int' in str(self.data_df[col].dtype) or 'float' in str(self.data_df[col].dtype):
                self.assertGreaterEqual(0, len(self.data_df[self.data_df[col] < 0]), msg=f"{col} 含有负数")

    def test_check_suspended(self):
        """检测停牌数据"""
        sus_df = self.data_df[(self.data_df['volume'] < 1) & (self.data_df['amount'] < 1)]
        status_df = DataSource('stock_status_CN_STOCK_A').read(start_date=self.start_date,
                                                               end_date=self.end_date, fields=['suspended'])
        # status_df = status_df[status_df['suspended'] == True]
        status_df = status_df.query("(suspended == True) and (suspend_type in ('停牌一天','连续停牌'))")
        self.assertListEqual(sorted(sus_df['instrument']), sorted(status_df['instrument']),
                             msg='停牌数据异常 bar1d_CN_STOCK_A: %s, stock_status_CN_STOCK_A: %s' % (len(sus_df), len(status_df)))

    def test_check_price(self):
        """检查高开低收是否有0值"""
        for col in ['high', 'low', 'open', 'close']:
            self.assertEqual(len(self.data_df[self.data_df[col] == 0]), 0, msg=f"价格字段 {col} 含有0值")


if __name__ == '__main__':
    unittest.main()
