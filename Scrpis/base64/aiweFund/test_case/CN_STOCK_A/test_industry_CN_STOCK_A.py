import datetime
import unittest
import pandas as pd
from sdk.datasource import DataSource


class industry_CN_STOCK_A(unittest.TestCase):
    def setUp(self):
        self.table = "industry_CN_STOCK_A"
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
        """检查空值"""
        null_df = self.data_df[self.data_df.isnull().any(axis=1)]
        self.assertTrue(null_df.empty, msg="含有空值")

    def check_diff_version_sw_industry(self):
        if (self.end_date > '2021-07-30') and (self.start_date <= '2021-07-30'):
            old_df = self.data_df[self.data_df.date < datetime.datetime(2021, 7, 30)]
            new_df = self.data_df[self.data_df.date >= datetime.datetime(2021, 7, 30)]
        elif self.end_date < '2021-07-30':
            old_df = self.data_df
            new_df = pd.DataFrame(columns=self.data_df.columns.tolist())
        elif self.start_date > '2021-07-30':
            new_df = self.data_df
            old_df = pd.DataFrame(columns=self.data_df.columns.tolist())

        error1_df = old_df[~(old_df.industry_sw_level1.isin([0, 110000, 210000, 220000, 230000, 240000, 270000, 280000,
                                                             330000, 340000, 350000, 360000, 370000, 410000, 420000,
                                                             430000, 450000, 460000, 480000, 490000, 510000, 610000,
                                                             620000, 630000, 640000, 650000, 710000, 720000, 730000]))]

        error2_df = new_df[~(new_df.industry_sw_level1.isin([0, 110000, 220000, 230000, 240000, 270000, 280000, 330000,
                                                             340000, 350000, 360000, 370000, 410000, 420000, 430000,
                                                             450000, 460000, 480000, 490000, 510000, 610000, 620000,
                                                             630000, 640000, 650000, 710000, 720000, 730000, 740000,
                                                             750000, 760000, 770000]))]
        self.assertTrue(error1_df.empty and error2_df.empty, msg="存在不在规定版本的申万一级代码")


if __name__ == '__main__':
    unittest.main()
