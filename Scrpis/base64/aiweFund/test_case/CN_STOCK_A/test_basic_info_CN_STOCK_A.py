import datetime
import unittest
from sdk.datasource import DataSource



class basic_info_CN_STOCK_A(unittest.TestCase):
    def setUp(self):
        self.table = "basic_info_CN_STOCK_A"
        self.start_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        self.end_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        self.data_df = DataSource(self.table).read(start_date=self.start_date,
                                                   end_date=self.end_date)
        self.check_nan_fields = ['name', 'company_name', 'list_date']

    def check_base_count(self):
        """检查基本数量"""
        self.assertTrue(len(self.data_df) >= 5000, msg="基本信息表数量少于5000")

    def check_fields_nan(self):
        """检查空值"""
        for col in self.check_nan_fields:
            self.assertEqual(0, self.data_df[col].isna().sum(), msg=f"字段{col} 含有空值")

    def test_list_board_name(self):
        """
        测试list_board名字
        """
        print(self.data_df)
        df_err = self.data_df[(self.data_df.list_board != "主板")
                              & (self.data_df.list_board != "创业板")
                              & (self.data_df.list_board != "科创板")
                              & (self.data_df.list_board != "北证")
        ]
        self.assertEqual(df_err.shape[0], 0, msg="上市板名不规范数据,共{}条".format(df_err.shape[0]))


if __name__ == '__main__':
    unittest.main()
