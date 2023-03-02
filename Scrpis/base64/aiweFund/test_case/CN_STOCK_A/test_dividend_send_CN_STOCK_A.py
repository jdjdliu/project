import datetime
import unittest
from sdk.datasource import DataSource


class basic_info_CN_STOCK_A(unittest.TestCase):
    def setUp(self):
        self.table = "dividend_send_CN_STOCK_A"
        self.start_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        self.end_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        self.data_df = DataSource(self.table).read(start_date=self.start_date,
                                                   end_date=self.end_date)

    def check_fields_nan(self):
        """检查空值"""
        for col in self.data_df.columns:
            self.assertEqual(0, self.data_df[col].isna().sum(), msg=f"字段{col} 含有空值")

    def check_value(self):
        """每股送股比例、每股转增比例、每股派息（税前）三个字段不能同时为0"""
        pass


if __name__ == '__main__':
    unittest.main()
