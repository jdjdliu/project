import re
import click
import datetime
import pandas as pd

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

from CN_STOCK_A.build_bigquant_table.template import Build
from CN_FUTURE.schema_category import bar1d_CN_FUTURE as category_info


class BuildBar1d(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildBar1d, self).__init__(start_date, end_date)
        self.map_dic = {
            # 'ftr_cd': 'ftr_cd',
            'instrument': 'instrument',
            'date': 'date',
            'opn_prc': 'open',
            'lo_prc': 'low',
            'cls_prc': 'close',
            'hi_prc': 'high',
            'vol': 'volume',
            'amt': 'amount',
            'stl_prc': 'settle',
            'oi': 'open_intl',
            'dn_lmt_prc': 'low_limit',
            'up_lmt_prc': 'high_limit',
            'obj_smb': 'product_code',

            'is_main_ctr': 'is_main_ctr',

        }

        self.schema = {
            'friendly_name': category_info[2],
            'date_field': 'date',
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'active': True,
            'partition_date': 'Y',
            'primary_key': ['date', 'instrument'],
            'partition_field': 'product_code',
            'fields': {
                # 'ftr_cd': {'desc': '期货内部编码', 'type': 'str'},  # 只有basic_info有这个字段，后续加工表不加此字段
                'instrument': {'desc': '合约代码', 'type': 'str'},
                'date': {'desc': '日期', 'type': 'datetime64[ns]'},

                'close': {'desc': '收盘价', 'type': 'float32'},
                'high': {'desc': '最高价', 'type': 'float32'},
                'low': {'desc': '最低价', 'type': 'float32'},
                'open': {'desc': '开盘价', 'type': 'float32'},
                'open_intl': {'desc': '持仓量', 'type': 'float64'},
                'settle': {'desc': '结算价', 'type': 'float32'},
                'volume': {'desc': '成交量', 'type': 'float64'},
                'amount': {'desc': '成交额', 'type': 'float64'},
                'product_code': {'desc': '品种代码', 'type': 'str'},
                'low_limit': {'desc': '跌停价', 'type': 'float64'},
                'high_limit': {'desc': '涨停价', 'type': 'float64'},
            },
        }

        self.write_mode = write_mode
        self.alias = 'bar1d_CN_FUTURE'

    def run(self):
        fields_lst = list(self.map_dic.keys())
        source_df = self._read_DataSource_by_date(table='FTR_EODPRICE', fields=fields_lst)
        source_df = source_df[list(self.map_dic.keys())]

        source_df = source_df.rename(columns=self.map_dic)

        print('>>>>before filter shape: ', source_df.shape)
        basic_df = self._read_DataSource_all(table='basic_info_CN_FUTURE', fields=['instrument'])
        source_df = pd.merge(source_df, basic_df[['instrument']], on=['instrument'], how='inner')
        print('>>>>basic filtered shape: ', source_df.shape)
        source_df['product_code'] = source_df.instrument.apply(lambda x: re.findall(r"\D+", x.split('.')[0])[0])

        # 计算主力合约8888:这个表也是通过FTR_EODRICE来的
        # 这里不对主力合约的数据做判断，全部放在dominant构建脚本里面：如 主力合约每日都要有行情，每日的每个品种只有一个主力合约
        dominant_bar1d = source_df[source_df.is_main_ctr == '1']
        dominant_bar1d['instrument'] = dominant_bar1d.instrument.apply(lambda x: re.sub('\d+', '8888', x))

        bar1d_df = pd.concat([source_df, dominant_bar1d], ignore_index=True)
        print('>>>>instrument len: ', len(source_df.instrument.unique()))

        self._update_data(bar1d_df)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):
    obj = BuildBar1d(start_date=start_date, end_date=end_date, write_mode='update')
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()
