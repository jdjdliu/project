import os
import sys
import click
import datetime
import numpy as np
import pandas as pd
from sdk.datasource import UpdateDataSource, DataSource

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)
from common import change_fields_type


class BuildBar1d:

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

        self.rename_map = {
            'fnd_cd': 'fnd_cd',
            'instrument': 'instrument',
            'date': 'date',
            'unt_nav': 'nav',
            'is_xrt_dt': 'is_ex_date',
            'ccy_cd': 'ccy_cd',
            'nav_inc_rat': 'nav_chg_pct',
            'adj_fct': 'adjust_factor',
            'adj_unt_nav': 'adjust_nav',
            'acm_unt_nav': 'accum_nav',
            'anc_dt': 'publish_date',
                      }

        self.schema = {
            "desc": "基金(场外)历史净值",
            "active": True,
            "date_field": 'date',
            'category': '场外基金数据/行情数据',
            'rank': 1002010,
            "friendly_name": "基金(场外)历史净值",
            'primary_key': ['date', 'instrument', 'fnd_cd'],
            'fields': {
                'instrument': {'desc': '基金代码', 'type': 'str'},
                'date': {'desc': '净值日期', 'type': 'datetime64[ns]'},
                'end_date': {'desc': '净值日期', 'type': 'datetime64[ns]'},
                'fnd_cd': {'desc': '基金内部编码', 'type': 'str'},

                'nav': {'type': 'float64', 'desc': '单位净值(元)'},
                'is_ex_date': {'type': 'str', 'desc': '是否净值除权日'},
                'ccy_cd': {'type': 'str', 'desc': '币种'},
                'nav_chg_pct': {'type': 'float64', 'desc': '单位净值涨跌幅(%)'},
                'accum_nav': {'type': 'float64', 'desc': '累计净值(元)'},
                'adjust_factor': {'type': 'float64', 'desc': '复权因子'},
                'adjust_nav': {'type': 'float64', 'desc': '复权单位净值(元)'},
                'publish_date': {'type': 'datetime64[ns]', 'desc': '净值公布日期'},
            },
            'version': '5.0',
            'partition_date': 'Y',
            'file_type': 'bdb'
        }

        self.alias = "history_nav_CN_MUTFUND"

    def _read_source_data(self):
        fields = list(self.rename_map.keys())
        fields.append('sys_isvld')
        df = DataSource('FUND_NETVALUE').read(start_date=self.start_date, end_date=self.end_date, fields=fields)
        df = df[df.sys_isvld != 0]
        del df['sys_isvld']
        return df


    def run(self):
        # UpdateDataSource().update_metadata(alias=self.alias, schema=self.schema)
        print(f'start deal {self.start_date} -- {self.end_date} -----------------')
        source_df = self._read_source_data()
        if (source_df is None) or source_df.empty:
            print('source df read is empty..... return None')
            return
        basic_df = DataSource('basic_info_CN_MUTFUND').read(fields=['instrument', 'fnd_cd'])

        source_df = source_df.rename(columns=self.rename_map)
        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        source_df = source_df[source_df.suffix.isin(['OFCN'])]

        # 判断日线行情数据的instrument需要是否全在basic_info里面---------
        source_ins_lst = source_df.instrument.unique().tolist()
        basic_ins_lst = basic_df.instrument.unique().tolist()
        outer_ins_set = set(source_ins_lst) - set(basic_ins_lst)
        assert not outer_ins_set, print(f'hava instrument not in basic_info: {outer_ins_set}')
        # ---------------------------------------------------------
        source_df['end_date'] = source_df.date
        source_df = source_df[list(self.schema['fields'].keys())]
        source_df = change_fields_type(source_df, schema=self.schema)
        print(source_df)
        print(source_df.dtypes)
        if source_df.empty:
            return
        UpdateDataSource().update(df=source_df, alias=self.alias, schema=self.schema)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):

    obj = BuildBar1d(start_date=start_date, end_date=end_date)
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()


