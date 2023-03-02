import os
import sys
import click
import datetime
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

            'opn_prc_adj': 'open',
            'hi_prc_adj': 'high',
            'lo_prc_adj': 'low',
            'cls_prc_adj': 'close',
            'vol': 'volume',
            'amt': 'amount',
            'adj_fct': 'adjust_factor',
            'turn_rat': 'turn',
                      }

        self.schema = {
            "desc": "基金(场内)日线行情",
            "active": True,
            "date_field": 'date',
            'category': '场内基金数据/行情数据',
            'rank': 1002004,
            "friendly_name": "基金(场内)日线行情",
            'primary_key': ['date', 'instrument', 'fnd_cd'],
            'fields': {
                'instrument': {'desc': '合约代码', 'type': 'str'},
                'date': {'desc': '日期', 'type': 'datetime64[ns]'},
                'fnd_cd': {'desc': '基金内部编码', 'type': 'str'},
                'high': {'desc': '复权最高价', 'type': 'float32'},
                'open': {'desc': '复权开盘价', 'type': 'float32'},
                'low': {'desc': '复权最低价', 'type': 'float32'},
                'close': {'desc': '复权收盘价', 'type': 'float32'},
                'amount': {'desc': '成交金额(元)', 'type': 'float64'},
                'volume': {'desc': '成交量(手)', 'type': 'float64'},
                'adjust_factor': {'desc': '复权因子', 'type': 'float64'}
            },
            'version': '5.0',
            'partition_date': 'Y',
            'file_type': 'bdb'
        }

        self.alias = "bar1d_CN_FUND"

    def _read_source_data(self):
        fields = list(self.rename_map.keys())
        fields.append('sys_isvld')
        df = DataSource('FUND_EODPRICE').read(start_date=self.start_date, end_date=self.end_date, fields=fields)
        if not isinstance(df, pd.DataFrame):
            return pd.DataFrame()
        df = df[df.sys_isvld != 0]
        del df['sys_isvld']
        return df

    def run(self):
        # UpdateDataSource().update_metadata(alias=self.alias, schema=self.schema)
        print(f'start deal {self.start_date} -- {self.end_date} -----------------')
        source_df = self._read_source_data()
        if (source_df is None) or source_df.empty:
            return
        basic_df = DataSource('basic_info_CN_FUND').read(fields=['instrument', 'fnd_cd'])

        source_df = source_df.rename(columns=self.rename_map)
        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        source_df = source_df[source_df.suffix.isin(['ZOF', 'HOF'])]

        # 判断日线行情数据的instrument需要是否全在basic_info里面---------
        source_ins_lst = source_df.instrument.unique().tolist()
        basic_ins_lst = basic_df.instrument.unique().tolist()
        outer_ins_set = set(source_ins_lst) - set(basic_ins_lst)
        assert not outer_ins_set, print(f'hava instrument not in basic_info: {outer_ins_set}')
        # ---------------------------------------------------------

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


