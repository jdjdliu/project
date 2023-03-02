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
            'adj_fct': 'adjust_factor',
            'dcnt_rat': 'discount_rate',
                      }

        self.schema = {
            "desc": "基金(场内)复权因子",
            "active": True,
            "date_field": 'date',
            'category': '场内基金数据/行情数据',
            'rank': 1002005,
            "friendly_name": "基金(场内)复权因子",
            'primary_key': ['date', 'instrument', 'fnd_cd'],
            'fields': {
                'instrument': {'desc': '合约代码', 'type': 'str'},
                'date': {'desc': '日期', 'type': 'datetime64[ns]'},
                'fnd_cd': {'desc': '基金内部编码', 'type': 'str'},
                'adjust_factor': {'desc': '复权因子', 'type': 'float32'},
                'discount_rate': {'desc': '贴水率', 'type': 'float32'},
            },
            'version': '5.0',
            'partition_date': 'Y',
            'file_type': 'bdb'
        }

        self.alias = "adjust_factor_CN_FUND"

    def _read_source_data(self):
        fields = list(self.rename_map.keys())
        fields.append('sys_isvld')
        df = DataSource('FUND_EODPRICE').read(start_date=self.start_date, end_date=self.end_date, fields=fields)
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

        # 截取复权因子行情数据的instrument需要是否全在basic_info里面---------
        source_df = pd.merge(source_df, basic_df, on=['instrument', 'fnd_cd'], how='inner')
        # ---------------------------------------------------------

        source_df = source_df.rename(columns=self.rename_map)
        # source_df['instrument'] = source_df.instrument.apply(lambda x: x.replace('SZ', 'ZOF').replace('SH', 'HOF'))
        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        suffix_lst = source_df.suffix.unique().tolist()
        assert set(suffix_lst) >= {'ZOF', 'HOF'}, print(f'have error suffix code: {suffix_lst}')

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
