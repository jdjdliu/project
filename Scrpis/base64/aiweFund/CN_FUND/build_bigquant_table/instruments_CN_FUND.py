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


class Build:

    def __init__(self, start_date, end_date):
        self.schema = {
            'category': '场内基金数据/基本信息',
            'rank': 1001007,  # 跟在 基金/基础信息 目录的原表后面的编号
            'active': True,
            'date_field': 'date',
            'primary_key': ['date', 'instrument', 'fnd_cd'],
            'partition_date': 'Y',
            'fields': {
                'instrument': {'desc': '证券代码', 'type': 'str'},
                'fnd_cd': {'desc': '基金内部编码', 'type': 'str'},
                'name': {'desc': '证券名称', 'type': 'str'},
                'date': {'desc': '日期', 'type': 'datetime64'}},
            'friendly_name': '每日(场内)基金列表',
            'desc': '每日(场内)基金列表',
        }

        self.alias = 'instruments_CN_FUND'

        self.start_date = start_date
        self.end_date = end_date

    def run(self):
        bar1d_df = DataSource('bar1d_CN_FUND').read(start_date=self.start_date, end_date=self.end_date,
                                                    fields=['instrument', 'date', 'fnd_cd'])
        if (bar1d_df is None) or bar1d_df.empty:
            print('bar1d df read is empty..... return None')
            return
        basic_df = DataSource('basic_info_CN_FUND').read(fields=['instrument', 'name', 'fnd_cd'])
        ins_df = pd.merge(bar1d_df, basic_df, on=['instrument', 'fnd_cd'], how='left')
        ins_df = ins_df[list(self.schema['fields'].keys())]
        ins_df = change_fields_type(ins_df, schema=self.schema)
        print(ins_df)
        if ins_df.empty:
            return
        UpdateDataSource().update(df=ins_df, alias=self.alias, schema=self.schema)


@click.command()
@click.option('--start_date', default=(datetime.date.today()-datetime.timedelta(10)).isoformat(), help='start_date')
@click.option('--end_date', default=datetime.date.today().isoformat(), help='end_date')
def entry(start_date, end_date):
    Build(start_date=start_date, end_date=end_date).run()


if __name__ == '__main__':
    # entry(start_date='2021-01-01', end_date='2021-09-07')
    import warnings
    warnings.filterwarnings('ignore')
    entry()


