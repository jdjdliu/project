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


class BuildBasicInfo:

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

        self.rename_map = {
            'fnd_cd': 'fnd_cd',
            'instrument': 'instrument',
            'lst_dt': 'list_date',
            'delst_dt': 'delist_date',
            'chn_nm': 'name',

            'found_dt': 'found_date',
            "due_dt": "due_date",
                      }

        self.schema = {
            'friendly_name': '基金(场内)基本信息',
            'desc': '基金(场内)基本信息',
            'category': '场内基金数据/基本信息',
            'rank': 1001006,  # 跟在 基金/基础信息 目录的原表后面的编号
            'date_field': None,
            'primary_key': ['instrument', 'fnd_cd'],
            'active': True,
            'fields': {
                # 'abbreviation_name': {'desc': '缩写简称', 'type': 'str'},
                'delist_date': {'desc': '退市日期,如果没有退市则为2200-01-01', 'type': 'datetime64[ns]'},
                # 'type': {'desc': '类型，etf(ETF基金)，fja（分级A），fjb（分级B）', 'type': 'str'},
                'instrument': {'desc': '证券代码', 'type': 'str'},
                'name': {'desc': '证券名称', 'type': 'str'},
                'list_date': {'desc': '上市日期', 'type': 'datetime64[ns]'},

                'found_date': {'desc': '成立日期', 'type': 'datetime64[ns]'},
                'due_date': {'desc': '到期日期', 'type': 'datetime64[ns]'},
                'fnd_cd': {'desc': '基金内部编码', 'type': 'str'},


            },
            'version': '5.0',
            'partition_date': None,
            'file_type': 'bdb',
        }

        self.alias = "basic_info_CN_FUND"

    def _read_source_data(self):
        fields = list(self.rename_map.keys())
        # 每次都读取的全量FUND_BASICINFO
        fields.append('sys_isvld')
        df = DataSource('FUND_BASICINFO').read(fields=fields)
        df = df[df.sys_isvld != 0]
        del df['sys_isvld']
        return df

    def run(self):
        print(f'start deal {self.start_date} -- {self.end_date} -----------------')
        source_df = self._read_source_data()
        if (source_df is None) or source_df.empty:
            return
        source_df = source_df.rename(columns=self.rename_map)
        # source_df['instrument'] = source_df.instrument.apply(lambda x: x.replace('SZ', 'ZOF').replace('SH', 'HOF'))
        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        suffix_lst = source_df.suffix.unique().tolist()
        assert set(suffix_lst) >= {'ZOF', 'HOF', 'OFCN'}, print(f'have error suffix code: {suffix_lst}')

        source_df = source_df[source_df.suffix.isin(['ZOF', 'HOF'])]
        source_df = source_df[list(self.schema['fields'].keys())]
        source_df.loc[source_df.delist_date.isnull(), 'delist_date'] = datetime.datetime(2200, 1, 1)
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

    obj = BuildBasicInfo(start_date=start_date, end_date=end_date)
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()


