import re
import click
import datetime
import numpy as np
import pandas as pd
from sdk.datasource import DataSource, UpdateDataSource, D

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

from CN_STOCK_A.build_bigquant_table.template import Build
from CN_FUTURE.schema_category import dominant_CN_FUTURE as category_info


class BuildDominant(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildDominant, self).__init__(start_date, end_date)

        self.schema = {
            'friendly_name': category_info[2],
            'date_field': 'date',
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'primary_key': ['date', 'instrument'],
            'active': True,
            'fields': {
                'instrument': {'desc': '合约代码', 'type': 'str'},
                'date': {'desc': '日期', 'type': 'datetime64[ns]'},
                'dominant': {'desc': '对应合约代码', 'type': 'str'},
                'product_code': {'desc': '品种代码', 'type': 'str'}
                       },
            'partition_date': 'Y',
            'partition_field': 'product_code',
        }

        self.future_suffix = {
            "DCE": "DCE",
            "SHFE": "SHF",
            "INE": "INE",
            "CZCE": "CZC",
            "CFFEX": "CFX",
            "CFE": "CFX"
        }
        self.write_mode = write_mode
        self.alias = 'dominant_CN_FUTURE'

    @staticmethod
    def conv_symbol(instrument):
        instrument = instrument.upper()
        if ('CZCE' in instrument) or ('CZC' in instrument):
            symbol, market = instrument.split('.')
            if len(symbol) == 5:
                curYear = 2
                symbol = '{}{}{}'.format(symbol[:2], curYear, symbol[2:])
            else:
                symbol = symbol[:2] + symbol[3:]
            instrument = symbol + '.' + market
        instrument = instrument.replace('CFFEX', 'CFX').replace('CZCE', 'CZC').replace('SHFE', 'SHF').replace('CFE',
                                                                                                              'CFX')
        return instrument

    @staticmethod
    def _read_basic_info():
        df = DataSource('basic_info_CN_FUTURE').read(fields=['instrument', 'product_code', 'list_date', 'delist_date'])
        df = df[(~df.instrument.str.contains('8888')) & (~df.instrument.str.contains('0000')) &
                (~df.instrument.str.contains('9999'))]
        return df

    def calculate(self):

        source_df = self._read_DataSource_by_date(table='FTR_EODPRICE', fields=['instrument', 'date', 'is_main_ctr'])
        
        if not isinstance(source_df, pd.DataFrame):
            print(f"{self.start_date} - {self.end_date} have no data, return")
            return 
        source_df = source_df[source_df.is_main_ctr == '1']
        source_df = source_df[['instrument', 'date']]

        all_basic_df = self._read_DataSource_all(table='basic_info_CN_FUTURE',
                                                 fields=['instrument', 'product_code', 'list_date', 'delist_date'])
        basic_df = all_basic_df[(~all_basic_df.instrument.str.contains('8888')) &
                                (~all_basic_df.instrument.str.contains('0000')) &
                                (~all_basic_df.instrument.str.contains('9999'))][['instrument', 'product_code']]

        print(f"before filter by basic_info, shape is: {source_df.shape}")
        rel_df = pd.merge(source_df, basic_df, on=['instrument'], how='inner')
        print(f"after filter by basic_info, shape is: {source_df.shape}")

        rel_df.rename(columns={'instrument': 'dominant'}, inplace=True)
        rel_df['suffix'] = rel_df.dominant.apply(lambda x: x.split('.')[1])
        rel_df['instrument'] = rel_df.product_code + '8888.' + rel_df.suffix
        del rel_df['suffix']

        # 是否要检测一下basic_info里面有的product_code必须有主力合约的行情？？ Todo
        # dominant_basic_df = all_basic_df[all_basic_df.instrument.str.contains('8888')]

        self._update_data(rel_df)


@click.command()
@click.option('--start_date', default=(datetime.date.today()-datetime.timedelta(15)).isoformat(), help='start_date')
@click.option('--end_date', default=datetime.date.today().isoformat(), help='end_date')
def entry(start_date, end_date):
    BuildDominant(start_date, end_date).calculate()


if __name__ == '__main__':
    # entry(start_date='2015-01-01', end_date='2021-09-07')
    entry()
