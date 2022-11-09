# -*- coding:utf-8 -*-
import os
import warnings
import datetime
import json
from collections import defaultdict

import pandas as pd

from .v5.bigdatasource import DataSource
from sdk.datasource.settings import deprecated_factors, CN_FUTURE_dic_path, CN_FUND_dic_path, \
    CN_STOCK_A_dic_path, features_map_file, features_dic_path
from sdk.datasource.api.constant import Market


class DataReader(DataSource):

    @staticmethod
    def _zx_trading_days(market='CN', start_date=None, end_date=None):
        df = DataSource('AShareCalendar').read()
        df['date'] = pd.to_datetime(df.trade_days)
        df.rename_axis({x: x.lower() for x in df.columns}, axis=1, inplace=True)
        filter_args = df.s_info_exchmarket == 'SSE'
        if start_date:
            filter_args &= df.date >= start_date
        if end_date:
            filter_args &= df.date <= end_date
        df = df[filter_args]
        return df[['date']]

    @staticmethod
    def _zx_instruments(start_date='2005-01-01', end_date=None, market='CN_STOCK_A'):
        df = DataSource('AShareDescription').read()
        df.rename_axis({x: x.lower() for x in df.columns}, axis=1, inplace=True)
        df['list_date'] = pd.to_datetime(df['s_info_listdate'])
        df['delist_date'] = pd.to_datetime(df['s_info_delistdate']).fillna(pd.to_datetime('2199-01-01'))
        return sorted(set(df[~((end_date < df.list_date) | (df.delist_date < start_date))].instrument))

    @staticmethod
    def trading_days(market='CN', start_date=None, end_date=None):
        '''
        获取指定时间段某个交易市场的交易日历

        :param market: 证券市场，默认值是 CN (中国A股)，更多见 :doc:`exchange_markets`
        :param start_date: 字符串，开始日期
        :param end_date: 字符串，结束日期
        '''
        if not isinstance(market, str):
            market = 'CN'
        country_code = market.split('_')[0]

        df = DataSource('trading_days').read()
        filter_args = df.country_code == country_code
        if start_date:
            filter_args &= df.date >= start_date
        if end_date:
            filter_args &= df.date <= end_date
        df = df[filter_args]
        return df.drop(['country_code'], axis=1)

    @staticmethod
    def instruments(start_date='2005-01-01', end_date=datetime.datetime.today().isoformat(),
                    market='CN_STOCK_A'):
        '''
        获取指定时间段内有效的股票代码列表
        :param start_date: 字符串，开始日期
        :param end_date: 字符串，结束日期
        :param market: 证券市场，默认值是 CN_STOCK_A (中国A股)，更多见 :doc:`exchange_markets`
        '''
        if market == "CN":
            market = "CN_STOCK_A"
        elif market == "HK":
            market = 'HK_STOCK'
        elif market == "US":
            market = 'US_STOCK'
        if str(start_date) > str(end_date):
            raise Exception("Given start_date > end_date ! {} > {}".format(start_date, end_date))
        df = DataSource('basic_info_%s' % market).read()
        instruments = sorted(set(df[~((end_date < df.list_date) | (df.delist_date < start_date))].instrument))
        return instruments

    @staticmethod
    def financial_statements(instruments, start_date, end_date=None, fields=None):
        '''
        财务报表API。

        :param instruments: 字符串数组，股票代码列表
        :param start_date: 字符串，开始日期
        :param end_date: 字符串，结束日期
        :param fields: 字符串数组，请求的字段列表，详见 数据字段，None 表示所有字段
        '''

        if isinstance(instruments, str):
            instruments = instruments.split(',')
        instruments = set(instruments)
        field = fields
        if fields:
            if isinstance(fields, str):
                fields = fields.split(',')
            field = [x + '_0' for x in fields]
        df = DataSource('financial_statement_ff_CN_STOCK_A').read(instruments, start_date=start_date, end_date=end_date,
                                                                  fields=field)
        if df is None:
            return None
        df.columns = [x.replace('_0', '') for x in df.columns]
        df.reset_index(drop=True, inplace=True)
        return df

    def holidays(self, start_date=None, end_date=None, market='CN'):
        """ 获取节假日信息 """
        table = "holidays_{}".format(market)
        df = DataSource(table).read()
        if start_date:
            df = df[df.date >= start_date]
        if end_date:
            df = df[df.date <= end_date]
        return df

    def _get_table_fields_map(self, config_file, fields):
        """根据配置文件读取数据"""
        with open(config_file, mode='r', encoding='utf-8') as f:
            # field_table_map: {field: table, field2: table2, ....}
            field_table_map = json.loads(f.read().strip())
        table_fields = defaultdict(list)
        table_fields_custom = defaultdict(list)  # 带有 __ 的field 在后， 先抽取标准因子，最后把自定义特征添加上去
        for field in fields:
            if field in deprecated_factors:
                self.log.warning("factor [{}] will deprecated，you can replace "
                                 "with [{}]".format(field, deprecated_factors.get(field)))
            table = field_table_map.get(field)
            if table:
                table_fields[table].append(field)
            elif "__" in field:
                table = field.split("__")[0]
                field = field.split("__")[1]
                table_fields_custom[table].append(field)
            else:
                self.log.warning("cannot find filed [{}] table in field_table_map!".format(field))
        return [table_fields, table_fields_custom]

    def features(self, instruments=None, start_date='2005-01-01', end_date=datetime.datetime.today().isoformat(),
                 fields=None, groupped_by_instrument=False, market=Market.CN_STOCK_A):
        # if not instruments:
        #     instruments = self.instruments(start_date=start_date, end_date=end_date)
        if isinstance(instruments, set):
            instruments = list(instruments)
        if isinstance(instruments, str):
            instruments = instruments.split(',')

        fields = fields if fields else ['open_0', 'close_0']
        if not isinstance(fields, list):
            raise Exception("fields must be list!")

        market = market or Market.CN_STOCK_A
        config_file = features_map_file.format(market=market)
        if not os.path.exists(config_file):
            if market == Market.CN_STOCK_A:
                # 兼容B端旧版feature map文件
                config_file = features_dic_path
            else:
                raise Exception("Unknow market {} !".format(market))

        table_fields_list = self._get_table_fields_map(config_file, fields)
        df = pd.DataFrame()
        for index, table_fields_map in enumerate(table_fields_list):
            for table, _fields in table_fields_map.items():
                sub_df = DataSource(table).read(start_date=start_date, end_date=end_date,
                                                fields=_fields, instruments=instruments)
                if sub_df is None or sub_df.empty:
                    continue
                if index == 1:
                    # 混合自定义抽取需要带表名
                    rename_dic = {}
                    for f in _fields:
                        rename_dic[f] = "{}__{}".format(table, f)
                    sub_df = sub_df.rename(columns=rename_dic)

                if df.empty:
                    df = sub_df
                else:
                    df = df.merge(sub_df, on=['date', 'instrument'], how='left')
        if df.empty:
            return df
        lost_fields = list(set(fields) - set(df.columns))
        if lost_fields:
            self.log.warning('unknown fields: {}'.format(lost_fields))
        df = df.sort_values(by=["instrument", "date"])
        if groupped_by_instrument:
            df_list = []
            for instrument, instrument_df in df.groupby('instrument'):
                df_list.append((instrument, instrument_df.reset_index(drop=True)))
            return dict(df_list)
        return df.reset_index(drop=True)

    def history_data(self, instruments, start_date='2005-01-01', end_date=datetime.datetime.now().strftime("%Y-%m-%d"),
                     fields=['open', 'close', 'volume'], market='CN_STOCK_A'):
        if isinstance(instruments, set):
            instruments = list(instruments)
        if isinstance(instruments, str):
            instruments = instruments.split(',')

        # 获取需要的表名
        fields = list(set(fields) - {'date', 'instrument'})
        if market == 'CN_FUTURE':
            config_file = CN_FUTURE_dic_path
        elif market == 'CN_FUND':
            config_file = CN_FUND_dic_path
        else:
            config_file = CN_STOCK_A_dic_path
        table_fields_list = self._get_table_fields_map(config_file, fields)

        df = pd.DataFrame()
        for table_fields_map in table_fields_list:
            for table, _fields in table_fields_map.items():
                if table == 'bar1d_CN_STOCK_A':
                    table = 'bar1d_CN_STOCK_A_all'
                if table == 'financial_statement_CN_STOCK_A':
                    table = 'financial_statement_ff_CN_STOCK_A'
                    _fields = [x + '_0' for x in _fields]
                sub_df = DataSource(table).read(start_date=start_date, end_date=end_date,
                                                fields=_fields, instruments=instruments)
                if sub_df is None:
                    continue
                if table == 'financial_statement_ff_CN_STOCK_A':
                    sub_df.columns = [x.replace('_0', '') for x in sub_df.columns]

                if df.empty:
                    df = sub_df
                else:
                    if 'date' in df.columns and 'date' in sub_df.columns:
                        on_key = ['instrument', 'date']
                    else:
                        on_key = ['instrument']
                    df = pd.merge(df, sub_df, how='outer', on=on_key)
        table_fields = {}
        for t in table_fields_list:
            table_fields.update(t)

        if 'date' in df.columns:
            df.sort_values(['date', 'instrument'], inplace=True)
        else:
            df.sort_values(['instrument'], inplace=True)
        return df.reset_index(drop=True)


D, DataReaderV2 = DataReader(), DataReader
DataReaderV3 = DataReader


warnings.warn(
    "This module is deprecated. Please use `from bigdatasource.api import D`.", DeprecationWarning, stacklevel=2
)
