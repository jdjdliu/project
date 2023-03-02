import re
import click
import datetime
import numpy as np
import pandas as pd
from collections import defaultdict
from sdk.datasource import DataSource, UpdateDataSource, D

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

from common import change_fields_type
from CN_FUTURE.schema_category import bar1d_CN_FUTURE as category_info


schema = {
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


def _groupby(instruments):
    pattern = re.compile('([A-Z]+)(\d*)\.([A-Z]+)')  # noqa
    group = defaultdict(list)
    for i in instruments:
        result = pattern.search(i)
        if not result:
            continue
        category, code, market = result.groups()
        group[(category, market)].append(i)
    return group


def build_0000(date):
    all_data = []
    print('processing date: {}'.format(date))
    bar_data = DataSource('bar1d_CN_FUTURE').read(start_date=date, end_date=date)
    if bar_data is None or bar_data.empty:
        print("No data in bar1d_CN_FUTURE {}".format(date))
        return
    bar_data = bar_data[
        (~bar_data.instrument.str.contains("8888")) & (~bar_data.instrument.str.contains("0000")) &
        (~bar_data.instrument.str.contains("9999"))]
    if bar_data.empty:
        return
    inst = list(bar_data.instrument.unique())
    if not inst:
        return
    for k, v in _groupby(inst).items():
        data = {}
        gbar = bar_data[bar_data.instrument.isin(v)]
        if gbar.empty:
            print('gbar empty', v)
            continue
        gbar['open_intl_weight'] = gbar['open_intl'] / gbar.open_intl.sum()
        gbar.loc[gbar.open_intl_weight.isnull(), 'open_intl_weight'] = 1 / gbar.instrument.count()
        # 新算法，和通达信客户端指数偏差3个点左右
        data['open'] = (gbar['open'] * gbar['open_intl_weight']).sum()  # noqa
        data['high'] = (gbar['high'] * gbar['open_intl_weight']).sum()  # noqa
        data['low'] = (gbar['low'] * gbar['open_intl_weight']).sum()  # noqa
        data['close'] = (gbar['close'] * gbar['open_intl_weight']).sum()  # noqa
        data['settle'] = (gbar['settle'] * gbar['open_intl_weight']).sum()  # noqa

        data['amount'] = gbar['amount'].sum()
        data['volume'] = gbar['volume'].sum()
        data['open_intl'] = gbar['open_intl'].sum()
        data['low_limit'] = (gbar['low_limit'] * gbar['open_intl_weight']).sum()
        data['high_limit'] = (gbar['high_limit'] * gbar['open_intl_weight']).sum()
        data['date'] = date
        data['instrument'] = '{}0000.{}'.format(k[0], k[1])
        # data['contunit'] = gbar.contunit.iloc[0]
        all_data.append(data)
    df = pd.DataFrame(all_data)
    df['close'] = df.close.astype(np.float32)
    df['high'] = df.high.astype(np.float32)
    df['low'] = df.low.astype(np.float32)
    df['open'] = df.open.astype(np.float32)
    df['settle'] = df.settle.astype(np.float32)
    df['date'] = pd.to_datetime(df.date)
    return df


@click.command()
@click.option('--start_date', default=(datetime.date.today()-datetime.timedelta(15)).strftime('%Y-%m-%d'), help='start_date')
@click.option('--end_date', default=datetime.date.today().strftime('%Y-%m-%d'), help='end_date')
def main(start_date, end_date):
    trading_days = D.trading_days(start_date=start_date, end_date=end_date)

    df_list = []
    for day in trading_days.date:
        date = day.strftime(("%Y-%m-%d"))
        df = build_0000(date)
        if df is None:
            continue
        df_list.append(df)

    all_df = pd.concat(df_list, ignore_index=True)
    table = "bar1d_CN_FUTURE"
    all_df['product_code'] = all_df['instrument'].apply(lambda x: re.findall('\D+', x)[0])
    all_df = all_df[list(schema['fields'].keys())]
    all_df = change_fields_type(df=all_df, schema=schema)
    print(all_df.shape)
    UpdateDataSource().update(df=all_df, alias=table, schema=schema)


if __name__ == '__main__':
    main()
