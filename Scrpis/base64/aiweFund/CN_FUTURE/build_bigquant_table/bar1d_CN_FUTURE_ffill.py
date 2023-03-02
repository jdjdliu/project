import click
import datetime
import pandas as pd
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)
from CN_FUTURE.schema_category import bar1d_CN_FUTURE as category_info
from sdk.datasource import DataSource, UpdateDataSource

DAY_FORMAT = "%Y-%m-%d"


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

require_columns = ['instrument', 'date', 'open', 'close', 'high', 'low', 'open_intl', 'settle', 'amount', 'volume',
                   'low_limit', 'high_limit']


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def ffill_bar1d_CN_FUTURE(start_date, end_date):
    load_data_start_date = (datetime.datetime.strptime(start_date, DAY_FORMAT) - datetime.timedelta(days=30)).strftime(
        DAY_FORMAT)  # noqa

    df = DataSource('bar1d_CN_FUTURE').read(start_date=load_data_start_date, end_date=end_date)
    df_err = df[df.open.isnull() | df.close.isnull() | df.high.isnull() | df.low.isnull() | df.settle.isnull()]
    df_err = df_err[~(df_err.instrument.str.contains('0000'))]  # 2022-01-21修改指数合约计算方式，会导致整个品种open_intl的数据计算close\settle等为None
    # df[df.open.isnull() | df.open_intl == 0].shape

    df_true = df[~df.open.isnull() & ~df.close.isnull()]

    if df_err.empty:
        print("没有缺失数据 !")
        return

    df_repaired_list = []

    df_err_settle = df_err[~df_err.settle.isnull()]
    df_err_settle['open'] = df_err_settle['settle']
    df_err_settle['high'] = df_err_settle['settle']
    df_err_settle['low'] = df_err_settle['settle']
    df_err_settle['close'] = df_err_settle['settle']
    df_err_settle['volume'] = 0
    df_err_settle['amount'] = 0
    df_true = pd.concat([df_true, df_err_settle])
    df_repaired_list.append(df_err_settle)

    df_err = df_err[df_err.settle.isnull()]

    df_err_close = df_err[~df_err.close.isnull()]
    df_err_close['settle'] = df_err_close['close']
    df_true = pd.concat([df_true, df_err_close])
    df_repaired_list.append(df_err_close)

    df_err = df_err[df_err.close.isnull()]

    def get_pre_open_intl_settle(instrument, date):
        _df = df_true[(df_true.instrument == instrument) & (df_true.date <= date)]
        if len(_df) < 2:
            _df = df_true[(df_true.instrument == instrument) & (df_true.date >= date)]
            _df.sort_values(['date'], inplace=True)
            open_intl = _df.head(2).open_intl.iloc[0]
            settle = _df.head(2).settle.iloc[0]
        else:
            _df.sort_values(['date'], inplace=True)
            open_intl = _df.tail(2).open_intl.iloc[0]
            settle = _df.tail(2).settle.iloc[0]
        return open_intl, settle

    data_list = []
    bar1d_null_list = df_err.to_dict("records")
    for i, item in enumerate(bar1d_null_list):
        date = item.get("date")
        instrument = item.get("instrument")
        open_intl, pre_settle = get_pre_open_intl_settle(instrument, date)
        item["open"] = pre_settle
        item["high"] = pre_settle
        item["low"] = pre_settle
        item["close"] = pre_settle
        item["settle"] = pre_settle
        item["open_intl"] = open_intl
        item["amount"] = 0
        item["volume"] = 0
        print("填充数据 {}/{} {} {}: {}".format(i, len(bar1d_null_list), instrument, date, item))
        data_list.append(item)

    _df = pd.DataFrame(data_list)

    df_repaired_list.append(_df)

    all_df = pd.concat(df_repaired_list)

    if not all_df.empty:
        # 更新数据
        alias = 'bar1d_CN_FUTURE'
        import re
        all_df = all_df[require_columns]
        all_df['product_code'] = all_df['instrument'].apply(lambda x: re.findall('\D+', x)[0])

        UpdateDataSource().update(alias=alias, df=all_df, schema=schema)
        print(f'填充数据完成，填充数量：{all_df.shape[0]}')
    else:
        print("没有要填充的数据!!!!")


if __name__ == '__main__':
    # 测试环境跑这个脚本要报错，应该是None值太多的问题，往前取的时候没有数据用iloc out of range 了
    # 手动处理的，还有4000左右的None值无法处理 --20220827
    ffill_bar1d_CN_FUTURE()
