import click
import datetime
import pandas as pd
from sdk.datasource import DataSource, UpdateDataSource
from template import Build
from CN_STOCK_A.schema_catetory import bar1d_CN_STOCK_A_all as category_info
from common import change_fields_type


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def build(start_date, end_date):
    schema = {
        'friendly_name': category_info[2],
        'date_field': 'date',
        'category': category_info[0],
        'rank': category_info[1],
        'desc': category_info[2],
        "active": True,
        "partition_date": "Y",
        "primary_key": ["date", "instrument"],
        "file_type": "bdb",
        'fields': {
            # 'stk_cd': {'desc': '股票内部编码', 'type': 'str'},  #合并了bar1d_index的数据，这部分stk_cd为None,就不再存这个字段了
            'instrument': {'desc': '证券代码', 'type': 'str'},
            'date': {'desc': '日期', 'type': 'datetime64[ns]'},

            'close': {'desc': '收盘价(后复权)', 'type': 'float32'},
            'high': {'desc': '最高价(后复权)', 'type': 'float32'},
            'low': {'desc': '最低价(后复权)', 'type': 'float32'},
            'open': {'desc': '开盘价(后复权)', 'type': 'float32'},
            'adjust_factor': {'desc': '复权因子', 'type': 'float32'},
            'amount': {'desc': '交易额', 'type': 'float64'},
            'volume': {'desc': '交易量', 'type': 'float64'},
            'deal_number': {'desc': '成交笔数', 'type': 'float64'},
            'turn': {'desc': '换手率', 'type': 'float64'},
        },
    }
    df1 = DataSource('bar1d_CN_STOCK_A').read(start_date=start_date, end_date=end_date)
    df2 = DataSource('bar1d_CN_STOCK_A_index').read(start_date=start_date, end_date=end_date)
    df3 = DataSource('bar1d_index_CN_STOCK_A').read(start_date=start_date, end_date=end_date)
    df = pd.concat([df1, df3, df2], ignore_index=True)
    df['deal_number'] = df['deal_number'].fillna(0)
    print("concat bar1d_CN_STOCK_A bar1d_index_CN_STOCK_A to bar1d_CN_STOCK_A_all, {}".format(df.shape))

    alias = 'bar1d_CN_STOCK_A_all'
    df = df[list(schema['fields'].keys())]
    df = change_fields_type(df, schema=schema)
    UpdateDataSource().update(df=df, alias=alias, schema=schema, write_mode='update')


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore')
    build()
