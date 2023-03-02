# 停牌暂停交易的股票当日的价格用前一天的填充，成交量为0
import click
import datetime
import pandas as pd
from template import Build as TmpBuild
from sdk.datasource import DataSource, UpdateDataSource
from CN_STOCK_A.schema_catetory import bar1d_CN_STOCK_A as category_info


DAY_FORMAT = '%Y-%m-%d'


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def ffill_bar1d(start_date, end_date):
    alias = 'bar1d_CN_STOCK_A'
    schema = {
        'friendly_name': category_info[2],
        'date_field': 'date',
        'category': category_info[0],
        'rank': category_info[1],
        'desc': category_info[2],
        'active': True,
        'partition_date': 'Y',
        'primary_key': ['date', 'instrument'],
        'fields': {
            'stk_cd': {'desc': '股票内部编码', 'type': 'str'},
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
    read_start_date = (datetime.datetime.strptime(start_date, DAY_FORMAT) - datetime.timedelta(days=30)).strftime(
        DAY_FORMAT)
    print("开始检查填充 bar1d 数据 {} {}".format(start_date, end_date))
    bar1d_df = DataSource(alias).read(start_date=read_start_date, end_date=end_date)

    bar1d_df = bar1d_df.sort_values(['instrument', 'date'])

    bar1d_df['close'] = bar1d_df.groupby(['instrument']).close.ffill()
    bar1d_df.loc[bar1d_df.open.isnull(), 'open'] = bar1d_df.close
    bar1d_df.loc[bar1d_df.high.isnull(), 'high'] = bar1d_df.close
    bar1d_df.loc[bar1d_df.low.isnull(), 'low'] = bar1d_df.close
   
    bar1d_df['adjust_factor'] = bar1d_df.groupby(['instrument']).adjust_factor.ffill()
    bar1d_df['amount'] = bar1d_df.amount.fillna(value=0)
    bar1d_df['volume'] = bar1d_df.volume.fillna(value=0)
    bar1d_df['turn'] = bar1d_df.turn.fillna(value=0)

    # 更新数据
    if not bar1d_df.empty:
        UpdateDataSource().update(alias=alias, df=bar1d_df, schema=schema)


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore')
    ffill_bar1d()
