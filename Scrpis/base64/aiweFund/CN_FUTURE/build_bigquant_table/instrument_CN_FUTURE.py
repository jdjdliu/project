import click
import datetime
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

from common import change_fields_type
from sdk.datasource import DataSource, UpdateDataSource
from CN_FUTURE.schema_category import instruments_CN_FUTURE as category_info


schema = {
    'friendly_name': category_info[2],
    'date_field': 'date',
    'category': category_info[0],
    'rank': category_info[1],
    'desc': category_info[2],
    'primary_key': ['date', 'instrument'],
    'active': True,
    'fields': {
        'name': {'desc': '合约名称', 'type': 'str'},
        'date': {'desc': '日期', 'type': 'datetime64[ns]'},
        'instrument': {'desc': '合约代码', 'type': 'str'},
        'product_code': {'desc': '品种代码', 'type': 'str'}},
    'partition_date': 'Y',
    'partition_field': 'product_code'
          }


def main(start_date, end_date):
    table = "instruments_CN_FUTURE"
    all_df = DataSource("bar1d_CN_FUTURE").read(start_date=start_date, end_date=end_date, fields=['instrument', 'date'])
    all_df = all_df[['instrument', 'date']]
    basic_info_df = DataSource("basic_info_CN_FUTURE").read(fields=["instrument", "name", 'product_code'])
    all_df = all_df.merge(basic_info_df, how='left', on=['instrument'])

    all_df = all_df[['date', "instrument", "name", 'product_code']]
    all_df = all_df[(~all_df.instrument.str.contains("0000")) & (~all_df.instrument.str.contains("8888")) &
                    (~all_df.instrument.str.contains("9999"))]

    all_df = change_fields_type(df=all_df, schema=schema)

    UpdateDataSource().update(df=all_df, alias=table, schema=schema)


@click.command()
@click.option('--start_date', default=(datetime.date.today()-datetime.timedelta(15)).strftime('%Y-%m-%d'), help='start_date')
@click.option('--end_date', default=datetime.date.today().strftime('%Y-%m-%d'), help='end_date')
def entry(start_date, end_date):
    main(start_date, end_date)


if __name__ == '__main__':
    entry()
