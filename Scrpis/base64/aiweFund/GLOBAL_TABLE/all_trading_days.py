import click
import datetime
import pandas as pd
# from bigdatasource.api import UpdateDataSource, DataSource
from sdk.datasource import UpdateDataSource, DataSource


class Build:

    def __init__(self):
        self.schema = {
            'active': True,
            'category': '通用数据/日历',
            'date_field': 'date',
            'primary_key': ['date', 'country_code'],
            'partition_date': None,
            'fields': {
                'date': {'desc': '日期', 'type': 'datetime64'},
                'country_code': {'desc': 'country_code', 'type': 'str'}},
            'friendly_name': '交易日历',
            'desc': '交易日历'}

        self.schema_all_trading = {
            'active': True,
            'category': '通用数据/日历',
            'date_field': 'date',
            'primary_key': ['date', 'country_code'],
            'partition_date': None,
            'fields': {
                'date': {'desc': '日期', 'type': 'datetime64'},
                'country_code': {'desc': 'country_code', 'type': 'str'}},
            'friendly_name': '交易日历(包含未来一年)',
            'desc': '交易日历(包含未来一年)'}
        self.trading_alias = 'trading_days'
        self.all_trading_alias = 'all_trading_days'

    def merge_df(self, start_date, end_date):
        df = DataSource('REF_CALENDAR').read(start_date=start_date,
                                             end_date=(datetime.date.today() + datetime.timedelta(days=365)).strftime('%Y-%m-%d'))
        df = df[df.exg_cd.isin(["SZ", "HKEX", "NASDAQ"])]
        df = df[df.is_trd_dt == '1']  # add in 2022-07-28
        df['country_code'] = df.exg_cd.replace({"SZ": 'CN', 'HKEX': 'HK', 'NASDAQ': 'US'})
        df = df[['date', 'country_code']].drop_duplicates()
        country_lst = df.country_code.unique().tolist()
        assert {'CN', 'HK', 'US'} >= set(country_lst), print("have other country_code: ", country_lst)
        df['date'] = pd.to_datetime(df.date)
        trading_df = df[df.date.between(start_date, end_date)]
        return df, trading_df

    def run(self, start_date, end_date):
        all_trading_df, trading_df = self.merge_df(start_date, end_date)
        print(all_trading_df)
        print(trading_df)
        UpdateDataSource().update(df=trading_df, alias=self.trading_alias, schema=self.schema, update_schema=True)
        UpdateDataSource().update(df=all_trading_df, alias=self.all_trading_alias, schema=self.schema_all_trading, update_schema=True)


@click.command()
@click.option('--start_date', default=(datetime.date.today() - datetime.timedelta(15)).strftime('%Y-%m-%d'), help='start_date')
@click.option('--end_date', default=datetime.date.today().strftime('%Y-%m-%d'), help='end_date')
def entry(start_date, end_date):
    
    # keep T+1
    if end_date == datetime.date.today().strftime("%Y-%m-%d"):
        end_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    Build().run(start_date, end_date)


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore')
    entry()

