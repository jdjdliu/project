import click
import datetime
import pandas as pd
from template import Build
from CN_STOCK_A.schema_catetory import market_value_CN_STOCK_A as category_info


class BuildMarketValue(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildMarketValue, self).__init__(start_date, end_date)
        self.map_dic = {
            'stk_cd': 'stk_cd',
            'instrument': 'instrument',
            'end_dt': 'date',

            'tot_mkv': 'market_cap',
            'crc_mkv': 'market_cap_float',   # 确认单位是否一致：单位一致
            'pe_ttm': 'pe_ttm',
            'pe_lyr': 'pe_lyr',         # 源数据有扣非，这里用的pe_lyr没有用扣非，和线上数据对比过
            'ps_ttm': 'ps_ttm',
            'pb_lf': 'pb_lf',

        }

        self.schema = {
            'friendly_name': category_info[2],
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'date_field': 'date',
            'active': True,
            "primary_key": ['date', 'instrument'],
            "partition_date": "Y",
            "fields": {
                'stk_cd': {'desc': '股票内部编码', 'type': 'str'},
                'date': {'desc': '日期', 'type': 'datetime64'},
                'instrument': {'desc': '证券代码', 'type': 'str'},
                'market_cap': {'type': 'float64', 'desc': '总市值'},
                'market_cap_float': {'type': 'float64', 'desc': '流通市值'},
                'pe_ttm': {'type': 'float64', 'desc': '市盈率 (TTM)'},
                'pe_lyr': {'type': 'float64', 'desc': '市盈率 (LYR)'},
                'pb_lf': {'type': 'float64', 'desc': '市净率 (LF)'},
                'ps_ttm': {'type': 'float64', 'desc': '市销率 (TTM)'},
            },

        }

        self.write_mode = write_mode
        self.alias = 'market_value_CN_STOCK_A'

    def run(self):
        fields_lst = list(self.map_dic.keys())
        source_df = self._read_DataSource_by_date(table='STK_VALUATION', fields=fields_lst)
        del source_df['date']
        source_df = source_df.rename(columns=self.map_dic)

        print('>>>>before filter shape: ', source_df.shape)
        basic_df = self._read_DataSource_all(table='basic_info_CN_STOCK_A', fields=['instrument', 'list_date', 'delist_date'])
        source_df = pd.merge(source_df, basic_df, on=['instrument'], how='inner')
        print('>>>>basic filtered shape: ', source_df.shape)
        source_df = source_df[(source_df.date >= source_df.list_date) &
                              ((source_df.date <= source_df.delist_date) | (source_df.delist_date.isnull()))]
        del source_df['list_date']
        del source_df['delist_date']
        print('>>>>basic list_date and delist_date filtered shape: ', source_df.shape)
        trading_df = self._read_DataSource_by_date(table="trading_days")
        trading_df = trading_df[trading_df.country_code=='CN']
        source_df = pd.merge(source_df, trading_df[['date']], on='date', how='inner')

        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        source_df = source_df[source_df.suffix.isin(['SZA', 'SHA', 'BJA'])]
        
        self._update_data(source_df)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):
    obj = BuildMarketValue(start_date=start_date, end_date=end_date, write_mode='update')
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()
