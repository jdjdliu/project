import click
import datetime
import pandas as pd
from template import Build
from CN_STOCK_A.schema_catetory import index_element_weight as category_info


class BuildIndexWeight(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildIndexWeight, self).__init__(start_date, end_date)
        self.map_dic = {
            'idx_cd': 'idx_cd',
            # 'instrument': 'instrument',   # 不从此原表获取，从basic_info_index获取有后缀的
            'trd_dt': 'date',
            'cpn_cd': 'stk_cd',  # 成分内部编码，去basic_info_CN_STOCK_A获取instrument
            'wt': 'weight',
        }

        self.schema = {
            'friendly_name': category_info[2],
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'date_field': 'date',
            'active': True,
            'primary_key': ['date', 'instrument', 'instrument_index'],
            "partition_date": "Y",

            'fields': {
                'idx_cd': {'desc': '指数内部编码', 'type': 'str'},
                'instrument_index': {'desc': '所属指数代码', 'type': 'str'},
                'stk_cd': {'desc': '股票内部编码', 'type': 'str'},
                'instrument': {'desc': '证券代码', 'type': 'str'},

                'weight': {'desc': '权重', 'type': 'float32'},
                'date': {'desc': '日期', 'type': 'datetime64[ns]'},

            },
            'file_type': 'bdb'

        }

        self.write_mode = write_mode
        self.alias = 'index_element_weight'

    def run(self):
        fields_lst = list(self.map_dic.keys())
        source_df = self._read_DataSource_by_date(table='IDX_WT_STK', fields=fields_lst)
        del source_df['instrument']
        del source_df['date']

        source_df = source_df.rename(columns=self.map_dic)
        print(source_df)
        print('>>>>before filter shape: ', source_df.shape)
        basic_df = self._read_DataSource_all(table='basic_info_index_CN_STOCK_A', fields=['idx_cd', 'instrument'])

        # 只获取在basic_info_index_CN_STOCK_A表中有的指数标的
        source_df = pd.merge(source_df, basic_df, on=['idx_cd'], how='inner')
        source_df = source_df.rename(columns={'instrument': 'instrument_index'})
        print('>>>>basic filtered shape: ', source_df.shape)
        print(source_df)
        stk_basic_df = self._read_DataSource_all(table='basic_info_CN_STOCK_A', fields=['stk_cd', 'instrument'])
        source_df = pd.merge(source_df, stk_basic_df, on=['stk_cd'], how='left')
        error_df = source_df[source_df.instrument.isnull()]
        if not error_df.empty:
            print('>>>>>>>>>>>>IDX_WT_STK have other stock instrument not CN_STOCK_A')
            print(error_df)
            raise Exception('IDX_WT_STK have other stock instrument not CN_STOCK_A')
            # source_df = source_df[source_df.instrument.notnull()]  # deal 2006-01-01 -- 2006-12-31
        print(source_df)
        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        source_df = source_df[source_df.suffix.isin(['SZA', 'SHA', 'BJA'])]

        self._update_data(source_df)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):
    obj = BuildIndexWeight(start_date=start_date, end_date=end_date, write_mode='update')
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()
