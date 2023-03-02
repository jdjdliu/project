import click
import datetime
import pandas as pd
from template import Build
from CN_STOCK_A.schema_catetory import basic_info_index_CN_STOCK_A as category_info


class BuildBasic(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildBasic, self).__init__(start_date, end_date)

        self.schema = {
            'friendly_name': category_info[2],
            'date_field': None,
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'primary_key': ['instrument'],
            'active': True,
            'partition_date': None,
            'fields': {
                'idx_cd': {'desc': '指数内部编码', 'type': 'str'},
                'instrument': {'desc': '指数代码', 'type': 'str'},
                'name': {'desc': '指数名称', 'type': 'str'},
            },

        }
        self.write_mode = write_mode
        self.alias = 'basic_info_index_CN_STOCK_A'

    def run(self):
        source_df = self._read_DataSource_all(table='IDX_BASICINFO', fields=['idx_cd', 'smb', 'chn_nm', 'idx_typ_wind'])
        # source_df = source_df[source_df.idx_typ_wind == '001']
        print(source_df[source_df.chn_nm.str.endswith("制造(申万)")])
        print(source_df[source_df.chn_nm.str.endswith("轻工")])
        source_df = source_df[~(source_df.smb.str.startswith('SW'))]  # 避免和下面的自造编码混淆
        source_df['smb_len'] = source_df.smb.str.len()
        source_df = source_df[source_df.smb_len == 6]   # 只要smb长度为6的指数代码
        sw_index_name_dic = {'801010': 'SW110000.HIX', '801020': 'SW210000.HIX', '801030': 'SW220000.HIX',
                             '801040': 'SW230000.HIX', '801050': 'SW240000.HIX', '801080': 'SW270000.HIX',
                             '801110': 'SW330000.HIX', '801120': 'SW340000.HIX', '801130': 'SW350000.HIX',
                             '801140': 'SW360000.HIX', '801150': 'SW370000.HIX', '801160': 'SW410000.HIX',
                             '801170': 'SW420000.HIX', '801180': 'SW430000.HIX', '801200': 'SW450000.HIX',
                             '801210': 'SW460000.HIX', '801230': 'SW510000.HIX', '801710': 'SW610000.HIX',
                             '801720': 'SW620000.HIX', '801730': 'SW630000.HIX', '801740': 'SW650000.HIX',
                             '801750': 'SW710000.HIX', '801760': 'SW720000.HIX', '801770': 'SW730000.HIX',
                             '801780': 'SW480000.HIX', '801790': 'SW490000.HIX', '801880': 'SW280000.HIX',
                             '801890': 'SW640000.HIX',
                             '801950': 'SW740000.HIX',  # 煤炭(申万)
                             '801960': 'SW750000.HIX',  # 石油石化(申万)
                             '801970': 'SW760000.HIX',  # 环保(申万)
                             '801980': 'SW770000.HIX',  # 美容护理(申万)

                             }
        source_df['smb'] = source_df.smb.apply(lambda x: sw_index_name_dic.get(x, x))

        # 对于idx_basicinfo里面的数据，只获取以 39和00开头的数据
        source_df['prefix'] = source_df.smb.apply(lambda x: x[:2])
        source_df = source_df[source_df.prefix.isin(['39', '00', 'SW'])]
        source_df.loc[source_df.prefix == 'SW', 'instrument'] = source_df.smb
        source_df.loc[source_df.prefix == '00', 'instrument'] = source_df.smb + ".HIX"   # 线上SW开头的也是HIX在dic里已处理
        source_df.loc[source_df.prefix == '39', 'instrument'] = source_df.smb + '.ZIX'
        source_df = source_df.rename(columns={'chn_nm': 'name'})
        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        print(source_df[source_df.prefix == 'SW'])
        print(source_df.groupby(['prefix']).instrument.count())

        # basic_info_index_CN_STOCK_A里面只获取上交所、深交所;北交所暂无指数,后续北交所出指数了要根据它是以什么数字开头而确定
        print(source_df)
        source_df = source_df[source_df.suffix.isin(['ZIX', 'HIX', 'BIX'])]  # 暂时无BIX
        source_df = source_df[list(self.schema['fields'].keys())]
        self._update_data(source_df)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):
    obj = BuildBasic(start_date=start_date, end_date=end_date, write_mode='update')
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()
