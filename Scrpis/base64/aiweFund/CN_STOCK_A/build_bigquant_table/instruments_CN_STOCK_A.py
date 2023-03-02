import click
import datetime
import pandas as pd
from template import Build
from CN_STOCK_A.schema_catetory import instruments_CN_STOCK_A as category_info


class BuildInstruments(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildInstruments, self).__init__(start_date, end_date)

        self.schema = {
            'friendly_name': category_info[2],
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'active': True,
            'date_field': 'date',
            'primary_key': ['date', 'instrument'],
            'partition_date': 'Y',
            'fields': {
                'stk_cd': {'desc': '股票内部编码', 'type': 'str'},
                'instrument': {'desc': '证券代码', 'type': 'str'},
                'name': {'desc': '证券名称', 'type': 'str'},
                'date': {'desc': '日期', 'type': 'datetime64'},
            },
        }
        self.write_mode = write_mode
        self.alias = 'instruments_CN_STOCK_A'

    def run(self):
        source_df = self._read_DataSource_by_date(table='bar1d_CN_STOCK_A', fields=['instrument', 'date', 'stk_cd'])
        basic_df = self._read_DataSource_all(table='basic_info_CN_STOCK_A', fields=['instrument', 'name', 'stk_cd'])

        ins_df = pd.merge(source_df, basic_df, on=['instrument', 'stk_cd'], how='inner')
        # 这里可以加一个left join有为空的判断！！！！！！ 直接用inner,因为bar1d的计算也是被限制在basic_info
        ins_df['suffix'] = ins_df.instrument.apply(lambda x: x.split('.')[1])
        ins_df = ins_df[ins_df.suffix.isin(['SHA', 'SZA', 'BJA'])]
        ins_df = ins_df[list(self.schema['fields'].keys())]
        print(ins_df)
        print(self.schema)
        self._update_data(ins_df)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):
    obj = BuildInstruments(start_date=start_date, end_date=end_date, write_mode='update')
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()
