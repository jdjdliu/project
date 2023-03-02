import click
import datetime
import pandas as pd
from template import Build
from CN_STOCK_A.schema_catetory import rightsissue_CN_STOCK_A as category_info


class BuildDividend(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildDividend, self).__init__(start_date, end_date)
        self.map_dic = {
            'stk_cd': 'stk_cd',
            'instrument': 'instrument',
       }

        self.schema = {
            'friendly_name': category_info[2],
            'date_field': 'date',
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'active': True,

        }

        self.write_mode = write_mode
        self.alias = 'dividend_send_CN_STOCK_A'

    def run(self):
        # 缺字段！！！！！！ 此表未构建！！
        fields_lst = list(self.map_dic.keys())
        source_df = self._read_DataSource_by_date(table='STK_DIVIDEND', fields=fields_lst)
        source_df = source_df.rename(columns=self.map_dic)
        # 009-股东大会未通过，001-股东提议，005-否决，008-延期实施，003-董事会预案，006-实施，002-董事会预案预披露，004-股东大会通过，007-停止实施；
        progress_dic = {
            '009': '股东大会未通过','001': '股东提议', '005': '否决', '008': '延期实施', '003': '董事会预案', '006': '实施',
            '002': '董事会预案预披露', '004': '股东大会通过', '007': '停止实施'}
        source_df['progress'] = source_df.progress.map(progress_dic)

        for col in ['bonus_rate', 'conversed_rate', 'bonus_conversed_sum']:
            source_df[col] = source_df[col] / 10
        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        source_df = source_df[source_df.suffix.isin(['SZA', 'SHA', 'BJA'])]

        self._update_data(source_df)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):
    obj = BuildDividend(start_date=start_date, end_date=end_date, write_mode='update')
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()
