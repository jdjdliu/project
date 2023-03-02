import click
import datetime
import pandas as pd
from template import Build
from CN_STOCK_A.schema_catetory import dividend_send_CN_STOCK_A as category_info


class BuildDividend(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildDividend, self).__init__(start_date, end_date)
        self.map_dic = {
            'stk_cd': 'stk_cd',
            'instrument': 'instrument',
            'anc_dt_prps': 'date',
            'dvd_aft_tax': 'cash_after_tax',
            'dvd_bfr_tax': 'cash_before_tax',
            'csh_dtrb_dt': 'pay_date',   # 核对：cah_dtrb_dt与wind的dvd_payout_dt对应
            'reg_dt': 'record_date',
            'xrt_dt': 'ex_date',
            'bns_shr_rat': 'bonus_rate',  # 需要除以10
            'trsf_shr_rat': 'conversed_rate',   # 需要除以10
            'bns_trsf_rat': 'bonus_conversed_sum', # bns_trsf_rat与wind的stk_dvd_per_sh*10对应，需要除以10转回来
            'dvd_prgr': 'progress',   # !!!需要转换映射
            'dvd_year': 'report_year',        # bigquant原字段是report_date,但wind字段report_period被取年份入库了，因此这里不用report_date，用report_year，str类型
            'dvd_tms': 'dvd_tms',        # 年度第N次分红，report_year被切分了，加这个字段做主键避免年内有多次分红
        }

        self.schema = {
            'friendly_name': category_info[2],
            'date_field': 'date',
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'active': True,
            'primary_key': ['date', 'instrument', 'report_year', 'dvd_tms'],
            'partition_date': None,
            'fields': {
                'stk_cd': {'desc': '股票内部编码', 'type': 'str'},
                'instrument': {'desc': '股票代码', 'type': 'str'},
                'date': {'desc': '预案公告日', 'type': 'datetime64[ns]'},
                'report_year': {'desc': '分红年度', 'type': 'str'},
                'dvd_tms': {'desc': '年度第N次分红', 'type': 'str'},
                'pay_date': {'desc': '派息日', 'type': 'datetime64[ns]'},
                'record_date': {'desc': '股权登记日', 'type': 'datetime64[ns]'},
                'ex_date': {'desc': '除权除息日', 'type': 'datetime64[ns]'},
                'cash_after_tax': {'desc': '每股派息(税后)(元)', 'type': 'float64'},
                'cash_before_tax': {'desc': '每股派息(税前)(元)', 'type': 'float64'},
                'bonus_rate': {'desc': '每股送股比例', 'type': 'float64'},
                'conversed_rate': {'desc': '每股转增比例', 'type': 'float64'},
                'bonus_conversed_sum': {'desc': '每股送转比例(每股送股+每股转增)', 'type': 'float64'},
                'progress': {'desc': '方案进度', 'type': 'str'},
            },
        }

        self.write_mode = write_mode
        self.alias = 'dividend_send_CN_STOCK_A'

    def run(self):

        fields_lst = list(self.map_dic.keys())
        source_df = self._read_DataSource_by_date(table='STK_DIVIDEND', fields=fields_lst)
        if not isinstance(source_df, pd.DataFrame):
            return 
        del source_df['date']
        source_df = source_df.rename(columns=self.map_dic)
        # 009-股东大会未通过，001-股东提议，005-否决，008-延期实施，003-董事会预案，006-实施，002-董事会预案预披露，004-股东大会通过，007-停止实施；
        # progress_dic = {
        #     '009': '股东大会未通过', '001': '股东提议', '005': '否决', '008': '延期实施', '003': '董事会预案', '006': '实施',
        #     '002': '董事会预案预披露', '004': '股东大会通过', '007': '停止实施'}
        # source_df['progress'] = source_df.progress.map(progress_dic)

        source_df = source_df[source_df.progress == '006']
        source_df['progress'] = source_df.progress.map({'006': '实施'})
        # 需要专门保留progress=="实施"的；前复权计算逻辑中读取到dividend表的数据直接用的，线上也得分红表progress也只有["实施"]

        for col in ['bonus_rate', 'conversed_rate', 'bonus_conversed_sum']:
            source_df[col] = source_df[col] / 10

        for col in ['cash_after_tax', 'cash_before_tax', 'bonus_rate', 'conversed_rate', 'bonus_conversed_sum']:
            source_df[col] = source_df[col].fillna(0)
        print('>>>>before filter shape: ', source_df.shape)
        basic_df = self._read_DataSource_all(table='basic_info_CN_STOCK_A', fields=['instrument'])
        source_df = pd.merge(source_df, basic_df, on=['instrument'], how='inner')
        print('>>>>basic filtered shape: ', source_df.shape)

        source_df['suffix'] = source_df.instrument.apply(lambda x: x.split('.')[1])
        source_df = source_df[source_df.suffix.isin(['SZA', 'SHA', 'BJA'])]
        print(source_df)
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
