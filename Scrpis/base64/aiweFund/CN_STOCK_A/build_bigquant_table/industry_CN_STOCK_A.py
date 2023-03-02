"""
# 通过查库中数据对比发现线上与数据库值能够对应上（查询条件：idt_sys_cd = '001'）：
#   industry_sw_level1--> IDT_CD_1ST  industry_sw_level2-->IDT_CD_2ND  industry_sw_level2-->IDT_CD_3RD
# select * from  aiweglobal.pty_industry where idt_sys_cd = '001' and pty_cd = 'PTY016196348'  # 查的股票000001.SZA
# select idt_cd_1st , IDT_NM_1ST from aiweglobal.PTY_INDUSTRY where idt_cd_1st in ('110000', '220000', '230000', '240000', '270000')  # 抽查几个，能对应上

# 宽邦线上的数据结构没有参考价值：通过中文字段INDUSTRY_SW_2021来进行拆分匹配code,而且是每日有数据更新
# 而这里需要读取历史最后一条数据来按照start_dt, end_dt通过日线行情表的交易日ffill计算
"""


import click
import datetime
import pandas as pd
from template import Build
from CN_STOCK_A.schema_catetory import industry_CN_STOCK_A as category_info


class BuildIndustry(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildIndustry, self).__init__(start_date, end_date)

        self.schema = {
            'friendly_name': category_info[2],
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'active': True,
            'date_field': 'date',
            'partition_date': 'Y',
            "primary_key": ["date", "instrument"],
            "fields": {
                'stk_cd': {'desc': '股票内部编码', 'type': 'str'},
                'instrument': {'desc': '证券代码', 'type': 'str'},
                'date': {'desc': '日期', 'type': 'datetime64'},
                'industry_sw_level1': {'type': 'int32', 'desc': '申万一级行业类别'},
                'industry_sw_level2': {'type': 'int32', 'desc': '申万二级行业类别'},
                'industry_sw_level3': {'type': 'int32', 'desc': '申万三级行业类别'},
            },

        }

        schema = {

        }
        self.write_mode = write_mode
        self.alias = 'industry_CN_STOCK_A'

    def run(self):
        basic_df = self._read_DataSource_all(table='STK_BASICINFO', fields=['stk_cd', 'instrument', 'pty_cd'])
        # basic_df = basic_df[basic_df.instrument.isin(['601398.SHA', '000001.SZA'])]
        fields_lst = ['pty_cd', 'idt_sys_cd', 'start_dt', 'end_dt', 'idt_cd_1st', 'idt_cd_2nd', 'idt_cd_3rd']
        # 读取冗余的时间长度，再获取小于self.start_date的最大日期数据
        pty_industry_df = self._read_DataSource_by_date(table='PTY_INDUSTRY', fields=fields_lst, start_date='1990-01-01')
        del pty_industry_df['instrument']
        del pty_industry_df['date']
        # idt_sys_cd 001-申万行业分类
        pty_industry_df = pty_industry_df[pty_industry_df.idt_sys_cd.isin(['054', '001'])]

        pty_industry_df = pty_industry_df[pty_industry_df.idt_cd_1st != '']

        pty_industry_df = pty_industry_df.merge(basic_df, on=['pty_cd'], how='inner')
        pty_industry_df = pty_industry_df.sort_values(by=['pty_cd', 'start_dt'])

        industry_df = pty_industry_df.sort_values(by=['pty_cd', 'start_dt'])

        # 需要对有多条start_dt, end_dt, 但是上一条的end_dt大于当前start_dt的数据做检测以及确认处理方案
        # 一个pty_cd有多条数据： start_dt, end_dt; 异常：（1）end_dt > next_start_dt (2) 非最后一条数据的end_dt为空
        industry_df['next_start_dt'] = industry_df.groupby(['instrument']).start_dt.shift(-1)
        error_df = industry_df[(industry_df.next_start_dt < industry_df.end_dt) |
                               ((industry_df.next_start_dt.notnull()) & (industry_df.end_dt.isnull()))]
        if not error_df.empty:
            print('have error df data --------')
            print(error_df)
        # 测试环境肯定有
        #     raise Exception("have error start_dt and end_dt")
        # 处理方案------------------
        industry_df.loc[(industry_df.next_start_dt < industry_df.end_dt), 'end_dt'] = industry_df.next_start_dt - datetime.timedelta(days=1)
        industry_df.loc[((industry_df.next_start_dt.notnull()) & (industry_df.end_dt.isnull())), 'end_dt'] = industry_df.next_start_dt - datetime.timedelta(days=1)
        # ----------------------------

        industry_df.loc[industry_df.end_dt.isnull(), 'end_dt'] = pd.to_datetime(self.end_date) + datetime.timedelta(days=1)

        # bar1d_df的数据是做了交易日插补的，即停牌也有数据，所以按照bar1d_df里面的日期来
        bar1d_df = self._read_DataSource_by_date(table='bar1d_CN_STOCK_A', fields=['stk_cd', 'instrument', 'date', 'close'])

        cal_df = pd.merge(bar1d_df, industry_df, on=['stk_cd', 'instrument'], how='left')
        # 这个会切掉两条start_dt--end_dt 之间的空白期； 不排除有两组start_dt--end_dt之间不是交易日连续的，后面再按照bar1d来merge一次
        cal_df = cal_df[(cal_df.date >= cal_df.start_dt) & (cal_df.date <= cal_df.end_dt)]
        duplicates_df = cal_df[(cal_df[['stk_cd', 'instrument', 'date']].duplicated(keep=False))]
        if not duplicates_df.empty:
            print('have duplicates df -----------------')
            print(duplicates_df)
            # raise Exception("deal data have duplicates date")
        # 保险措施，注释了上面的raise Exception
        print('before drop duplicates')
        cal_df = cal_df[~(cal_df[['stk_cd', 'instrument', 'date']].duplicated(keep='last'))]
        print('droped .....')
        cal_df = cal_df.merge(bar1d_df[['stk_cd', 'instrument', 'date']], on=['stk_cd', 'instrument', 'date'], how='right')
        cal_df = cal_df.sort_values(['stk_cd', 'instrument', 'date'])

        cal_df = cal_df[['stk_cd', 'instrument', 'date', 'idt_cd_1st', 'idt_cd_2nd', 'idt_cd_3rd']]
        for col in ['idt_cd_1st', 'idt_cd_2nd', 'idt_cd_3rd']:
            cal_df.loc[((cal_df[col] == 'None') | (cal_df[col] == '')), col] = None
            cal_df[col] = cal_df.groupby(['stk_cd', 'instrument'])[col].ffill()
        map_dic = {
            'idt_cd_1st': 'industry_sw_level1',
            'idt_cd_2nd': 'industry_sw_level2',
            'idt_cd_3rd': 'industry_sw_level3'

        }
        cal_df = cal_df.rename(columns=map_dic)
        print('have null data shape: ', cal_df[cal_df.isnull().any(axis=1)].shape)
        print(cal_df[cal_df.isnull().any(axis=1)])
        for col in ['industry_sw_level1', 'industry_sw_level2', 'industry_sw_level3']:
            cal_df[col] = cal_df[col].fillna(0)
        cal_df = cal_df[~(cal_df.isnull().any(axis=1))]   # 做了ffill都还是None的就丢弃

        print('>>>>before filter shape: ', cal_df.shape)
        basic_df = self._read_DataSource_all(table='basic_info_CN_STOCK_A', fields=['instrument'])
        cal_df = pd.merge(cal_df, basic_df, on=['instrument'], how='inner')
        print('>>>>basic filtered shape: ', cal_df.shape)
        
        cal_df['suffix'] = cal_df.instrument.apply(lambda x: x.split('.')[1])
        cal_df = cal_df[cal_df.suffix.isin(['SZA', 'SHA', 'BJA'])]
        cal_df = cal_df[list(self.schema['fields'].keys())]
        self._update_data(cal_df)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):
    obj = BuildIndustry(start_date=start_date, end_date=end_date, write_mode='update')
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()
