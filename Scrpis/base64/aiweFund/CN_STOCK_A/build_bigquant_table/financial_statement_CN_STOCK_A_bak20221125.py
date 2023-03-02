
# 盈利因子
# s_fa_roe 净资产收益率 无
m_str1 = """s_fa_roa2 总资产报酬率(总资产报酬率?) PTY_FIN_DRVIND.roa2
s_fa_roa 总资产净利润(总资产净利率?) PTY_FIN_DRVIND.roa
s_fa_optogr 营业利润率(利润率?) PTY_FIN_DRVIND.op_gr
s_fa_profittogr 净利润率(营业总收入利润率?) PTY_FIN_DRVIND.pro_tog
s_fa_gctogr 营业总成本/营业总收入(营业总成本率?) PTY_FIN_DRVIND.gc_gr
s_fa_ebittogr 息税前利润率(息税前利润同营业总收入比?) PTY_FIN_DRVIND.ebit_gr
s_fa_grossprofitmargin 销售毛利率(销售毛利率,通过中文找到) PTY_FIN_DRVIND.gro_pro_mar
s_fa_netprofitmargin 销售净利率(销售净利率,通过中文找到) PTY_FIN_DRVIND.net_pro_mar
s_fa_cogstosales 销售成本率(销售成本率,通过中文找到) PTY_FIN_DRVIND.cog_sal"""
# col_lst1 = ['roa2', 'roa', 'op_gr', 'pro_tog', 'gc_gr', 'ebit_gr', 'gro_pro_mar', 'net_pro_mar', 'cog_sal']


# 成长因子
m_str2 = """s_fa_yoyeps_basic 同比增长率-基本每股收益(%)(同比增长率-基本每股收益(%),通过中文找到) PTY_FIN_DRVIND.yoy_eps_bas
s_fa_yoyeps_diluted 同比增长率-稀释每股收益(%)(同比增长率-稀释每股收益(%),通过中文找到) PTY_FIN_DRVIND.yoy_eps_dil
s_fa_yoyocfps 同比增长率-每股经营活动产生的现金流量净额(%)(同比增长率-每股经营活动产生的现金流量净额(%),通过中文找到) PTY_FIN_DRVIND.yoy_ocf
s_fa_yoyop 同比增长率-营业利润(%)(同比增长率-营业利润(%),通过中文找到) PTY_FIN_DRVIND.yoy_op
s_fa_yoyebt 同比增长率-利润总额(%)(同比增长率-利润总额(%),通过中文找到) PTY_FIN_DRVIND.yoy_ebt_tot
s_fa_yoynetprofit 同比增长率-归属母公司股东的净利润(%)(同比增长率-归属母公司股东的净利润(%),通过中文找到) PTY_FIN_DRVIND.yoy_net_pro
s_fa_yoy_tr 营业总收入同比增长率(%)(同比增长率-营业总收入(%)?,通过中文找到) PTY_FIN_DRVIND.yoy_tr
s_fa_yoy_or 营业收入同比增长率(%)(同比增长率-营业收入(%)?,通过中文找到) PTY_FIN_DRVIND.yoy_or
s_fa_yoybps 相对年初增长率-每股净资产(%)(相对年初增长率-每股净资产(%)?,通过中文找到) PTY_FIN_DRVIND.yoy_bps
s_fa_yoyassets 相对年初增长率-资产总计(%)(相对年初增长率-资产总计(%)?,通过中文找到) PTY_FIN_DRVIND.yoy_ass
s_fa_yoyequity 相对年初增长率-归属母公司的股东权益(%)(相对年初增长率-归属母公司的股东权益(%)?,通过中文找到) PTY_FIN_DRVIND.yoy_equ_sha"""

# 偿债因子---改叫做 质量因子 了
m_str3 = """s_fa_debttoassets 资产负债率(资产负债率(%),通过中文找到,PTY_FIN_INDICATOR也有，名字一样) PTY_FIN_DRVIND.acc_lia_pro
s_fa_assetstoequity 权益乘数(权益乘数,通过中文找到) PTY_FIN_DRVIND.ass_equ
s_fa_debttoequity 产权比率(产权比率,通过中文找到) PTY_FIN_DRVIND.lia_tot_equ
s_fa_ebittointerest 已获利息倍数(EBIT/利息费用)(已获利息倍数(EBIT/利息费用)?) PTY_FIN_DRVIND.ebit_int
s_fa_current 流动比率(流动比(%)??,通过中文找到,PTY_FIN_INDICATOR也有,名字一样) PTY_FIN_DRVIND.cur_rat
s_fa_quick 速动比率(速动比(%)??,通过中文找到,PTY_FIN_INDICATOR也有,但叫qck_rat) PTY_FIN_DRVIND.qui_rat
s_fa_cashratio 保守速动比率(保守速动比率,通过中文找到) PTY_FIN_DRVIND.cas_rat
s_fa_catoassets 流动资产/总资产(流动资产占总资产比率?,通过中文找到) PTY_FIN_DRVIND.cat_ass
s_fa_ocftoshortdebt 经营活动产生的现金流量净额/流动负债(经营活动产生的现金流量净额占流动负债比率??) PTY_FIN_DRVIND.ocf_lia_cur
s_fa_equitytodebt 归属于母公司的股东权益/负债合计(归属于母公司的股东权益占总负债比率?,通过中文找到) PTY_FIN_DRVIND.equ_deb
s_fa_ocftodebt 经营活动产生的现金流量净额/负债合计(经营活动产生的现金流量净额同总负债比?) PTY_FIN_DRVIND.ocf_deb"""


# 营运因子
m_str4 = """s_fa_invturn 存货周转率(存货周转率,通过中文找到) PTY_FIN_DRVIND.inv_rn
s_fa_arturn 应收账款周转率(应收账款周转率,通过中文找到) PTY_FIN_DRVIND.art_rn
s_fa_arturndays 应收账款周转天数(应收账款周转天数,通过中文找到) PTY_FIN_DRVIND.art_day
s_fa_faturn 固定资产周转率(固定资产周转率,通过中文找到) PTY_FIN_DRVIND.fat_rn
s_fa_caturn 流动资产周转率(流动资产周转率,通过中文找到) PTY_FIN_DRVIND.cat_rn
s_fa_assetsturn 总资产周转率(总资产周转率) PTY_FIN_DRVIND.ass_rn
s_fa_fcff 企业自由现金流量(FCFF)(企业自由现金流量(FCFF)) PTY_FIN_DRVIND.fcff
s_fa_workingcapital 营运资金(营运资金,通过中文找到) PTY_FIN_DRVIND.wor_cap
s_fa_networkingcapital 营运流动资本(营运流动资本,通过中文找到) PTY_FIN_DRVIND.net_wor_cap"""

# 每股因子
m_str5 = """s_fa_eps_basic 基本每股收益(基本每股收益) PTY_FIN_INCOME.eps_bas
s_fa_eps_diluted 稀释每股收益(稀释每股收益) PTY_FIN_INCOME.eps_dlt
s_fa_bps 每股净资产(每股净资产) PTY_FIN_DRVIND.bps
s_fa_ocfps 每股经营活动产生的现金流量净额(每股经营活动产生的现金流量净额) PTY_FIN_DRVIND.ocfps
s_fa_grps 每股营业总收入(每股营业总收入) PTY_FIN_DRVIND.gr_ps
s_fa_orps 每股营业收入(每股营业收入) PTY_FIN_DRVIND.orps
s_fa_surpluscapitalps 每股资本公积(每股资本公积) PTY_FIN_DRVIND.sur_cap
s_fa_surplusreserveps 每股盈余公积(每股盈余公积) PTY_FIN_DRVIND.sur_res
s_fa_undistributedps 每股未分配利润(每股未分配利润) PTY_FIN_DRVIND.und_ps
s_fa_retainedps 每股留存收益(每股留存收益) PTY_FIN_DRVIND.ret_ps
s_fa_cfps 每股现金流量净额(每股现金流量净额) PTY_FIN_DRVIND.cfps
s_fa_ebitps 每股息税前利润(每股息税前利润) PTY_FIN_DRVIND.ebit_ps
s_fa_fcffps 每股企业自由现金流量(每股企业自由现金流量) PTY_FIN_DRVIND.fcff_ps
s_fa_fcfeps 每股股东自由现金流量(每股股东自由现金流量) PTY_FIN_DRVIND.fcfe_ps"""

# from GLOBAL_TABLE.schema import TABLES_MAPS

# table_col_dic = {}
# res_fields_dic = {}
# for tmp_str in [m_str1, m_str2, m_str3, m_str4, m_str5]:
#     tmp_lst = tmp_str.split('\n')
#     tmp_lst = [x for x in tmp_lst if x != '']
#     col_info_lst = [x.split(' ')[2] for x in tmp_lst]
#     for col_info in col_info_lst:
#         try:
#             table, col = col_info.split('.')
#         except ValueError:
#             print(col_info)
#             assert 1> 2
#         have_col = table_col_dic.get(table, [])
#         have_col.append(col)
#         table_col_dic[table] = have_col

        # print(list(TABLES_MAPS.keys()))
#         field_dic = TABLES_MAPS[table]['schema']['fields'].get(col)
#         res_fields_dic[col] = field_dic
        # print(field_dic)
# print(res_fields_dic)
# print(table_col_dic)



fields_dic = {
    'roa2': {'desc': '总资产报酬率', 'type': 'float64'},
    'roa': {'desc': '总资产净利率', 'type': 'float64'},
    'op_gr': {'desc': '利润率', 'type': 'float64'},
    'pro_tog': {'desc': '营业总收入利润率', 'type': 'float64'},
    'gc_gr': {'desc': '营业总成本率', 'type': 'float64'},
    'ebit_gr': {'desc': '息税前利润同营业总收入比', 'type': 'float64'},
    'gro_pro_mar': {'desc': '销售毛利率', 'type': 'float64'},
    'net_pro_mar': {'desc': '销售净利率', 'type': 'float64'},
    'cog_sal': {'desc': '销售成本率', 'type': 'float64'},
    'yoy_eps_bas': {'desc': '同比增长率-基本每股收益(%)', 'type': 'float64'},
    'yoy_eps_dil': {'desc': '同比增长率-稀释每股收益(%)', 'type': 'float64'},
    'yoy_ocf': {'desc': '同比增长率-每股经营活动产生的现金流量净额(%)', 'type': 'float64'},
    'yoy_op': {'desc': '同比增长率-营业利润(%)', 'type': 'float64'},
    'yoy_ebt_tot': {'desc': '同比增长率-利润总额(%)', 'type': 'float64'},
    'yoy_net_pro': {'desc': '同比增长率-归属母公司股东的净利润(%)', 'type': 'float64'},
    'yoy_tr': {'desc': '同比增长率-营业总收入(%)', 'type': 'float64'},
    'yoy_or': {'desc': '同比增长率-营业收入(%)', 'type': 'float64'},
    'yoy_bps': {'desc': '相对年初增长率-每股净资产(%)', 'type': 'float64'},
    'yoy_ass': {'desc': '相对年初增长率-资产总计(%)', 'type': 'float64'},
    'yoy_equ_sha': {'desc': '相对年初增长率-归属母公司的股东权益(%)', 'type': 'float64'},
    'acc_lia_pro': {'desc': '资产负债率(%)', 'type': 'float64'},
    'ass_equ': {'desc': '权益乘数', 'type': 'float64'},
    'lia_tot_equ': {'desc': '产权比率', 'type': 'float64'},
    'ebit_int': {'desc': '已获利息倍数(EBIT/利息费用)', 'type': 'float64'},
    'cur_rat': {'desc': '流动比(%)', 'type': 'float64'},
    'qui_rat': {'desc': '速动比(%)', 'type': 'float64'},
    'cas_rat': {'desc': '保守速动比率', 'type': 'float64'},
    'cat_ass': {'desc': '流动资产占总资产比率', 'type': 'float64'},
    'ocf_lia_cur': {'desc': '经营活动产生的现金流量净额占流动负债比率', 'type': 'float64'},
    'equ_deb': {'desc': '归属于母公司的股东权益占总负债比率', 'type': 'float64'},
    'ocf_deb': {'desc': '经营活动产生的现金流量净额同总负债比', 'type': 'float64'},
    'inv_rn': {'desc': '存货周转率', 'type': 'float64'},
    'art_rn': {'desc': '应收账款周转率', 'type': 'float64'},
    'art_day': {'desc': '应收账款周转天数', 'type': 'float64'},
    'fat_rn': {'desc': '固定资产周转率', 'type': 'float64'},
    'cat_rn': {'desc': '流动资产周转率', 'type': 'float64'},
    'ass_rn': {'desc': '总资产周转率', 'type': 'float64'},
    'fcff': {'desc': '企业自由现金流量(FCFF)', 'type': 'float64'},
    'wor_cap': {'desc': '营运资金', 'type': 'float64'},
    'net_wor_cap': {'desc': '营运流动资本', 'type': 'float64'},
    'eps_bas': {'desc': '基本每股收益', 'type': 'float64'},
    'eps_dlt': {'desc': '稀释每股收益', 'type': 'float64'},
    'bps': {'desc': '每股净资产', 'type': 'float64'},
    'ocfps': {'desc': '每股经营活动产生的现金流量净额', 'type': 'float64'},
    'gr_ps': {'desc': '每股营业总收入', 'type': 'float64'},
    'orps': {'desc': '每股营业收入', 'type': 'float64'},
    'sur_cap': {'desc': '每股资本公积', 'type': 'float64'},
    'sur_res': {'desc': '每股盈余公积', 'type': 'float64'},
    'und_ps': {'desc': '每股未分配利润', 'type': 'float64'},
    'ret_ps': {'desc': '每股留存收益', 'type': 'float64'},
    'cfps': {'desc': '每股现金流量净额', 'type': 'float64'},
    'ebit_ps': {'desc': '每股息税前利润', 'type': 'float64'},
    'fcff_ps': {'desc': '每股企业自由现金流量', 'type': 'float64'},
    'fcfe_ps': {'desc': '每股股东自由现金流量', 'type': 'float64'},
}



import click
import datetime
import pandas as pd
import numpy as np
from template import Build
from CN_STOCK_A.schema_catetory import financial_statement_CN_STOCK_A as category_info


class BuildFinancial(Build):

    def __init__(self, start_date, end_date, write_mode='update'):
        super(BuildFinancial, self).__init__(start_date, end_date)

        self.schema = {
            'friendly_name': category_info[2],
            'date_field': 'date',
            'category': category_info[0],
            'rank': category_info[1],
            'desc': category_info[2],
            'active': True,
            'partition_date': 'Y',
            'primary_key': ['date', 'instrument', 'fs_quarter_index'],
            'fields':
                {
                    'date': {'desc': '公告日期', 'type': 'datetime64[ns]'},
                    'instrument': {'desc': '证券代码', 'type': 'str'},
                    'stk_cd': {'desc': '股票内部编码', 'type': 'str'},
                    'pty_cd': {'desc': '主体内部编码', 'type': 'str'},
                    'fs_quarter': {'desc': '财报对应的年份季度，如20151231', 'type': 'str'},
                    'fs_quarter_year': {'desc': '财报对应年份', 'type': 'int16'},
                    'fs_quarter_index': {'desc': '财报对应季度 1：一季报，2：半年报，3：三季报，4：年报，9：其他', 'type': 'int8'},
                    # 'fs_publish_date': {'desc': '公告日期', 'type': 'datetime64[ns]'},
                    # 在financial_statement_ff_CN_STOCK_A里面需要加fs_publish_date, 因为date是交易日期插补得到

                    'roa2': {'desc': '总资产报酬率', 'type': 'float64'},
                    'roa': {'desc': '总资产净利率', 'type': 'float64'},
                    'op_gr': {'desc': '利润率', 'type': 'float64'},
                    'pro_tog': {'desc': '营业总收入利润率', 'type': 'float64'},
                    'gc_gr': {'desc': '营业总成本率', 'type': 'float64'},
                    'ebit_gr': {'desc': '息税前利润同营业总收入比', 'type': 'float64'},
                    'gro_pro_mar': {'desc': '销售毛利率', 'type': 'float64'},
                    'net_pro_mar': {'desc': '销售净利率', 'type': 'float64'},
                    'cog_sal': {'desc': '销售成本率', 'type': 'float64'},
                    'yoy_eps_bas': {'desc': '同比增长率-基本每股收益(%)', 'type': 'float64'},
                    'yoy_eps_dil': {'desc': '同比增长率-稀释每股收益(%)', 'type': 'float64'},
                    'yoy_ocf': {'desc': '同比增长率-每股经营活动产生的现金流量净额(%)', 'type': 'float64'},
                    'yoy_op': {'desc': '同比增长率-营业利润(%)', 'type': 'float64'},
                    'yoy_ebt_tot': {'desc': '同比增长率-利润总额(%)', 'type': 'float64'},
                    'yoy_net_pro': {'desc': '同比增长率-归属母公司股东的净利润(%)', 'type': 'float64'},
                    'yoy_tr': {'desc': '同比增长率-营业总收入(%)', 'type': 'float64'},
                    'yoy_or': {'desc': '同比增长率-营业收入(%)', 'type': 'float64'},
                    'yoy_bps': {'desc': '相对年初增长率-每股净资产(%)', 'type': 'float64'},
                    'yoy_ass': {'desc': '相对年初增长率-资产总计(%)', 'type': 'float64'},
                    'yoy_equ_sha': {'desc': '相对年初增长率-归属母公司的股东权益(%)', 'type': 'float64'},
                    'acc_lia_pro': {'desc': '资产负债率(%)', 'type': 'float64'},
                    'ass_equ': {'desc': '权益乘数', 'type': 'float64'},
                    'lia_tot_equ': {'desc': '产权比率', 'type': 'float64'},
                    'ebit_int': {'desc': '已获利息倍数(EBIT/利息费用)', 'type': 'float64'},
                    'cur_rat': {'desc': '流动比(%)', 'type': 'float64'},
                    'qui_rat': {'desc': '速动比(%)', 'type': 'float64'},
                    'cas_rat': {'desc': '保守速动比率', 'type': 'float64'},
                    'cat_ass': {'desc': '流动资产占总资产比率', 'type': 'float64'},
                    'ocf_lia_cur': {'desc': '经营活动产生的现金流量净额占流动负债比率', 'type': 'float64'},
                    'equ_deb': {'desc': '归属于母公司的股东权益占总负债比率', 'type': 'float64'},
                    'ocf_deb': {'desc': '经营活动产生的现金流量净额同总负债比', 'type': 'float64'},
                    'inv_rn': {'desc': '存货周转率', 'type': 'float64'},
                    'art_rn': {'desc': '应收账款周转率', 'type': 'float64'},
                    'art_day': {'desc': '应收账款周转天数', 'type': 'float64'},
                    'fat_rn': {'desc': '固定资产周转率', 'type': 'float64'},
                    'cat_rn': {'desc': '流动资产周转率', 'type': 'float64'},
                    'ass_rn': {'desc': '总资产周转率', 'type': 'float64'},
                    'fcff': {'desc': '企业自由现金流量(FCFF)', 'type': 'float64'},
                    'wor_cap': {'desc': '营运资金', 'type': 'float64'},
                    'net_wor_cap': {'desc': '营运流动资本', 'type': 'float64'},
                    'eps_bas': {'desc': '基本每股收益', 'type': 'float64'},
                    'eps_dlt': {'desc': '稀释每股收益', 'type': 'float64'},
                    'bps': {'desc': '每股净资产', 'type': 'float64'},
                    'ocfps': {'desc': '每股经营活动产生的现金流量净额', 'type': 'float64'},
                    'gr_ps': {'desc': '每股营业总收入', 'type': 'float64'},
                    'orps': {'desc': '每股营业收入', 'type': 'float64'},
                    'sur_cap': {'desc': '每股资本公积', 'type': 'float64'},
                    'sur_res': {'desc': '每股盈余公积', 'type': 'float64'},
                    'und_ps': {'desc': '每股未分配利润', 'type': 'float64'},
                    'ret_ps': {'desc': '每股留存收益', 'type': 'float64'},
                    'cfps': {'desc': '每股现金流量净额', 'type': 'float64'},
                    'ebit_ps': {'desc': '每股息税前利润', 'type': 'float64'},
                    'fcff_ps': {'desc': '每股企业自由现金流量', 'type': 'float64'},
                    'fcfe_ps': {'desc': '每股股东自由现金流量', 'type': 'float64'},
                }

        }
        self.write_mode = write_mode
        self.alias = 'financial_statement_CN_STOCK_A'

    def run(self):
        common_col = [
            'pty_cd',  # 主体内部编码，主键
            'date',    # 主要是公告日期rename得到的，非主键
            'end_dt',  # 报告截止日期 对应wind的report_period 主键
            'rpt_rtm_typ',  # 报告期类型：对应wind
                            # （1）如果源表的SUBSTR(REPORT_PERIOD,5,4)='0331',以'001'入库；
                            # （2）如果源表的SUBSTR(REPORT_PERIOD,5,4)='0630',以'002'入库；
                            # （3）如果源表的SUBSTR(REPORT_PERIOD,5,4)='0930',以'003'入库；
                            # （4）如果源表的SUBSTR(REPORT_PERIOD,5,4)='1231',以'004'入库；
                            # （5）其它情况,以'999'入库；"
            'rpt_typ',      # 报表类型 001-合并报表，002-母公司报表，003-合并报表(调整)，004-母公司报表(调整)，005-合并报表(单季度
            'rpt_trm_typ',      # 报告期类型 001-一季报，002-半年报，003-三季报，004-年报，999-其他；
        ]
        read_source_info = {
            'PTY_FIN_DRVIND':
                ['roa2', 'roa', 'op_gr', 'pro_tog', 'gc_gr', 'ebit_gr', 'gro_pro_mar', 'net_pro_mar', 'cog_sal',
                 'yoy_eps_bas', 'yoy_eps_dil', 'yoy_ocf', 'yoy_op', 'yoy_ebt_tot', 'yoy_net_pro', 'yoy_tr', 'yoy_or',
                 'yoy_bps', 'yoy_ass', 'yoy_equ_sha', 'acc_lia_pro', 'ass_equ', 'lia_tot_equ', 'ebit_int', 'cur_rat',
                 'qui_rat', 'cas_rat', 'cat_ass', 'ocf_lia_cur', 'equ_deb', 'ocf_deb', 'inv_rn', 'art_rn', 'art_day',
                 'fat_rn', 'cat_rn', 'ass_rn', 'fcff', 'wor_cap', 'net_wor_cap', 'bps', 'ocfps', 'gr_ps', 'orps',
                 'sur_cap', 'sur_res', 'und_ps', 'ret_ps', 'cfps', 'ebit_ps', 'fcff_ps', 'fcfe_ps'] + common_col,
            'PTY_FIN_INCOME':
                ['eps_bas', 'eps_dlt'] + common_col,
            'PTY_FIN_BALANCE':
                ["lbl_tot", "ast_tot"] + common_col,
        }
        from sdk.datasource import DataSource
        t_basic = DataSource("basic_info_CN_STOCK_A").read(fields=['instrument', 'stk_cd'])        
        t_source = DataSource("STK_BASICINFO").read(fields=['instrument', 'stk_cd', 'pty_cd'])
        t_source = t_source[t_source.stk_cd.isin(t_basic.stk_cd)]
        t_pty_lst = t_source.pty_cd.unique().tolist()

        drvind_df = self._read_DataSource_by_date(table='PTY_FIN_DRVIND', fields=read_source_info.get('PTY_FIN_DRVIND'))
        if not isinstance(drvind_df, pd.DataFrame):
            drvind_df = pd.DataFrame(columns=read_source_info.get('PTY_FIN_DRVIND'))
        drvind_df['date'] = pd.to_datetime(drvind_df.date)
        drvind_df['end_dt'] = pd.to_datetime(drvind_df.end_dt)
        drvind_df = drvind_df.sort_values(['pty_cd', 'date', 'end_dt', 'rpt_typ'])
        drvind_df1 = drvind_df[drvind_df.pty_cd.isin(t_pty_lst)].copy()
        # drvind_df1 = drvind_df1[drvind_df1.rpt_typ.isin(['001', '003'])]

        drvind_df = drvind_df.drop_duplicates(['pty_cd', 'date', 'end_dt'])  # 即有多个报表类型的，只保留 001-合并报表的
        del drvind_df['rpt_typ']

        income_df = self._read_DataSource_by_date(table='PTY_FIN_INCOME', fields=read_source_info.get('PTY_FIN_INCOME'))
        if not isinstance(income_df, pd.DataFrame):
            income_df = pd.DataFrame(columns=read_source_info.get('PTY_FIN_INCOME'))
        income_df['date'] = pd.to_datetime(income_df.date)
        income_df['end_dt'] = pd.to_datetime(income_df.end_dt)
        income_df = income_df.sort_values(['pty_cd', 'date', 'end_dt', 'rpt_typ'])
        income_df1 = income_df[income_df.pty_cd.isin(t_pty_lst)].copy()

        income_df = income_df.drop_duplicates(['pty_cd', 'date', 'end_dt'])  # 即有多个报表类型的，只保留 001-合并报表的
        del income_df['rpt_typ']
        
        # add in 2022-10-14-------------------
        balance_df = self._read_DataSource_by_date(table='PTY_FIN_BALANCE', fields=read_source_info.get('PTY_FIN_BALANCE'))
        if not isinstance(balance_df, pd.DataFrame):
            balance_df = pd.DataFrame(columns=read_source_info.get('PTY_FIN_BALANCE'))
        balance_df['date'] = pd.to_datetime(balance_df.date)
        balance_df['end_dt'] = pd.to_datetime(balance_df.end_dt)
        balance_df = balance_df[(balance_df.lbl_tot.notnull()) & (balance_df.ast_tot.notnull())]
        balance_df['acc_lia_pro_cal'] = np.round(balance_df.lbl_tot/balance_df.ast_tot * 100, 2)
        balance_df = balance_df.sort_values(['pty_cd', 'date', 'end_dt', 'rpt_typ'])
        balance_df1 = balance_df[balance_df.pty_cd.isin(t_pty_lst)].copy()
        # balance_df1 = balance_df1[balance_df1.rpt_typ.isin(['001', '003'])]

        balance_df = balance_df.drop_duplicates(['pty_cd', 'date', 'end_dt'])  # 即有多个报表类型的，只保留 001-合并报表的
        del balance_df['rpt_typ']
        # del balance_df['instrument']
        # ------------------------------------
        # 这里mergezi字段加了rpt_trm_typ，虽然end_dt就是季、年报的时间，但是加上rpt_trm_typ避免变成rpt_trm_typ_x
        cal_df = pd.merge(drvind_df, income_df, on=['pty_cd', 'date', 'end_dt', 'rpt_trm_typ'], how='left')  # 2022-10-14 change to left, outer will have many columns to None

        # add in 2022-10-14
        cal_df = pd.merge(cal_df, balance_df, on=['pty_cd', 'date', 'end_dt', 'rpt_trm_typ'], how='left') # 2022-10-14 change to left
        print(cal_df.columns)
        cal_df['acc_lia_pro'] = cal_df.acc_lia_pro_cal

        print("drivind_df1 columns: ", drvind_df1.columns, drvind_df1.shape)
        print("income_df1 columns: ", income_df1.columns, income_df1.shape)
        print("balance_df1 columns: ", balance_df1.columns, balance_df1.shape)
        print(drvind_df1[['pty_cd', 'date', 'end_dt', 'rpt_trm_typ', 'rpt_typ']].dtypes)
        print(income_df1[['pty_cd', 'date', 'end_dt', 'rpt_trm_typ', 'rpt_typ']].dtypes)
        temp_df = pd.merge(drvind_df1, income_df1, on=['pty_cd', 'date', 'end_dt', 'rpt_trm_typ', 'rpt_typ'], how='left')
        temp_df = pd.merge(temp_df, balance_df1, on=['pty_cd', 'date', 'end_dt', 'rpt_trm_typ', 'rpt_typ'], how='left')
        temp_df.loc[temp_df['rpt_typ'] == '003', 'rpt_typ'] = '000'  # 多个报表类型以 003-合并报表（调整）为第一优先级，然后才是001-合并报表
        temp_df = temp_df.sort_values(['pty_cd', 'date', 'end_dt', 'rpt_typ'])
 
        print("temp_df columns", temp_df.columns, temp_df.shape)
        print(temp_df[['pty_cd', 'rpt_typ']])
        
        temp_df = temp_df.drop_duplicates(['pty_cd', 'date', 'end_dt'])
        temp_df['acc_lia_pro'] = temp_df.acc_lia_pro_cal
        print(temp_df[['pty_cd', 'rpt_typ']])
        del temp_df['rpt_typ']
        print(temp_df.columns)
        # assert 1 > 2
        stock_basic_df = self._read_DataSource_all(table='basic_info_CN_STOCK_A', fields=['stk_cd', 'instrument'])
        stock_source_df = self._read_DataSource_all(table='STK_BASICINFO')[['stk_cd', 'pty_cd']]
        stock_basic_df = pd.merge(stock_basic_df, stock_source_df, on=['stk_cd'], how='inner')
        
        cal_df = temp_df.copy()
        if 'instrument' in cal_df.columns.tolist():
            del cal_df['instrument']

        cal_df = pd.merge(cal_df, stock_basic_df, on=['pty_cd'], how='inner')  # add column: stk_cd, instrument

        cal_df = cal_df.drop_duplicates(['instrument', 'date', 'end_dt'], keep='last')
        if cal_df.empty:
            return
        cal_df = cal_df.rename(columns={'end_dt': 'fs_quarter'})  # 'date': 'fs_publish_date',
        cal_df['fs_quarter'] = cal_df.fs_quarter.dt.strftime('%Y%m%d')  # 是否需要转？？

        cal_df['fs_quarter_year'] = cal_df['fs_quarter'].apply(lambda x: int(x[:4])).astype('int')
        cal_df['fs_quarter_index'] = cal_df['rpt_trm_typ'].map({'001': 1, '002': 2, '003': 3, '004': 4, '999': 9})
        cal_df = cal_df[cal_df.fs_quarter_index.notnull()]

        cal_df['suffix'] = cal_df.instrument.apply(lambda x: x.split('.')[1])
        suffix = cal_df.suffix.unique().tolist()
        assert {'SZA', 'SHA', 'BJA'} >= set(suffix), print(f'error suffix: {suffix}')
        print(cal_df.groupby(['fs_quarter_year']).instrument.count())
        self._update_data(cal_df)


@click.command()
@click.option("--start_date", default=(datetime.datetime.now() - datetime.timedelta(5)).strftime("%Y-%m-%d"), help="start_date")
@click.option("--end_date", default=datetime.datetime.now().strftime("%Y-%m-%d"), help="end_date")
def entry(start_date, end_date):
    obj = BuildFinancial(start_date=start_date, end_date=end_date, write_mode='update')
    obj.run()


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    entry()

