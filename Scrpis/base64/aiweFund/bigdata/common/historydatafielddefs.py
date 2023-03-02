from collections import namedtuple
from bigdata.common.market import index_constituent_list,Market


FIELDTUPE = namedtuple('FIELDTUPE', ['field', 'category', 'dtype', 'desc', 'wind_code',
                                     'fill_missing_value', 'wind_value_converter', 'markets'])


def F(name, category, dtype, desc, wind_code, fill_missing_value=None, wind_value_converter=None,markets=''):
    if markets:
        markets = _parse_markets(markets)
        return lambda default_markets:FIELDTUPE(name, category, dtype, desc, wind_code, fill_missing_value, wind_value_converter, markets)
    else:
        return lambda default_markets:FIELDTUPE(name, category, dtype, desc, wind_code, fill_missing_value, wind_value_converter, default_markets)

def _parse_markets(markets):
    if markets == 'all':
        markets = [x.symbol for x in Market.MG_ALL.sub_groups]
    elif isinstance(markets, str):
        markets = markets.split(',')
    elif not isinstance(markets, list):
        markets = [markets]
    return [x if isinstance(x, str) else x.symbol for x in markets]

def __make(items):
    markets = ''
    results = []
    for item in items:
        if isinstance(item, str):
            if item.startswith('markets:'):
                markets = _parse_markets(item[8:].strip())
        elif isinstance(item, list):
            results.extend([f(markets) for f in item])
        else:
            results.append(item(markets))
    return results

MVE = Exception('missing value exception')

## NOTTE： 文档更新： bigdocs/docs/deps/updatedeps.py

_HISTORY_DATA_FIELDS = __make([
    F('date', '', 'datetime64', '交易日期', 'DateTime', MVE),
    F('instrument', '', 'str', '证券代码', 'wind_code', MVE),

    'markets:all',
    F('name', '基本信息', 'str', '证券名字', 'sec_name', MVE),
    F('list_date', '基本信息', 'datetime64', '上市日期', 'IPO_DATE', MVE),
    F('list_board', '基本信息', 'str', '上市板', 'MKT', MVE),
    F('delist_date', '基本信息', 'datetime64', '退市日期，如果未退市，则为pandas.NaT', 'DELIST_DATE'),
    'markets:CN_STOCK_A',
    F('company_name', '基本信息', 'str', '公司名称', 'COMP_NAME'),
    F('company_type', '基本信息', 'str', '公司类型', 'NATURE'),
    F('company_found_date', '基本信息', 'datetime64', '公司成立日期', 'FOUNDDATE'),
    F('company_province', '基本信息', 'str', '公司省份', 'PROVINCE'),
    F('contunit', '基本信息', 'str', '合约单位', 'TUNIT', markets='CN_FUTURE'),

    'markets:all',
    F('open', '行情数据', 'float32', '开盘价(后复权)', 'OPEN'),
    F('close', '行情数据', 'float32', '收盘价(后复权)', 'CLOSE'), # TODO MVE
    F('high', '行情数据', 'float32', '最高价(后复权)', 'HIGH'),
    F('low', '行情数据', 'float32', '最低价(后复权)', 'LOW'),
    F('adjust_factor', '行情数据', 'float32', '复权因子', 'ADJFACTOR', markets='CN_STOCK_A,CN_FUND'),
    F('amount', '行情数据', 'float32', '交易额', 'AMT', 0),
    F('volume', '行情数据', 'int64', '交易量', 'VOLUME', 0),
    F('open_intl', '行情数据', 'float32', '持仓量', 'OI', 0, markets='CN_FUTURE'),
    F('settle', '行情数据', 'float32', '结算价', 'SETTLE', 0, markets='CN_FUTURE'),
    F('deal_number', '行情数据', 'int32', '成交笔数', 'DEALNUM', 0, markets='CN_STOCK_A'),
    F('turn', '行情数据', 'float32', '换手率', 'TURN', 0, markets='CN_STOCK_A,CN_FUND'),
    F('price_limit_status', '行情数据', 'int8', '股价在收盘时的涨跌停状态：1表示跌停，2表示未涨跌停，3则表示涨停', 'MAXUPORDOWN', 0, wind_value_converter=lambda x: x + 2, markets='CN_STOCK_A,CN_FUND'),
    'markets:CN_STOCK_A',
    F('suspended', '行情数据', 'bool', '是否停牌', ''),
    F('suspend_type', '行情数据', 'str', '停牌类型', 'SUSPEND_TYPE'),
    F('suspend_reason', '行情数据', 'str', '停牌原因', 'SUSPEND_REASON'),
    F('st_status', '行情数据', 'int8', 'ST状态：0：正常股票，1：ST，2：*ST，11：暂停上市', '', MVE),

    'markets:CN_STOCK_A',
    F('market_cap', '估值分析', 'float32', '总市值', 'MKT_CAP_ARD'),
    #F('market_cap_float', '估值分析', 'float32', '流通市值', 'MKT_CAP_FLOAT'),
    F('market_cap_float', '估值分析', 'float32', '流通市值', 'MKT_CAP_ASHARE'),
    F('pe_ttm', '估值分析', 'float32', '市盈率 (TTM)', 'PE_TTM'),
    F('pe_lyr', '估值分析', 'float32', '市盈率 (LYR)', 'PE_LYR'),
    F('pb_lf', '估值分析', 'float32', '市净率 (LF)', 'PB_LF'),
    F('pb_mrq', '估值分析', 'float32', '市净率 (MRQ)', 'PB_MRQ'),
    F('ps_ttm', '估值分析', 'float32', '市销率 (TTM)', 'PS_TTM'),

    F('west_netprofit_ftm', '估值分析', 'float32', '一致预测净利润（未来12个月）', 'WEST_NETPROFIT_FTM'),
    F('west_eps_ftm', '估值分析', 'float32', '一致预测每股收益（未来12个月）', 'WEST_EPS_FTM'),
    F('west_avgcps_ftm', '估值分析', 'float32', '一致预测每股现金流（未来12个月）', 'WEST_AVGCPS_FTM'),

    F('mf_net_amount', '资金流分析', 'float32', '净主动买入额，= 买入金额 - 卖出金额 (包括超大单、大单、中单或小单)', 'MF_AMT'),

    F('mf_net_amount_main', '资金流分析', 'float32', '主力净流入净额', 'net_amount_main'),
    F('mf_net_pct_main', '资金流分析', 'float32', '主力净流入占比', 'net_pct_main'),

    F('mf_net_amount_xl', '资金流分析', 'float32', '超大单净流入净额', 'net_amount_xl'),
    F('mf_net_pct_xl', '资金流分析', 'float32', '超大单净流入占比', 'net_pct_xl'),

    F('mf_net_amount_l', '资金流分析', 'float32', '大单净流入净额', 'net_amount_l'),
    F('mf_net_pct_l', '资金流分析', 'float32', '大单净流入占比', 'net_pct_l'),

    F('mf_net_amount_m', '资金流分析', 'float32', '中单净流入净额', 'net_amount_m'),
    F('mf_net_pct_m', '资金流分析', 'float32', '中单净流入占比', 'net_pct_m'),

    F('mf_net_amount_s', '资金流分析', 'float32', '小单净流入净额', 'net_amount_s'),
    F('mf_net_pct_s', '资金流分析', 'float32', '小单净流入占比', 'net_pct_s'),

    F('fs_publish_date', '财报数据', 'datetime64', '发布日期（数据可以开始使用的日期）', 'STM_ISSUINGDATE'),
    F('fs_quarter', '财报数据', 'str', '财报对应的年份季度，例如 20151231', '', fill_missing_value=''),
    F('fs_quarter_year', '财报数据', 'int16', '财报对应的年份。0表示无可用财报', '', 0),
    F('fs_quarter_index', '财报数据', 'int8', '财报对应的季度，取值 1/2/3/4，1表示第一季度，以此类推。0表示无可用财报', '', 0),
    F('fs_net_profit', '财报数据', 'float32', '归属母公司股东的净利润', 'NP_BELONGTO_PARCOMSH'),
    F('fs_net_profit_ttm', '财报数据', 'float32', '归属母公司股东的净利润 (TTM)', 'NETPROFIT_TTM2'),
    F('fs_net_profit_yoy', '财报数据', 'float32', '归属母公司股东的净利润同比增长率', 'YOYNETPROFIT'),
    F('fs_net_profit_qoq', '财报数据', 'float32', '归属母公司股东的净利润单季度环比增长率', 'QFA_CGRNETPROFIT'),
    F('fs_deducted_profit', '财报数据', 'float32', '扣除非经常性损益后的净利润', 'DEDUCTEDPROFIT'),
    F('fs_deducted_profit_ttm', '财报数据', 'float32', '扣除非经常性损益后的净利润 (TTM)', 'DEDUCTEDPROFIT_TTM2'),
    F('fs_roe', '财报数据', 'float32', '净资产收益率', 'ROE'),
    F('fs_roe_ttm', '财报数据', 'float32', '净资产收益率 (TTM)', 'ROE_TTM2'),
    F('fs_roa', '财报数据', 'float32', '总资产报酬率', 'ROA2'),
    F('fs_roa_ttm', '财报数据', 'float32', '总资产报酬率 (TTM)', 'ROA2_TTM2'),
    F('fs_gross_profit_margin', '财报数据', 'float32', '销售毛利率', 'GROSSPROFITMARGIN'),
    F('fs_gross_profit_margin_ttm', '财报数据', 'float32', '销售毛利率 (TTM)', 'GROSSPROFITMARGIN_TTM2'),
    F('fs_net_profit_margin', '财报数据', 'float32', '销售净利率', 'NETPROFITMARGIN'),
    F('fs_net_profit_margin_ttm', '财报数据', 'float32', '销售净利率 (TTM)', 'NETPROFITMARGIN_TTM2'),
    F('fs_operating_revenue', '财报数据', 'float32', '营业收入', 'OPER_REV'),
    F('fs_operating_revenue_ttm', '财报数据', 'float32', '营业收入 (TTM)', 'OR_TTM2'),
    F('fs_operating_revenue_yoy', '财报数据', 'float32', '营业收入同比增长率', 'YOY_OR'),
    F('fs_operating_revenue_qoq', '财报数据', 'float32', '营业收入单季度环比增长率', 'QFA_CGRSALES'),
    F('fs_free_cash_flow', '财报数据', 'float32', '企业自由现金流', 'FCFF'),
    F('fs_net_cash_flow', '财报数据', 'float32', '经营活动产生的现金流量净额', 'NET_CASH_FLOWS_OPER_ACT'),
    F('fs_net_cash_flow_ttm', '财报数据', 'float32', '经营活动现金净流量 (TTM)', 'OPERATECASHFLOW_TTM2'),
    F('fs_eps', '财报数据', 'float32', '每股收益', 'EPS_BASIC'),
    F('fs_eps_yoy', '财报数据', 'float32', '每股收益同比增长率', 'YOYEPS_BASIC'),
    F('fs_bps', '财报数据', 'float32', '每股净资产', 'BPS'),
    F('fs_current_assets', '财报数据', 'float32', '流动资产', 'TOT_CUR_ASSETS'),
    F('fs_non_current_assets', '财报数据', 'float32', '非流动资产', 'TOT_NON_CUR_ASSETS'),
    F('fs_current_liabilities', '财报数据', 'float32', '流动负债', 'TOT_CUR_LIAB'),
    F('fs_non_current_liabilities', '财报数据', 'float32', '非流动负债', 'TOT_NON_CUR_LIAB'),
    F('fs_cash_ratio', '财报数据', 'float32', '现金比率', 'CASHTOCURRENTDEBT'),
    F('fs_common_equity', '财报数据', 'float32', '普通股权益总额', 'WGSD_COM_EQ_PAHOLDER'),


    F('fs_cash_equivalents', '财报数据', 'float32', '货币资金', 'MONETARY_CAP'),
    F('fs_account_receivable', '财报数据', 'float32', '应收账款', 'ACCT_RCV'),
    F('fs_fixed_assets', '财报数据', 'float32', '固定资产', 'FIX_ASSETS'),
    F('fs_proj_matl', '财报数据', 'float32', '工程物资', 'PROJ_MATL'),
    F('fs_construction_in_process', '财报数据', 'float32', '在建工程', 'CONST_IN_PROG'),
    F('fs_fixed_assets_disp', '财报数据', 'float32', '固定资产清理', 'FIX_ASSETS_DISP'),

    F('fs_account_payable', '财报数据', 'float32', '应付账款', 'ACCT_PAYABLE'),
    F('fs_total_liability', '财报数据', 'float32', '负债合计', 'TOT_LIAB'),

    F('fs_paicl_up_capital', '财报数据', 'float32', '实收资本（或股本）', 'CAP_STK'),
    F('fs_capital_reserves', '财报数据', 'float32', '资本公积金', 'CAP_RSRV'),
    F('fs_surplus_reserves', '财报数据', 'float32', '盈余公积金', 'SURPLUS_RSRV'),
    F('fs_undistributed_profit', '财报数据', 'float32', '未分配利润', 'UNDISTRIBUTED_PROFIT'),
    F('fs_eqy_belongto_parcomsh', '财报数据', 'float32', '归属母公司股东的权益', 'EQY_BELONGTO_PARCOMSH'),
    F('fs_total_equity', '财报数据', 'float32', '所有者权益合计', 'TOT_EQUITY'),

    F('fs_gross_revenues', '财报数据', 'float32', '营业总收入', 'TOT_OPER_REV'),
    F('fs_total_operating_costs', '财报数据', 'float32', '营业总成本', 'TOT_OPER_COST'),
    F('fs_selling_expenses', '财报数据', 'float32', '销售费用', 'SELLING_DIST_EXP'),
    F('fs_financial_expenses', '财报数据', 'float32', '财务费用', 'FIN_EXP_IS'),
    F('fs_general_expenses', '财报数据', 'float32', '管理费用', 'GERL_ADMIN_EXP'),
    F('fs_operating_profit', '财报数据', 'float32', '营业利润', 'OPPROFIT'),
    F('fs_total_profit', '财报数据', 'float32', '利润总额', 'TOT_PROFIT'),
    F('fs_income_tax', '财报数据', 'float32', '所得税', 'TAX'),
    F('fs_net_income', '财报数据', 'float32', '净利润', 'NET_PROFIT_IS'),


    F('sh_holder_avg_pct', '股东数据', 'float32', '户均持股比例', 'HOLDER_AVGPCT'),
    F('sh_holder_avg_pct_3m_chng', '股东数据', 'float32', '户均持股比例季度增长率', 'HOLDER_QAVGPCTCHANGE'),
    F('sh_holder_avg_pct_6m_chng', '股东数据', 'float32', '户均持股比例半年增长率', 'HOLDER_HAVGPCTCHANGE'),
    F('sh_holder_num', '股东数据', 'float32', '股东户数', 'HOLDER_NUM'),
    F('industry_sw_level1', '行业数据', 'int32', '申万一级行业类别', 'industry_sw_level1'),
    F('industry_sw_level2', '行业数据', 'int32', '申万二级行业类别', 'industry_sw_level2'),
    F('industry_sw_level3', '行业数据', 'int32', '申万三级行业类别', 'industry_sw_level3'),
    F('concept', '板块数据', 'str', '概念板块', 'CONCEPT'),
    *[
        F(x.feature_name, '指数数据', 'int8', '是否属于%s指数成份'%x.name, x.feature_name) for x in index_constituent_list
    ],


    'markets:MacroData',
    F('cpi', '宏观经济数据', 'float32', 'CPI当月同比', 'M0000612'),
    F('ppi', '宏观经济数据', 'float32', 'PPI当月同比', 'M0001227'),
    F('gdp', '宏观经济数据', 'float32', 'GDP当季同比', 'M0039354'),
    F('pmi', '宏观经济数据', 'float32', 'PMI', 'M0017126'),
    F('m0', '宏观经济数据', 'float32', 'M0同比', 'M0001381'),
    F('m2', '宏观经济数据', 'float32', 'M2同比', 'M0001385'),
    F('m1', '宏观经济数据', 'float32', 'M1同比', 'M0001383'),
    F('foreign_exchange_reserve', '宏观经济数据', 'float32', '国家外汇储备', 'M0010049'),
    F('gold_reserve', '宏观经济数据', 'float32', '黄金储备', 'M0010048'),
    F('new_investors_num', '宏观经济数据', 'float32', '新增投资者数量', 'M5558005'),
    F('end_investor_num', '宏观经济数据', 'float32', '期末投资者数量', 'M5558008'),
    F('bdi', '宏观经济数据', 'float32', '波罗的海干散货指数', 'S0031550'),
    F('chibor_on', '宏观经济数据', 'float32', 'Chibor同业间拆借利率_隔夜', 'M0220173'),
    F('chibor_1w', '宏观经济数据', 'float32', 'Chibor同业间拆借利率_1周', 'M0220174'),
    F('chibor_2w', '宏观经济数据', 'float32', 'Chibor同业间拆借利率_2周', 'M0220175'),
    F('chibor_3w', '宏观经济数据', 'float32', 'Chibor同业间拆借利率_3周', 'M0220176'),
    F('chibor_1M', '宏观经济数据', 'float32', 'Chibor同业间拆借利率_1个月', 'M0220177'),
    F('chibor_2M', '宏观经济数据', 'float32', 'Chibor同业间拆借利率_2个月', 'M0220178'),
    F('chibor_3M', '宏观经济数据', 'float32', 'Chibor同业间拆借利率_3个月', 'M0220179'),
    F('chibor_4M', '宏观经济数据', 'float32', 'Chibor同业间拆借利率_4个月', 'M0220180'),
    F('chibor_6M', '宏观经济数据', 'float32', 'Chibor同业间拆借利率_6个月', 'M0220181'),
    F('chibor_9M', '宏观经济数据', 'float32', 'Chibor同业间拆借利率_9个月', 'M0220182'),
    F('chibor_1Y', '宏观经济数据', 'float32', 'Chibor同业间拆借利率_1年', 'M0220183'),

])



class HistoryDataFieldDefs:
    FIELD_LIST = _HISTORY_DATA_FIELDS
    FIELD_MAP = {f.field: f for f in FIELD_LIST}

    WIND_TO_BQ_MAP = {
        **{f.wind_code: f.field for f in FIELD_LIST if f.wind_code},
        **{ # 补充数据
            'suspend_type': 'suspend_type',
            'suspend_reason': 'suspend_reason',
            'Code': 'instrument',
            'quarter': 'fs_quarter',
        }
    }

if __name__ == '__main__':
    # test code
    #for f in HistoryDataFieldDefs.FIELD_LIST[18:]:
    #    print("'%s'," % f.wind_code)
    print(_HISTORY_DATA_FIELDS)
