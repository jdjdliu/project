# 按使用频率/场景做partition，规则如下，TODO：改进？自动？

from .featuredefs import FeatureDefs
from bigdata.common.market import index_constituent_list


def _F(feature_names, low=None, up=None):
    s = []
    if isinstance(feature_names, str):
        feature_names = [feature_names]
    for feature_name in feature_names:
        s += FeatureDefs.FEATURE_MAP[feature_name].expand_fields(with_params=False, low_1=low, up_1=up)
    return s


_DATA_FIELD_GROUPS = {
    # 'date', 'instrument' 是必须字段，否则读取的时候会出错
    # G 1xx：基础信息
    # 交易数据
    'G100': [
        'date', 'instrument', 'open', 'high', 'low', 'close', 'volume', 'amount', 'deal_number', 'turn',
        'adjust_factor', 'st_status', 'mf_net_amount', 'price_limit_status', 'open_intl', 'settle',
        'mf_net_amount_main', 'mf_net_pct_main', 'mf_net_amount_xl', 'mf_net_pct_xl', 'mf_net_amount_l',
        'mf_net_pct_l', 'mf_net_amount_m', 'mf_net_pct_m', 'mf_net_amount_s', 'mf_net_pct_s',
    ],
    # 股票名字，上市日期，停牌日期，等等基本信息
    'G101': [
        'date', 'instrument', 'name', 'list_date', 'list_board', 'delist_date', 'company_name', 'company_type',
        'company_found_date', 'company_province', 'contunit',
    ],
    # 交易数据，停牌情况，稀疏，低使用频率
    'G102': [
        'date', 'instrument', 'suspend_type', 'suspend_reason', 'suspended'
    ],
    # 财报数据
    'G103': [
        'date', 'instrument',
        'fs_publish_date', 'fs_quarter', 'fs_quarter_year', 'fs_quarter_index', 'fs_net_profit',
        'fs_net_profit_ttm', 'fs_net_profit_yoy', 'fs_net_profit_qoq', 'fs_deducted_profit',
        'fs_deducted_profit_ttm', 'fs_roe', 'fs_roe_ttm', 'fs_roa', 'fs_roa_ttm', 'fs_gross_profit_margin',
        'fs_gross_profit_margin_ttm', 'fs_net_profit_margin', 'fs_net_profit_margin_ttm', 'fs_operating_revenue',
        'fs_operating_revenue_ttm', 'fs_operating_revenue_yoy', 'fs_operating_revenue_qoq', 'fs_free_cash_flow',
        'fs_net_cash_flow', 'fs_net_cash_flow_ttm', 'fs_eps', 'fs_eps_yoy', 'fs_bps', 'fs_current_assets',
        'fs_non_current_assets', 'fs_current_liabilities', 'fs_non_current_liabilities', 'fs_cash_ratio',
        'fs_common_equity',

        'fs_cash_equivalents', 'fs_account_receivable', 'fs_fixed_assets', 'fs_proj_matl', 'fs_construction_in_process',
        'fs_fixed_assets_disp',
        'fs_account_payable', 'fs_total_liability',
        'fs_paicl_up_capital', 'fs_capital_reserves', 'fs_surplus_reserves', 'fs_undistributed_profit',
        'fs_eqy_belongto_parcomsh', 'fs_total_equity',
        'fs_gross_revenues', 'fs_total_operating_costs', 'fs_selling_expenses',
        'fs_financial_expenses', 'fs_general_expenses', 'fs_operating_profit',
        'fs_total_profit', 'fs_income_tax', 'fs_net_income',

        'sh_holder_avg_pct', 'sh_holder_avg_pct_3m_chng', 'sh_holder_avg_pct_6m_chng', 'sh_holder_num',
    ],
    # 估值相关
    'G104': [
        'date', 'instrument', 'market_cap', 'market_cap_float', 'pe_ttm', 'pe_lyr', 'pb_lf', 'ps_ttm', 'pb_mrq',
        'industry_sw_level1', 'industry_sw_level2', 'industry_sw_level3', 'concept', *[x.feature_name for x in index_constituent_list],
        'west_netprofit_ftm', 'west_eps_ftm', 'west_avgcps_ftm'
    ],

    # G 3xx：因子/特征信息，TODO 太复杂了，需要优化，昨天改的，今天看得都晕了，可以用运行 featureextractor/mainv2.py 的 test 来查看未覆盖的feature

    # 高频使用
    'G300': [
        'date', 'instrument',
        *_F(['open', 'high', 'low', 'volume', 'adjust_factor', 'deal_number', 'turn', 'price_limit_status', 'mf_net_amount', 'avg_mf_net_amount'], 0, 5 + 1),
        *_F(['close', 'amount', 'daily_return', 'return', 'avg_amount', 'avg_turn'], 0, 20 + 1),
        'st_status_0',
        'market_cap_0', 'market_cap_float_0', 'pe_ttm_0', 'pe_lyr_0', 'pb_lf_0', 'pb_mrq_0','ps_ttm_0', 'fs_publish_date_0',
        'fs_quarter_year_0', 'fs_quarter_index_0', 'fs_net_profit_0', 'fs_net_profit_ttm_0', 'fs_net_profit_yoy_0',
        'fs_net_profit_qoq_0', 'fs_deducted_profit_0', 'fs_deducted_profit_ttm_0', 'fs_roe_0', 'fs_roe_ttm_0',
        'fs_roa_0', 'fs_roa_ttm_0', 'fs_gross_profit_margin_0', 'fs_gross_profit_margin_ttm_0',
        'fs_net_profit_margin_0', 'fs_net_profit_margin_ttm_0', 'fs_operating_revenue_0',
        'fs_operating_revenue_ttm_0', 'fs_operating_revenue_yoy_0', 'fs_operating_revenue_qoq_0',
        'fs_free_cash_flow_0', 'fs_net_cash_flow_0', 'fs_net_cash_flow_ttm_0',
        'fs_eps_0', 'fs_eps_yoy_0', 'fs_bps_0', 'fs_current_assets_0', 'fs_non_current_assets_0',
        'fs_current_liabilities_0', 'fs_non_current_liabilities_0', 'fs_cash_ratio_0', 'fs_common_equity_0',
        'sh_holder_avg_pct_0', 'sh_holder_avg_pct_3m_chng_0', 'sh_holder_avg_pct_6m_chng_0', 'sh_holder_num_0',
        'industry_sw_level1_0','industry_sw_level2_0','industry_sw_level3_0',
        *['%s_0'%x.feature_name for x in index_constituent_list],
        'west_netprofit_ftm_0', 'west_eps_ftm_0', 'west_avgcps_ftm_0',
        'mf_net_amount_main_0', 'mf_net_pct_main_0', 'mf_net_amount_xl_0', 'mf_net_pct_xl_0', 'mf_net_amount_l_0',
        'mf_net_pct_l_0', 'mf_net_amount_m_0', 'mf_net_pct_m_0', 'mf_net_amount_s_0', 'mf_net_pct_s_0',
    ],

    # 中频使用
    'G301': [
        'date', 'instrument',
        *_F(['open', 'high', 'low', 'volume', 'adjust_factor', 'deal_number', 'turn', 'price_limit_status', 'mf_net_amount', 'avg_mf_net_amount'], 5 + 1, None),
        *_F(['close', 'amount', 'daily_return', 'return', 'avg_amount', 'avg_turn'], 20 + 1, None),
        'list_days_0', 'list_board_0', 'company_found_date_0',
        'fs_cash_equivalents_0', 'fs_account_receivable_0', 'fs_fixed_assets_0', 'fs_proj_matl_0', 'fs_construction_in_process_0',
        'fs_fixed_assets_disp_0',
        'fs_account_payable_0', 'fs_total_liability_0',
        'fs_paicl_up_capital_0', 'fs_capital_reserves_0', 'fs_surplus_reserves_0', 'fs_undistributed_profit_0',
        'fs_eqy_belongto_parcomsh_0', 'fs_total_equity_0',
        'fs_gross_revenues_0', 'fs_total_operating_costs_0', 'fs_selling_expenses_0',
        'fs_financial_expenses_0', 'fs_general_expenses_0', 'fs_operating_profit_0',
        'fs_total_profit_0', 'fs_income_tax_0', 'fs_net_income_0',
    ],
    'G302': [
        'date', 'instrument',
        *_F('ta_sma_$i_0'),
        *_F('ta_ema_$i_0'),
        *_F('ta_wma_$i_0'),
        *_F('ta_ad'),
        *_F('ta_aroon_down_$i_0'),
        *_F('ta_aroon_up_$i_0'),
        *_F('ta_aroonosc_$i_0'),
        *_F('ta_atr_$i_0'),
        *_F('ta_bbands_upperband_$i_0'),
        *_F('ta_bbands_middleband_$i_0'),
        *_F('ta_bbands_lowerband_$i_0'),
        *_F('ta_adx_$i_0'),
        *_F('ta_cci_$i_0'),
        *_F('ta_trix_$i_0'),
        *_F('ta_macd_macd_12_26_9'),
        *_F('ta_macd_macdsignal_12_26_9'),
        *_F('ta_macd_macdhist_12_26_9'),
        *_F('ta_obv'),
        *_F('ta_stoch_slowk_5_3_0_3_0'),
        *_F('ta_stoch_slowd_5_3_0_3_0'),
        *_F('ta_mfi_$i_0'),
        *_F('ta_rsi_$i_0'),
        *_F('ta_sar'),
        *_F('ta_mom_$i_0'),
        *_F('ta_willr_$i_0'),
        *_F('swing_volatility_$i_0'),
        *_F('volatility_$i_0'),
        *_F('beta_sse50_$i_0'),
        *_F('beta_csi300_$i_0'),
        *_F('beta_csi500_$i_0'),
        *_F('beta_csi800_$i_0'),
        *_F('beta_sse180_$i_0'),
        *_F('beta_csi100_$i_0'),
        *_F('beta_szzs_$i_0'),
        *_F('beta_gem_$i_0'),
        *_F('beta_industry_$i_0'),
    ],

    # rank 相关feature
    # 高频
    'G310': [
        'date', 'instrument',
        *_F(['rank_return', 'rank_avg_amount', 'rank_avg_mf_net_amount', 'rank_turn', 'rank_avg_turn', 'rank_amount'], 0, 20 + 1),
        'rank_market_cap_0', 'rank_market_cap_float_0', 'rank_pe_ttm_0', 'rank_pe_lyr_0', 'rank_pb_lf_0', 'rank_pb_mrq_0',
        'rank_ps_ttm_0', 'rank_fs_net_profit_yoy_0', 'rank_fs_net_profit_qoq_0', 'rank_fs_roe_0', 'rank_fs_roe_ttm_0',
        'rank_fs_roa_0', 'rank_fs_roa_ttm_0', 'rank_fs_operating_revenue_yoy_0', 'rank_fs_operating_revenue_qoq_0',
        'rank_fs_eps_0', 'rank_fs_eps_yoy_0', 'rank_fs_bps_0', 'rank_fs_cash_ratio_0', 'rank_sh_holder_avg_pct_0',
        'rank_sh_holder_avg_pct_3m_chng_0', 'rank_sh_holder_avg_pct_6m_chng_0','rank_sh_holder_num_0',
    ],

    # 中频
    'G311': [
        'date', 'instrument',
        *_F(['rank_return', 'rank_avg_amount', 'rank_avg_turn', 'rank_amount'], 20 + 1, None),
        *_F('rank_swing_volatility_$i_0'),
        *_F('rank_volatility_$i_0'),
        *_F('rank_beta_sse50_$i_0'),
        *_F('rank_beta_csi300_$i_0'),
        *_F('rank_beta_csi500_$i_0'),
        *_F('rank_beta_csi800_$i_0'),
        *_F('rank_beta_sse180_$i_0'),
        *_F('rank_beta_csi100_$i_0'),
        *_F('rank_beta_szzs_$i_0'),
        *_F('rank_beta_gem_$i_0'),
        *_F('rank_beta_industry_$i_0'),
    ],
}


class DataFieldFamily:
    def __init__(self, name, groups):
        self.name = name
        # 所有组
        self.groups = groups
        # 字段 -> group 映射
        self.field_to_group_map = self.__build_data_field_to_group_map(groups)
        # 所有字段
        self.fields = set(self.field_to_group_map.keys())

    def __build_data_field_to_group_map(self, group_names):
        data = {}
        for group_name in group_names:
            for field in _DATA_FIELD_GROUPS[group_name]:
                data[field] = group_name
        return data


# interface wrapper
class DataDefs:
    GROUPS = _DATA_FIELD_GROUPS

    FAMILY_HISTORY_DATA = DataFieldFamily('history_data', ['G100', 'G101', 'G102', 'G103', 'G104'])
    FAMILY_FEATURE = DataFieldFamily('feature', ['G300', 'G301', 'G302', 'G310', 'G311'])
    FAMILIES = {f.name : f for f in [FAMILY_HISTORY_DATA, FAMILY_FEATURE]}
