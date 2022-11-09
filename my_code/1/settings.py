from alpha.settings import TORTOISE_ORM
from pydantic import BaseModel, Field
from sdk.common import OSEnv

TASK_HOST = OSEnv.str("TASK_HOST", default="http://127.0.0.1:8003")
ALPHA_HOST = OSEnv.str("ALPHA_HOST", default="http://127.0.0.1:8001")

creator_id = "2803909a723b472487ba2aa8aeff26fa"

JWT = "eyJ0eXAiOiAiSldUIiwgImFsZyI6ICJTTTNXaXRoU00yIn0.eyJ1c2VyTmFtZSI6ICJiaWdxdWFudCIsICJzdWIiOiAiYmlncXVhbnQiLCAidXNlcklkIjogIjY4NjJmOTYzLTEzOGMtNDM5OC05YWZhLTlkMTcwY2Y5ZDM5OSJ9.MzA0NjAyMjEwMGYyYzA4NDk4NWI3NGM1Yjg5NGJkNDkzZTgzOTQ0NWRlNTc3MDg5OTVmNTc5NTFlZDQ5Y2RiMjRhNzcwODVkODgwMjIxMDBhNDk2Nzk1MTM5OGUyZTU5MzdlMTZiZGFiNTFiY2I3NmVmMTE1MDM2YTYwMGIyZTEyM2YzNjJjZWJlZWEyMDI2"

backtest_file = "backtest_ids.txt"
alpha_file = "alpha_ids.txt"
PUBLIC_REPO = "股票因子库"

HEADERS = {"Authorization": JWT}
INDEX_PARAMETER = {
    "factor_name": "",
    "weight_method": "市值加权",
    "stock_pool": "全市场",
    "benchmark": "中证500",
    "sort": "升序",
    "rebalance_days": 2,
    "cost": 0.001,
    "quantile_ratio": 0.0,
    "stock_num": 0,
}

catalpg_mapping = {
    "质量因子": "5be1f38e-a385-4ddb-b060-e0fd39b5b451",
    "盈利因子": "01d4a5d8-5228-4c41-80d5-d06698c03c34",
    "营运因子": "4ef381d9-4d19-4bd4-95b6-fe99eeafbff7",
    "每股因子": "aaae5e7a-1efd-496b-9166-3b4977c8b2d0",
    "成长因子": "439f4e9b-a3cd-4775-b03c-675650538dfe",
    "技术分析因子": "446215fb-7741-4476-8943-d3b000e5069e",
    "估值因子": "241a6a14-1765-41c3-83de-417fd2aa6856",
    "股东因子": "31702d2e-d6ad-4ffd-9ec8-c62d11486622",
    "波动率因子": "ddff7be6-314f-497c-a44d-217264201c29",
}

alpha_mapping = {
    "rank_sh_holder_avg_pct_0": "股东因子",
    "rank_sh_holder_num_0": "股东因子",
    "sh_holder_num_0": "股东因子",
    "market_cap_0": "估值因子",
    "market_cap_float_0": "估值因子",
    "pb_lf_0": "估值因子",
    "pe_lyr_0": "估值因子",
    "pe_ttm_0": "估值因子",
    "ps_ttm_0": "估值因子",
    "ta_bbands_lowerband_14_0": "技术分析因子",
    "ta_bbands_middleband_14_0": "技术分析因子",
    "ta_bbands_upperband_14_0": "技术分析因子",
    "ta_macd_macd_12_26_9_0": "技术分析因子",
    "ta_macd_macdhist_12_26_9_0": "技术分析因子",
    "ta_macd_macdsignal_12_26_9_0": "技术分析因子",
    "ta_obv_0": "技术分析因子",
    "ta_rsi_14_0": "技术分析因子",
    "ta_rsi_28_0": "技术分析因子",
    "ta_sma_5_0": "技术分析因子",
    "ta_sma_10_0": "技术分析因子",
    "ta_sma_20_0": "技术分析因子",
    "rank_volatility_5_0": "波动率因子",
    "swing_volatility_5_0": "波动率因子",
    "roa_0": "盈利因子",
    "op_gr_0": "盈利因子",
    "pro_tog_0": "盈利因子",
    "yoy_eps_bas_0": "成长因子",
    "yoy_ocf_0": "成长因子",
    "yoy_op_0": "成长因子",
    "yoy_tr_0": "成长因子",
    "acc_lia_pro_0": "质量因子",
    "cas_rat_0": "质量因子",
    "ass_equ_0": "质量因子",
    "lia_tot_equ_0": "质量因子",
    "ebit_int_0": "质量因子",
    "cur_rat_0": "质量因子",
    "qui_rat_0": "质量因子",
    "inv_rn_0": "营运因子",
    "art_rn_0": "营运因子",
    "fat_rn_0": "营运因子",
    "cat_rn_0": "营运因子",
    "ass_rn_0": "营运因子",
    "eps_bas_0": "每股因子",
    "bps_0": "每股因子",
    "gr_ps_0": "每股因子",
    "orps_0": "每股因子",
    "cfps_0": "每股因子",
}

alpha_name = {
    "rank_sh_holder_avg_pct_0": "户均持股比例，升序百分比排名",
    "rank_sh_holder_num_0": "股东户数，升序百分比排名",
    "sh_holder_num_0": "股东户数",
    "market_cap_0": "总市值",
    "market_cap_float_0": "流通市值",
    "pb_lf_0": "市净率 (LF)",
    "pe_lyr_0": "市盈率 (LYR)",
    "pe_ttm_0": "市盈率 (TTM)",
    "ps_ttm_0": "市销率 (TTM)",
    "ta_bbands_lowerband_14_0": "bbands_lowerband_14",
    "ta_bbands_middleband_14_0": "bbands_middleband_14",
    "ta_bbands_upperband_14_0": "bbands_upperband_14",
    "ta_macd_macd_12_26_9_0": "MACD",
    "ta_macd_macdhist_12_26_9_0": "MACD-hist",
    "ta_macd_macdsignal_12_26_9_0": "MACD-signal",
    "ta_obv_0": "obv",
    "ta_rsi_14_0": "rsi_14",
    "ta_rsi_28_0": "rsi_28",
    "ta_sma_5_0": "sma_5",
    "ta_sma_10_0": "sma_10",
    "ta_sma_20_0": "sma_20",
    "rank_volatility_5_0": "5日波动率升序百分比排名",
    "swing_volatility_5_0": "5日振幅波动率",
    "roa_0": "总资产净利润",
    "op_gr_0": "营业利润率",
    "pro_tog_0": "净利润率",
    "yoy_eps_bas_0": "同比增长率-基本每股收益(%)",
    "yoy_ocf_0": "同比增长率-每股经营活动产生的现金流量净额(%)",
    "yoy_op_0": "同比增长率-营业利润(%)",
    "yoy_tr_0": "营业总收入同比增长率(%)",
    "acc_lia_pro_0": "资产负债率",
    "ass_equ_0": "权益乘数",
    "lia_tot_equ_0": "产权比率",
    "ebit_int_0": "已获利息倍数(EBIT/利息费用)",
    "cur_rat_0": "流动比率",
    "qui_rat_0": "速动比率",
    "cas_rat_0": "保守速动比率",
    "inv_rn_0": "存货周转率",
    "art_rn_0": "应收账款周转率",
    "fat_rn_0": "固定资产周转率",
    "cat_rn_0": "流动资产周转率",
    "ass_rn_0": "总资产周转率",
    "eps_bas_0": "基本每股收益",
    "bps_0": "每股净资产",
    "gr_ps_0": "每股营业总收入",
    "orps_0": "每股营业收入",
    "cfps_0": "每股现金流量净额",
}

final_mapping = {k: catalpg_mapping[v] for k, v in alpha_mapping.items() if catalpg_mapping.get(v)}
