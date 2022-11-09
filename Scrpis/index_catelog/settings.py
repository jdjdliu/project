from pydantic import BaseModel, Field
from sdk.common import OSEnv

from alpha.settings import TORTOISE_ORM

TASK_HOST = OSEnv.str("TASK_HOST", default="http://127.0.0.1:8003")
ALPHA_HOST = OSEnv.str("ALPHA_HOST", default="http://127.0.0.1:8001")

creator_id = "2803909a723b472487ba2aa8aeff26fa"

JWT = "eyJ0eXAiOiAiSldUIiwgImFsZyI6ICJTTTNXaXRoU00yIn0.eyJ1c2VyTmFtZSI6ICJiaWdxdWFudCIsICJzdWIiOiAiYmlncXVhbnQiLCAidXNlcklkIjogIjY4NjJmOTYzLTEzOGMtNDM5OC05YWZhLTlkMTcwY2Y5ZDM5OSJ9.MzA0NjAyMjEwMGYyYzA4NDk4NWI3NGM1Yjg5NGJkNDkzZTgzOTQ0NWRlNTc3MDg5OTVmNTc5NTFlZDQ5Y2RiMjRhNzcwODVkODgwMjIxMDBhNDk2Nzk1MTM5OGUyZTU5MzdlMTZiZGFiNTFiY2I3NmVmMTE1MDM2YTYwMGIyZTEyM2YzNjJjZWJlZWEyMDI2"

backtest_file = "backtest_ids.txt"
alpha_file = "alpha_ids.txt"
PUBLIC_REPO = "股票因子库"

HEADERS = {"Authorization": JWT}
ALPHA_PARAMETER = {
    "start_date": "2017-01-01",
    "end_date": "",
    "rebalance_period": 22,
    "delay_rebalance_days": 0,
    "rebalance_price": "close_0",
    "stock_pool": "全市场",
    "quantile_count": 5,
    "commission_rate": 0.0016,
    "returns_calculation_method": "累乘",
    "benchmark": "无",
    "drop_new_stocks": 60,
    "drop_price_limit_stocks": True,
    "drop_st_stocks": True,
    "drop_suspended_stocks": True,
    "normalization": True,
    "cutoutliers": False,
    "neutralization": ["行业"],
    "metrics": ["因子表现概览", "因子分布", "因子行业分布", "因子市值分布", "IC分析", "买入信号重合分析", "因子估值分析", "因子拥挤度分析", "因子值最大/最小股票", "表达式因子值", "多因子相关性分析"],
    "factor_coverage": 0.1,
    "user_data_merge": "left",
}

catalpg_mapping = {
    "质量因子": "07b7f227-18f9-49e4-b924-484aad76033e",
    "盈利因子": "399fcfce-094b-44ac-9891-e594acd68c33",
    "营运因子": "6c8a48b8-1a08-4107-81be-68e3d190e008",
    "每股因子": "778a5e2b-f83a-4a4b-b330-3b4268763366",
    "成长因子": "7bc854a9-1aea-4488-843e-e8e106e6c4b3",
    "技术分析因子": "94cdc334-d51e-487b-8995-00e65957bd07",
    "估值因子": "9b86158f-27e8-4ade-b5fa-4ff71432a27f",
    "股东因子": "a731a4c5-9007-470a-a87a-42ea4be14d15",
    "波动率因子": "f03bb289-a54d-4eaf-bb69-7bb9e87f3055",
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
