import os
import sys
import datetime
import pandas as pd
import numpy as np
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname((os.path.abspath(__file__))))))
sys.path.append(BASE_DIR)

print(sys.path)
from talib import abstract
from common_features import parallel
from common_features.utils import default_args_parser, truncate
from common_features.features import FeatureDefs
from sdk.utils.logger import BigLogger
from sdk.datasource import DataSource, UpdateDataSource, D


DAY_FORMAT = "%Y-%m-%d"

# 向前取历史数据天数
data_forward_days = 365 * 8

schema = {
    'desc': 'A股内置基础因子数据',
    'category': 'A股数据/预计算因子',
    'rank': 1004001,
    'primary_key': ['date', 'instrument'],
    'active': True,
    'partition_date': 'Y',
    'friendly_name': 'A股内置基础因子数据',
    'date_field': 'date',
    'agg_name': '基础因子',
    'agg_key': 'features_CN_STOCK_A',
    'split_field': True,
    'split_file': True,
    'file_type': 'bdb',
    'fields': {
        'instrument': {'desc': '证券代码', 'type': 'str'},
        'date': {'desc': '日期', 'type': 'datetime64[ns]'},

        'adjust_factor_0': {'desc': '第前0个交易日的复权因子', 'type': 'float32'},
        'amount_0': {'desc': '第前0个交易日的交易额', 'type': 'float64'},
        'avg_amount_0': {'desc': '过去0个交易日的平均交易额', 'type': 'float64'},
        'close_0': {'desc': '第前0个交易日的收盘价', 'type': 'float32'},
        'daily_return_0': {'desc': '第前0个交易日的收益', 'type': 'float32'},
        'deal_number_0': {'desc': '第前0个交易日的成交笔数', 'type': 'int32'},
        'high_0': {'desc': '第前0个交易日的最高价', 'type': 'float32'},
        'low_0': {'desc': '第前0个交易日的最低价', 'type': 'float32'},
        'open_0': {'desc': '第前0个交易日的开盘价', 'type': 'float32'},
        'price_limit_status_0': {'desc': '股价在收盘时的涨跌停状态：1表示跌停，2表示未涨跌停，3则表示涨停', 'type': 'float32'},
        'rank_amount_0': {'desc': '第前0个交易日的交易额百分比排名', 'type': 'float32'},
        'rank_avg_amount_0': {'desc': '过去0个交易日的平均交易额，百分比排名', 'type': 'float32'},
        'rank_return_0': {'desc': '过去0个交易日的收益排名', 'type': 'float32'},
        'return_0': {'desc': '过去0个交易日的收益', 'type': 'float32'},
        'volume_0': {'desc': '第前0个交易日的交易量', 'type': 'int64'},
        'avg_turn_0': {'desc': '过去0个交易日的平均换手率', 'type': 'float32'},
        'rank_avg_turn_0': {'desc': '过去0个交易日的平均换手率排名', 'type': 'float32'},
        'turn_0': {'desc': '第前0个交易日的换手率', 'type': 'float32'},
        'rank_turn_0': {'desc': '过去0个交易日的换手率排名', 'type': 'float32'},
        'company_found_date_0': {'desc': '公司成立天数', 'type': 'int32'},
        'in_csi100_0': {'desc': '是否属于中证100指数成份', 'type': 'int8'},
        'in_csi300_0': {'desc': '是否属于沪深300指数成份', 'type': 'int8'},
        'in_csi500_0': {'desc': '是否属于中证500指数成份', 'type': 'int8'},
        'in_csi800_0': {'desc': '是否属于中证800指数成份', 'type': 'int8'},
        'in_sse180_0': {'desc': '是否属于上证180指数成份', 'type': 'int8'},
        'in_sse50_0': {'desc': '是否属于上证50指数成份', 'type': 'int8'},
        'in_szse100_0': {'desc': '是否属于深证100指数成份', 'type': 'int8'},
        'industry_sw_level1_0': {'desc': '申万一级行业类别', 'type': 'int32'},
        'industry_sw_level2_0': {'desc': '申万二级行业类别', 'type': 'int32'},
        'industry_sw_level3_0': {'desc': '申万三级行业类别', 'type': 'int32'},
        'list_board_0': {'desc': '上市板，主板：1，中小企业板：2，创业板：3', 'type': 'float32'},
        'list_days_0': {'desc': '已经上市的天数，按自然日计算', 'type': 'float32'},
        'st_status_0': {'desc': 'ST状态：0：正常股票，1：ST，2：*ST，11：暂停上市', 'type': 'float32'},
        'market_cap_0': {'desc': '总市值', 'type': 'float64'},
        'market_cap_float_0': {'desc': '流通市值', 'type': 'float64'},
        'pb_lf_0': {'desc': '市净率 (LF)', 'type': 'float32'},
        'pe_lyr_0': {'desc': '市盈率 (LYR)', 'type': 'float32'},
        'pe_ttm_0': {'desc': '市盈率 (TTM)', 'type': 'float32'},
        'ps_ttm_0': {'desc': '市销率 (TTM)', 'type': 'float32'},
        'rank_market_cap_0': {'desc': '总市值，升序百分比排名', 'type': 'float32'},
        'rank_market_cap_float_0': {'desc': '流通市值，升序百分比排名', 'type': 'float32'},
        'rank_pb_lf_0': {'desc': '市净率 (LF)，升序百分比排名', 'type': 'float32'},
        'rank_pe_lyr_0': {'desc': '市盈率 (LYR)，升序百分比排名', 'type': 'float32'},
        'rank_pe_ttm_0': {'desc': '市盈率(TTM),升序百分比排名', 'type': 'float32'},
        'rank_ps_ttm_0': {'desc': '市销率 (TTM)，升序百分比排名', 'type': 'float32'},
        'ta_ad_0': {'desc': '收集派发指标', 'type': 'float64'},
        'ta_adx_14_0': {'desc': 'ADX指标，timeperiod=14', 'type': 'float32'},
        'ta_adx_28_0': {'desc': 'ADX指标，timeperiod=28', 'type': 'float32'},
        'ta_aroon_down_14_0': {'desc': '阿隆指标aroondown，timeperiod=14', 'type': 'float32'},
        'ta_aroon_down_28_0': {'desc': '阿隆指标aroondown，timeperiod=28', 'type': 'float32'},
        'ta_aroon_up_14_0': {'desc': '阿隆指标aroonup，timeperiod=14', 'type': 'float32'},
        'ta_aroon_up_28_0': {'desc': '阿隆指标aroonup，timeperiod=28', 'type': 'float32'},
        'ta_aroonosc_14_0': {'desc': 'AROONOSC指标，timeperiod=14', 'type': 'float32'},
        'ta_aroonosc_28_0': {'desc': 'AROONOSC指标，timeperiod=28', 'type': 'float32'},
        'ta_atr_14_0': {'desc': 'ATR指标，timeperiod=14', 'type': 'float32'},
        'ta_atr_28_0': {'desc': 'ATR指标，timeperiod=28', 'type': 'float32'},
        'ta_bbands_lowerband_14_0': {'desc': 'BBANDS指标，timeperiod=14', 'type': 'float32'},
        'ta_bbands_lowerband_28_0': {'desc': 'BBANDS指标，timeperiod=28', 'type': 'float32'},
        'ta_bbands_middleband_14_0': {'desc': 'BBANDS指标，timeperiod=14', 'type': 'float32'},
        'ta_bbands_middleband_28_0': {'desc': 'BBANDS指标，timeperiod=28', 'type': 'float32'},
        'ta_bbands_upperband_14_0': {'desc': 'BBANDS指标，timeperiod=14', 'type': 'float32'},
        'ta_bbands_upperband_28_0': {'desc': 'BBANDS指标，timeperiod=28', 'type': 'float32'},
        'ta_cci_14_0': {'desc': 'CCI指标，timeperiod=14', 'type': 'float32'},
        'ta_cci_28_0': {'desc': 'CCI指标，timeperiod=28', 'type': 'float32'},
        'ta_ema_5_0': {'desc': '收盘价的5日指数移动平均值', 'type': 'float32'},
        'ta_ema_10_0': {'desc': '收盘价的10日指数移动平均值', 'type': 'float32'},
        'ta_ema_20_0': {'desc': '收盘价的20日指数移动平均值', 'type': 'float32'},
        'ta_ema_30_0': {'desc': '收盘价的30日指数移动平均值', 'type': 'float32'},
        'ta_ema_60_0': {'desc': '收盘价的60日指数移动平均值', 'type': 'float32'},
        'ta_macd_macd_12_26_9_0': {'desc': 'MACD', 'type': 'float32'},
        'ta_macd_macdhist_12_26_9_0': {'desc': 'MACD', 'type': 'float32'},
        'ta_macd_macdsignal_12_26_9_0': {'desc': 'MACD', 'type': 'float32'},
        'ta_mfi_14_0': {'desc': 'MFI指标，timeperiod=14', 'type': 'float32'},
        'ta_mfi_28_0': {'desc': 'MFI指标，timeperiod=28', 'type': 'float32'},
        'ta_mom_10_0': {'desc': 'MOM指标，timeperiod=10', 'type': 'float32'},
        'ta_mom_20_0': {'desc': 'MOM指标，timeperiod=20', 'type': 'float32'},
        'ta_mom_30_0': {'desc': 'MOM指标，timeperiod=30', 'type': 'float32'},
        'ta_mom_60_0': {'desc': 'MOM指标，timeperiod=60', 'type': 'float32'},
        'ta_obv_0': {'desc': 'OBV指标', 'type': 'float64'},
        'ta_rsi_14_0': {'desc': 'RSI指标，timeperiod=14', 'type': 'float32'},
        'ta_rsi_28_0': {'desc': 'RSI指标，timeperiod=28', 'type': 'float32'},
        'ta_sar_0': {'desc': 'SAR指标', 'type': 'float32'},
        'ta_sma_5_0': {'desc': '收盘价的5日简单移动平均值', 'type': 'float32'},
        'ta_sma_10_0': {'desc': '收盘价的10日简单移动平均值', 'type': 'float32'},
        'ta_sma_20_0': {'desc': '收盘价的20日简单移动平均值', 'type': 'float32'},
        'ta_sma_30_0': {'desc': '收盘价的30日简单移动平均值', 'type': 'float32'},
        'ta_sma_60_0': {'desc': '收盘价的60日简单移动平均值', 'type': 'float32'},
        'ta_stoch_slowd_5_3_0_3_0_0': {'desc': 'STOCH (KDJ) 指标D值', 'type': 'float32'},
        'ta_stoch_slowk_5_3_0_3_0_0': {'desc': 'STOCH (KDJ) 指标K值', 'type': 'float32'},
        'ta_trix_14_0': {'desc': 'TRIX指标，timeperiod=14', 'type': 'float32'},
        'ta_trix_28_0': {'desc': 'TRIX指标，timeperiod=28', 'type': 'float32'},
        'ta_willr_14_0': {'desc': 'WILLR指标，timeperiod=14', 'type': 'float32'},
        'ta_willr_28_0': {'desc': 'WILLR指标，timeperiod=28', 'type': 'float32'},
        'ta_wma_5_0': {'desc': '收盘价的5日加权移动平均值', 'type': 'float32'},
        'ta_wma_10_0': {'desc': '收盘价的10日加权移动平均值', 'type': 'float32'},
        'ta_wma_20_0': {'desc': '收盘价的20日加权移动平均值', 'type': 'float32'},
        'ta_wma_30_0': {'desc': '收盘价的30日加权移动平均值', 'type': 'float32'},
        'ta_wma_60_0': {'desc': '收盘价的60日加权移动平均值', 'type': 'float32'},
        'beta_csi100_10_0': {'desc': 'BETA值(中证100)，timeperiod=10', 'type': 'float32'},
        'beta_csi100_120_0': {'desc': 'BETA值(中证100)，timeperiod=120', 'type': 'float32'},
        'beta_csi100_180_0': {'desc': 'BETA值(中证100)，timeperiod=180', 'type': 'float32'},
        'beta_csi100_30_0': {'desc': 'BETA值(中证100)，timeperiod=30', 'type': 'float32'},
        'beta_csi100_5_0': {'desc': 'BETA值(中证100)，timeperiod=5', 'type': 'float32'},
        'beta_csi100_60_0': {'desc': 'BETA值(中证100)，timeperiod=60', 'type': 'float32'},
        'beta_csi100_90_0': {'desc': 'BETA值(中证100)，timeperiod=90', 'type': 'float32'},
        'beta_csi300_10_0': {'desc': 'BETA值(中证300)，timeperiod=10', 'type': 'float32'},
        'beta_csi300_120_0': {'desc': 'BETA值(中证300)，timeperiod=120', 'type': 'float32'},
        'beta_csi300_180_0': {'desc': 'BETA值(中证300)，timeperiod=180', 'type': 'float32'},
        'beta_csi300_30_0': {'desc': 'BETA值(中证300)，timeperiod=30', 'type': 'float32'},
        'beta_csi300_5_0': {'desc': 'BETA值(中证300)，timeperiod=5', 'type': 'float32'},
        'beta_csi300_60_0': {'desc': 'BETA值(中证300)，timeperiod=60', 'type': 'float32'},
        'beta_csi300_90_0': {'desc': 'BETA值(中证300)，timeperiod=90', 'type': 'float32'},
        'beta_csi500_10_0': {'desc': 'BETA值(中证500)，timeperiod=10', 'type': 'float32'},
        'beta_csi500_120_0': {'desc': 'BETA值(中证500)，timeperiod=120', 'type': 'float32'},
        'beta_csi500_180_0': {'desc': 'BETA值(中证500)，timeperiod=180', 'type': 'float32'},
        'beta_csi500_30_0': {'desc': 'BETA值(中证500)，timeperiod=30', 'type': 'float32'},
        'beta_csi500_5_0': {'desc': 'BETA值(中证500)，timeperiod=5', 'type': 'float32'},
        'beta_csi500_60_0': {'desc': 'BETA值(中证500)，timeperiod=60', 'type': 'float32'},
        'beta_csi500_90_0': {'desc': 'BETA值(中证500)，timeperiod=90', 'type': 'float32'},
        'beta_csi800_10_0': {'desc': 'BETA值(中证800)，timeperiod=10', 'type': 'float32'},
        'beta_csi800_120_0': {'desc': 'BETA值(中证800)，timeperiod=120', 'type': 'float32'},
        'beta_csi800_180_0': {'desc': 'BETA值(中证800)，timeperiod=180', 'type': 'float32'},
        'beta_csi800_30_0': {'desc': 'BETA值(中证800)，timeperiod=30', 'type': 'float32'},
        'beta_csi800_5_0': {'desc': 'BETA值(中证800)，timeperiod=5', 'type': 'float32'},
        'beta_csi800_60_0': {'desc': 'BETA值(中证800)，timeperiod=60', 'type': 'float32'},
        'beta_csi800_90_0': {'desc': 'BETA值(中证800)，timeperiod=90', 'type': 'float32'},
        'beta_gem_10_0': {'desc': 'BETA值(创业板)，timeperiod=10', 'type': 'float32'},
        'beta_gem_120_0': {'desc': 'BETA值(创业板)，timeperiod=120', 'type': 'float32'},
        'beta_gem_180_0': {'desc': 'BETA值(创业板)，timeperiod=180', 'type': 'float32'},
        'beta_gem_30_0': {'desc': 'BETA值(创业板)，timeperiod=30', 'type': 'float32'},
        'beta_gem_5_0': {'desc': 'BETA值(创业板)，timeperiod=5', 'type': 'float32'},
        'beta_gem_60_0': {'desc': 'BETA值(创业板)，timeperiod=60', 'type': 'float32'},
        'beta_gem_90_0': {'desc': 'BETA值(创业板)，timeperiod=90', 'type': 'float32'},
        'beta_industry_10_0': {'desc': 'BETA值(所在行业)，timeperiod=10', 'type': 'float32'},
        'beta_industry_120_0': {'desc': 'BETA值(所在行业)，timeperiod=120', 'type': 'float32'},
        'beta_industry_180_0': {'desc': 'BETA值(所在行业)，timeperiod=180', 'type': 'float32'},
        'beta_industry_30_0': {'desc': 'BETA值(所在行业)，timeperiod=30', 'type': 'float32'},
        'beta_industry_5_0': {'desc': 'BETA值(所在行业)，timeperiod=5', 'type': 'float32'},
        'beta_industry_60_0': {'desc': 'BETA值(所在行业)，timeperiod=60', 'type': 'float32'},
        'beta_industry_90_0': {'desc': 'BETA值(所在行业)，timeperiod=90', 'type': 'float32'},
        'beta_sse180_10_0': {'desc': 'BETA值(上证180)，timeperiod=10', 'type': 'float32'},
        'beta_sse180_120_0': {'desc': 'BETA值(上证180)，timeperiod=120', 'type': 'float32'},
        'beta_sse180_180_0': {'desc': 'BETA值(上证180)，timeperiod=180', 'type': 'float32'},
        'beta_sse180_30_0': {'desc': 'BETA值(上证180)，timeperiod=30', 'type': 'float32'},
        'beta_sse180_5_0': {'desc': 'BETA值(上证180)，timeperiod=5', 'type': 'float32'},
        'beta_sse180_60_0': {'desc': 'BETA值(上证180)，timeperiod=60', 'type': 'float32'},
        'beta_sse180_90_0': {'desc': 'BETA值(上证180)，timeperiod=90', 'type': 'float32'},
        'beta_sse50_10_0': {'desc': 'BETA值(上证50)，timeperiod=10', 'type': 'float32'},
        'beta_sse50_120_0': {'desc': 'BETA值(上证50)，timeperiod=120', 'type': 'float32'},
        'beta_sse50_180_0': {'desc': 'BETA值(上证50)，timeperiod=180', 'type': 'float32'},
        'beta_sse50_30_0': {'desc': 'BETA值(上证50)，timeperiod=30', 'type': 'float32'},
        'beta_sse50_5_0': {'desc': 'BETA值(上证50)，timeperiod=5', 'type': 'float32'},
        'beta_sse50_60_0': {'desc': 'BETA值(上证50)，timeperiod=60', 'type': 'float32'},
        'beta_sse50_90_0': {'desc': 'BETA值(上证50)，timeperiod=90', 'type': 'float32'},
        'beta_szzs_120_0': {'desc': 'BETA值(上证综指)，timeperiod=120', 'type': 'float32'},
        'beta_szzs_180_0': {'desc': 'BETA值(上证综指)，timeperiod=180', 'type': 'float32'},
        'beta_szzs_30_0': {'desc': 'BETA值(上证综指)，timeperiod=30', 'type': 'float32'},
        'beta_szzs_5_0': {'desc': 'BETA值(上证综指)，timeperiod=5', 'type': 'float32'},
        'beta_szzs_60_0': {'desc': 'BETA值(上证综指)，timeperiod=60', 'type': 'float32'},
        'beta_szzs_90_0': {'desc': 'BETA值(上证综指)，timeperiod=90', 'type': 'float32'},
        'beta_szzs_10_0': {'desc': 'BETA值(上证综指)，timeperiod=10', 'type': 'float32'},
        'rank_beta_csi100_10_0': {'desc': 'BETA值(中证100)，timeperiod=10, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi100_180_0': {'desc': 'BETA值(中证100)，timeperiod=180, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi300_10_0': {'desc': 'BETA值(中证300)，timeperiod=10, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi300_180_0': {'desc': 'BETA值(中证300)，timeperiod=180, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi300_30_0': {'desc': 'BETA值(中证300)，timeperiod=30, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi300_5_0': {'desc': 'BETA值(中证300)，timeperiod=5, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi500_10_0': {'desc': 'BETA值(中证500)，timeperiod=10, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi500_120_0': {'desc': 'BETA值(中证500)，timeperiod=120, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi500_180_0': {'desc': 'BETA值(中证500)，timeperiod=180, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi500_30_0': {'desc': 'BETA值(中证500)，timeperiod=30, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi500_90_0': {'desc': 'BETA值(中证500)，timeperiod=90, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi800_10_0': {'desc': 'BETA值(中证800)，timeperiod=10, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi800_120_0': {'desc': 'BETA值(中证800)，timeperiod=120, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi800_180_0': {'desc': 'BETA值(中证800)，timeperiod=180, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi800_5_0': {'desc': 'BETA值(中证800)，timeperiod=5, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi800_60_0': {'desc': 'BETA值(中证800)，timeperiod=60, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi800_90_0': {'desc': 'BETA值(中证800)，timeperiod=90, 升序百分比排名', 'type': 'float32'},
        'rank_beta_gem_10_0': {'desc': 'BETA值(创业板)，timeperiod=10, 升序百分比排名', 'type': 'float32'},
        'rank_beta_gem_120_0': {'desc': 'BETA值(创业板)，timeperiod=120, 升序百分比排名', 'type': 'float32'},
        'rank_beta_gem_180_0': {'desc': 'BETA值(创业板)，timeperiod=180, 升序百分比排名', 'type': 'float32'},
        'rank_beta_gem_30_0': {'desc': 'BETA值(创业板)，timeperiod=30, 升序百分比排名', 'type': 'float32'},
        'rank_beta_gem_90_0': {'desc': 'BETA值(创业板)，timeperiod=90, 升序百分比排名', 'type': 'float32'},
        'rank_beta_industry_120_0': {'desc': 'BETA值(所在行业)，timeperiod=120, 升序百分比排名', 'type': 'float32'},
        'rank_beta_industry_180_0': {'desc': 'BETA值(所在行业)，timeperiod=180, 升序百分比排名', 'type': 'float32'},
        'rank_beta_industry_30_0': {'desc': 'BETA值(所在行业)，timeperiod=30, 升序百分比排名', 'type': 'float32'},
        'rank_beta_industry_5_0': {'desc': 'BETA值(所在行业)，timeperiod=5, 升序百分比排名', 'type': 'float32'},
        'rank_beta_industry_90_0': {'desc': 'BETA值(所在行业)，timeperiod=90, 升序百分比排名', 'type': 'float32'},
        'rank_beta_sse180_10_0': {'desc': 'BETA值(上证180)，timeperiod=10, 升序百分比排名', 'type': 'float32'},
        'rank_beta_sse180_5_0': {'desc': 'BETA值(上证180)，timeperiod=5, 升序百分比排名', 'type': 'float32'},
        'rank_beta_sse180_60_0': {'desc': 'BETA值(上证180)，timeperiod=60, 升序百分比排名', 'type': 'float32'},
        'rank_beta_sse50_120_0': {'desc': 'BETA值(上证50)，timeperiod=120, 升序百分比排名', 'type': 'float32'},
        'rank_beta_sse50_180_0': {'desc': 'BETA值(上证50)，timeperiod=180, 升序百分比排名', 'type': 'float32'},
        'rank_beta_sse50_30_0': {'desc': 'BETA值(上证50)，timeperiod=30, 升序百分比排名', 'type': 'float32'},
        'rank_beta_sse50_5_0': {'desc': 'BETA值(上证50)，timeperiod=5, 升序百分比排名', 'type': 'float32'},
        'rank_beta_sse50_60_0': {'desc': 'BETA值(上证50)，timeperiod=60, 升序百分比排名', 'type': 'float32'},
        'rank_beta_sse50_90_0': {'desc': 'BETA值(上证50)，timeperiod=90, 升序百分比排名', 'type': 'float32'},
        'rank_beta_szzs_120_0': {'desc': 'BETA值(上证综指)，timeperiod=120, 升序百分比排名', 'type': 'float32'},
        'rank_beta_szzs_180_0': {'desc': 'BETA值(上证综指)，timeperiod=180, 升序百分比排名', 'type': 'float32'},
        'rank_beta_szzs_60_0': {'desc': 'BETA值(上证综指)，timeperiod=60, 升序百分比排名', 'type': 'float32'},

        'rank_beta_csi100_120_0': {'desc': 'BETA值(中证100)，timeperiod=120, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi100_30_0': {'desc': 'BETA值(中证100)，timeperiod=30, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi100_5_0': {'desc': 'BETA值(中证100)，timeperiod=5, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi100_60_0': {'desc': 'BETA值(中证100)，timeperiod=60, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi100_90_0': {'desc': 'BETA值(中证100)，timeperiod=90, 升序百分比排名', 'type': 'float32'},

        'rank_beta_csi300_120_0': {'desc': 'BETA值(沪深300)，timeperiod=120, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi300_60_0': {'desc': 'BETA值(沪深300)，timeperiod=60, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi300_90_0': {'desc': 'BETA值(沪深300)，timeperiod=90, 升序百分比排名', 'type': 'float32'},

        'rank_beta_csi500_5_0': {'desc': 'BETA值(中证500)，timeperiod=5, 升序百分比排名', 'type': 'float32'},
        'rank_beta_csi500_60_0': {'desc': 'BETA值(中证500)，timeperiod=60, 升序百分比排名', 'type': 'float32'},

        'rank_beta_csi800_30_0': {'desc': 'BETA值(中证800)，timeperiod=30, 升序百分比排名', 'type': 'float32'},

        'rank_beta_gem_5_0': {'desc': 'BETA值(创业板)，timeperiod=5, 升序百分比排名', 'type': 'float32'},
        'rank_beta_gem_60_0': {'desc': 'BETA值(创业板)，timeperiod=60, 升序百分比排名', 'type': 'float32'},

        'rank_beta_industry_10_0': {'desc': 'BETA值(所在行业)，timeperiod=10, 升序百分比排名', 'type': 'float32'},
        'rank_beta_industry_60_0': {'desc': 'BETA值(所在行业)，timeperiod=60, 升序百分比排名', 'type': 'float32'},

        'rank_beta_sse180_120_0': {'desc': 'BETA值(上证180)，timeperiod=120, 升序百分比排名', 'type': 'float32'},
        'rank_beta_sse180_180_0': {'desc': 'BETA值(上证180)，timeperiod=180, 升序百分比排名', 'type': 'float32'},
        'rank_beta_sse180_30_0': {'desc': 'BETA值(上证180)，timeperiod=30, 升序百分比排名', 'type': 'float32'},
        'rank_beta_sse180_90_0': {'desc': 'BETA值(上证180)，timeperiod=90, 升序百分比排名', 'type': 'float32'},

        'rank_beta_sse50_10_0': {'desc': 'BETA值(上证50)，timeperiod=10, 升序百分比排名', 'type': 'float32'},

        'rank_beta_szzs_10_0': {'desc': 'BETA值(上证综指)，timeperiod=10, 升序百分比排名', 'type': 'float32'},
        'rank_beta_szzs_30_0': {'desc': 'BETA值(上证综指)，timeperiod=30, 升序百分比排名', 'type': 'float32'},
        'rank_beta_szzs_5_0': {'desc': 'BETA值(上证综指)，timeperiod=5, 升序百分比排名', 'type': 'float32'},
        'rank_beta_szzs_90_0': {'desc': 'BETA值(上证综指)，timeperiod=90, 升序百分比排名', 'type': 'float32'},

        'swing_volatility_10_0': {'desc': '振幅波动率，timeperiod=10', 'type': 'float32'},
        'swing_volatility_120_0': {'desc': '振幅波动率，timeperiod=120', 'type': 'float32'},
        'swing_volatility_240_0': {'desc': '振幅波动率，timeperiod=240', 'type': 'float32'},
        'swing_volatility_30_0': {'desc': '振幅波动率，timeperiod=30', 'type': 'float32'},
        'swing_volatility_5_0': {'desc': '振幅波动率，timeperiod=5', 'type': 'float32'},
        'swing_volatility_60_0': {'desc': '振幅波动率，timeperiod=60', 'type': 'float32'},
        'volatility_10_0': {'desc': '波动率，timeperiod=10', 'type': 'float32'},
        'volatility_120_0': {'desc': '波动率，timeperiod=120', 'type': 'float32'},
        'volatility_240_0': {'desc': '波动率，timeperiod=240', 'type': 'float32'},
        'volatility_30_0': {'desc': '波动率，timeperiod=30', 'type': 'float32'},
        'volatility_5_0': {'desc': '波动率，timeperiod=5', 'type': 'float32'},
        'rank_swing_volatility_120_0': {'desc': '振幅波动率，timeperiod=120, 升序百分比排名', 'type': 'float32'},
        'rank_swing_volatility_240_0': {'desc': '振幅波动率，timeperiod=240, 升序百分比排名', 'type': 'float32'},
        'rank_swing_volatility_30_0': {'desc': '振幅波动率，timeperiod=30，升序百分比排名', 'type': 'float32'},
        'rank_swing_volatility_5_0': {'desc': '振幅波动率，timeperiod=5，升序百分比排名', 'type': 'float32'},
        'rank_volatility_120_0': {'desc': '波动率，timeperiod=120, 升序百分比排名', 'type': 'float32'},
        'rank_volatility_30_0': {'desc': '波动率，timeperiod=30，升序百分比排名', 'type': 'float32'},
        'rank_volatility_5_0': {'desc': '波动率，timeperiod=5，升序百分比排名', 'type': 'float32'},
        'rank_volatility_60_0': {'desc': '波动率，timeperiod=60，升序百分比排名', 'type': 'float32'},
        'rank_volatility_10_0': {'desc': '波动率，timeperiod=10，升序百分比排名', 'type': 'float32'},
        'rank_volatility_240_0': {'desc': '波动率，timeperiod=240，升序百分比排名', 'type': 'float32'},

        'volatility_60_0': {'desc': '波动率，timeperiod=60', 'type': 'float32'},
        'rank_swing_volatility_10_0': {'desc': '振幅波动率，timeperiod=10，升序百分比排名', 'type': 'float32'},
        'rank_swing_volatility_60_0': {'desc': '振幅波动率，timeperiod=60，升序百分比排名', 'type': 'float32'},

        # 财务相关的，自己加的
        'fs_quarter_year_0': {'desc': '财报对应年份', 'type': 'int16'},
        'fs_quarter_index_0': {'desc': '财报对应季度 1：一季报，2：半年报，3：三季报，4：年报，9：其他', 'type': 'int8'},
        'fs_publish_date_0': {'desc': '最近财报发布距今天数，按自然日计算，当天为0', 'type': 'int64'},

        'roa2_0': {'desc': '总资产报酬率', 'type': 'float64'},
        'roa_0': {'desc': '总资产净利率', 'type': 'float64'},
        'op_gr_0': {'desc': '利润率', 'type': 'float64'},
        'pro_tog_0': {'desc': '营业总收入利润率', 'type': 'float64'},
        'gc_gr_0': {'desc': '营业总成本率', 'type': 'float64'},
        'ebit_gr_0': {'desc': '息税前利润同营业总收入比', 'type': 'float64'},
        'gro_pro_mar_0': {'desc': '销售毛利率', 'type': 'float64'},
        'net_pro_mar_0': {'desc': '销售净利率', 'type': 'float64'},
        'cog_sal_0': {'desc': '销售成本率', 'type': 'float64'},
        'yoy_eps_bas_0': {'desc': '同比增长率-基本每股收益(%)', 'type': 'float64'},
        'yoy_eps_dil_0': {'desc': '同比增长率-稀释每股收益(%)', 'type': 'float64'},
        'yoy_ocf_0': {'desc': '同比增长率-每股经营活动产生的现金流量净额(%)', 'type': 'float64'},
        'yoy_op_0': {'desc': '同比增长率-营业利润(%)', 'type': 'float64'},
        'yoy_ebt_tot_0': {'desc': '同比增长率-利润总额(%)', 'type': 'float64'},
        'yoy_net_pro_0': {'desc': '同比增长率-归属母公司股东的净利润(%)', 'type': 'float64'},
        'yoy_tr_0': {'desc': '同比增长率-营业总收入(%)', 'type': 'float64'},
        'yoy_or_0': {'desc': '同比增长率-营业收入(%)', 'type': 'float64'},
        'yoy_bps_0': {'desc': '相对年初增长率-每股净资产(%)', 'type': 'float64'},
        'yoy_ass_0': {'desc': '相对年初增长率-资产总计(%)', 'type': 'float64'},
        'yoy_equ_sha_0': {'desc': '相对年初增长率-归属母公司的股东权益(%)', 'type': 'float64'},
        'acc_lia_pro_0': {'desc': '资产负债率(%)', 'type': 'float64'},
        'ass_equ_0': {'desc': '权益乘数', 'type': 'float64'},
        'lia_tot_equ_0': {'desc': '产权比率', 'type': 'float64'},
        'ebit_int_0': {'desc': '已获利息倍数(EBIT/利息费用)', 'type': 'float64'},
        'cur_rat_0': {'desc': '流动比(%)', 'type': 'float64'},
        'qui_rat_0': {'desc': '速动比(%)', 'type': 'float64'},
        'cas_rat_0': {'desc': '保守速动比率', 'type': 'float64'},
        'cat_ass_0': {'desc': '流动资产占总资产比率', 'type': 'float64'},
        'ocf_lia_cur_0': {'desc': '经营活动产生的现金流量净额占流动负债比率', 'type': 'float64'},
        'equ_deb_0': {'desc': '归属于母公司的股东权益占总负债比率', 'type': 'float64'},
        'ocf_deb_0': {'desc': '经营活动产生的现金流量净额同总负债比', 'type': 'float64'},
        'inv_rn_0': {'desc': '存货周转率', 'type': 'float64'},
        'art_rn_0': {'desc': '应收账款周转率', 'type': 'float64'},
        'art_day_0': {'desc': '应收账款周转天数', 'type': 'float64'},
        'fat_rn_0': {'desc': '固定资产周转率', 'type': 'float64'},
        'cat_rn_0': {'desc': '流动资产周转率', 'type': 'float64'},
        'ass_rn_0': {'desc': '总资产周转率', 'type': 'float64'},
        'fcff_0': {'desc': '企业自由现金流量(FCFF)', 'type': 'float64'},
        'wor_cap_0': {'desc': '营运资金', 'type': 'float64'},
        'net_wor_cap_0': {'desc': '营运流动资本', 'type': 'float64'},
        'eps_bas_0': {'desc': '基本每股收益', 'type': 'float64'},
        'eps_dlt_0': {'desc': '稀释每股收益', 'type': 'float64'},
        'bps_0': {'desc': '每股净资产', 'type': 'float64'},
        'ocfps_0': {'desc': '每股经营活动产生的现金流量净额', 'type': 'float64'},
        'gr_ps_0': {'desc': '每股营业总收入', 'type': 'float64'},
        'orps_0': {'desc': '每股营业收入', 'type': 'float64'},
        'sur_cap_0': {'desc': '每股资本公积', 'type': 'float64'},
        'sur_res_0': {'desc': '每股盈余公积', 'type': 'float64'},
        'und_ps_0': {'desc': '每股未分配利润', 'type': 'float64'},
        'ret_ps_0': {'desc': '每股留存收益', 'type': 'float64'},
        'cfps_0': {'desc': '每股现金流量净额', 'type': 'float64'},
        'ebit_ps_0': {'desc': '每股息税前利润', 'type': 'float64'},
        'fcff_ps_0': {'desc': '每股企业自由现金流量', 'type': 'float64'},
        'fcfe_ps_0': {'desc': '每股股东自由现金流量', 'type': 'float64'},

    }

}


class FeatureContext:
    def __init__(self, df, base_returns):
        self.df = df
        self.base_returns = base_returns
        self.__ta_input_data = None
        self.__ta_cached_data = {}

    @property
    def ta_input_data(self):
        if self.__ta_input_data is None:
            self.__ta_input_data = {
                'open': self.df.open.astype(float),
                'high': self.df.high.astype(float),
                'low': self.df.low.astype(float),
                'close': self.df.close.astype(float),
                'volume': self.df.volume.astype(float),
                'amount': self.df.amount.astype(float),
            }
        return self.__ta_input_data

    def ta(self, ta_func_name, *args):
        key = '%s\t%s' % (ta_func_name, '\t'.join([str(a) for a in args]))
        if key not in self.__ta_cached_data:
            func = abstract.Function(ta_func_name)
            self.__ta_cached_data[key] = func(self.__ta_input_data, *args)
        return self.__ta_cached_data[key]

    def beta(self, base_name, timeperiod):
        if base_name == 'INDUSTRY':
            sw_code = self.df.industry_sw_level1.iloc[-1]
            base_name = 'SW{}.SHA'.format(int(sw_code))
        if base_name not in self.base_returns:
            return pd.Series([np.NaN] * len(self.df))
        close = self.df.set_index('date')['close']
        v_returns = close / close.shift(1) - 1
        b_returns = self.base_returns[base_name].loc[v_returns.index]
        ret = v_returns.rolling(window=timeperiod).cov(other=b_returns, pairwise=True) / \
              (b_returns.rolling(center=False, window=timeperiod).std() ** 2)  # noqa
        ret.index = self.df.index
        return ret


class Build(object):
    args = default_args_parser()
    start_date = args.start_date
    end_date = args.end_date
    markets = args.markets or ['CN_STOCK_A']
    fields = args.fields or ['基本信息', '量价因子', '估值因子', '技术分析因子',
                             '波动率', 'BETA值', '换手率因子', '财务因子']  # , '资金流', '股东因子'
    # fields = ['财务因子']
    # fields = ['量价因子']
    log = BigLogger("features_CN_STOCK_A")
    fields = [fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7]]

    # fields = [fields[7]]
    history_start_date = (datetime.datetime.strptime(start_date, DAY_FORMAT) - datetime.timedelta(days=data_forward_days)).strftime(DAY_FORMAT)  # noqa

    def __init__(self):
        self.__main_feature_list = [f for f in FeatureDefs.FEATURE_LIST if not f.field.startswith('rank_')]
        self.__rank_feature_list = [f for f in FeatureDefs.FEATURE_LIST if f.field.startswith('rank_')]
        self.__base_instruments = ['000016.SHA', '000300.SHA', '000905.SHA', '000906.SHA', '000010.SHA',
                                   '000903.SHA', '000001.SHA', '399006.SZA']
        self.__base_returns = {}
        self.features_fields_map = {
            'features_CN_STOCK_A': [
                'instrument', 'date',
                # 量价因子
                'adjust_factor_0', 'amount_0', 'avg_amount_0', 'close_0', 'daily_return_0', 'deal_number_0',
                'high_0', 'low_0', 'open_0', 'price_limit_status_0', 'rank_amount_0', 'rank_avg_amount_0',
                'rank_return_0', 'return_0', 'volume_0',

                # 换手率因子
                'avg_turn_0', 'rank_avg_turn_0', 'turn_0', 'rank_turn_0',

                # 基本信息因子
                'company_found_date_0',
                'in_csi100_0', 'in_csi300_0', 'in_csi500_0', 'in_csi800_0', 'in_sse180_0', 'in_sse50_0', 'in_szse100_0',
                'industry_sw_level1_0', 'industry_sw_level2_0',
                'industry_sw_level3_0', 'list_board_0', 'list_days_0', 'st_status_0',

                # 资金流因子：缺表
                # 'avg_mf_net_amount_0', 'mf_net_amount_0', 'mf_net_amount_l_0', 'mf_net_amount_m_0',
                # 'mf_net_amount_main_0', 'mf_net_amount_s_0', 'mf_net_amount_xl_0', 'mf_net_pct_l_0',
                # 'mf_net_pct_m_0', 'mf_net_pct_main_0', 'mf_net_pct_s_0', 'mf_net_pct_xl_0',
                # 'rank_avg_mf_net_amount_0', 'rank_avg_mf_net_amount_0',

                # # 股东因子：缺表
                # 'rank_sh_holder_avg_pct_0', 'rank_sh_holder_avg_pct_3m_chng_0', 'rank_sh_holder_avg_pct_6m_chng_0',
                # 'rank_sh_holder_num_0', 'sh_holder_avg_pct_0', 'sh_holder_avg_pct_3m_chng_0',
                # 'sh_holder_avg_pct_6m_chng_0', 'sh_holder_num_0',

                # 估值因子
                'market_cap_0', 'market_cap_float_0', 'pb_lf_0', 'pe_lyr_0', 'pe_ttm_0', 'ps_ttm_0', 'rank_market_cap_0',
                'rank_market_cap_float_0', 'rank_pb_lf_0', 'rank_pe_lyr_0', 'rank_pe_ttm_0', 'rank_ps_ttm_0',
                # 针对估值因子下面三列：需要west_CN_STOCK_A，先注释掉不计算，features.py里面也注释，后续有数据了要放开
                # !!!!!!
                # 'west_avgcps_ftm_0', 'west_eps_ftm_0', 'west_netprofit_ftm_0',

                # 技术分析因子
                'ta_ad_0', 'ta_adx_14_0', 'ta_adx_28_0', 'ta_aroon_down_14_0', 'ta_aroon_down_28_0', 'ta_aroon_up_14_0',
                'ta_aroon_up_28_0', 'ta_aroonosc_14_0', 'ta_aroonosc_28_0', 'ta_atr_14_0', 'ta_atr_28_0',
                'ta_bbands_lowerband_14_0', 'ta_bbands_lowerband_28_0', 'ta_bbands_middleband_14_0',
                'ta_bbands_middleband_28_0', 'ta_bbands_upperband_14_0', 'ta_bbands_upperband_28_0', 'ta_cci_14_0',
                'ta_cci_28_0', 'ta_ema_5_0', 'ta_ema_10_0', 'ta_ema_20_0', 'ta_ema_30_0', 'ta_ema_60_0',
                'ta_macd_macd_12_26_9_0', 'ta_macd_macdhist_12_26_9_0', 'ta_macd_macdsignal_12_26_9_0',
                'ta_mfi_14_0', 'ta_mfi_28_0', 'ta_mom_10_0', 'ta_mom_20_0', 'ta_mom_30_0', 'ta_mom_60_0',
                'ta_obv_0', 'ta_rsi_14_0', 'ta_rsi_28_0', 'ta_sar_0', 'ta_sma_5_0', 'ta_sma_10_0', 'ta_sma_20_0',
                'ta_sma_30_0', 'ta_sma_60_0', 'ta_stoch_slowd_5_3_0_3_0_0', 'ta_stoch_slowk_5_3_0_3_0_0',
                'ta_trix_14_0', 'ta_trix_28_0', 'ta_willr_14_0', 'ta_willr_28_0', 'ta_wma_5_0', 'ta_wma_10_0',
                'ta_wma_20_0', 'ta_wma_30_0', 'ta_wma_60_0',

                # beta值因子
                'beta_csi100_10_0', 'beta_csi100_120_0','beta_csi100_180_0', 'beta_csi100_30_0',
                'beta_csi100_5_0', 'beta_csi100_60_0', 'beta_csi100_90_0',

                'beta_csi300_10_0', 'beta_csi300_120_0', 'beta_csi300_180_0', 'beta_csi300_30_0',
                'beta_csi300_5_0', 'beta_csi300_60_0', 'beta_csi300_90_0',

                'beta_csi500_10_0', 'beta_csi500_120_0', 'beta_csi500_180_0', 'beta_csi500_30_0',
                'beta_csi500_5_0', 'beta_csi500_60_0', 'beta_csi500_90_0',

                'beta_csi800_10_0', 'beta_csi800_120_0', 'beta_csi800_180_0', 'beta_csi800_30_0',
                'beta_csi800_5_0', 'beta_csi800_60_0', 'beta_csi800_90_0',

                'beta_gem_10_0', 'beta_gem_120_0', 'beta_gem_180_0', 'beta_gem_30_0',
                'beta_gem_5_0', 'beta_gem_60_0', 'beta_gem_90_0',

                'beta_industry_10_0', 'beta_industry_120_0', 'beta_industry_180_0', 'beta_industry_30_0',
                'beta_industry_5_0', 'beta_industry_60_0', 'beta_industry_90_0',

                #     'beta_industry1_10_0', 'beta_industry1_120_0', 'beta_industry1_180_0',
                #     'beta_industry1_30_0', 'beta_industry1_5_0', 'beta_industry1_60_0',
                #     'beta_industry1_90_0',

                'beta_sse180_10_0', 'beta_sse180_120_0', 'beta_sse180_180_0','beta_sse180_30_0',
                'beta_sse180_5_0', 'beta_sse180_60_0', 'beta_sse180_90_0',

                'beta_sse50_10_0', 'beta_sse50_120_0', 'beta_sse50_180_0', 'beta_sse50_30_0',
                'beta_sse50_5_0', 'beta_sse50_60_0', 'beta_sse50_90_0',

                'beta_szzs_120_0', 'beta_szzs_180_0', 'beta_szzs_30_0', 'beta_szzs_5_0',
                'beta_szzs_60_0', 'beta_szzs_90_0', 'beta_szzs_10_0',

                'rank_beta_csi100_10_0',
                'rank_beta_csi100_120_0', 'rank_beta_csi100_180_0', 'rank_beta_csi100_30_0',
                'rank_beta_csi100_5_0', 'rank_beta_csi100_60_0', 'rank_beta_csi100_90_0',

                'rank_beta_csi300_10_0', 'rank_beta_csi300_120_0',
                'rank_beta_csi300_180_0', 'rank_beta_csi300_30_0', 'rank_beta_csi300_5_0',
                'rank_beta_csi300_60_0', 'rank_beta_csi300_90_0',

                'rank_beta_csi500_10_0', 'rank_beta_csi500_120_0',
                'rank_beta_csi500_180_0',
                'rank_beta_csi500_30_0', 'rank_beta_csi500_5_0', 'rank_beta_csi500_60_0',
                'rank_beta_csi500_90_0',

                'rank_beta_csi800_10_0', 'rank_beta_csi800_120_0', 'rank_beta_csi800_180_0',
                'rank_beta_csi800_30_0',
                'rank_beta_csi800_5_0', 'rank_beta_csi800_60_0', 'rank_beta_csi800_90_0',
                'rank_beta_gem_10_0',
                'rank_beta_gem_120_0', 'rank_beta_gem_180_0', 'rank_beta_gem_30_0',
                'rank_beta_gem_5_0',
                'rank_beta_gem_60_0', 'rank_beta_gem_90_0',
                'rank_beta_industry_10_0',
                'rank_beta_industry_120_0',
                'rank_beta_industry_180_0', 'rank_beta_industry_30_0',
                'rank_beta_industry_5_0',
                'rank_beta_industry_60_0', 'rank_beta_industry_90_0',

                # 'rank_beta_industry1_10_0', 'rank_beta_industry1_120_0',
                # 'rank_beta_industry1_180_0', 'rank_beta_industry1_30_0',
                # 'rank_beta_industry1_60_0', 'rank_beta_industry1_90_0',
                # 'rank_beta_industry1_5_0',
                'rank_beta_sse180_10_0',
                'rank_beta_sse180_120_0', 'rank_beta_sse180_180_0', 'rank_beta_sse180_30_0',
                'rank_beta_sse180_5_0',
                'rank_beta_sse180_60_0', 'rank_beta_sse180_90_0', 'rank_beta_sse50_10_0',
                'rank_beta_sse50_120_0',
                'rank_beta_sse50_180_0', 'rank_beta_sse50_30_0', 'rank_beta_sse50_5_0',
                'rank_beta_sse50_60_0',
                'rank_beta_sse50_90_0',

                'rank_beta_szzs_10_0', 'rank_beta_szzs_120_0',
                'rank_beta_szzs_180_0',
                'rank_beta_szzs_30_0', 'rank_beta_szzs_5_0', 'rank_beta_szzs_60_0',
                'rank_beta_szzs_90_0',

                # 波动率因子
                'swing_volatility_10_0', 'swing_volatility_120_0', 'swing_volatility_240_0',
                'swing_volatility_30_0',
                'swing_volatility_5_0', 'swing_volatility_60_0',

                'volatility_10_0', 'volatility_120_0', 'volatility_240_0',
                'volatility_30_0',
                'volatility_5_0', 'volatility_60_0',

                'rank_swing_volatility_10_0', 'rank_swing_volatility_120_0',
                'rank_swing_volatility_240_0', 'rank_swing_volatility_30_0',
                'rank_swing_volatility_5_0',
                'rank_swing_volatility_60_0',

                'rank_volatility_10_0', 'rank_volatility_120_0', 'rank_volatility_240_0',
                'rank_volatility_30_0', 'rank_volatility_5_0', 'rank_volatility_60_0',

                # # 财务相关-盈利因子
                'fs_quarter_year_0', 'fs_quarter_index_0', 'fs_publish_date_0',
                
                 'roa2_0', 'roa_0', 'op_gr_0', 'pro_tog_0', 'gc_gr_0', 'ebit_gr_0', 'gro_pro_mar_0', 'net_pro_mar_0',
                 'cog_sal_0',

                # # 财务相关-成长因子
            
          
                'yoy_eps_bas_0', 'yoy_eps_dil_0', 'yoy_ocf_0', 'yoy_op_0', 'yoy_ebt_tot_0', 'yoy_net_pro_0', 'yoy_tr_0',
                'yoy_or_0', 'yoy_bps_0', 'yoy_ass_0', 'yoy_equ_sha_0',

                # # 财务相关-质量因子
                
              
                'acc_lia_pro_0', 'ass_equ_0', 'lia_tot_equ_0', 'ebit_int_0', 'cur_rat_0', 'qui_rat_0', 'cas_rat_0',
                'cat_ass_0', 'ocf_lia_cur_0', 'equ_deb_0', 'ocf_deb_0',

                # # 财务相关-营运因子
                
                'inv_rn_0', 'art_rn_0', 'art_day_0', 'fat_rn_0', 'cat_rn_0', 'ass_rn_0', 'fcff_0', 'wor_cap_0',
                'net_wor_cap_0',

                # # 财务相关-每股因子
              
            
                'eps_bas_0', 'eps_dlt_0', 'bps_0', 'ocfps_0', 'gr_ps_0', 'orps_0', 'sur_cap_0', 'sur_res_0', 'und_ps_0',
                'ret_ps_0', 'cfps_0', 'ebit_ps_0', 'fcff_ps_0', 'fcfe_ps_0',

            ],

        }
        # self.features_fields_map = {
        #     'features_CN_STOCK_A': [
        #         # 基本信息因子
        #         'company_found_date_0', 'in_csi100_0', 'in_csi300_0', 'in_csi500_0', 'in_csi800_0',
        #         'in_sse180_0', 'in_sse50_0', 'in_szse100_0', 'industry_sw_level1_0', 'industry_sw_level2_0',
        #         'industry_sw_level3_0', 'list_board_0', 'list_days_0', 'st_status_0',
        # ],
        # }

    def process_main_worker(self, args):
        df, instrument, feature_list = args
        if isinstance(df, pd.Series):
            df = pd.DataFrame([df])
            df.index.name = 'instrument'
        df = df.reset_index()
        context = FeatureContext(df, self.__base_returns)
        if df.empty:
            return None
        ret_df = pd.DataFrame(df[['date', 'instrument']])
        for f in feature_list:
            for feature_name, args in f.expand_fields(with_params=True):
                ret_df[feature_name] = f.eval_expr(df, args[0] if len(args) > 0 else 0, context)
        ret_df = truncate(ret_df, 'date', self.start_date, self.end_date)
        return ret_df

    def __get_base_returns(self, sw_base_names, end_date):
        _df = DataSource('bar1d_CN_STOCK_A_index').read(
            self.__base_instruments + sw_base_names,
            start_date=self.history_start_date,
            # start_date="2005-01-01",
            end_date=end_date, fields=['close'])
        gs = _df.groupby('instrument')
        ret = {}
        for k in gs.groups.keys():
            df = gs.get_group(k)
            df.sort_values('date', inplace=True)
            df.set_index('date', inplace=True)
            b_returns = df.close / df.close.shift(1) - 1
            ret[k] = b_returns
        return ret

    def start(self):  # noqa
        # from bigshared.common.utils import is_trading_day
        from common import is_trading_day,change_fields_type
        # today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        # if not is_trading_day():
        #     print("{} 不是交易日, 跳过数据构建".format(today_str))
        #     return
        if len(self.markets) > 1:
            self.log.error('行情数据不能同时构建多个市场!')
            raise RuntimeError('行情数据不能同时构建多个市场!')

        # 股票基本数据，在某段时间段上市的股票数据
        market = self.markets[0]
        categorys = self.fields
        self.log.info("build features {} {} {} {}".format(market, self.start_date, self.end_date, self.fields))
        field_groups = defaultdict(list)
        for f in self.__main_feature_list:
            if f.category not in categorys:
                continue
            if (market not in f.markets) and f.markets:
                self.log.info("{} {}".format(f.markets, f.base_field))
                continue
            field_groups[f.category].append(f)

        instruments = D.instruments(start_date=self.start_date, end_date=self.end_date, market=market)
        instruments.sort()

        # 根据日线数据获取起始日期
        df_bar1d = DataSource('bar1d_CN_STOCK_A').read(
            instruments=instruments,
            # start_date="2005-01-01",
            start_date=self.history_start_date,
            end_date=self.end_date)
        df_bar1d = df_bar1d[df_bar1d.amount > 0]
        data_list = []
        for i, j in df_bar1d.groupby('instrument'):
            df_tem = j.sort_values(['date'])
            df_tem = df_tem.tail(FeatureDefs.FEATURE_DAYS_LOOK_BACK + len(df_tem[df_tem.date >= self.start_date]))
            data_list.append(df_tem)
        df_bar1d = pd.concat(data_list, copy=False)
        del data_list

        # start_date = str(df_bar1d.date.min().date())
        df_result = pd.DataFrame()
        field_list = []
        temp_field = []
        for group_name, feature_list in field_groups.items():
            if group_name == '量价因子':
                temp_field = [(group_name, feature_list)]
            else:
                field_list.append((group_name, feature_list))
        if temp_field:
            field_list = temp_field + field_list
        for group_name, feature_list in field_list:
            feature_table_name = feature_list[0].table_name
            base_fields = {'date', 'instrument'}
            for feature in feature_list:
                if isinstance(feature.base_field, str):
                    base_fields.add(feature.base_field)
                else:
                    base_fields.update(set(feature.base_field))
            if group_name == 'BETA值':
                base_fields.add('industry_sw_level1')

            self.log.info('compute group name %s..' % group_name)
            tables = feature_list[0].tables.split(',')

            df = pd.DataFrame()
            # 根据数据来源组合数据
            if 'bar1d_index' in tables:
                df_tem = DataSource('bar1d_CN_STOCK_A_index').read(
                    # instruments=instruments, start_date="2005-01-01", end_date=self.end_date)
                    instruments=instruments, start_date=self.history_start_date, end_date=self.end_date)
                df_bar1d = pd.concat([df_bar1d, df_tem])
                df_bar1d.drop_duplicates(keep='last', inplace=True)
                tables.remove('bar1d_index')

            if 'bar1d' in tables:
                tables.remove('bar1d')
            if not tables:
                df = df_bar1d
            base_fields = [i.strip() for i in base_fields]
            for table in tables:
                print(table)
                if table == 'financial_statement':
                    df_tem = DataSource('financial_statement_ff_CN_STOCK_A').read(
                        instruments=instruments, start_date=self.history_start_date, end_date=self.end_date)
                    df_tem.columns = [x.replace('_0', '') for x in df_tem.columns]
                else:
                    table_name = table + '_' + market
                    df_tem = DataSource(table_name).read(instruments=instruments, start_date=self.history_start_date,
                                                         end_date=self.end_date)

                col = set(base_fields) & set(df_tem.columns)
                df_tem = df_tem[list(col)]
                if df.empty:
                    if 'basic_info' == table:
                        df = pd.merge(df_bar1d, df_tem, on=['instrument'], how='left', copy=False)
                    else:
                        df = pd.merge(df_bar1d, df_tem, on=['instrument', 'date'], how='left', copy=False)
                else:
                    if 'basic_info' == table:
                        df = pd.merge(df, df_tem, on=['instrument'], how='left', copy=False)
                    else:
                        df = pd.merge(df, df_tem, on=['instrument', 'date'], how='left', copy=False)

            self.log.info("{} {} {}".format(base_fields, df.columns, group_name))
            df = df[list(base_fields)]
            df.sort_values(['instrument', 'date'], inplace=True)
            df.set_index('instrument', inplace=True)
            if group_name == 'BETA值':
                sw_base_names = ['SW{}.SHA'.format(int(x)) for x in df.industry_sw_level1.unique() if not pd.isnull(x)]
                self.__base_returns = self.__get_base_returns(sw_base_names, self.end_date)
            items = [(df.loc[instrument], instrument, feature_list) for instrument in instruments if instrument in df.index]  # noqa

            df_list = parallel.map(self.process_main_worker, items, processes_count=12, show_progress=False, chunksize=40)
            df = pd.concat([x for x in df_list if x is not None], ignore_index=True, copy=False)
            del df_list
            df = truncate(df, 'date', self.start_date, self.end_date)
            if df.empty:
                self.log.warning("{} is empty".format(table))
                continue
            for feature in feature_list:
                if feature.type.startswith('int'):
                    for feature_name, args in feature.expand_fields(with_params=True):
                        df[feature_name].fillna(0, inplace=True)
            self.log.info("{} {}".format(feature_table_name, df.shape))
            if df_result.empty:
                df_result = df.copy()
                if group_name == '基本信息':
                    # 设置nan为0
                    for col in ['in_sse50_0', 'in_csi300_0', 'in_csi500_0', 'in_csi800_0',
                                'in_sse180_0', 'in_csi100_0', 'in_szse100_0']:
                        df_result[col] = df_result[col].fillna(value=0)
                    pass
            else:
                if group_name == '基本信息':
                    # 设置nan为0
                    for col in ['in_sse50_0', 'in_csi300_0', 'in_csi500_0', 'in_csi800_0',
                                'in_sse180_0', 'in_csi100_0', 'in_szse100_0']:
                        df[col] = df[col].fillna(value=0)
                    pass
                df_result = pd.merge(df_result, df, how='left', on=['date', 'instrument'], copy=False)
        # 将df_result传递给BuildRank,计算rank相关的数据
        obj = BuildRank(source_df=df_result)
        rank_df = obj.start()

        df_result = pd.merge(df_result, rank_df, on=['date', 'instrument'], how='left')
        # print(df_result[['instrument', 'date', 'list_board_0', 'stock_status_0', 'list_days_0']])
        for table_name in self.features_fields_map.keys():
            fields = [i.strip().replace('\n', '') for i in self.features_fields_map[table_name]]
            _df = df_result[fields]
            tmp_fields = {}
            for col in fields:
                tmp_fields[col] = schema['fields'][col]
            schema['fields'] = tmp_fields
            print(_df.shape)
            print(_df.date.min(), _df.date.max())
            _df = change_fields_type(df=_df, schema=schema)
            print("finish change_fields_type......")
            UpdateDataSource().update(df=_df, alias=table_name,schema=schema, update_schema=True)


class BuildRank(object):
    args = default_args_parser()
    start_date = args.start_date
    end_date = args.end_date
    markets = args.markets or ['CN_STOCK_A']

    # 原始数据表
    # original_data_tables = ['features_CN_STOCK_A_G300', 'features_CN_STOCK_A_G301', 'features_CN_STOCK_A_G302']

    def __init__(self, source_df):
        self.log = BigLogger("features_rank_CN_STOCK_A")
        self.source_df = source_df
        self.__main_feature_list = [f for f in FeatureDefs.FEATURE_LIST if not f.field.startswith('rank_')]
        self.__rank_feature_list = [f for f in FeatureDefs.FEATURE_LIST if f.field.startswith('rank_')]

    def start(self):
        import datetime
        from common import is_trading_day,change_fields_type
        # today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        # if not is_trading_day():
        #     print("{} 不是交易日, 跳过数据构建".format(today_str))
        #     return
        if len(self.markets) > 1:
            self.log.error('行情数据不能同时构建多个市场!')
            raise RuntimeError('行情数据不能同时构建多个市场!')

        # 股票基本数据，在某段时间段上市的股票数据
        market = self.markets[0]

        self.log.info("开始计算rank因子 {} {} {}".format(market, self.start_date, self.end_date))
        # instruments = D.instruments(start_date=self.start_date, end_date=self.end_date, market=market)

        field_groups = {}
        for f in self.__rank_feature_list:
            field_groups.setdefault(f.category, [])
            field_groups[f.category].append(f)

        df = pd.DataFrame()
        for group_id, feature_list in field_groups.items():
            self.log.info('compute group id %s..' % group_id)
            base_fields = set()
            for f in feature_list:
                for feature_name, other in f.expand_fields(with_params=True):
                    base_fields.add(feature_name.replace('rank_', ''))
            # 组合数据来源
            # all_df = pd.DataFrame()
            # for table in self.original_data_tables:
            #     df_tem = DataSource(table).read(
            #         instruments, start_date=self.start_date, end_date=self.end_date)
            #     if all_df.empty:
            #         all_df = df_tem.copy()
            #     else:
            #         all_df = pd.merge(all_df, df_tem, on=['instrument', 'date'], how='outer')
            all_df = self.source_df.copy()
            print(all_df.columns.tolist())
            grouped = all_df.groupby('date')
            ret_df = pd.DataFrame(all_df[['date', 'instrument']])
            for f in feature_list:
                for feature_name, other in f.expand_fields(with_params=True):
                    if f.base_field not in list(all_df.columns) \
                            and feature_name.replace('rank_', '') not in list(all_df.columns):
                        self.log.warning("{} not in df columns, {}".format(feature_name, f.base_field))
                        continue
                    self.log.info('计算因子 {}'.format(feature_name))
                    ret_df[feature_name] = grouped[feature_name.replace('rank_', '')].rank(pct=True)
            ret_df = truncate(ret_df, 'date', self.start_date, self.end_date)
            if df.empty:
                df = ret_df.copy()
            else:
                df = pd.merge(df, ret_df, on=['instrument', 'date'], how='outer')
        self.log.info('计算rank因子结束...')
        return df


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore')
    Build().start()
