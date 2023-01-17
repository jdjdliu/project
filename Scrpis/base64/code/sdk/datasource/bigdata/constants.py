from enum import Enum

STORAGE_KEY = "data"

DATA_START_YEAR = 2005

MARKET_CHINA = "china"

history_daily_data_path = "bigquant/historydata-daily/v3"
fundamental_data_path = "bigquant/fundamental/v1"

trading_days_data_dir = "bigquant/bigdata/v1/bigdb/main/tradingdays"
instruments_data_dir = "bigquant/bigdata/v1/bigdb/main/instruments"
bar1d_data_dir = "bigquant/bigdata/v1/bigdb/main/bar1d"
bar1m_data_dir = "bigquant/bigdata/v1/bigdb/main/bar1m"
merged_data_dir = "bigquant/bigdata/v1/bigdb/merged"

backtest_data_dir = "bigquant/bigdata/v1/bigdb/merged"
backtest_data_dir_real = "bigquant/backtest/real"
backtest_data_dir_pre_right = "bigquant/backtest/pre_right"

sina_trade_data_dir = "bigquant/bigdata/v1/webdata/tick/sina/STOCK_A"
tick_hdf_data_dir = "bigquant/bigdata/v1/webdata/tick/hdf/STOCK_A"
tick_minute_data_dir = "bigquant/bigdata/v1/webdata/tick/minute/STOCK_A"
index_sogou_data_dir = "bigquant/bigdata/v1/webdata/index/sougou"
index_hdf_data_dir = "bigquant/bigdata/v1/webdata/index/hdf"
eastmoney_moneyflow_data_dir = "bigquant/bigdata/v1/webdata/eastmoney/money_flow"
eastmoney_hsgt_data_dir = "bigquant/bigdata/v1/webdata/eastmoney/hsgt"
eastmoney_dividends_data_dir = "bigquant/bigdata/v1/webdata/eastmoney/dividends"
eastmoney_margin_trading_data_dir = "bigquant/bigdata/v1/webdata/eastmoney/margin_trading"

jrj_hsgt_data_dir = "bigquant/bigdata/v1/webdata/jrj/hsgt"
index_baidu_data_dir = "bigquant/bigdata/v1/index/baidu/data"
index_baidu_password_dir = "bigquant/bigdata/v1/index/baidu/password"
index_baidu_hdf_data_dir = "bigquant/bigdata/v1/index/baidu/hdf"
sse_volatility_data_dir = "bigquant/bigdata/v1/webdata/sse/volatility"
sse_margin_trading_data_dir = "bigquant/bigdata/v1/webdata/sse/margin_trading"
szse_margin_trading_data_dir = "bigquant/bigdata/v1/webdata/szse/margin_trading"
baidugupiao_index_data_dir = "bigquant/bigdata/v1/webdata/baidugupiao/index"
sina_index_data_dir = "bigquant/bigdata/v1/webdata/sina/index"
sina_top10_circulate_stockholder_data_dir = "bigquant/bigdata/v1/webdata/sina/top10_circulate_stockholder"
netease_index_data_dir = "bigquant/bigdata/v1/webdata/netease/index"
netease_top10_circulate_stockholder_data_dir = "bigquant/bigdata/v1/webdata/netease/top10_circulate_stockholder"
shibor_shibor_data_dir = "bigquant/bigdata/v1/webdata/shibor/shibor"

rq_bar1m_data_dir = "bigquant/bigdata/v1/webdata/rq/bar1m"

public_announcement_dir = "bigquant/bigdata/v1/webdata/public_announcement"
public_newsdata_dir = "bigquant/bigdata/v1/webdata/public_newsdata"
public_discussion_dir = "bigquant/bigdata/v1/webdata/public_discussion"
public_report_dir = "bigquant/bigdata/v1/webdata/public_report"

data_fetcher_wind_instruments_dir = "bigquant/bigdata/v1/winddata/instruments"
data_fetcher_wind_basic_info_dir = "bigquant/bigdata/v1/winddata/basicinfo"
data_fetcher_wind_bar1d_dir = "bigquant/bigdata/v1/winddata/bar1d"
data_fetcher_wind_bar1m_dir = "bigquant/bigdata/v1/winddata/bar1m"
data_fetcher_wind_top_dir = "bigquant/bigdata/v1/winddata/top"
data_fetcher_wind_index_constituent_dir = "bigquant/bigdata/v1/winddata/indexconstituent"
data_fetcher_wind_edb_dir = "bigquant/bigdata/v1/winddata/edb"
data_fetcher_wind_financial_statements_dir = "bigquant/bigdata/v1/winddata/financialstatements"
data_fetcher_wind_dividends_dir = "bigquant/bigdata/v1/winddata/dividends"

# options
data_fetcher_wind_options_bar1d_dir = "bigquant/bigdata/v1/winddata/options/bar1d"
data_fetcher_wind_options_bar1m_dir = "bigquant/bigdata/v1/winddata/options/bar1m"
data_fetcher_wind_options_instruments_dir = "bigquant/bigdata/v1/winddata/options/instruments"

data_fetcher_tdx_tick_dir = "bigquant/bigdata/v1/tdxdata/tick"
data_fetcher_tdx_bar1m_dir = "bigquant/bigdata/v1/tdxdata/bar1m"

backtest_data_dir_post_right_v2 = "bigquant/bigdata/v2/backtest/post_right"
backtest_data_dir_real_v2 = "bigquant/bigdata/v2/backtest/real"

# blockchain and cryptocurrency
# binance
binance_bar1m_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/binance/bar1m"
binance_trades_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/binance/trades"
binance_order_book_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/binance/order_book"

# bitfinex
bitfinex_bar1m_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/bitfinex/bar1m"

# huobi
huobi_bar1m_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/huobi/bar1m"

# okex
okex_bar1m_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/okex/bar1m"
okex_bar1d_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/okex/bar1d"

# zb
zb_bar1m_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/zb/bar1m"
zb_bar1d_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/zb/bar1d"

# topbtc
topbtc_order_book_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/topbtc/order_book"
# coinegg
coinegg_order_book_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/coinegg/order_book"
# allcoin
allcoin_order_book_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/allcoin/order_book"
# bitz
bitz_order_book_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/bitz/order_book"
# coinbene
coinbene_order_book_data_dir = "bigquant/bigdata/v1/webdata/crypto_currency/coinbene/order_book"

# ethereum blockchain
blockchain_block_meta_data_dir = "bigquant/bigdata/v1/crypto_currency/ethereum/blockchain/block_meta"
# topc tx
topc_tx_data_dir = "bigquant/bigdata/v1/crypto_currency/ethereum/token/topc/tx"
# sgcc tx
sgcc_tx_data_dir = "bigquant/bigdata/v1/crypto_currency/ethereum/token/sgcc/tx"

# 新闻 h5 数据地址
# 目前有同花顺、雪球的主页新闻和雪球的股票新闻
# 股票新闻目前有上证和深证
news_data_dir = "bigquant/bigdata/v2/news"

# 龙虎榜存放地址
dragon_data_dir = "bigquant/bigdata/v2/dragon"

# data serving
backtest_fields = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjust_factor",
    "price_limit_status",
]
backtest_source_group = "G100"
FUTURE_INDEX_ID = "000"

frequency_daily = "daily"
frequency_minute = "minute"
frequency_hour = "hour"

price_type_post_right = "post_right"
price_type_pre_right = "pre_right"
price_type_real = "real"

price_type_backward_adjusted = "backward_adjusted"
price_type_forward_adjusted = "forward_adjusted"
price_type_original = "original"

CN_STOCK_A_dic = {
    "fs_operating_revenue_qoq_0": "financial_statement_ff_CN_STOCK_A",
    "pe_lyr": "market_value_CN_STOCK_A",
    "chibor_3M": "macrodata",
    "fs_net_profit_ttm_0": "financial_statement_ff_CN_STOCK_A",
    "chibor_1Y": "macrodata",
    "ppi": "macrodata",
    "company_province": "basic_info_CN_STOCK_A",
    "fs_construction_in_process_0": "financial_statement_ff_CN_STOCK_A",
    "fs_net_income": "financial_statement_CN_STOCK_A",
    "fs_cash_equivalents_0": "financial_statement_ff_CN_STOCK_A",
    "fs_financial_expenses": "financial_statement_CN_STOCK_A",
    "fs_quarter_year_0": "financial_statement_ff_CN_STOCK_A",
    "volume": "bar1d_CN_STOCK_A",
    "fs_capital_reserves": "financial_statement_CN_STOCK_A",
    "fs_surplus_reserves": "financial_statement_CN_STOCK_A",
    "fs_deducted_profit_0": "financial_statement_ff_CN_STOCK_A",
    "mf_net_amount": "net_amount_CN_STOCK_A",
    "fs_current_liabilities_0": "financial_statement_ff_CN_STOCK_A",
    "fs_total_equity_0": "financial_statement_ff_CN_STOCK_A",
    "close": "bar1d_CN_STOCK_A",
    "pmi": "macrodata",
    "chibor_2w": "macrodata",
    "mf_net_pct_l": "net_amount_CN_STOCK_A",
    "concept": "industry_CN_STOCK_A",
    "fs_eps_yoy_0": "financial_statement_ff_CN_STOCK_A",
    "chibor_3w": "macrodata",
    "fs_roe": "financial_statement_CN_STOCK_A",
    "fs_deducted_profit": "financial_statement_CN_STOCK_A",
    "fs_common_equity": "financial_statement_CN_STOCK_A",
    "fs_roa_ttm": "financial_statement_CN_STOCK_A",
    "price_limit_status": "stock_status_CN_STOCK_A",
    "in_csi500": "index_constituent_CN_STOCK_A",
    "fs_bps_0": "financial_statement_ff_CN_STOCK_A",
    "fs_current_assets": "financial_statement_CN_STOCK_A",
    "fs_eps": "financial_statement_CN_STOCK_A",
    "fs_cash_ratio_0": "financial_statement_ff_CN_STOCK_A",
    "mf_net_pct_main": "net_amount_CN_STOCK_A",
    "fs_roe_ttm": "financial_statement_CN_STOCK_A",
    "fs_net_profit_qoq": "financial_statement_CN_STOCK_A",
    "list_date": "basic_info_CN_STOCK_A",
    "foreign_exchange_reserve": "macrodata",
    "amount": "bar1d_CN_STOCK_A",
    "fs_total_profit_0": "financial_statement_ff_CN_STOCK_A",
    "fs_proj_matl_0": "financial_statement_ff_CN_STOCK_A",
    "fs_income_tax": "financial_statement_CN_STOCK_A",
    "fs_quarter_index": "financial_statement_CN_STOCK_A",
    "mf_net_pct_s": "net_amount_CN_STOCK_A",
    "suspended": "stock_status_CN_STOCK_A",
    "ps_ttm": "market_value_CN_STOCK_A",
    "fs_net_cash_flow_ttm": "financial_statement_CN_STOCK_A",
    "pe_ttm": "market_value_CN_STOCK_A",
    "fs_quarter_index_0": "financial_statement_ff_CN_STOCK_A",
    "fs_total_operating_costs": "financial_statement_CN_STOCK_A",
    "fs_account_payable": "financial_statement_CN_STOCK_A",
    "industry_sw_level3": "industry_CN_STOCK_A",
    "fs_net_profit_ttm": "financial_statement_CN_STOCK_A",
    "fs_account_payable_0": "financial_statement_ff_CN_STOCK_A",
    "fs_operating_revenue": "financial_statement_CN_STOCK_A",
    "fs_roa_0": "financial_statement_ff_CN_STOCK_A",
    "fs_fixed_assets": "financial_statement_CN_STOCK_A",
    "fs_net_profit_margin_ttm_0": "financial_statement_ff_CN_STOCK_A",
    "fs_operating_revenue_0": "financial_statement_ff_CN_STOCK_A",
    "fs_publish_date": "financial_statement_CN_STOCK_A",
    "mf_net_amount_l": "net_amount_CN_STOCK_A",
    "fs_capital_reserves_0": "financial_statement_ff_CN_STOCK_A",
    "fs_current_assets_0": "financial_statement_ff_CN_STOCK_A",
    "in_csi100": "index_constituent_CN_STOCK_A",
    "company_name": "basic_info_CN_STOCK_A",
    "fs_publish_date_0": "financial_statement_ff_CN_STOCK_A",
    "west_eps_ftm": "west_CN_STOCK_A",
    "chibor_1M": "macrodata",
    "fs_construction_in_process": "financial_statement_CN_STOCK_A",
    "fs_net_cash_flow_ttm_0": "financial_statement_ff_CN_STOCK_A",
    "fs_net_profit_yoy_0": "financial_statement_ff_CN_STOCK_A",
    "industry_sw_level2": "industry_CN_STOCK_A",
    "cpi": "macrodata",
    "fs_operating_profit": "financial_statement_CN_STOCK_A",
    "fs_non_current_assets_0": "financial_statement_ff_CN_STOCK_A",
    "chibor_1w": "macrodata",
    "fs_free_cash_flow": "financial_statement_CN_STOCK_A",
    "fs_gross_profit_margin_ttm_0": "financial_statement_ff_CN_STOCK_A",
    "fs_net_profit_margin_0": "financial_statement_ff_CN_STOCK_A",
    "fs_proj_matl": "financial_statement_CN_STOCK_A",
    "fs_general_expenses": "financial_statement_CN_STOCK_A",
    "end_investor_num": "macrodata",
    "adjust_factor": "bar1d_CN_STOCK_A",
    "fs_net_cash_flow": "financial_statement_CN_STOCK_A",
    "sh_holder_avg_pct_3m_chng_0": "financial_statement_ff_CN_STOCK_A",
    "fs_gross_revenues_0": "financial_statement_ff_CN_STOCK_A",
    "fs_net_profit_0": "financial_statement_ff_CN_STOCK_A",
    "market_cap_float": "market_value_CN_STOCK_A",
    "in_szse100": "index_constituent_CN_STOCK_A",
    "sh_holder_num_0": "financial_statement_ff_CN_STOCK_A",
    "mf_net_amount_m": "net_amount_CN_STOCK_A",
    "sh_holder_avg_pct": "financial_statement_CN_STOCK_A",
    "deal_number": "bar1d_CN_STOCK_A",
    "low": "bar1d_CN_STOCK_A",
    "fs_net_profit_margin_ttm": "financial_statement_CN_STOCK_A",
    "mf_net_pct_xl": "net_amount_CN_STOCK_A",
    "fs_account_receivable_0": "financial_statement_ff_CN_STOCK_A",
    "m2": "macrodata",
    "company_found_date": "basic_info_CN_STOCK_A",
    "fs_paicl_up_capital": "financial_statement_CN_STOCK_A",
    "fs_total_profit": "financial_statement_CN_STOCK_A",
    "m0": "macrodata",
    "in_csi300": "index_constituent_CN_STOCK_A",
    "fs_roe_ttm_0": "financial_statement_ff_CN_STOCK_A",
    "gdp": "macrodata",
    "in_sse180": "index_constituent_CN_STOCK_A",
    "turn": "bar1d_CN_STOCK_A",
    "fs_roa": "financial_statement_CN_STOCK_A",
    "in_sse50": "index_constituent_CN_STOCK_A",
    "suspend_type": "stock_status_CN_STOCK_A",
    "fs_operating_revenue_yoy": "financial_statement_CN_STOCK_A",
    "fs_fixed_assets_disp_0": "financial_statement_ff_CN_STOCK_A",
    "fs_net_profit_qoq_0": "financial_statement_ff_CN_STOCK_A",
    "fs_undistributed_profit_0": "financial_statement_ff_CN_STOCK_A",
    "fs_gross_revenues": "financial_statement_CN_STOCK_A",
    "fs_operating_revenue_qoq": "financial_statement_CN_STOCK_A",
    "mf_net_pct_m": "net_amount_CN_STOCK_A",
    "fs_gross_profit_margin_0": "financial_statement_ff_CN_STOCK_A",
    "mf_net_amount_xl": "net_amount_CN_STOCK_A",
    "fs_operating_revenue_ttm_0": "financial_statement_ff_CN_STOCK_A",
    "suspend_reason": "stock_status_CN_STOCK_A",
    "fs_deducted_profit_ttm_0": "financial_statement_ff_CN_STOCK_A",
    "fs_selling_expenses": "financial_statement_CN_STOCK_A",
    "fs_roe_0": "financial_statement_ff_CN_STOCK_A",
    "fs_eqy_belongto_parcomsh": "financial_statement_CN_STOCK_A",
    "fs_quarter_year": "financial_statement_CN_STOCK_A",
    "fs_selling_expenses_0": "financial_statement_ff_CN_STOCK_A",
    "chibor_on": "macrodata",
    "bdi": "macrodata",
    "fs_undistributed_profit": "financial_statement_CN_STOCK_A",
    "fs_free_cash_flow_0": "financial_statement_ff_CN_STOCK_A",
    "gold_reserve": "macrodata",
    "west_avgcps_ftm": "west_CN_STOCK_A",
    "fs_current_liabilities": "financial_statement_CN_STOCK_A",
    "chibor_9M": "macrodata",
    "west_netprofit_ftm": "west_CN_STOCK_A",
    "fs_eps_yoy": "financial_statement_CN_STOCK_A",
    "fs_net_profit_margin": "financial_statement_CN_STOCK_A",
    "fs_gross_profit_margin_ttm": "financial_statement_CN_STOCK_A",
    "fs_total_operating_costs_0": "financial_statement_ff_CN_STOCK_A",
    "fs_net_income_0": "financial_statement_ff_CN_STOCK_A",
    "sh_holder_num": "financial_statement_CN_STOCK_A",
    "fs_eps_0": "financial_statement_ff_CN_STOCK_A",
    "sh_holder_avg_pct_0": "financial_statement_ff_CN_STOCK_A",
    "fs_bps": "financial_statement_CN_STOCK_A",
    "fs_net_profit": "financial_statement_CN_STOCK_A",
    "fs_financial_expenses_0": "financial_statement_ff_CN_STOCK_A",
    "fs_operating_profit_0": "financial_statement_ff_CN_STOCK_A",
    "m1": "macrodata",
    "fs_income_tax_0": "financial_statement_ff_CN_STOCK_A",
    "fs_fixed_assets_0": "financial_statement_ff_CN_STOCK_A",
    "fs_surplus_reserves_0": "financial_statement_ff_CN_STOCK_A",
    "fs_cash_ratio": "financial_statement_CN_STOCK_A",
    "fs_non_current_liabilities_0": "financial_statement_ff_CN_STOCK_A",
    "chibor_6M": "macrodata",
    "fs_net_profit_yoy": "financial_statement_CN_STOCK_A",
    "chibor_2M": "macrodata",
    "open": "bar1d_CN_STOCK_A",
    "new_investors_num": "macrodata",
    "mf_net_amount_main": "net_amount_CN_STOCK_A",
    "fs_quarter_0": "financial_statement_ff_CN_STOCK_A",
    "fs_cash_equivalents": "financial_statement_CN_STOCK_A",
    "fs_fixed_assets_disp": "financial_statement_CN_STOCK_A",
    "fs_common_equity_0": "financial_statement_ff_CN_STOCK_A",
    "fs_non_current_assets": "financial_statement_CN_STOCK_A",
    "fs_quarter": "financial_statement_CN_STOCK_A",
    "fs_general_expenses_0": "financial_statement_ff_CN_STOCK_A",
    "in_csi800": "index_constituent_CN_STOCK_A",
    "fs_operating_revenue_yoy_0": "financial_statement_ff_CN_STOCK_A",
    "fs_deducted_profit_ttm": "financial_statement_CN_STOCK_A",
    "fs_net_cash_flow_0": "financial_statement_ff_CN_STOCK_A",
    "fs_account_receivable": "financial_statement_CN_STOCK_A",
    "industry_sw_level1": "industry_CN_STOCK_A",
    "fs_non_current_liabilities": "financial_statement_CN_STOCK_A",
    "list_board": "basic_info_CN_STOCK_A",
    "fs_eqy_belongto_parcomsh_0": "financial_statement_ff_CN_STOCK_A",
    "company_type": "basic_info_CN_STOCK_A",
    "market_cap": "market_value_CN_STOCK_A",
    "fs_paicl_up_capital_0": "financial_statement_ff_CN_STOCK_A",
    "high": "bar1d_CN_STOCK_A",
    "fs_total_equity": "financial_statement_CN_STOCK_A",
    "sh_holder_avg_pct_6m_chng_0": "financial_statement_ff_CN_STOCK_A",
    "fs_gross_profit_margin": "financial_statement_CN_STOCK_A",
    "chibor_4M": "macrodata",
    "delist_date": "basic_info_CN_STOCK_A",
    "fs_total_liability_0": "financial_statement_ff_CN_STOCK_A",
    "fs_roa_ttm_0": "financial_statement_ff_CN_STOCK_A",
    "fs_total_liability": "financial_statement_CN_STOCK_A",
    "fs_operating_revenue_ttm": "financial_statement_CN_STOCK_A",
    "sh_holder_avg_pct_3m_chng": "financial_statement_CN_STOCK_A",
    "st_status": "stock_status_CN_STOCK_A",
    "sh_holder_avg_pct_6m_chng": "financial_statement_CN_STOCK_A",
    "pb_lf": "market_value_CN_STOCK_A",
    "mf_net_amount_s": "net_amount_CN_STOCK_A",
}
CN_FUTURE_dic = {
    "open_intl": "bar1d_CN_FUTURE",
    "close": "bar1d_CN_FUTURE",
    "list_date": "basic_info_CN_FUTURE",
    "dominant": "dominant_CN_FUTURE",
    "high": "bar1d_CN_FUTURE",
    "amount": "bar1d_CN_FUTURE",
    "volume": "bar1d_CN_FUTURE",
    "delist_date": "basic_info_CN_FUTURE",
    "low": "bar1d_CN_FUTURE",
    "symbol": "basic_info_CN_FUTURE",
    "settle": "bar1d_CN_FUTURE",
    "open": "bar1d_CN_FUTURE",
}
CN_FUND_dic = {
    "amount": "bar1d_CN_FUND",
    "close": "bar1d_CN_FUND",
    "unit_net_value": "extras_CN_FUND",
    "avg": "bar1d_CN_FUND",
    "list_date": "basic_info_CN_FUND",
    "paused": "bar1d_CN_FUND",
    "adjust_factor": "bar1d_CN_FUND",
    "display_name": "basic_info_CN_FUND",
    "volume": "bar1d_CN_FUND",
    "open": "bar1d_CN_FUND",
    "delist_date": "basic_info_CN_FUND",
    "pre_close": "bar1d_CN_FUND",
    "acc_net_value": "extras_CN_FUND",
    "name": "basic_info_CN_FUND",
    "type": "basic_info_CN_FUND",
    "high": "bar1d_CN_FUND",
    "low_limit": "bar1d_CN_FUND",
    "low": "bar1d_CN_FUND",
    "high_limit": "bar1d_CN_FUND",
}
features_dic_path = "/var/app/data/bigquant/datasource/bigquant/features_map.json"
CN_STOCK_A_dic_path = "/var/app/data/bigquant/datasource/bigquant/CN_STOCK_A.json"
CN_FUND_dic_path = "/var/app/data/bigquant/datasource/bigquant/CN_FUND.json"
CN_FUTURE_dic_path = "/var/app/data/bigquant/datasource/bigquant/CN_FUTURE.json"


class EnumFrequency(Enum):
    Minute = 1
    Minute_3 = 2
    Minute_5 = 3
    Minute_10 = 4
    Minute_15 = 5
    Minute_30 = 6
    Hour = 7
    Hour_2 = 8
    Day = 9
    Week = 10
    Month = 11
    Quarter = 12
    Year = 13
