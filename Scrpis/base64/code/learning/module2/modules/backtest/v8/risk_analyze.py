#!/usr/bin/env python
# encoding: utf-8
import learning.api.tools as T
import numpy as np
import pandas as pd
from sdk.datasource import DataReaderV2, DataSource, IndustryDefs

D2 = DataReaderV2()
factor_name_maps = {
    "fin_quality": "财务质量",
    "beta": "市场因子",
    "size": "规模因子",
    "growth": "成长因子",
    "liquidity": "流动性因子",
    "holder": "股东因子",
    "leverage": "杠杆因子",
    "momentum": "动量因子",
    "volatility": "波动率因子",
    "value": "价值因子",
}


def load_factor_data(start_date, end_date):
    st = str(start_date)[0:4] + "-01-01"
    et = str(end_date)[0:4] + "-12-31"
    df = DataSource("factor_data_CN_STOCK_A").read(start_date=st, end_date=et)
    df.set_index(["date", "instrument"], inplace=True)
    return df


def factor_analyze(perf_df, return_values=False):
    start_date = perf_df.index.min().date()
    end_date = perf_df.index.max().date()
    factor_data = load_factor_data(start_date, end_date)

    def compute_factor(series):
        cur_factor_data = factor_data.loc[series.name.date()]
        values = np.zeros(cur_factor_data.shape[1])
        weight_sum = 0
        for idx, item in enumerate(series.positions):
            instrument = item["sid"].symbol
            if instrument not in cur_factor_data.index:
                continue
            weight = item["last_sale_price"] * item["amount"]
            values += cur_factor_data.loc[instrument].values * weight
            weight_sum += weight
        values /= weight_sum
        return pd.Series(values)

    factor_values = perf_df.apply(compute_factor, axis=1)
    factor_values.columns = factor_data.columns
    absolute_factor_values = factor_values[[x for x in factor_values.columns if not x.startswith("Rank")]]
    absolute_factor_values.rename_axis(factor_name_maps, axis=1, inplace=True)

    rank_factor_values = factor_values[[x for x in factor_values.columns if x.startswith("Rank")]]
    rank_factor_values.rename_axis({"Rank_" + x: y for x, y in factor_name_maps.items()}, axis=1, inplace=True)

    T.plot(rank_factor_values.mean(), chart_type="column", title="区间风险暴露（排名）", options={"legend": {"enabled": False}})

    T.plot(absolute_factor_values.mean(), chart_type="column", title="区间风险暴露（绝对值）", options={"legend": {"enabled": False}})

    T.plot(rank_factor_values.resample("3m").mean(), chart_type="line", title="风险暴露趋势（排名）")

    T.plot(absolute_factor_values.resample("3m").mean(), chart_type="line", title="风险暴露趋势（绝对值）")
    if return_values:
        return rank_factor_values, absolute_factor_values


def industry_analyze(perf_df, return_values=False):
    start_date = perf_df.index.min().date()
    end_date = perf_df.index.max().date()
    instruments = D2.instruments(start_date=start_date, end_date=end_date)
    industry_data = D2.history_data(instruments, start_date=start_date, end_date=end_date, fields=["industry_sw_level1"])
    industry_data.set_index(["date", "instrument"], inplace=True)
    industry_list = sorted(set(industry_data.industry_sw_level1))
    if 0 in industry_list:
        industry_list.remove(0)
    industry_id_map = {x: idx for idx, x in enumerate(industry_list)}

    def compute_industry_factor(series):
        cur_factor_data = industry_data.loc[series.name.date()]
        values = np.zeros(len(industry_list))
        weight_sum = 0
        for idx, item in enumerate(series.positions):
            instrument = item["sid"].symbol
            if instrument not in cur_factor_data.index:
                continue
            weight = item["last_sale_price"] * item["amount"]
            industry_id = cur_factor_data.loc[instrument].industry_sw_level1
            if industry_id == 0:
                continue
            values[industry_id_map[industry_id]] += weight
            weight_sum += weight
        values /= weight_sum
        return pd.Series(values)

    industry_factor = perf_df.apply(compute_industry_factor, axis=1)
    industry_factor.columns = industry_list
    industry_factor.rename_axis(IndustryDefs.SW_CODE_TO_NAME, axis=1, inplace=True)

    T.plot(industry_factor.mean(), chart_type="column", title="区间行业风险暴露", options={"legend": {"enabled": False}})

    T.plot(
        industry_factor.resample("3m").mean(),
        chart_type="column",
        title="行业风险暴露趋势（百分比）",
        options={"plotOptions": {"column": {"stacking": "percent"}}},
    )
    if return_values:
        return industry_factor
