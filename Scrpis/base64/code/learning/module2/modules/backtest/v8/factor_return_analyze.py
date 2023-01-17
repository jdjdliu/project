#!/usr/bin/env python
# encoding: utf-8
import datetime

import learning.api.tools as T
import numpy as np
import pandas as pd
from sdk.datasource import DataReaderV2, DataSource

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
    return df


def factor_return_analyze(perf_df):
    start_date = perf_df.index.min().date() - datetime.timedelta(60)
    end_date = perf_df.index.max().date()
    factor_data = load_factor_data(start_date, end_date)
    factor_data = factor_data[[x for x in factor_data.columns if not x.startswith("Rank")]]

    instruments = D2.instruments(start_date=start_date, end_date=end_date)
    df = D2.history_data(instruments, start_date=start_date, end_date=end_date, fields=["close", "amount", "industry_sw_level1"])
    df = df[(df.amount > 0) & (df.industry_sw_level1 != 0)]

    df = df.merge(factor_data, on=["date", "instrument"], how="inner")
    df = df.dropna()

    def resample(df):
        month_df = df.resample("M", label="right").last()
        factor_cols = ["beta", "fin_quality", "growth", "holder", "leverage", "liquidity", "momentum", "size", "value", "volatility"]
        month_df[factor_cols] = df[factor_cols].resample("M", label="right").mean()
        month_df = month_df.dropna()
        month_df.reset_index(inplace=True)
        return month_df

    month_df = df.set_index("date").groupby("instrument").apply(resample)
    month_df.index = month_df.index.droplevel()

    industry_df = pd.get_dummies(month_df.industry_sw_level1, prefix="industry")
    month_df = pd.concat([month_df, industry_df], axis=1)

    month_df = month_df.groupby("instrument").apply(lambda x: x.assign(label=x.close / x.close.shift(1) - 1))
    month_df.reset_index(drop=True, inplace=True)
    month_df = month_df.dropna()
    month_df.drop(["industry_sw_level1", "amount", "close"], axis=1, inplace=True)

    x_factor = [x for x in month_df.columns if x not in set(["date", "instrument", "label"])]

    def _compute_beta(x, y):
        import statsmodels.api as sm

        model = sm.OLS(y, x)
        results = model.fit()
        return results.params

    factor_beta = month_df.groupby("date").apply(lambda x: _compute_beta(x=x[x_factor], y=x["label"]))
    # factor_beta = month_df.groupby('date').apply(lambda x: pd.ols(x=x[x_factor], y=x['label'], intercept=False).beta)
    factor_beta = factor_beta[sorted(list(factor_name_maps.keys()))]

    factor_data.set_index(["date", "instrument"], inplace=True)

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
    factor_values = factor_values.dropna()
    factor_values = factor_values.resample("1m").mean()
    factor_values.index = factor_values.index.date
    factor_values.columns = factor_data.columns

    factor_return = factor_beta * factor_values
    factor_return.rename_axis(factor_name_maps, axis=1, inplace=True)
    T.plot(factor_return, chart_type="column", title="因子收益分析")
