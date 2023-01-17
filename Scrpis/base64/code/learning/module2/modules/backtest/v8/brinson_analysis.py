import datetime
import itertools

import learning.api.tools as T
import numpy as np
import pandas as pd
from sdk.datasource import DataReaderV2, DataSource

D = DataReaderV2()


class BrinsonAnalysis:
    def __init__(self, result):
        self._result = result.copy()
        self._index_element_weight = DataSource("index_element_weight").read()
        self._trading_days = D.trading_days()
        self._start_date = datetime.datetime(result.index[0].year, result.index[0].month, result.index[0].day).strftime("%Y-%m-%d")
        self._end_date = datetime.datetime(result.index[-1].year, result.index[-1].month, result.index[-1].day).strftime("%Y-%m-%d")
        self._benchmark = "000300.HIX"
        self._asset_lst = self.get_asset_list()
        self._port_weight = self.get_portfolio_weights()
        self._benchmark_weights = self.get_asset_weights_for_benchmark()
        self._benchmark_returns = self.get_benchmark_returns()
        self._asset_returns = self.get_asset_returns()
        self._asset_weights = self.get_asset_weights_for_portfolio()
        self._P_I_returns = self.get_P_I_return()
        self._P_II_returns = self.get_P_II_return()
        self._P_III_returns = self.get_P_III_return()
        self._P_IV_returns = self.get_P_IV_return()

    def get_start_date(self):
        return self._start_date

    def get_end_date(self):
        return self._end_date

    def get_asset_list(self):
        """获取股票列表
            策略股票集合与基准股票集合的并集
        Parameters
        ----------
        result : dataframe
            策略回测结果数据集
        benchmark : str
            策略基准
        Returns
        -------
        asset_lst : list
            资产列表
        """
        # 获取基准资产列表,暂只支持沪深300

        df = DataSource("index_element_weight").read()
        benchmark_assets = set(
            df[(df.date >= self._start_date) & (df.date <= self._end_date) & (df.instrument_index == self._benchmark)]["instrument"]
            .drop_duplicates()
            .tolist()
        )
        strategy_assets = set(itertools.chain(*self._result.orders.apply(lambda orders: [order["sid"].symbol for order in orders])))
        asset_lst = list(benchmark_assets.union(strategy_assets))
        return asset_lst

    def get_asset_returns(self):
        """获取每只股票当日收益率
        Parameters
        ----------
        result : dataframe
            策略回测结果数据集
        Returns
        -------
        asset_returns : Series->{symbol:return}
        """

        asset_M = self._result.positions.apply(lambda positions: {pos["sid"].symbol: pos["last_sale_price"] * pos["amount"] for pos in positions})
        asset_M.index = asset_M.index.map(lambda x: datetime.datetime(x.year, x.month, x.day))
        asset_C_out = self._result.transactions.apply(
            lambda transactions: {
                trans["sid"].symbol: trans["transaction_money"] - trans["commission"] if trans["transaction_money"] > 0 else 0.0
                for trans in transactions
            }
        )
        asset_C_out.index = asset_C_out.index.map(lambda x: datetime.datetime(x.year, x.month, x.day))
        asset_C_in = self._result.transactions.apply(
            lambda transactions: {
                trans["sid"].symbol: abs(trans["transaction_money"]) - trans["commission"] if trans["transaction_money"] < 0 else 0.0
                for trans in transactions
            }
        )
        asset_C_in.index = asset_C_in.index.map(lambda x: datetime.datetime(x.year, x.month, x.day))

        asset_returns = pd.Series({dt: {} for dt in asset_M.index})

        for idx in range(len(asset_M.index)):
            if idx == 0:
                continue
            dt_prev = asset_M.index[idx - 1]
            dt = asset_M.index[idx]
            for asset in self._asset_lst:
                v = asset_M[dt].setdefault(asset, 0.0)
                v_prev = asset_M[dt_prev].setdefault(asset, 0)
                cout = asset_C_out[dt].setdefault(asset, 0)
                cin = asset_C_in[dt].setdefault(asset, 0)
                asset_returns[dt][asset] = (v + cin) / (v_prev + cout) - 1 if v_prev + cout else 0
        return asset_returns

    def get_asset_weights_for_portfolio(self):
        """获取每只股票在组合中的权重
        Parameters
        ----------
        result : dataframe
            策略回测结果数据集
        Returns
        -------
        asset_weights : Series->{symbol:return}
        """
        asset_M = self._result.positions.apply(lambda positions: {pos["sid"].symbol: pos["last_sale_price"] * pos["amount"] for pos in positions})
        asset_M.index = asset_M.index.map(lambda x: datetime.datetime(x.year, x.month, x.day))
        asset_C_out = self._result.transactions.apply(
            lambda transactions: {
                trans["sid"].symbol: trans["transaction_money"] - trans["commission"] if trans["transaction_money"] > 0 else 0.0
                for trans in transactions
            }
        )
        asset_C_out.index = asset_C_out.index.map(lambda x: datetime.datetime(x.year, x.month, x.day))
        port_M = asset_M.apply(lambda mx: np.sum(list(mx.values())))
        port_C_out = asset_C_out.apply(lambda mx: np.sum(list(mx.values())))
        asset_weights = pd.Series({dt: {} for dt in asset_M.index})

        for idx in range(len(asset_M.index)):
            if idx == 0:
                continue
            dt_prev = asset_M.index[idx - 1]
            dt = asset_M.index[idx]
            vp_prev = port_M[dt_prev]
            cp = port_C_out[dt]
            for asset in self._asset_lst:
                v_prev = asset_M[dt_prev].setdefault(asset, 0)
                cout = asset_C_out[dt].setdefault(asset, 0)
                asset_weights[dt][asset] = (v_prev + cout) / (vp_prev + cp) if vp_prev + cp else 0.0
        return asset_weights

    def get_portfolio_weights(self):
        """获取风险组合在总资产的权重
        Parameters
        ----------
        result : dataframe
            策略回测结果数据集
        Returns
        -------
        port_weight : Series
            风险组合在总资产的权重
        """
        asset_M = self._result.positions.apply(lambda positions: {pos["sid"].symbol: pos["last_sale_price"] * pos["amount"] for pos in positions})
        asset_M.index = asset_M.index.map(lambda x: datetime.datetime(x.year, x.month, x.day))
        asset_C_out = self._result.transactions.apply(
            lambda transactions: {
                trans["sid"].symbol: trans["transaction_money"] - trans["commission"] if trans["transaction_money"] > 0 else 0.0
                for trans in transactions
            }
        )
        asset_C_out.index = asset_C_out.index.map(lambda x: datetime.datetime(x.year, x.month, x.day))
        port_M = asset_M.apply(lambda mx: np.sum(list(mx.values())))
        port_C_out = asset_C_out.apply(lambda mx: np.sum(list(mx.values())))
        cash = self._result.starting_cash
        cash.index = cash.index.map(lambda x: datetime.datetime(x.year, x.month, x.day))
        port_weight = pd.Series(np.zeros(len(asset_M.index)), index=asset_M.index)

        for idx in range(len(asset_M.index)):
            if idx == 0:
                continue
            dt_prev = asset_M.index[idx - 1]
            dt = asset_M.index[idx]
            vp_prev = port_M[dt_prev]
            cp = port_C_out[dt]
            c = cash[dt]
            port_weight[dt] = (vp_prev + cp) / (vp_prev + c) if vp_prev + cp else 0.0
        return port_weight

    def get_asset_weights_for_benchmark(self):
        """获取基准组合权重(沪深300市值加权组合)
        Parameters
        ----------
        Returns
        -------
        benchmark_weights :  Series->{symbol:weight}
            基准组合权重序列
        """
        # inplemention version 1
        df = DataSource("index_element_weight").read()
        df = df[(df.instrument_index == self._benchmark) & (df.date >= self._start_date) & (df.date <= self._end_date)]
        benchmark_weights = df.groupby("date").apply(lambda x: {c: w * 0.01 for c, w in zip(x.instrument, x.weight)})
        return benchmark_weights

    def get_benchmark_returns(self):
        idx = self._trading_days[self._trading_days.date == self._start_date].index[0] - 1
        pre_date = self._trading_days.iloc[idx].date.strftime("%Y-%m-%d")
        df = self._index_element_weight
        benchmark_assets = list(
            set(
                df[(df.date >= pre_date) & (df.date <= self._end_date) & (df.instrument_index == self._benchmark)]["instrument"]
                .drop_duplicates()
                .tolist()
            )
        )
        price_df = D.history_data(benchmark_assets, start_date=pre_date, end_date=self._end_date, fields=["close"])
        # index_df = D.history_data(
        #     self._benchmark, start_date=pre_date, end_date=self._end_date, fields=['close'])
        ret_df = price_df.pivot(index="date", columns="instrument", values="close").pct_change().dropna(how="all")
        benchmark_returns = ret_df.apply(lambda r: {ret_df.columns[i]: r[i] if not np.isnan(r[i]) else 0.0 for i in range(len(r))}, axis=1)
        return benchmark_returns

    def get_P_I_return(self):
        # trading_days = D.trading_days()
        idx = self._trading_days[self._trading_days.date == self._start_date].index[0] - 1
        pre_date = self._trading_days.iloc[idx].date.strftime("%Y-%m-%d")
        price_df = D.history_data(self._asset_lst, start_date=pre_date, end_date=self._end_date, fields=["close"])
        # index_df = D.history_data(
        #     self._benchmark, start_date=pre_date, end_date=self._end_date, fields=['close'])
        ret_df = price_df.pivot(index="date", columns="instrument", values="close").pct_change().dropna(how="all")
        asset_returns = pd.Series(
            [
                np.sum(
                    [
                        ret_df.loc[dt][asset] * self._benchmark_weights[dt].setdefault(asset, 0.0)
                        for asset in self._asset_lst
                        if not np.isnan(ret_df.loc[dt][asset])
                    ]
                )
                for dt in ret_df.index
            ],
            index=ret_df.index,
        )
        return asset_returns

    def get_P_II_return(self):
        """获取组合每日收益率
        Parameters
        ----------
        asset_returns : series
            股票收益序列
        asset_weights : series
            股票权重序列
        Returns
        -------
        port_returns : Series->{symbol:return}
            组合收益率序列
        """
        port_returns = pd.Series({dt: 0.0 for dt in self._asset_weights.index})
        for dt in port_returns.index:
            port_returns[dt] = (
                np.sum([x * y for x, y in zip(self._benchmark_returns[dt].values(), self._asset_weights[dt].values())]) * self._port_weight[dt]
            )
        return port_returns

    def get_P_III_return(self):
        """获取组合每日收益率
        Parameters
        ----------
        asset_returns : series
            股票收益序列
        asset_weights : series
            股票权重序列
        Returns
        -------
        port_returns : Series->{symbol:return}
            组合收益率序列
        """
        port_returns = pd.Series({dt: 0.0 for dt in self._asset_returns.index})
        for dt in port_returns.index:
            port_returns[dt] = np.sum([x * y for x, y in zip(self._asset_returns[dt].values(), self._benchmark_weights[dt].values())])
        return port_returns

    def get_P_IV_return(self):
        """获取组合每日收益率
        Parameters
        ----------
        asset_returns : series
            股票收益序列
        asset_weights : series
            股票权重序列
        Returns
        -------
        port_returns : Series->{symbol:return}
            组合收益率序列
        """
        port_returns = pd.Series({dt: 0.0 for dt in self._asset_returns.index})
        for dt in port_returns.index:
            port_returns[dt] = (
                np.sum([x * y for x, y in zip(self._asset_returns[dt].values(), self._asset_weights[dt].values())]) * self._port_weight[dt]
            )
        return port_returns

    def get_timing_return(self):
        """获取择时收益
                II - I
        Parameters
        ----------
        Returns
        -------
        port_returns : float
            累计择时收益
        """
        cumreturn_I = np.cumprod(1 + self._P_I_returns)[-1] - 1
        cumreturn_II = np.cumprod(1 + self._P_II_returns)[-1] - 1
        return cumreturn_II - cumreturn_I

    def get_selection_return(self):
        """获取选股收益
                III - I
        Parameters
        ----------
        Returns
        -------
        port_returns : float
            累计选股收益
        """
        cumreturn_I = np.cumprod(1 + self._P_I_returns)[-1] - 1
        cumreturn_III = np.cumprod(1 + self._P_III_returns)[-1] - 1
        return cumreturn_III - cumreturn_I

    def get_interaction_return(self):
        """获取交互收益
                IV - I - II - III
        Parameters
        ----------
        Returns
        -------
        port_returns : float
            累计交互收益
        """
        cumreturn_I = np.cumprod(1 + self._P_I_returns)[-1] - 1
        cumreturn_II = np.cumprod(1 + self._P_II_returns)[-1] - 1
        cumreturn_III = np.cumprod(1 + self._P_III_returns)[-1] - 1
        cumreturn_IV = np.cumprod(1 + self._P_IV_returns)[-1] - 1
        return cumreturn_IV - cumreturn_I - cumreturn_II - cumreturn_III

    def get_excess_return(self):
        """获取超额收益
                IV - I
        Parameters
        ----------
        Returns
        -------
        port_returns : float
            累计超额收益
        """
        cumreturn_I = np.cumprod(1 + self._P_I_returns)[-1] - 1
        cumreturn_IV = np.cumprod(1 + self._P_IV_returns)[-1] - 1
        return cumreturn_IV - cumreturn_I

    def plot_return_path(self):
        """画出四个象限的累计回报率曲线"""
        show_ret = pd.DataFrame(
            {
                "I": np.cumprod(1 + self._P_I_returns),
                "II": np.cumprod(1 + self._P_II_returns),
                "III": np.cumprod(1 + self._P_III_returns),
                "IV": np.cumprod(1 + self._P_IV_returns),
            }
        )
        T.plot(show_ret, chart_type="line", title="return path")

    def print_enddate_return_analysis(self):
        """打印期末收益分析信息"""
        IV = np.cumprod(1 + self._P_IV_returns)[-1] - 1
        III = np.cumprod(1 + self._P_III_returns)[-1] - 1
        II = np.cumprod(1 + self._P_II_returns)[-1] - 1
        I = np.cumprod(1 + self._P_I_returns)[-1] - 1
        print("-" * 80)
        print("|" + " " * 18 + "IV" + " " * 18 + "|" + " " * 18 + "III" + " " * 18 + "|")
        print("|" + " " * 17 + "{:.1f}%".format(IV * 100) + " " * 16 + "|" + " " * 18 + "{:.1f}%".format(III * 100) + " " * 17 + "|")
        print("-" * 80)
        print("|" + " " * 18 + "II" + " " * 18 + "|" + " " * 18 + "I" + " " * 20 + "|")
        print("|" + " " * 17 + "{:.1f}%".format(II * 100) + " " * 16 + "|" + " " * 18 + "{:.1f}%".format(I * 100) + " " * 16 + "|")
        print("-" * 80)
        print("择时收益：{:.1f}%".format(self.get_timing_return() * 100))
        print("选股收益：{:.1f}%".format(self.get_selection_return() * 100))
        print("交互收益：{:.1f}%".format(self.get_interaction_return() * 100))
        print("超额收益：{:.1f}%".format(self.get_excess_return() * 100))

    def plot_periods_return_analysis(self):

        import numpy

        I_RET = numpy.cumprod(1 + self._P_I_returns) - 1
        II_RET = numpy.cumprod(1 + self._P_II_returns) - 1
        III_RET = numpy.cumprod(1 + self._P_III_returns) - 1
        IV_RET = numpy.cumprod(1 + self._P_IV_returns) - 1

        interaction_ret = IV_RET - I_RET - II_RET - III_RET
        timing_ret = II_RET - I_RET
        selection_ret = III_RET - I_RET
        excess_ret = IV_RET - I_RET
        show_ret = pd.DataFrame(
            {"interaction_ret": interaction_ret, "timing_ret": timing_ret, "selection_ret": selection_ret, "excess_ret": excess_ret}
        )
        T.plot(show_ret, chart_type="column", title="periods_return", options={"plotOptions": {"column": {"grouping": False}}})
