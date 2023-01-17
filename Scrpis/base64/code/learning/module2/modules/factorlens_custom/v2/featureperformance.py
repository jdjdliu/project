import time

import learning.api.tools as T
import learning.module2.common.interface as I  # noqa
import numpy as np
import pandas as pd
from learning.module2.common.utils import smart_list
from sdk.datasource import DataSource
from sdk.datasource.extensions.bigshared.dataframe import evalex


class FeaturePerformance:
    def __init__(
        self,
        start_date,
        end_date,
        rebalance_period,
        buy_commission_rate,
        sell_commission_rate,
        ic_method,
        quantile_num,
        is_standardlize,
        is_winsorize,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.rebalance_period = rebalance_period  # 调仓天数
        self.buy_commission_rate = buy_commission_rate  # 买入佣金(百分比)
        self.sell_commission_rate = sell_commission_rate  # 卖出佣金（百分比）
        self.ic_method = ic_method
        self.quantile_num = quantile_num
        self.is_standardlize = is_standardlize  # 是否标准化
        self.is_winsorize = is_winsorize  # 是否去极值

    def data_processing(self, continus_contract_df, factor_expr):
        # 表达式抽取
        start_time = time.time()
        T.log.info("data_processing start ...")

        def _handle_data(df, price_type):
            # 计算当期因子和未来一段时间收益率
            # 持有期收益率
            df["ret"] = df[price_type].shift(-1 * self.rebalance_period) / df[price_type] - 1
            df["ret"] = df.ret.shift(-1)  # 下一期的收益率
            df["daily_ret_1"] = df["close"].pct_change().shift(-1)  # 次日收益率
            return df

        # 极值数据处理
        def _winsorize(df):
            df = df.copy()
            factor_columns = ["factor"]
            for factor in factor_columns:
                mean = df[factor].mean()
                sigma = df[factor].std()
                df[factor] = df[factor].clip(mean - 3 * sigma, mean + 3 * sigma)
            return df

        # 标准数据处理
        def _standardlize(df):
            df = df.copy()
            factor_columns = ["factor"]
            for factor in factor_columns:
                mean = df[factor].mean()
                sigma = df[factor].std()
                df[factor] = (df[factor] - mean) / sigma
            return df

        factor_df = continus_contract_df.groupby("instrument").apply(_handle_data, price_type="close")
        factor_df.rename(columns={factor_expr: "factor"}, inplace=True)

        base_factor_df = factor_df[["date", "instrument", "close", "ret", "factor", "daily_ret_1"]]
        # 标准化，去极值处理
        if self.is_standardlize and not self.is_winsorize:
            base_factor_df = base_factor_df.groupby("date").apply(lambda x: _standardlize(x)).reset_index(drop=True)
        elif self.is_winsorize and not self.is_standardlize:
            base_factor_df = base_factor_df.groupby("date").apply(lambda x: _winsorize(x)).reset_index(drop=True)
        elif self.is_winsorize and self.is_standardlize:
            base_factor_df = base_factor_df.groupby("date").apply(lambda x: _standardlize(_winsorize(x))).reset_index(drop=True)
        # 对数据根据时间进行过滤
        base_factor_df = base_factor_df[(base_factor_df["date"] > self.start_date) & ((base_factor_df["date"] < self.end_date))]

        td = DataSource("trading_days").read(start_date=base_factor_df.date.min().strftime("%Y-%m-%d"))
        td = td[td.country_code == "CN"]
        rebalance_days = td[:: self.rebalance_period]  # 调仓期
        rebalance_days_df = pd.DataFrame({"date": rebalance_days["date"], "ix": range(len(rebalance_days))})
        rebalance_days_df.index = range(len(rebalance_days_df))
        merge_df = pd.merge(base_factor_df, rebalance_days_df, on="date", how="inner")
        # 将因子名或因子表达式抽取出来做展示处理
        T.log.info("data_processing process %.3fs" % (time.time() - start_time))
        return merge_df, base_factor_df, factor_expr

    def ic_processing(self, merge_df, factor_name):
        start_time = time.time()
        T.log.info("ic_processing start ...")

        def _cal_IC(df, method="Rank_IC"):
            """计算IC系数"""
            from scipy.stats import pearsonr, spearmanr

            df = df.dropna()
            if df.shape[0] == 0:
                return np.nan

            if method == "Rank_IC":
                return spearmanr(df["factor"], df["ret"])[0]
            if method == "IC":
                return pearsonr(df["factor"], df["ret"])[0]

        ic = merge_df.groupby("date").apply(_cal_IC, method=self.ic_method)

        # ic相关指标
        ic_mean = np.round(ic.mean(), 4)
        ic_std = np.round(ic.std(), 4)
        ic_ir = np.round(ic_mean / ic_std, 4)
        positive_ic_cnt = len(ic[ic > 0])
        negative_ic_cnt = len(ic[ic < 0])
        ic_skew = np.round(ic.skew(), 4)
        ic_kurt = np.round(ic.kurt(), 4)

        # IC指标展示
        results = {
            "stats": {
                "ic_mean": ic_mean,
                "ic_std": ic_std,
                "ic_ir": ic_ir,
                "positive_ic_cnt": positive_ic_cnt,
                "negative_ic_cnt": negative_ic_cnt,
                "ic_skew": ic_skew,
                "ic_kurt": ic_kurt,
            },
            "title": f"{factor_name}: IC分析",
        }

        ic.name = "ic"
        ic_df = ic.to_frame()
        ic_df["ic_cumsum"] = ic_df["ic"].cumsum()
        T.log.info("ic_processing process  %.3fs" % (time.time() - start_time))
        return ic_df, results

    def ols_stats_processing(self, merge_df, factor_name):
        start_time = time.time()
        T.log.info("ols_stats_processing start ...")

        def _get_model_stats(X, y):
            from pyfinance import ols

            model = ols.OLS(y=y, x=X)
            return [model.beta, model.tstat_beta, model.pvalue_beta, model.se_beta]

        ols_stats = merge_df.dropna().groupby("date").apply(lambda df: _get_model_stats(df[["factor"]], df["ret"]))
        ols_stats_df = pd.DataFrame(ols_stats)
        ols_stats_df.rename(columns={0: "ols_result"}, inplace=True)

        ols_stats_df["beta"] = ols_stats_df["ols_result"].apply(lambda x: x[0])
        ols_stats_df["tstat_beta"] = ols_stats_df["ols_result"].apply(lambda x: x[1])
        ols_stats_df["pvalue_beta"] = ols_stats_df["ols_result"].apply(lambda x: x[2])
        ols_stats_df["se_beta"] = ols_stats_df["ols_result"].apply(lambda x: x[3])
        ols_stats_df = ols_stats_df[["beta", "tstat_beta", "pvalue_beta", "se_beta"]]

        roll_beta_period = 12
        ols_stats_df["cum_beta"] = ols_stats_df["beta"].cumsum()
        ols_stats_df["roll_beta"] = ols_stats_df["beta"].rolling(roll_beta_period).mean()

        # 因子收益率数据加工
        ols_stats_df["abs_t_value"] = ols_stats_df["tstat_beta"].abs()
        # 相应指标
        beta_mean = np.round(ols_stats_df["beta"].mean(), 4)
        beta_std = np.round(ols_stats_df["beta"].std(), 4)
        positive_beta_ratio = np.round(len(ols_stats_df["beta"][ols_stats_df["beta"] > 0]) / len(ols_stats_df), 4) * 100
        abs_t_mean = np.round(ols_stats_df["abs_t_value"].mean(), 4)
        abs_t_value_over_two_ratio = np.round(len(ols_stats_df["abs_t_value"][ols_stats_df["abs_t_value"] > 2]) / len(ols_stats_df["abs_t_value"]), 4)
        p_value_less_ratio = np.round(len(ols_stats_df["pvalue_beta"][ols_stats_df["pvalue_beta"] < 0.05]) / len(ols_stats_df["pvalue_beta"]), 4)

        results = {
            "stats": {
                "beta_mean": beta_mean,
                "beta_std": beta_std,
                "positive_beta_ratio": positive_beta_ratio,
                "abs_t_mean": abs_t_mean,
                "abs_t_value_over_two_ratio": abs_t_value_over_two_ratio,
                "p_value_less_ratio": p_value_less_ratio,
            },
            "title": f"{factor_name}: 因子收益率分析",
        }
        T.log.info("ols_stats_processing process  %.3fs" % (time.time() - start_time))
        return ols_stats_df, results

    def group_processing(self, merge_df, base_factor_df, factor_name):
        start_time = time.time()
        T.log.info("group_processing start ...")

        def _fill_ix_na(df):
            df["rebalance_index"] = df["ix"].fillna(method="ffill")
            return df

        def _unify_factor(tmp):
            """因子以调仓期首日因子为准"""
            tmp["factor"] = list(tmp["factor"])[0]
            return tmp

        def _cut_box(df, quantile_num=5):
            if df.factor.isnull().sum() == len(df):  # 因子值全是nan的话
                df["factor_group"] = [np.nan] * len(df)
            else:
                labels = [str(i) for i in range(quantile_num)]
                df["factor_group"] = pd.qcut(df["factor"], quantile_num, labels=labels)  # qcut 改成cut  # 升序排序，分成5组
            return df

        # 计算绩效指标
        def _get_stats(results, col_name):
            import empyrical

            return_ratio = np.round(empyrical.cum_returns_final(results[col_name]), 4)
            annual_return_ratio = np.round(empyrical.annual_return(results[col_name]), 4)
            sharp_ratio = np.round(empyrical.sharpe_ratio(results[col_name], 0.035 / 252), 4)
            return_volatility = np.round(empyrical.annual_volatility(results[col_name]), 4)
            max_drawdown = np.round(empyrical.max_drawdown(results[col_name]), 4)

            res = {"收益率": [return_ratio]}
            date_dict = {1: "1日", 5: "1周", 22: "1月"}
            for n in [1, 5, 22]:
                res["近{}收益率".format(date_dict[n])] = np.round(
                    results[col_name.replace("ret", "pv")][-1] / results[col_name.replace("ret", "pv")][-(n + 1)] - 1, 4
                )
            res.update({"年化收益率": [annual_return_ratio], "夏普比率": [sharp_ratio], "收益波动率": [return_volatility], "最大回撤": [max_drawdown]})
            return pd.DataFrame(res)

        # 这一步可以删除
        merge_df2 = pd.merge(
            base_factor_df[["date", "instrument", "factor", "daily_ret_1"]],
            merge_df[["date", "instrument", "ix"]],
            how="left",
            on=["date", "instrument"],
        )

        merge_df2 = merge_df2.groupby("instrument").apply(_fill_ix_na)
        unify_factor_df = merge_df2.groupby(["rebalance_index", "instrument"]).apply(_unify_factor)
        group_df = unify_factor_df.groupby("date").apply(_cut_box, quantile_num=self.quantile_num)

        # 计算每组每天的收益率
        result = group_df[["date", "factor_group", "daily_ret_1", "rebalance_index", "ix"]].groupby(["factor_group", "date"]).mean().reset_index()
        # 调仓日的收益率需要扣除交易成本
        result["daily_ret_1"] -= (self.buy_commission_rate + self.sell_commission_rate) * np.where(result["ix"].isna(), 0, 1)

        result_table = result.pivot(values="daily_ret_1", columns="factor_group", index="date")

        result_table.rename(columns={i: "top%s_ret" % i for i in result_table.columns}, inplace=True)

        small_quantile_name = result_table.columns.min()  ## 选取top0
        big_quantile_name = result_table.columns.max()  ## 选取top4
        long_short_name = "LS_ret"
        result_table["LS_ret"] = (result_table[small_quantile_name] - result_table[big_quantile_name]) / 2
        # 移除na值,防止收益计算为难
        result_table.dropna(inplace=True)

        for i in result_table.columns:
            col_name = i.split("_")[0] + "_" + "pv"
            result_table[col_name] = (1 + result_table[i]).cumprod()

        small_quantile_perf = _get_stats(result_table, small_quantile_name)
        big_quantile_perf = _get_stats(result_table, big_quantile_name)
        long_short_perf = _get_stats(result_table, long_short_name)
        df = pd.concat([small_quantile_perf, big_quantile_perf, long_short_perf])
        df.index = [small_quantile_name, big_quantile_name, long_short_name]
        results = {
            "stats": df.T.to_dict(),
            "title": f"{factor_name}: 因子绩效分析",
        }
        T.log.info("group_processing process  %.3fs" % (time.time() - start_time))
        return result_table, results

    def process(self, continus_contract_df, factor_exprs):
        factor_data = []
        performance_data = []
        # 更新结束日期
        is_live_run = T.live_run_param("trading_date", None) is not None
        if is_live_run:
            self.end_date = T.live_run_param("trading_date", "trading_date")
        # 进行因子计算

        for factor_expr in factor_exprs:
            merge_df, base_factor_df, factor_name = self.data_processing(continus_contract_df, factor_expr)

            ic_data = self.ic_processing(merge_df, factor_name)
            ols_data = self.ols_stats_processing(merge_df, factor_name)
            group_data = self.group_processing(merge_df, base_factor_df, factor_name)

            # 保存因子相关信息
            options_data = {
                "start_date": self.start_date,
                "end_date": self.end_date,
                "rebalance_period": self.rebalance_period,
                "buy_commission_rate": self.buy_commission_rate,
                "sell_commission_rate": self.sell_commission_rate,
                "ic_method": self.ic_method,
                "quantile_num": self.quantile_num,
                "is_standardlize": self.is_standardlize,
                "is_winsorize": self.is_winsorize,
            }
            result = {
                "summary": {"IC": ic_data[1], "FactorReturns": ols_data[1], "QuantileReturns": group_data[1]},
                "data": {"IC": ic_data[0], "FactorReturns": ols_data[0], "QuantileReturns": group_data[0]},
                "options": options_data,
            }
            factor_data.append(base_factor_df)
            performance_data.append(result)

        return factor_data, performance_data
