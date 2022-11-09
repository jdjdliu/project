import time

import bigexpr
import learning.api.tools as T
import learning.module2.common.interface as I  # noqa
import numpy as np
import pandas as pd
from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.utils import smart_list
from sdk.datasource.extensions.bigshared.dataframe import evalex
from sdk.utils import BigLogger

bigquant_public = True
bigquant_cacheable = True
bigquant_category = "因子研究"
bigquant_friendly_name = "因子分析(自定义)"
bigquant_doc_url = "https://bigquant.com/docs/"
log = BigLogger(bigquant_friendly_name)


def bigquant_run(
    features: I.port("输入因子-来自输入特征列表等模块", specific_type_name="列表|DataSource"),
    # user_factor_data: I.port('用户自定义特征数据', specific_type_name='DataSource', optional=True)=None,
    # title: I.str('分析报告标题, {factor_name}表示因子名')='因子分析: {factor_name}',
    start_date: I.str("开始日期，分析数据开始日期", can_set_liverun_param=True) = "2019-01-01",
    end_date: I.str("结束日期，分析数据结束日期", can_set_liverun_param=True) = "2019-12-31",
    instrument_list: I.code("代码列表，每行一个，如果指定，market参数将被忽略", auto_complete_type="stocks") = None,
    market: I.choice("交易市场", ["CN_FUND", "CN_FUTURE", "CN_MUTFUND"]) = "CN_FUND",
    expr: I.str(
        r"过滤表达式， 参考示例代码和[DataFrame.query](http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.query.html)，包含特使字符的列名需要使用反单引号(\`)引起来，例如 \`close_10/close0\` > 0.91",
        specific_type_name="字符串",
    ) = None,
    rebalance_period: I.int("调仓周期(交易日)，单位为交易日") = 22,
    quantile_num: I.int("分组数量") = 5,
    buy_commission_rate: I.float("手续费及滑点") = 0.0005,
    sell_commission_rate: I.float("手续费及滑点") = 0.0005,
    ic_method: I.choice("IC类型", ["Rank_IC", "IC"]) = "Rank_IC",
    is_winsorize: I.bool("因子去极值") = True,
    is_standardlize: I.bool("因子标准化") = True,
) -> [I.port("数据", "data_1"), I.port("数据", "data_2"), I.port("保存数据,连接保存因子模块", "data_3"),]:
    """
    因子分析。对输入的因子公式或者因子数据，做因子分析。
    """

    class FuturesPerformance:
        def __init__(
            self,
            start_date=start_date,
            end_date=end_date,
            rebalance_period=rebalance_period,
            buy_commission_rate=buy_commission_rate,
            sell_commission_rate=sell_commission_rate,
            ic_method=ic_method,
            quantile_num=quantile_num,
            is_standardlize=is_standardlize,
            is_winsorize=is_winsorize,
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

            def _handle_data(df, assignment_value, price_type):
                # 计算当期因子和未来一段时间收益率
                df["factor"] = bigexpr.evaluate(df, assignment_value, None)
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

            assignment_targets, assignment_value = bigexpr.parse_assignment(factor_expr)
            factor_df = continus_contract_df.groupby("instrument").apply(_handle_data, assignment_value=assignment_value, price_type="close")

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

            # 对应用户抽取的列名
            if assignment_targets:
                for target in assignment_targets:
                    base_factor_df[target] = base_factor_df["factor"]

            td = DataSource("trading_days").read(start_date=base_factor_df.date.min().strftime("%Y-%m-%d"))
            rebalance_days = td[:: self.rebalance_period]  # 调仓期
            rebalance_days_df = pd.DataFrame({"date": rebalance_days["date"], "ix": range(len(rebalance_days))})
            rebalance_days_df.index = range(len(rebalance_days_df))
            merge_df = pd.merge(base_factor_df, rebalance_days_df, on="date", how="inner")

            # 将因子名或因子表达式抽取出来做展示处理
            factor_name = assignment_targets[0] if assignment_targets else assignment_value
            T.log.info("data_processing process %.3fs" % (time.time() - start_time))
            return merge_df, base_factor_df, factor_name

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
            abs_t_value_over_two_ratio = np.round(
                len(ols_stats_df["abs_t_value"][ols_stats_df["abs_t_value"] > 2]) / len(ols_stats_df["abs_t_value"]), 4
            )
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
                    df["factor_group"] = pd.qcut(df["factor"], quantile_num, labels=labels)  # 升序排序，分成5组
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
                # continus_contract_df = self.load_continus_instrument(df)
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
                    # "is_roll_rebalance": self.is_roll_rebalance,
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

    instruments = []
    if isinstance(instrument_list, list) and len(instrument_list):
        instruments = instrument_list
    elif isinstance(instrument_list, str) and instrument_list.strip():
        instruments = smart_list(instrument_list, sort=True)

    factor_exprs = features.read()
    start_date = start_date
    end_date = end_date
    if market == "CN_FUND":
        if instruments:
            df = DataSource("bar1d_CN_FUND").read(instruments=instruments, start_date=start_date, end_date=end_date)
        else:
            df = DataSource("bar1d_CN_FUND").read(start_date=start_date, end_date=end_date)
        if expr:
            df = df[evalex(df, expr)]
    if market == "CN_FUTURE":
        if instruments:
            df = DataSource("bar1d_CN_FUTURE").read(instruments=instruments, start_date=start_date, end_date=end_date)
        else:
            df = DataSource("bar1d_CN_FUTURE").read(start_date=start_date, end_date=end_date)
        if expr:
            df = df[evalex(df, expr)]
    if market == "CN_MUTFUND":
        if instruments:
            df = DataSource("history_nav_CN_MUTFUND").read(instruments=instruments, start_date=start_date, end_date=end_date)
        else:
            df = DataSource("history_nav_CN_MUTFUND").read(start_date=start_date, end_date=end_date)
        df = df.sort_values(["date", "end_date"])
        df.drop_duplicates(subset=["instrument", "date"], keep="last", inplace=True)
        td = DataSource("trading_days").read(start_date=start_date, end_date=end_date)
        df = df[df.date.isin(td.date)]
        # df = df[df.instrument.str.contains('OFCN')].reset_index(drop=True)
        df["close"] = df["nav"]
        if expr:
            df = df[evalex(df, expr)]

    fp = FuturesPerformance(
        start_date, end_date, rebalance_period, buy_commission_rate, sell_commission_rate, ic_method, quantile_num, is_standardlize, is_winsorize
    )

    data_1, data_2 = fp.process(df, factor_exprs)

    df = data_1[0]
    df = df.rename(columns={"factor": factor_exprs[0]})
    factor_name = factor_exprs[0]
    start_date = data_2[0]["options"]["start_date"]
    end_date = data_2[0]["options"]["end_date"]
    rebalance_period = data_2[0]["options"]["rebalance_period"]
    buy_commission_rate = data_2[0]["options"]["buy_commission_rate"]
    sell_commission_rate = data_2[0]["options"]["sell_commission_rate"]
    ic_method = data_2[0]["options"]["ic_method"]
    quantile_num = data_2[0]["options"]["quantile_num"]
    is_standardlize = data_2[0]["options"]["is_standardlize"]
    is_winsorize = data_2[0]["options"]["is_winsorize"]
    ic_mean = data_2[0]["summary"]["IC"]["stats"]["ic_mean"]
    ic_std = data_2[0]["summary"]["IC"]["stats"]["ic_std"]
    ic_ir = data_2[0]["summary"]["IC"]["stats"]["ic_ir"]
    ret_min_1d = data_2[0]["summary"]["QuantileReturns"]["stats"]["top0_ret"]["近1日收益率"]
    ret_min_1w = data_2[0]["summary"]["QuantileReturns"]["stats"]["top0_ret"]["近1周收益率"]
    ret_min_1m = data_2[0]["summary"]["QuantileReturns"]["stats"]["top0_ret"]["近1月收益率"]
    ret_min_1y = data_2[0]["summary"]["QuantileReturns"]["stats"]["top0_ret"]["年化收益率"]
    ret_max_1d = data_2[0]["summary"]["QuantileReturns"]["stats"]["top4_ret"]["近1日收益率"]
    ret_max_1w = data_2[0]["summary"]["QuantileReturns"]["stats"]["top4_ret"]["近1周收益率"]
    ret_max_1m = data_2[0]["summary"]["QuantileReturns"]["stats"]["top4_ret"]["近1月收益率"]
    ret_max_1y = data_2[0]["summary"]["QuantileReturns"]["stats"]["top4_ret"]["年化收益率"]
    long_short_1d = data_2[0]["summary"]["QuantileReturns"]["stats"]["LS_ret"]["近1日收益率"]
    long_short_1w = data_2[0]["summary"]["QuantileReturns"]["stats"]["LS_ret"]["近1周收益率"]
    long_short_1m = data_2[0]["summary"]["QuantileReturns"]["stats"]["LS_ret"]["近1月收益率"]
    long_short_1y = data_2[0]["summary"]["QuantileReturns"]["stats"]["LS_ret"]["年化收益率"]
    res = {
        factor_name: {
            "options": {
                "开始日期": start_date,
                "结束日期": end_date,
                "调仓周期": rebalance_period,
                "买入费用": buy_commission_rate,
                "卖出费用": sell_commission_rate,
                "IC计算方式": ic_method,
                "分组数量": quantile_num,
                "是否标准化": is_standardlize,
                "是否极值处理": is_winsorize,
            },
            "metrics": {
                "IC均值": ic_mean,
                "IC标准差": ic_std,
                "IR值": ic_ir,
                "昨日收益(最小分位)": ret_min_1d,
                "近1周收益(最小分位)": ret_min_1w,
                "近1月收益(最小分位)": ret_min_1m,
                "近1年收益(最小分位)": ret_min_1y,
                "昨日收益(最大分位)": ret_max_1d,
                "近1周收益(最大分位)": ret_max_1w,
                "近1月收益(最大分位)": ret_max_1m,
                "近1年收益(最大分位)": ret_max_1y,
                "昨日收益(多空组合)": long_short_1d,
                "近1周收益(多空组合)": long_short_1w,
                "近1月收益(多空组合)": long_short_1m,
                "近1年收益(多空组合)": long_short_1y,
            },
            "datasource": df,
            "column_name": factor_name,
            "expr": factor_name,
        }
    }

    data_1 = DataSource.write_pickle(data_1)
    data_2 = DataSource.write_pickle(data_2)
    data_3 = DataSource.write_pickle(res)

    return Outputs(data_1=data_1, data_2=data_2, data_3=data_3)


# 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
def bigquant_postrun(outputs):

    from jinja2 import Template
    from learning.module2.common.utils import display_html

    class RenderHtml:
        ic_stats_template = """
        <div style="width:100%;text-align:center;color:#333333;margin-bottom:16px;font-size:12px;"><h2>{{ title }}</h2></div>
        <div class='kpicontainer'>
            <ul class='kpi'>
                <li><span class='title'>IC均值</span><span class='value'>{{ stats.ic_mean }}</span></li>
                <li><span class='title'>IC标准差</span><span class='value'>{{ stats.ic_std }}</span></li>
                <li><span class='title'>ICIR</span><span class='value'>{{ stats.ic_ir }}</span></li>
                <li><span class='title'>IC正值次数</span><span class='value'>{{ stats.positive_ic_cnt }}次</span></li>
                <li><span class='title'>IC负值次数</span><span class='value'>{{ stats.negative_ic_cnt }}次</span></li>
                <li><span class='title'>IC偏度</span><span class='value'>{{ stats.ic_skew }}</span></li>
                <li><span class='title'>IC峰度</span><span class='value'>{{ stats.ic_kurt }}</span></li>
            </ul>
        </div>
        """
        ols_stats_template = """
        <div style="width:100%;text-align:center;color:#333333;margin-bottom:16px;font-size:12px;"><h2>{{ title }}</h2></div>
        <div class='kpicontainer'>
            <ul class='kpi'>
                <li><span class='title'>因子收益均值</span><span class='value'>{{ stats.beta_mean }}</span></li>
                <li><span class='title'>因子收益标准差</span><span class='value'>{{ stats.beta_std }}</span></li>
                <li><span class='title'>因子收益为正比率</span><span class='value'>{{ stats.positive_beta_ratio }}%</span></li>
                <li><span class='title'>t值绝对值的均值</span><span class='value'>{{ stats.abs_t_mean }}</span></li>
                <li><span class='title'>t值绝对值大于2的比率</span><span class='value'>{{ stats.abs_t_value_over_two_ratio }}</span></li>
                <li><span class='title'>因子收益t检验p值小于0.05的比率</span><span class='value'>{{ stats.p_value_less_ratio }}</span></li>
            </ul>
        </div>
        """
        group_stats_template = """
        <div style="width:100%;text-align:center;color:#333333;margin-bottom:16px;font-size:12px;"><h2>{{ title }}</h2></div>
        <div class='kpicontainer'>
            <ul class='kpi'>
                <li><span class='title'>&nbsp;</span>
                    {% for k in stats%}
                        <span class='value'>{{ k }}</span>
                    {% endfor %}
                </li>
                <li><span class='title'>收益率</span>
                    {% for k in stats%}
                        <span class='value'>{{ (stats[k].收益率 | string)[0:10] }}</span>
                    {% endfor %}
                </li>
                <li><span class='title'>近1日收益率</span>
                    {% for k in stats%}
                        <span class='value'>{{ (stats[k].近1日收益率 | string)[0:10] }}</span>
                    {% endfor %}
                </li>
                <li><span class='title'>近1周收益率</span>
                    {% for k in stats%}
                        <span class='value'>{{ (stats[k].近1周收益率 | string)[0:10] }}</span>
                    {% endfor %}
                </li>
                <li><span class='title'>近1月收益率</span>
                    {% for k in stats%}
                        <span class='value'>{{ (stats[k].近1月收益率 | string)[0:10] }}</span>
                    {% endfor %}
                </li>
                <li><span class='title'>年化收益率</span>
                    {% for k in stats%}
                        <span class='value'>{{ (stats[k].年化收益率 | string)[0:10] }}</span>
                    {% endfor %}
                </li>
                <li><span class='title'>夏普比率</span>
                    {% for k in stats%}
                        <span class='value'>{{ (stats[k].夏普比率 | string)[0:10] }}</span>
                    {% endfor %}
                </li>
                <li><span class='title'>收益波动率</span>
                    {% for k in stats%}
                        <span class='value'>{{ (stats[k].收益波动率 | string)[0:10] }}</span>
                    {% endfor %}
                </li>
                <li><span class='title'>最大回撤</span>
                    {% for k in stats%}
                        <span class='value'>{{ (stats[k].最大回撤 | string)[0:10] }}</span>
                    {% endfor %}
                </li>
             </ul>
        </div>
        """

        def __init__(self, ic_data, ic_summary, factor_returns_data, factor_returns_summary, quantile_returns_data, quantile_returns_summary):
            self.ic_df = ic_data
            self.ic_results = ic_summary
            self.ols_stats_df = factor_returns_data
            self.ols_stats_results = factor_returns_summary
            self.group_df = quantile_returns_data
            self.group_df_results = quantile_returns_summary

        def render_results(self, stats_template, results):
            """展示模板信息"""

            def render(stats_template, results):
                html = Template(stats_template).render(stats=results["stats"], title=results["title"])
                display_html(html)

            render(stats_template, results)

        def show_ic(self):
            self.render_results(self.ic_stats_template, self.ic_results)
            T.plot(
                self.ic_df,
                title="IC分析",
                panes=[["ic", "40%"], ["ic_cumsum", "20%"]],
                # height=500，设置高度为500
                options={
                    "chart": {"height": 500},
                    # 设置颜色
                    "series": [
                        {
                            "name": "ic",
                            "color": "#8085e8",
                            "type": "column",
                            "yAxis": 0,
                        },
                        {
                            "name": "ic_cumsum",
                            "color": "#8d4653",
                            "type": "spline",
                            "yAxis": 0,
                        },
                    ],
                },
            )

        def show_ols(self):
            self.render_results(self.ols_stats_template, self.ols_stats_results)
            T.plot(
                self.ols_stats_df[["beta", "cum_beta", "roll_beta"]],
                title="因子收益率",
                # high、low显示在第一栏，高度40%，open、close显示在第二栏，其他的在最后一栏
                panes=[["beta", "cum_beta", "40%"], ["roll_beta", "20%"]],
                # height=500，设置高度为500
                options={
                    "chart": {"height": 500},
                    # 设置颜色
                    "series": [
                        {
                            "name": "beta",
                            "color": "#8085e8",
                            "type": "column",
                            "yAxis": 0,
                        },
                        {
                            "name": "cum_beta",
                            "color": "#8d4653",
                            "type": "column",
                            "yAxis": 0,
                        },
                        {
                            "name": "roll_beta",
                            "color": "#91e8e1",
                            "type": "spline",
                            "yAxis": 1,
                        },
                    ],
                },
            )

        def show_group(self):
            self.render_results(self.group_stats_template, self.group_df_results)
            T.plot(self.group_df[[i for i in self.group_df.columns if "_pv" in i]])

        def show(self):
            self.show_ic()
            self.show_ols()
            self.show_group()

    # 读取 IC,FactorReturns,QuantileReturns用作展示
    performance_data = outputs.data_2.read()
    for data in performance_data:
        ic_data = data["data"]["IC"]
        factor_returns_data = data["data"]["FactorReturns"]
        quantile_returns_data = data["data"]["QuantileReturns"]
        ic_summary = data["summary"]["IC"]
        factor_returns_summary = data["summary"]["FactorReturns"]
        quantile_returns_summary = data["summary"]["QuantileReturns"]
        renderhtml = RenderHtml(ic_data, ic_summary, factor_returns_data, factor_returns_summary, quantile_returns_data, quantile_returns_summary)
        renderhtml.show()
    return outputs
