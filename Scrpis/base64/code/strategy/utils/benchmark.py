from datetime import datetime
from typing import List

import numpy as np
import pandas as pd
from strategy.schemas import BenchMarkSchema, PlotCutSchema, StrategyDailySchema

from .learning import RecordDatas


def get_cut_plot_new_return(
    cut_stamp: List[float],  # [1546444800000.0,1661356800000.0]
    cum_return_plot: List,
    benchmark_cum_return_plot: List,
) -> PlotCutSchema:
    new_cum_return_plot = return_plot_to_zero(return_plot_cut(cut_stamp, cum_return_plot))
    new_benchmark_cum_return_plot = return_plot_to_zero(return_plot_cut(cut_stamp, benchmark_cum_return_plot))
    new_max_drawdown_stamp = get_max_drawdown_stamp(new_cum_return_plot)
    new_relative_cum_return_plot = get_relative_cum_return_plot(new_cum_return_plot, new_benchmark_cum_return_plot)

    return PlotCutSchema(
        cum_return_plot=new_cum_return_plot,
        benchmark_cum_return_plot=new_benchmark_cum_return_plot,
        max_drawdown_stamp=new_max_drawdown_stamp,
        relative_cum_return_plot=new_relative_cum_return_plot,
    )


def calculate_return(cum_return: float, base_return: float):
    if base_return:
        re_calculate_return = (cum_return - base_return) / (1 + base_return)
    else:
        re_calculate_return = -1
    return re_calculate_return


def get_benchmark_profit(daily_schema: List[StrategyDailySchema], benchmark_return_plot: List) -> BenchMarkSchema:
    try:
        last_daily = daily_schema[0]
        trading_days = last_daily.trading_days
        last_daily_portfolio_dict = get_ago_portfolio_dict(daily_schema, last_daily.run_date)
        cum_return = benchmark_return_plot[-1][1]
        annual_return = pow(1.0 + cum_return, 252 / trading_days) - 1.0  # 年化收益

        max_drawdown_stamp = get_max_drawdown_stamp(benchmark_return_plot)
        if max_drawdown_stamp:
            max_drawdown = (max_drawdown_stamp[0][1] - max_drawdown_stamp[1][1]) / (1 + max_drawdown_stamp[0][1])
        else:
            max_drawdown = -1  # 默认为-1， 前端会处理为 -

        if len(benchmark_return_plot) >= 2:
            today_return = (benchmark_return_plot[-1][1] - benchmark_return_plot[-2][1]) / (1 + benchmark_return_plot[-2][1])
        else:
            today_return = -1

        sharpe = cum_return_plot_sharpe(benchmark_return_plot)
        if np.isnan(sharpe):
            sharpe = 0.0
        week_return = calculate_return(cum_return, last_daily_portfolio_dict["week"].get("000300.HIX.CUM"))
        month_return = calculate_return(cum_return, last_daily_portfolio_dict["month"].get("000300.HIX.CUM"))
        three_month_return = calculate_return(cum_return, last_daily_portfolio_dict["three_month"].get("000300.HIX.CUM"))
        six_month_return = calculate_return(cum_return, last_daily_portfolio_dict["six_month"].get("000300.HIX.CUM"))
        year_return = calculate_return(cum_return, last_daily_portfolio_dict["year"].get("000300.HIX.CUM"))

        return BenchMarkSchema(
            cum_return=cum_return,
            sharpe=sharpe,
            annual_return=annual_return,
            max_drawdown_stamp=max_drawdown_stamp,
            max_drawdown=max_drawdown,
            today_return=today_return,
            week_return=week_return,
            month_return=month_return,
            three_month_return=three_month_return,
            six_month_return=six_month_return,
            year_return=year_return,
        )
    except Exception:
        return BenchMarkSchema(
        cum_return=-1,
        sharpe=-1,
        annual_return=-1,
        max_drawdown_stamp=-1,
        max_drawdown=-1,
        today_return=-1,
        week_return=-1,
        month_return=-1,
        three_month_return=-1,
        six_month_return=-1,
        year_return=-1,
    )

def cum_return_plot_sharpe(cum_return_plot: List):
    from empyrical import stats

    cum_return_list = [u[1] + 1 for u in cum_return_plot]
    daily_return_list = []
    for index, value in enumerate(cum_return_list):
        if index == 0:
            daily_return = 0
        else:
            daily_return = (value - cum_return_list[index - 1]) / cum_return_list[index - 1] if cum_return_list[index - 1] != 0 else 0
        daily_return_list.append(daily_return)
    RISK_FREE_DAILY = 0.0001173
    liverun_so_far_series = pd.Series(daily_return_list)
    current_sharpe = stats.sharpe_ratio(liverun_so_far_series, RISK_FREE_DAILY)
    return current_sharpe


def return_plot_to_zero(plot_list: List) -> List:
    base_number = 1
    new_plot_list = []
    for index, value in enumerate(plot_list):
        if index == 0:
            new_value = 1
        else:
            new_value = base_number * (1 + value[1]) / (1 + plot_list[index - 1][1])
            base_number = new_value
        new_plot_list.append([value[0], new_value - 1])
    return new_plot_list


def return_plot_cut(cut_stamp: List[float], plot_list: List) -> List:
    start_index = None
    end_index = None
    for index, value in enumerate(plot_list):
        if cut_stamp[0] > value[0]:
            start_index = index + 1
        if cut_stamp[1] < value[0]:  # type: ignore
            end_index = index
            break
    return plot_list[start_index:end_index]


def get_max_drawdown_stamp(cum_plot: List) -> List:
    try:
        data = np.array(cum_plot)
        data_return = data[:, 1] + 1
        index_j = np.argmax((np.maximum.accumulate(data_return) - data_return) / np.maximum.accumulate(data_return))  # 结束位置
        # 开始位置
        index_i = np.argmax(data[:, 1][:index_j])  # type: ignore
        max_val = data[:, 1][index_i]
        max_val_date = data[:, 0][index_i]
        min_val = data[:, 1][index_j]
        min_val_date = data[:, 0][index_j]
        return [[max_val_date, max_val], [min_val_date, min_val]]
    except BaseException:
        return []


def get_relative_cum_return_plot(cum_return_plot: List, benchmark_cum_return_plot: List) -> List:
    try:
        if not cum_return_plot or not benchmark_cum_return_plot:
            return []
        df1 = pd.DataFrame(cum_return_plot, columns=["index", "algorithm_period_return"])
        df2 = pd.DataFrame(benchmark_cum_return_plot, columns=["index2", "benchmark_period_return"])
        results = df1.merge(df2[["benchmark_period_return"]], left_index=True, right_index=True, how="outer")
        """ 计算相对收益率 """
        results["a_delta"] = results["algorithm_period_return"].shift(1)
        results["a_delta"].iloc[0] = 0
        results["a_delta"] = (results["algorithm_period_return"] + 1.0) / (results["a_delta"] + 1.0) - 1.0
        results["b_delta"] = results["benchmark_period_return"].shift(1)
        results["b_delta"].iloc[0] = 0
        results["b_delta"] = (results["benchmark_period_return"] + 1.0) / (results["b_delta"] + 1.0) - 1.0
        results["r_delta"] = results["a_delta"] - results["b_delta"]
        var_set = {"last": 0.0}

        def cum_func(x):
            cur = (var_set["last"] + 1.0) * (1.0 + x) - 1.0
            var_set["last"] = cur
            return cur

        results["relative_ratio"] = results["r_delta"].map(cum_func)
        relative_cum_return_plot = results[["index", "relative_ratio"]].values.tolist()
        return relative_cum_return_plot
    except BaseException:
        return []


def get_ago_portfolio_dict(daily_schema: List[StrategyDailySchema], run_date: datetime):
    record_datas = RecordDatas(daily_schema, run_date)
    return {
        "week": record_datas.week_ago_portfolio_dict,
        "ten_days": record_datas.ten_days_ago_portfolio_dict,
        "month": record_datas.month_ago_portfolio_dict,
        "three_month": record_datas.three_month_ago_portfolio_dict,
        "six_month": record_datas.six_month_ago_portfolio_dict,
        "year": record_datas.year_ago_portfolio_dict,
    }
