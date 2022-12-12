import datetime

import learning.module2.common.interface as I  # noqa
import pandas as pd
from learning.module2.common.data import DataSource, Outputs
from sdk.utils import BigLogger

from .perf_render import render

bigquant_cacheable = True

bigquant_category = "策略绩效评价"
bigquant_friendly_name = "策略区间收益"
bigquant_doc_url = ""
log = BigLogger(bigquant_friendly_name)


def _cal_period_ret(df):
    """
    计算区间收益率,返回该区间基准的收益率，及策略的收益率，为tuple类型

    """
    bm_ret = df.tail(1)["benchmark_period_return"].values[0] / df.head(1)["benchmark_period_return"].values[0] - 1
    strategy_ret = df.tail(1)["algorithm_period_return"].values[0] / df.head(1)["algorithm_period_return"].values[0] - 1
    return round(bm_ret, 3), round(strategy_ret, 3)


def _gen_period_ret_data(name, perf, start_date, end_date, period_start_date=None, days=None):
    """
    生成区间收益的表格数据

    """
    if period_start_date is None and days is not None:
        period_start_date = end_date - datetime.timedelta(days=int(days))
    if start_date <= period_start_date:
        bm_ret, strategy_ret = _cal_period_ret(perf[perf.index >= period_start_date])
    else:
        bm_ret = "--"
        strategy_ret = "--"
    return [name, strategy_ret, bm_ret]


def _cal_fixed_term_ret(perf):
    """
    计算年度、季度、月度的收益

    """
    perf["year"] = perf.index.to_series().apply(lambda x: x.year).apply(str)
    perf["month"] = perf.index.to_series().apply(lambda x: x.month).apply(str)
    perf["quarter"] = perf.index.to_series().apply(lambda x: (x.month - 1) // 3 + 1).apply(str)
    perf["year_quarter"] = perf["year"] + "年" + perf["quarter"] + "季"
    perf["year_month"] = perf["year"] + "年" + perf["month"] + "月"

    return (
        perf.groupby("year").apply(_cal_period_ret),
        perf.groupby("year_quarter").apply(_cal_period_ret),
        perf.groupby("year_month", sort=False).apply(_cal_period_ret),
    )


def _gen_table_data(keep_n, data):
    """
    生成展示表格需要的年、月、季度数据格式

    """
    if keep_n > 0:
        data = data.tail(keep_n)
    index = data.index
    ret_data = list(zip(*data.values.tolist()))
    bm_list = list(ret_data[0])
    strategy_list = list(ret_data[1])
    bm_list.insert(0, "基准")
    strategy_list.insert(0, "本策略")
    return {"index": index, "bm": bm_list, "strategy": strategy_list}


def bigquant_run(
    raw_perf: I.port("回测详细数据", specific_type_name="DataSource"),
    keep_n_yearly: I.int("最大展示时间单位(年)") = 6,
    keep_n_quarterly: I.int("最大展示时间单位(季)") = 8,
    keep_n_monthly: I.int("最大展示时间单位(月)") = 8,
) -> [I.port("策略收益分布指标", "data")]:
    """
    对传入的回测结果数据进行区间收益分析，输出各时间段及各区间的策略及基准收益情况

    """

    perf = raw_perf.read()
    perf["bm_returns"] = (perf["benchmark_period_return"] + 1).pct_change()
    ret_data = perf[["returns", "bm_returns", "benchmark_period_return", "algorithm_period_return"]]
    ret_data = ret_data + 1

    results = {}
    period_ret = {}  # 区间收益数据
    range_ret = {}  # 年、月、季度收益数据
    strategy_ret_data = ["本策略"]
    bm_ret_data = ["基准"]

    end_date = perf.index.max()
    start_date = perf.index.min()
    current_year_start_date = pd.Timestamp(tz="UTC", year=end_date.year, month=1, day=1)

    # 区间收益
    params = [
        ["product_setup_ret", "成立至今", start_date, None],
        ["current_year_ret", "年初至今", current_year_start_date, None],
        ["one_month_ret", "近一月", None, 30 * 1],
        ["three_month_ret", "近三月", None, 30 * 3],
        ["half_year_ret", "近半年", None, 365 * 0.5],
        ["one_year_ret", "近一年", None, 365 * 1],
        ["two_year_ret", "近两年", None, 365 * 2],
        ["three_year_ret", "近三年", None, 365 * 3],
        ["five_year_ret", "近五年", None, 365 * 5],
    ]

    for param in params:
        res = _gen_period_ret_data(param[1], perf=ret_data, start_date=start_date, end_date=end_date, period_start_date=param[2], days=param[3])
        period_ret[param[0]] = res
        strategy_ret_data.append(res[1])
        bm_ret_data.append(res[2])

    period_ret["index"] = list(zip(*params))[1]
    period_ret["bm"] = bm_ret_data
    period_ret["strategy"] = strategy_ret_data

    # 年度、季度、月度收益
    yearly, quarterly, monthly = _cal_fixed_term_ret(ret_data)
    range_ret["year"] = _gen_table_data(keep_n_yearly, yearly)
    range_ret["quarter"] = _gen_table_data(keep_n_quarterly, quarterly)
    range_ret["month"] = _gen_table_data(keep_n_monthly, monthly)

    results["period_ret"] = period_ret
    results["range_ret"] = range_ret

    return Outputs(data=DataSource.write_pickle(results))


def bigquant_postrun(outputs):

    render(outputs.data.read())

    return outputs
