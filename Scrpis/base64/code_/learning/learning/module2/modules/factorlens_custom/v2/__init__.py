import learning.module2.common.interface as I
import pandas as pd
from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.utils import smart_list
from sdk.datasource.extensions.bigshared.dataframe import evalex
from sdk.utils import BigLogger

from .featureperformance import FeaturePerformance
from .judge_market import judge_market
from .render import RenderHtml

bigquant_public = True
bigquant_cacheable = True
bigquant_category = "因子研究"
bigquant_friendly_name = "因子分析(自定义)"
bigquant_doc_url = "https://bigquant.com/docs/"
log = BigLogger(bigquant_friendly_name)


def bigquant_run(
    features: I.port("输入因子-来自输入特征列表等模块", specific_type_name="DataSource"),
    factor_data: I.port("因子数据", specific_type_name="列表|DataSource"),
    start_date: I.str("开始日期，分析数据开始日期", can_set_liverun_param=True) = "2019-01-01",
    end_date: I.str("结束日期，分析数据结束日期", can_set_liverun_param=True) = "2019-12-31",
    instrument_list: I.code("代码列表，每行一个，如果指定，market参数将被忽略", auto_complete_type="stocks") = None,
    # market: I.choice("交易市场", ["CN_FUND", "CN_FUTURE", "CN_MUTFUND"]) = "CN_FUND",
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

    instruments = []
    if isinstance(instrument_list, list) and len(instrument_list):
        instruments = instrument_list
    elif isinstance(instrument_list, str) and instrument_list.strip():
        instruments = smart_list(instrument_list, sort=True)

    factor_exprs = features.read()
    factor_name = factor_exprs[0]
    factor_data = factor_data.read()

    market = judge_market(factor_data)  # 根据传入进来的因子数据判定市场类型

    if market == "CN_FUND":
        if instruments:
            df = DataSource("bar1d_CN_FUND").read(instruments=instruments, start_date=start_date, end_date=end_date)
        else:
            df = DataSource("bar1d_CN_FUND").read(start_date=start_date, end_date=end_date)
    if market == "CN_FUTURE":
        if instruments:
            df = DataSource("bar1d_CN_FUTURE").read(instruments=instruments, start_date=start_date, end_date=end_date)
        else:
            df = DataSource("bar1d_CN_FUTURE").read(start_date=start_date, end_date=end_date)
    if market == "CN_STOCK_A":
        if instruments:
            df = DataSource("bar1d_CN_STOCK_A").read(instruments=instruments, start_date=start_date, end_date=end_date)
        else:
            df = DataSource("bar1d_CN_STOCK_A").read(start_date=start_date, end_date=end_date)
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

    # 行情数据与因子数据合并，保证数据像行情数据一样的的连续性
    merge_df = pd.merge(df[["date", "instrument", "close"]], factor_data[["date", "instrument", factor_name]], on=["date", "instrument"], how="left")

    fp = FeaturePerformance(
        start_date, end_date, rebalance_period, buy_commission_rate, sell_commission_rate, ic_method, quantile_num, is_standardlize, is_winsorize
    )

    # 因子数据、绩效数据
    data_1, data_2 = fp.process(merge_df, [factor_name])

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
    ret_max_1d = data_2[0]["summary"]["QuantileReturns"]["stats"]["top{}_ret".format(quantile_num - 1)]["近1日收益率"]
    ret_max_1w = data_2[0]["summary"]["QuantileReturns"]["stats"]["top{}_ret".format(quantile_num - 1)]["近1周收益率"]
    ret_max_1m = data_2[0]["summary"]["QuantileReturns"]["stats"]["top{}_ret".format(quantile_num - 1)]["近1月收益率"]
    ret_max_1y = data_2[0]["summary"]["QuantileReturns"]["stats"]["top{}_ret".format(quantile_num - 1)]["年化收益率"]
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
    with open("/var/tmp/.status", "w") as f:
            f.write("done")
    return Outputs(data_1=data_1, data_2=data_2, data_3=data_3)


# 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
def bigquant_postrun(outputs):
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
