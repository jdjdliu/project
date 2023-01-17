import os

import learning.api.tools as T
import learning.module2.common.interface as I  # noqa
import numpy as np
import pandas as pd
from bigalpha.settings import SW_CHANGE_DATE
from dateutil.parser import isoparse  # noqa
from learning.api import M
from learning.module2.common.data import DataSource, Outputs
from sdk.alpha import AlphaClient, CreateIndexPerformanceSchema, IndexParameter, PerformanceSource, UpdateBacktestRequest
from sdk.auth import Credential
from sdk.utils import BigLogger

from .module_renderer import ModuleRenderer

SW_CHANGE_DATE = os.getenv("SW_CHANGE_DATE", "2021-07-30")


# 是否自动缓存结果
bigquant_cacheable = True
# bigquant_deprecated = '请更新到 ${MODULE_NAME} 最新版本'
bigquant_deprecated = None

# 模块是否外部可见，对外的模块应该设置为True
bigquant_public = True

# 是否开源
bigquant_opensource = False

bigquant_author = "BigQuant"

# 模块接口定义
bigquant_category = "量化分析"
bigquant_friendly_name = "自定义指数构建"
bigquant_doc_url = "https://bigquant.com/docs/"

log = BigLogger(bigquant_friendly_name)
# 行业分布映射
INDUSTRY_NAMES = {
    110000: "农林牧渔",
    210000: "采掘",
    220000: "化工",
    230000: "钢铁",
    240000: "有色金属",
    270000: "电子",
    280000: "汽车",
    330000: "家用电器",
    340000: "食品饮料",
    350000: "纺织服装",
    360000: "轻工制造",
    370000: "医药生物",
    410000: "公用事业",
    420000: "交通运输",
    430000: "房地产",
    450000: "商业贸易",
    460000: "休闲服务",
    480000: "银行",
    490000: "非银金融",
    510000: "综合",
    610000: "建筑材料",
    620000: "建筑装饰",
    630000: "电气设备",
    640000: "机械设备",
    650000: "国防军工",
    710000: "计算机",
    720000: "传媒",
    730000: "通信",
    0: "其他",
}

# 2021行业分布映射
INDUSTRY_NAMES_NEW = {
    110000: "农林牧渔",
    220000: "基础化工",
    230000: "钢铁",
    240000: "有色金属",
    270000: "电子",
    280000: "汽车",
    330000: "家用电器",
    340000: "食品饮料",
    350000: "纺织服装",
    360000: "轻工制造",
    370000: "医药生物",
    410000: "公用事业",
    420000: "交通运输",
    430000: "房地产",
    450000: "商贸零售",
    460000: "社会服务",
    480000: "银行",
    490000: "非银金融",
    510000: "综合",
    610000: "建筑材料",
    620000: "建筑装饰",
    630000: "电力设备",
    640000: "机械设备",
    650000: "国防军工",
    710000: "计算机",
    720000: "传媒",
    730000: "通信",
    740000: "煤炭",
    750000: "石油石化",
    760000: "环保",
    770000: "美容护理",
    0: "其他",
}
STOCK_POOL_MAP = {
    "全市场": "all",
    "沪深300": "in_csi300",
    "中证500": "in_csi500",
    "中证800": "in_csi800",
}

BENCHMARK_MAP = {
    "沪深300": "000300.HIX",
    "中证500": "000905.HIX",
    "中证800": "000906.HIX",
    "中证1000": "000852.HIX",
    "创业板": "399006.ZIX",
}


def get_daily_ret(df):
    df["ret"] = df["close"] / df["close"].shift(1) - 1
    return df


def get_market_cap_weight_ret(df):
    df["adj_ret"] = df["market_cap"] * df["ret"] / df["market_cap"].sum()
    return df["adj_ret"].sum()


def get_equal_weight_ret(df):
    return df["ret"].mean()


def bigquant_run(
    input_1: I.port("因子数据", specific_type_name="DataSource"),
    factor_name: I.str("因子名"),
    weight_method: I.choice("加权权重", values=["等权重", "市值加权"]) = "市值加权",
    stock_pool: I.choice("股票池", values=list(STOCK_POOL_MAP.keys())) = "全市场",
    benchmark: I.choice("基准", values=list(BENCHMARK_MAP.keys())) = "中证500",
    sort: I.choice("排序,", values=["升序", "降序"]) = "升序",
    rebalance_days: I.int("调仓天数") = 2,
    cost: I.float("交易成本") = 0.001,
    quantile_ratio: I.float("指数分位数") = 0.1,
    stock_num: I.int("指数成分数") = 0,
) -> [I.port("data_1", "data_1"), I.port("data_2", "data_2"), I.port("data_3", "data_3"),]:
    # 因子数据
    factor_data = input_1.read()

    # 提取开始日期和结束日期
    start_date = factor_data.date.min()
    assert start_date > pd.Timestamp("2013-01-01"), "因子数据起始时间不能小于2013年1月1日"
    end_date = factor_data.date.max()

    # 提取日线行情数据
    bar1d_data = DataSource("bar1d_CN_STOCK_A").read(start_date=start_date, end_date=end_date, fields=["instrument", "date", "close"])
    bar1d_data = bar1d_data.groupby("instrument").apply(get_daily_ret)

    # 提取公司基本数据
    basic_info = DataSource("basic_info_CN_STOCK_A").read(fields=["instrument", "name", "list_board"])

    # 提取行业数据
    industry_data = DataSource("industry_CN_STOCK_A").read(
        start_date=start_date, end_date=end_date, fields=["instrument", "date", "industry_sw_level1"]
    )
    if end_date < pd.Timestamp(SW_CHANGE_DATE):
        industry_data["industry_name"] = industry_data["industry_sw_level1"].apply(lambda x: INDUSTRY_NAMES.get(x))
    else:
        industry_data_2014 = industry_data[industry_data.date < SW_CHANGE_DATE]
        industry_data_2014["industry_name"] = industry_data_2014["industry_sw_level1"].apply(lambda x: INDUSTRY_NAMES.get(x))
        industry_data_2021 = industry_data[industry_data.date >= SW_CHANGE_DATE]
        industry_data_2021["industry_name"] = industry_data_2021["industry_sw_level1"].apply(lambda x: INDUSTRY_NAMES_NEW.get(x))
        industry_data = pd.concat([industry_data_2014, industry_data_2021])

    # 提取市值数据
    market_cap_data = DataSource("market_value_CN_STOCK_A").read(
        start_date=start_date, end_date=end_date, fields=["instrument", "date", "market_cap"]
    )

    # 读取指数 成分数据  默认会返回全部证券代码数据, 通过指定参数 instruments 可以读取到指定的证券代码数据
    index_constituent_df = DataSource("index_constituent_CN_STOCK_A").read(start_date=start_date, end_date=end_date)
    if stock_pool == "全市场":
        stock_pool_data = index_constituent_df[["instrument", "date"]]
    elif stock_pool in ["沪深300", "中证500", "中证800"]:
        stock_pool_data = index_constituent_df[index_constituent_df[STOCK_POOL_MAP[stock_pool]] == 1][["instrument", "date"]]

    # 因子值前移
    def shift_factor(df, factor_name):
        df[factor_name] = df[factor_name].shift(1)
        return df

    factor_data = factor_data.groupby("instrument").apply(shift_factor, factor_name=factor_name)

    # 合并数据
    factor_data = factor_data.merge(market_cap_data, how="left", on=["instrument", "date"])
    factor_data = factor_data.merge(stock_pool_data, how="left", on=["instrument", "date"])
    factor_data = factor_data.merge(bar1d_data, how="left", on=["instrument", "date"])
    factor_data = factor_data.merge(industry_data, how="left", on=["instrument", "date"])
    factor_data = factor_data.merge(basic_info, how="left", on=["instrument"])

    # 删除nan
    factor_data.dropna(inplace=True)

    # 因子值乘以因子的方向
    if sort == "降序":
        factor_data[factor_name] = factor_data[factor_name] * -1

    date_ins = factor_data[["instrument", "date"]]

    # 提取该时间段的交易日期，并转化为df和list两种格式
    date = DataSource("all_trading_days").read(start_date=start_date, end_date=end_date)
    date = date[date.country_code == "CN"]
    date_df = date[["date"]]
    # date_list = date["date"].tolist()

    # 计算出换仓周期以及换仓时的date和instrument数据
    period_date_list = date["date"].tolist()[::rebalance_days]
    period_date_df = date_ins[date_ins.date.isin(period_date_list)]

    # 把换仓时的date和instrument面板数据转化为时间序列数据
    period_date_df = period_date_df.groupby("date").apply(lambda x: x["instrument"].tolist())
    period_date_df = pd.DataFrame(period_date_df).reset_index()

    # 用整个时间段的交易日期进行填充并把时间序列数据转化为面板数据
    period_date_df = date_df.merge(period_date_df, how="left", on="date").fillna(method="ffill")
    period_date_df = period_date_df.set_index("date")[0].apply(pd.Series).stack().reset_index(level=0).rename(columns={0: "instrument"})

    # 因子值合并起来
    period_date_factor = period_date_df.merge(factor_data, how="inner", on=["date", "instrument"]).reset_index()

    display_data = period_date_factor[period_date_factor.date == end_date]

    # 再选出因子值排名靠前的n支
    period_date_factor = period_date_factor[["instrument", "date", "ret", factor_name, "market_cap"]]

    def get_quantile_df(df, factor_name, quantile_ratio):
        quantile_df = df.sort_values(factor_name, ascending=False)[: np.int(len(df) * quantile_ratio)]
        return quantile_df

    if stock_num > 0 and quantile_ratio == 0:
        period_date_factor = (
            period_date_factor.groupby("date").apply(lambda x: x.sort_values(factor_name, ascending=False)[:stock_num]).reset_index(drop=True)
        )
    elif stock_num == 0 and quantile_ratio > 0:
        period_date_factor = (
            period_date_factor.groupby("date").apply(get_quantile_df, factor_name=factor_name, quantile_ratio=quantile_ratio).reset_index(drop=True)
        )

    performance_stock_num = period_date_factor.groupby("date").count().tail(1)["ret"].values[0]

    # 按不同的加权方式计算收益
    if weight_method == "等权重":
        display_data["weight"] = 1 / len(display_data)
        display_data.sort_values("weight", ascending=False, inplace=True)
        period_date_factor = period_date_factor.groupby("date").apply(get_equal_weight_ret).reset_index()
    elif weight_method == "市值加权":
        display_data["weight"] = display_data["market_cap"] / display_data["market_cap"].sum()
        display_data.sort_values("weight", ascending=False, inplace=True)
        period_date_factor = period_date_factor.groupby("date").apply(get_market_cap_weight_ret).reset_index()
    period_date_factor.rename(columns={0: "ret"}, inplace=True)

    # 减去每日的单边手续费
    period_date_factor["ret"] = period_date_factor["ret"] - cost
    period_date_factor["index_price"] = np.cumproduct(period_date_factor.ret + 1) * 1000

    # 读取基准走势数据并合并画图
    bm_code = BENCHMARK_MAP[benchmark]
    bm_df = DataSource("bar1d_index_CN_STOCK_A").read(instruments=bm_code, start_date=start_date, end_date=end_date)
    bm_df["bm_ret"] = bm_df["close"].pct_change().fillna(0)
    plot_data = pd.merge(period_date_factor, bm_df[["date", "bm_ret"]], how="inner", on=["date"])

    raw_perf = pd.DataFrame()
    raw_perf["date"] = plot_data["date"]
    raw_perf["returns"] = plot_data["ret"]
    raw_perf["benchmark_period_return"] = np.cumprod(plot_data["bm_ret"] + 1) - 1
    raw_perf["algorithm_period_return"] = np.cumprod(plot_data["ret"] + 1) - 1
    raw_perf["date"] = raw_perf["date"].dt.tz_localize("UTC").dt.tz_convert("Europe/Berlin")
    raw_perf.set_index("date", inplace=True)

    # 市道分析
    plot_data["market_info"] = plot_data["ret"].apply(
        lambda x: "暴跌"
        if x < -0.03
        else (
            "大跌"
            if -0.03 <= x < -0.02
            else (
                "下跌"
                if -0.02 <= x < -0.01
                else (
                    "小幅下跌" if -0.01 <= x < 0 else ("小幅上扬" if 0 <= x < 0.01 else ("上涨" if 0.01 <= x < 0.02 else ("大涨" if 0.02 <= x < 0.03 else "暴涨")))
                )
            )
        )
    )
    plot_data["bm_market_info"] = plot_data["bm_ret"].apply(
        lambda x: "暴跌"
        if x < -0.03
        else (
            "大跌"
            if -0.03 <= x < -0.02
            else (
                "下跌"
                if -0.02 <= x < -0.01
                else (
                    "小幅下跌" if -0.01 <= x < 0 else ("小幅上扬" if 0 <= x < 0.01 else ("上涨" if 0.01 <= x < 0.02 else ("大涨" if 0.02 <= x < 0.03 else "暴涨")))
                )
            )
        )
    )
    market_info = plot_data[["ret", "market_info"]].groupby("market_info").mean().reset_index()
    bm_market_info = plot_data[["bm_ret", "bm_market_info"]].groupby("bm_market_info").mean().reset_index()
    bm_market_info.rename(columns={"bm_market_info": "market_info"}, inplace=True)
    plot_data["bm_price"] = np.cumprod(plot_data["bm_ret"] + 1) * 1000
    plot_data = plot_data[["date", "index_price", "bm_price"]].set_index("date")
    plot_data.rename(columns={"index_price": "因子构建指数", "bm_price": "%s" % benchmark}, inplace=True)

    data_2 = display_data[0:10][["instrument", "name", "industry_name", "list_board", "weight"]]

    data_3 = pd.merge(market_info, bm_market_info, on="market_info", how="outer")

    data_1 = DataSource.write_df(plot_data)
    data_2 = DataSource.write_df(data_2)
    data_3 = DataSource.write_df(data_3)
    data_4 = DataSource.write_df(raw_perf)

    credential = Credential.from_env()
    task_id = os.getenv("TASK_ID", "")
    run_mode = os.getenv("RUN_MODE", None)

    if isinstance(factor_name, str):
        factor_name = [factor_name]

    if run_mode in ["ALPHA_BACKTEST", "ALPHA_ALPHA"]:
        # create performance
        performance = CreateIndexPerformanceSchema(
            run_datetime=isoparse(os.getenv("TRADING_DATE", start_date)),
            source=PerformanceSource.BACKTEST if run_mode == "ALPHA_BACKTEST" else PerformanceSource.ALPHA,
            total_revenue=float(data_4.read()["algorithm_period_return"][-1]),
            stock_num=performance_stock_num,
        )
        AlphaClient.create_index_backtest_performance(task_id=task_id, request=performance, credential=credential)

    with open("/var/tmp/.status", "w") as f:
        f.write("done")
    return Outputs(data_1=data_1, data_2=data_2, data_3=data_3, data_4=data_4)


# 后处理函数，可选。输入是主函数的输出，可以在这里对数据做处理，或者返回更友好的outputs数据格式。此函数输出不会被缓存。
def bigquant_postrun(outputs):

    plot_data = outputs.data_1.read()  # 读取基准走势数据并合并画图
    data3 = outputs.data_3.read()
    data2 = outputs.data_2.read()
    data2.sort_values("weight", ascending=False, inplace=True)
    raw_perf = outputs.data_4
    T.plot(plot_data, title="指数与基准价格走势图")

    M.strategy_interval_return.v1(raw_perf=raw_perf, keep_n_yearly=6, keep_n_quarterly=8, keep_n_monthly=8)

    data3.set_index("market_info", inplace=True)
    data3.sort_values("ret", inplace=True)
    data3.index.name = ""
    data3.rename(columns={"ret": "指数", "bm_ret": "基准"}, inplace=True)
    T.plot(data3[["指数", "基准"]], title="市道分析", chart_type="column")
    multi_module_renderer = ModuleRenderer(data2)
    multi_module_renderer.render()

    return outputs
