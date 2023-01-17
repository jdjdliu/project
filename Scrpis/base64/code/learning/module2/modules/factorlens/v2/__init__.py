import json
import os
import uuid
from collections import OrderedDict
from datetime import datetime

import learning.module2.common.interface as I  # noqa
import pandas as pd
from dateutil.parser import isoparse  # noqa
from learning.module2.common.data import DataSource, Outputs
from learning.module2.common.utils import display_df, smart_dict, smart_list
from sdk import alpha
from sdk.alpha import AlphaClient, AlphaParameter, CreatePerformanceSchema, PerformanceSource, UpdateBacktestRequest
from sdk.auth import Credential
from sdk.datasource.api.v5.updatebigdatasource import UpdateDataSource
from sdk.utils import BigLogger

bigquant_public = True
bigquant_cacheable = True
bigquant_category = "因子研究"
bigquant_friendly_name = "因子分析"
bigquant_doc_url = "https://bigquant.com/docs/"
log = BigLogger(bigquant_friendly_name)

DEFAULT_FACTOR_FIELDS = """# 定义因子名称
# {
#     "列名": {'name': "因子名", 'desc': "因子描述"},
#     "列名": {'name': "因子名", 'desc': "因子描述"},
#     ... 
# }
{}
"""

STOCK_POOLS = OrderedDict(
    [
        ("全市场", "all"),
        ("沪深300", "in_csi300_0"),
        ("中证500", "in_csi500_0"),
        ("中证800", "in_csi800_0"),
    ]
)


NEUTRALIZATIO_RISK_FACTORS = OrderedDict(
    [
        ("行业", "industry"),
        ("市值", "size"),
    ]
)


METRICS = OrderedDict(
    [
        ("因子表现概览", "QuantileReturns"),
        ("因子分布", "BasicDescription"),
        ("因子行业分布", "Industry"),
        ("因子市值分布", "MarketCap"),
        ("IC分析", "IC"),
        ("买入信号重合分析", "RebalanceOverlap"),
        ("因子估值分析", "PBRatio"),
        ("因子拥挤度分析", "Turnover"),
        ("因子值最大/最小股票", "Stocks"),
        ("表达式因子值", "FactorValue"),
        ("多因子相关性分析", "FactorPairwiseCorrelationMerged"),
    ]
)

RETURNS_CALCULATION_METHODS = OrderedDict(
    [
        ("累乘", "cumprod"),
        ("累加", "cumsum"),
    ]
)

BENCHMARKS = OrderedDict(
    [
        ("无", "none"),
        ("沪深300", "000300.HIX"),
        ("中证500", "000905.HIX"),
        ("中证800", "000906.HIX"),
    ]
)

REBALANCE_PRICE = OrderedDict(
    [
        ("close_0", "close_0"),
        ("open_0", "open_0"),
        ("vwap", "vwap"),
    ]
)


# 构建因子保存数据
def get_save_data(ap, data):
    stock_pool_dic = {"all": "全市场", "in_csi300_0": "沪深300", "in_csi500_0": "中证500", "in_csi800_0": "中证800"}  # 股票市场
    cal_method_dic = {"cumprod": "累乘", "cumsum": "累加"}  # 收益计算方式
    benchmark_dic = {"none": "无", "000300.HIX": "沪深300", "000905.HIX": "中证500", "000906.HIX": "中证800"}  # 收益率基准
    yes_no_dic = {1: "是", 0: "否"}
    neutralization_dic = {"": "无", "industry,size": "行业,市值", "industry": "行业", "size": "市值"}  # 中性化
    factor_lst = data["data"]["factors"]
    factor_dict = {}

    for i in range(len(factor_lst)):
        factor = {}
        # 参数部分
        paras = {}
        paras["开始日期"] = factor_lst[i]["results"][0]["options"]["BacktestInterval"][0]
        paras["结束日期"] = factor_lst[i]["results"][0]["options"]["BacktestInterval"][1]
        paras["调仓周期"] = factor_lst[i]["results"][0]["options"]["RebalancePeriod"]
        paras["延迟建仓天数"] = factor_lst[i]["results"][0]["options"]["DelayRebalanceDays"]

        stock_pool = factor_lst[i]["results"][0]["options"]["StockPool"]
        paras["股票池"] = stock_pool_dic[stock_pool]

        paras["分组数量"] = factor_lst[i]["results"][0]["options"]["QuantileCount"]
        paras["手续费及滑点"] = factor_lst[i]["results"][0]["options"]["CommissionRates"]
        paras["收益价格"] = factor_lst[i]["results"][0]["options"]["RebalancePrice"]

        cal_method = factor_lst[i]["results"][0]["options"]["ReturnsCalculationMethod"]
        paras["收益计算方式"] = cal_method_dic[cal_method]

        neutralization = factor_lst[i]["results"][0]["options"]["Neutralization"]
        paras["中性化风险因子"] = neutralization_dic[neutralization]

        benchmark = factor_lst[i]["results"][0]["options"]["Benchmark"]
        paras["收益率基准"] = benchmark_dic[benchmark]

        paras["移除新股"] = factor_lst[i]["results"][0]["options"].get("DropNewStocks")
        paras["移除涨跌停股票"] = yes_no_dic[factor_lst[i]["results"][0]["options"].get("DropPriceLimitStocks")]
        paras["移除ST股票"] = yes_no_dic[factor_lst[i]["results"][0]["options"].get("DropSTStocks")]
        paras["移除停牌股票"] = yes_no_dic[factor_lst[i]["results"][0]["options"].get("DropSuspendedStocks")]
        paras["移除涨跌停股票"] = yes_no_dic[factor_lst[i]["results"][0]["options"].get("DropPriceLimitStocks")]
        paras["因子去极值"] = yes_no_dic[factor_lst[i]["results"][0]["options"].get("Cutoutliers")]
        paras["因子标准化"] = yes_no_dic[factor_lst[i]["results"][0]["options"].get("Normalization")]
        paras["原始因子值覆盖率"] = factor_lst[i]["results"][0]["options"].get("FactorCoverage")
        paras["用户数据合并方式"] = factor_lst[i]["results"][0]["options"].get("UserDataMerge")

        factor["options"] = paras

        # 指标部分
        metrics = {}
        # IC
        if factor_lst[i]["results"][0]["summary"].get("IC"):
            ic_mean = factor_lst[i]["results"][0]["summary"]["IC"]["ic_mean"]
            metrics["IC均值"] = ic_mean if ic_mean is not None else ""

            ic_std = factor_lst[i]["results"][0]["summary"]["IC"]["ic_std"]
            metrics["IC标准差"] = ic_std if ic_std is not None else ""

            ir = factor_lst[i]["results"][0]["summary"]["IC"]["ir"]
            metrics["IR值"] = ir if ir is not None else ""

            ic_significance_ratio = factor_lst[i]["results"][0]["summary"]["IC"]["ic_significance_ratio"]
            metrics["|IC| > 0.02比率"] = ic_significance_ratio if ic_significance_ratio is not None else ""

        # 表现概览
        # 最小分位
        if factor_lst[i]["results"][0]["summary"].get("QuantileReturns"):
            metrics["累计收益(最小分位)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"]["summary_l_0"]["returns_total"]
            metrics["近1年收益(最小分位)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"]["summary_l_0"]["returns_255"]
            metrics["近3月收益(最小分位)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"]["summary_l_0"]["returns_66"]
            metrics["近1月收益(最小分位)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"]["summary_l_0"]["returns_22"]
            metrics["近1周收益(最小分位)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"]["summary_l_0"]["returns_5"]
            metrics["昨日收益(最小分位)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"]["summary_l_0"]["returns_1"]
            metrics["最大回撤(最小分位)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"]["summary_l_0"]["max_drawdown"]
            metrics["盈亏比(最小分位)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"]["summary_l_0"]["profit_loss_ratio"]
            metrics["胜率(最小分位)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"]["summary_l_0"]["win_ratio"]
            metrics["夏普比率(最小分位)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"]["summary_l_0"]["sharpe_ratio"]
            metrics["收益波动率(最小分位)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"]["summary_l_0"]["returns_volatility"]

            # 最大分位
            metrics["累计收益(最大分位)"] = list(factor_lst[i]["results"][0]["summary"]["QuantileReturns"].values())[1]["returns_total"]
            metrics["近1年收益(最大分位)"] = list(factor_lst[i]["results"][0]["summary"]["QuantileReturns"].values())[1]["returns_255"]
            metrics["近3月收益(最大分位)"] = list(factor_lst[i]["results"][0]["summary"]["QuantileReturns"].values())[1]["returns_66"]
            metrics["近1月收益(最大分位)"] = list(factor_lst[i]["results"][0]["summary"]["QuantileReturns"].values())[1]["returns_22"]
            metrics["近1周收益(最大分位)"] = list(factor_lst[i]["results"][0]["summary"]["QuantileReturns"].values())[1]["returns_5"]
            metrics["昨日收益(最大分位)"] = list(factor_lst[i]["results"][0]["summary"]["QuantileReturns"].values())[1]["returns_1"]
            metrics["最大回撤(最大分位)"] = list(factor_lst[i]["results"][0]["summary"]["QuantileReturns"].values())[1]["max_drawdown"]
            metrics["盈亏比(最大分位)"] = list(factor_lst[i]["results"][0]["summary"]["QuantileReturns"].values())[1]["profit_loss_ratio"]
            metrics["胜率(最大分位)"] = list(factor_lst[i]["results"][0]["summary"]["QuantileReturns"].values())[1]["win_ratio"]
            metrics["夏普比率(最大分位)"] = list(factor_lst[i]["results"][0]["summary"]["QuantileReturns"].values())[1]["sharpe_ratio"]
            metrics["收益波动率(最大分位)"] = list(factor_lst[i]["results"][0]["summary"]["QuantileReturns"].values())[1]["returns_volatility"]

            # 多空组合
            metrics["累计收益(多空组合)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_total")
            metrics["近1年收益(多空组合)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_255")
            metrics["近3月收益(多空组合)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_66")
            metrics["近1月收益(多空组合)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_22")
            metrics["近1周收益(多空组合)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_5")
            metrics["昨日收益(多空组合)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_1")
            metrics["最大回撤(多空组合)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"].get("summary_l_top_bottom", {}).get("max_drawdown")
            metrics["盈亏比(多空组合)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"].get("summary_l_top_bottom", {}).get("profit_loss_ratio")
            metrics["胜率(多空组合)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"].get("summary_l_top_bottom", {}).get("win_ratio")
            metrics["夏普比率(多空组合)"] = factor_lst[i]["results"][0]["summary"]["QuantileReturns"].get("summary_l_top_bottom", {}).get("sharpe_ratio")
            metrics["收益波动率(多空组合)"] = (
                factor_lst[i]["results"][0]["summary"]["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_volatility")
            )

        factor["metrics"] = metrics

        # 因子数据
        factor_column = factor_lst[i]["meta"]["name"]
        if "date" not in ap.feature_df.columns or "instrument" not in ap.feature_df.columns:
            print("'date' or 'instrument' is not in factor data")
        need_columns = ["date", "instrument", "RAW_FACTOR"]
        factor["datasource"] = ap.feature_df[need_columns].rename(columns={"RAW_FACTOR": factor_column})

        # 列名
        factor["column_name"] = factor_column

        # 因子表达式
        factor["expr"] = factor_lst[i]["meta"]["expr"]

        # 每个因子所有信息
        factor_dict[factor_lst[i]["meta"]["name"]] = factor

    return factor_dict


def bigquant_run(
    features: I.port("输入因子-来自输入特征列表等模块", specific_type_name="列表|DataSource"),
    user_factor_data: I.port("用户自定义特征数据", specific_type_name="DataSource", optional=True) = None,
    title: I.str("分析报告标题, {factor_name}表示因子名") = "因子分析: {factor_name}",
    start_date: I.str("开始日期，分析数据开始日期", can_set_liverun_param=True) = "2019-01-01",
    end_date: I.str("结束日期，分析数据结束日期", can_set_liverun_param=True) = datetime.today().strftime("%Y-%m-%d"),
    rebalance_period: I.int("调仓周期(交易日)，单位为交易日") = 22,
    delay_rebalance_days: I.int("延迟建仓天数") = 0,
    rebalance_price: I.choice("收益价格", list(REBALANCE_PRICE.keys())) = "close_0",
    stock_pool: I.choice("股票池", list(STOCK_POOLS.keys())) = "全市场",
    quantile_count: I.int("分组数量") = 5,
    commission_rate: I.float("手续费及滑点") = 0.0016,
    returns_calculation_method: I.choice("收益计算方式", list(RETURNS_CALCULATION_METHODS.keys())) = "累乘",
    benchmark: I.choice("收益率基准，选中无则计算绝对收益，选中其他基准则计算对应基准的相对收益(分组收益计算)", list(BENCHMARKS.keys())) = "无",
    drop_new_stocks: I.int("移除新股") = 60,
    drop_price_limit_stocks: I.bool("移除涨跌停股票") = False,
    drop_st_stocks: I.bool("移除ST股票") = False,
    drop_suspended_stocks: I.bool("移除停牌股票") = False,
    cutoutliers: I.bool("因子去极值") = True,
    normalization: I.bool("因子标准化") = True,
    neutralization: I.choice("中性化风险因子。利用回归得到一个与风险因子线性无关的因子，用残差作为中性化后的新因子", ["行业", "市值"], multi=True) = [],  # noqa
    metrics: I.choice("指标。勾选需要输出的指标，不勾选为不输出", list(METRICS.keys()), multi=True) = list(METRICS.keys()),  # noqa
    factor_coverage: I.float("原始因子值覆盖率", min=0.0, max=1.0) = 0.5,
    user_data_merge: I.choice("用户数据合并方式", ["left", "inner"]) = "left",
) -> [I.port("数据", "data"), I.port("保存数据,连接保存因子模块", "save_data"),]:
    """
    因子分析。对输入的因子公式或者因子数据，做因子分析。
    """
    from bigalpha.api import AlphaPerformance, OptionNames

    run_mode = os.getenv("RUN_MODE", None)

    features = smart_list(features)
    # 检查因子数是否超出限制
    if len(features) < 1:
        raise Exception("至少须输入 1 个因子才可进行后续操作！")

    if user_factor_data is not None:
        user_factor_data = user_factor_data.read()

    if run_mode in ["ALPHA_ALPHA", "ALPHA_BACKTEST"]:
        # 适配CMB数据起始日期为2019-01-01
        start_date = os.getenv("START_DATE") or start_date
        if run_mode == "ALPHA_ALPHA":
            if len(metrics) == 1 and "因子表现概览" in metrics:  # 兼容系统因子内部
                metrics = list(METRICS.keys())

    ap = AlphaPerformance(
        log,
        option_values={
            OptionNames.BacktestInterval: [(start_date, end_date)],
            OptionNames.Benchmark: [BENCHMARKS.get(benchmark, benchmark)],
            OptionNames.StockPool: [STOCK_POOLS.get(stock_pool, stock_pool)],
            OptionNames.UserDataMerge: [user_data_merge],
            OptionNames.DropSTStocks: [int(drop_st_stocks)],
            OptionNames.DropPriceLimitStocks: [int(drop_price_limit_stocks)],
            OptionNames.DropNewStocks: [int(drop_new_stocks)],
            OptionNames.DropSuspendedStocks: [int(drop_suspended_stocks)],
            OptionNames.QuantileCount: [quantile_count],
            OptionNames.CommissionRates: [commission_rate],
            OptionNames.Cutoutliers: [int(cutoutliers)],
            OptionNames.Normalization: [int(normalization)],
            OptionNames.Neutralization: [",".join([NEUTRALIZATIO_RISK_FACTORS.get(f, f) for f in neutralization])],
            OptionNames.RebalancePrice: [rebalance_price],
            OptionNames.DelayRebalanceDays: [delay_rebalance_days],
            OptionNames.RebalancePeriod: [rebalance_period],
            OptionNames.ReturnsCalculationMethod: [RETURNS_CALCULATION_METHODS.get(returns_calculation_method, returns_calculation_method)],
            OptionNames.FactorCoverage: [factor_coverage],
        },
        metric_values=[METRICS.get(m, m) for m in metrics],
    )

    data = {
        "data": ap.batch_process(features, user_factor_data=user_factor_data),
        "title": title,
    }

    # 构建因子保存数据
    save_data = get_save_data(ap, data)

    if run_mode in ["ALPHA_BACKTEST", "ALPHA_ALPHA"]:
        factor_meta = data["data"]["factors"][-1]["meta"]
        credential = Credential.from_env()
        task_id = os.getenv("TASK_ID")
        if run_mode == "ALPHA_BACKTEST":  # 回测
            AlphaClient.update_backtest_by_task_id(  # 更新基础信息
                task_id=task_id,
                credential=credential,
                request=UpdateBacktestRequest(
                    column=data["data"]["factors"][-1]["meta"]["name"],
                    expression=factor_meta["expr"][-1],
                ),
            )
        factor_data = data["data"]["factors"][-1]["results"][0]["summary"]

        performance = CreatePerformanceSchema(
            run_datetime=isoparse(os.getenv("TRADING_DATE", start_date)),
            source=PerformanceSource.BACKTEST if run_mode == "ALPHA_BACKTEST" else PerformanceSource.ALPHA,
            # IC/IR
            ic_mean=factor_data.get("IC", {}).get("ic_mean"),
            ic_std=factor_data.get("IC", {}).get("ic_std"),
            ic_significance_ratio=factor_data.get("IC", {}).get("ic_significance_ratio"),
            ic_ir=factor_data.get("IC", {}).get("ir"),
            ic_positive_count=None,
            ic_negative_count=None,
            ic_skew=None,
            ic_kurt=None,
            # 最小分位表现
            returns_total_min_quantile=factor_data["QuantileReturns"].get("summary_l_0", {}).get("returns_total"),
            returns_255_min_quantile=factor_data["QuantileReturns"].get("summary_l_0", {}).get("returns_255"),
            returns_66_min_quantile=factor_data["QuantileReturns"].get("summary_l_0", {}).get("returns_66"),
            returns_22_min_quantile=factor_data["QuantileReturns"].get("summary_l_0", {}).get("returns_22"),
            returns_5_min_quantile=factor_data["QuantileReturns"].get("summary_l_0", {}).get("returns_5"),
            returns_1_min_quantile=factor_data["QuantileReturns"].get("summary_l_0", {}).get("returns_1"),
            max_drawdown_min_quantile=factor_data["QuantileReturns"].get("summary_l_0", {}).get("max_drawdown"),
            profit_loss_ratio_min_quantile=factor_data["QuantileReturns"].get("summary_l_0", {}).get("profit_loss_ratio"),
            win_ratio_min_quantile=factor_data["QuantileReturns"].get("summary_l_0", {}).get("win_ratio"),
            sharpe_ratio_min_quantile=factor_data["QuantileReturns"].get("summary_l_0", {}).get("sharpe_ratio"),
            returns_volatility_min_quantile=factor_data["QuantileReturns"].get("summary_l_0", {}).get("returns_volatility"),
            # 最大分位表现
            returns_total_max_quantile=list(factor_data["QuantileReturns"].values())[1]["returns_total"],
            returns_255_max_quantile=list(factor_data["QuantileReturns"].values())[1]["returns_255"],
            returns_66_max_quantile=list(factor_data["QuantileReturns"].values())[1]["returns_66"],
            returns_22_max_quantile=list(factor_data["QuantileReturns"].values())[1]["returns_22"],
            returns_5_max_quantile=list(factor_data["QuantileReturns"].values())[1]["returns_5"],
            returns_1_max_quantile=list(factor_data["QuantileReturns"].values())[1]["returns_1"],
            max_drawdown_max_quantile=list(factor_data["QuantileReturns"].values())[1]["max_drawdown"],
            profit_loss_ratio_max_quantile=list(factor_data["QuantileReturns"].values())[1]["profit_loss_ratio"],
            win_ratio_max_quantile=list(factor_data["QuantileReturns"].values())[1]["win_ratio"],
            sharpe_ratio_max_quantile=list(factor_data["QuantileReturns"].values())[1]["sharpe_ratio"],
            returns_volatility_max_quantile=list(factor_data["QuantileReturns"].values())[1]["returns_volatility"],
            # 多空组合表现
            returns_total_ls_combination=factor_data["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_total"),
            returns_255_ls_combination=factor_data["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_255"),
            returns_66_ls_combination=factor_data["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_66"),
            returns_22_ls_combination=factor_data["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_22"),
            returns_5_ls_combination=factor_data["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_5"),
            returns_1_ls_combination=factor_data["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_1"),
            max_drawdown_ls_combination=factor_data["QuantileReturns"].get("summary_l_top_bottom", {}).get("max_drawdown"),
            profit_loss_ratio_ls_combination=factor_data["QuantileReturns"].get("summary_l_top_bottom", {}).get("profit_loss_ratio"),
            win_ratio_ls_combination=factor_data["QuantileReturns"].get("summary_l_top_bottom", {}).get("win_ratio"),
            sharpe_ratio_ls_combination=factor_data["QuantileReturns"].get("summary_l_top_bottom", {}).get("sharpe_ratio"),
            returns_volatility_ls_combination=factor_data["QuantileReturns"].get("summary_l_top_bottom", {}).get("returns_volatility"),
            # 因子收益率
            beta_mean=None,
            beta_std=None,
            beta_positive_ratio=None,
            abs_t_mean=None,
            abs_t_value_over_2_ratio=None,
            p_value_less_ratio=None,
        )

        options = AlphaParameter(
            start_date=start_date,
            end_date=end_date,
            benchmark=benchmark,
            stock_pool=stock_pool,
            user_data_merge=user_data_merge,
            drop_st_stocks=drop_st_stocks,
            drop_price_limit_stocks=drop_price_limit_stocks,
            drop_new_stocks=drop_new_stocks,
            drop_suspended_stocks=drop_suspended_stocks,
            quantile_count=quantile_count,
            commission_rate=commission_rate,
            cutoutliers=cutoutliers,
            normalization=normalization,
            neutralization=neutralization,
            delay_rebalance_days=delay_rebalance_days,
            rebalance_period=rebalance_period,
            rebalance_price=rebalance_price,
            returns_calculation_method=returns_calculation_method,
            factor_coverage=factor_coverage,
        )
        if run_mode == "ALPHA_BACKTEST":  # 回测
            AlphaClient.update_backtest_by_task_id(
                task_id=task_id,
                credential=credential,
                request=UpdateBacktestRequest(
                    parameter=options,
                ),
            )
        AlphaClient.create_backtest_performance(task_id=task_id, request=performance, credential=credential)
        # 因子数据持久化, 因子和绩效分开  # TODO: 绩效
        if run_mode == "ALPHA_ALPHA":
            save_alpha_to_datasource(DataSource.write_pickle(save_data))
        with open("/var/tmp/.status", "w") as f:
            f.write("done")

    return Outputs(data=DataSource.write_pickle(data), save_data=DataSource.write_pickle(save_data))


def save_alpha_to_datasource(factors_info):
    credential = Credential.from_env()
    alpha_id = os.getenv("ALPHA_ID", None).strip()
    data = factors_info.read()
    factor_fields = smart_dict(DEFAULT_FACTOR_FIELDS)
    factors_info_data = {}
    parent_tables = []
    if isinstance(data, dict):
        factors_info_data = {}
        for factor_name, factor_data in data.items():
            factors_info_data[factor_name] = {
                "options": factor_data["options"],
                "metrics": factor_data["metrics"],
                "datasource": factor_data["datasource"],
                "column_name": factor_name,
                "description": factor_data.get("description", ""),
                "expr": factor_data.get("expr", ""),
            }
    elif isinstance(data, pd.DataFrame):
        # 生成规定格式数据
        try:
            # 数据最新日期
            latest_date = data.date.max().strftime("%Y-%m-%d")
        except:  # noqa
            latest_date = datetime.datetime.now().strftime("%Y-%m-%d")
        primary_key = ["instrument", "date"]
        rename_columns = {}
        factor_name_set = set()
        for col in data.columns:
            if col in primary_key:
                continue
            name_desc = factor_fields.get(col)
            if isinstance(name_desc, dict):
                factor_name = name_desc.get("name", col)
                factor_desc = name_desc.get("desc", "")
            else:
                factor_name = col
                factor_desc = ""
            _df = data[primary_key + [col]]
            _df.rename(columns={col: factor_name}, inplace=True)
            if factor_name in factor_name_set:
                raise Exception("重复的因子名，请检查【因子描述】中的 name 字段是否重复")
            rename_columns[col] = factor_name
            factor_name_set.add(factor_name)
            factors_info_data[factor_name] = {
                "options": {"最新数据日期": latest_date, "表名": table},
                "metrics": {},
                "datasource": _df,
                "column_name": factor_name,
                "description": factor_desc,
                "expr": col,
            }
        df = data.rename(columns=rename_columns)
        display_df(df, bigquant_friendly_name)
    else:
        raise Exception("UnKnow data type!")

    lost_factors = []
    log.info("开始检查因子数据 ...")

    alpha_name = os.getenv("ALPHA_NAME") or alpha_id

    for factor_name, factor_data in factors_info_data.items():
        run_mode = os.getenv("RUN_MODE", None)
        factor_df = factor_data["datasource"]
        column_name = factor_data["column_name"]
        factor_columns = factor_df.columns
        if column_name not in factor_columns:
            lost_factors.append(factor_name)
        if isinstance(factor_df, pd.DataFrame) and ("date" not in factor_columns or "instrument" not in factor_columns):
            raise Exception("factor data df must has columns [date, instrument]")
        if run_mode == "ALPHA_ALPHA":
            # 生成或更新因子值
            datasource_dict = os.getenv("DATASOURCE_DICT")
            if datasource_dict:
                datasource_dict = json.loads(datasource_dict)
                data_alias = datasource_dict[column_name]
            else:
                data_alias = "alpha_{}".format(uuid.uuid4().hex[:8])
            _schema = {
                "active": False,
                "friendly_name": alpha_name,
                "desc": "因子数据: {}".format(column_name),
                "fields": {
                    "date": {"desc": "日期", "type": "datetime64[ns]"},
                    "instrument": {"desc": "证券代码", "type": "str"},
                    column_name: {"desc": "因子", "type": str(factor_df[column_name].dtype)},
                },
                "file_type": "arrow",
                "partition_date": "Y",
                "date_field": "date",
                "primary_key": ["date", "instrument"],
            }
            log.info("开始更新因子数据: {}".format(alpha_id, parent_tables))
            UpdateDataSource(owner=str(credential.user.id)).update(df=factor_df, alias=alpha_id, schema=_schema)


def bigquant_postrun(outputs):
    from bigalpha.impl.module_renderer import MultiModuleRenderer

    data = outputs.data.read_pickle()
    multi_module_renderer = MultiModuleRenderer(data)
    multi_module_renderer.render()

    return outputs
