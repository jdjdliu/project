STRATEGY_TEMPLATE = """
from typing import Dict,List
STOCK_POOL_MAP = {{
    '全市场': 'all',
    '上证50': 'in_sse50_0',
    '沪深300': 'in_csi300_0',
    '中证500': 'in_csi500_0',
    '中证800': 'in_csi800_0',
}}

MARKET_MAP = {{
    '全市场': 'all',
    '上证主板': '60',
    '深证主板': '00',
    '创业板': '3',
    '科创板': '68',
    '北交所': '8',
}}

MARKET_INDEX_MAP = {{
    '上证综指': '000001.HIX',
    '上证50': '000016.HIX',
    '沪深300': '000300.HIX',
    '中证500': '000905.HIX',
    '深证成指': '399001.ZIX',
}}

BENCHMARK_MAP = {{
    '上证综指': '000001.HIX',
    '上证50': '000016.HIX',
    '沪深300': '000300.HIX',
    '中证500': '000905.HIX',
    '深证成指': '399001.ZIX',
}}
# 行业分布映射
INDUSTRY_MAP = {{
    '全行业': 'all',
    '农林牧渔': 110000,
    '采掘': 210000,
    '化工': 220000,
    '基础化工': 220000,
    '钢铁': 230000,
    '有色金属': 240000,
    '电子': 270000,
    '汽车': 280000,
    '家用电器': 330000,
    '食品饮料': 340000,
    '纺织服装': 350000,
    '轻工制造': 360000,
    '医药生物': 370000,
    '公用事业': 410000,
    '交通运输': 420000,
    '房地产': 430000,
    '商业贸易': 450000,
    '商贸零售': 450000,
    '休闲服务': 460000,
    '社会服务': 460000,
    '银行': 480000,
    '非银金融': 490000,
    '综合': 510000,
    '建筑材料': 610000,
    '建筑装饰': 620000,
    '电气设备': 630000,
    '电力设备': 630000,
    '机械设备': 640000,
    '国防军工': 650000,
    '计算机': 710000,
    '传媒': 720000,
    '通信': 730000,
    '煤炭': 740000,
    '石油石化': 750000,
    '环保': 760000,
    '美容护理': 770000,
    '其他': 0,
}}

WEIGHT_MAP = {{
    '等权重': 'all',
    '市值加权': 'market_cap_0',
    '流通市值加权': 'market_cap_float_0',
}}

FEATURE_MAP = {{
    'open_0/adjust_factor_0': '开盘价',
    'close_0/adjust_factor_0': '收盘价',
    'amount_0': '交易额',
    'daily_return_0': '当日收益',
}}


class BuildStrategy:
    def __init__(
        self,
        start_date: str,
        end_date: str,
        filter_cond: str,
        filter_name: Dict[str, str],
        sort_name: Dict[str, str],
        sort_direction: List[bool],
        sort_weight: List[int],
        drop_st_status: bool,
        stock_pool: str,
        market: List[str],
        industry: List[str],
        rebalance_days: int,
        weight_method: str,
        market_index: str,
        select_time_parameters: str,
        is_select_time: bool,
        view_date: str,
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.filter_cond = filter_cond
        self.filter_name = filter_name
        self.sort_direction = sort_direction
        self.sort_name = sort_name
        self.sort_weight = sort_weight
        self.drop_st_status = drop_st_status  # 是否剔除ST票
        self.stock_pool = stock_pool
        self.market = market
        self.industry = industry
        self.rebalance_days = rebalance_days
        self.weight_method = weight_method  # 加权方式
        self.market_index = market_index
        self.select_time_parameters = select_time_parameters
        self.is_select_time = is_select_time
        self.view_date = view_date
        self._date = DataSource("trading_days").read()
        self.date = self._date[self._date.country_code == 'CN'][['date']]
        self.instruments = M.instruments.v2(
            start_date=self.start_date, end_date=self.end_date, market="CN_STOCK_A", instrument_list="")

    def features_list(self) -> List[str]:
        features_base = list(FEATURE_MAP.keys())
        features = ["industry_sw_level1_0", "st_status_0",
                    self.filter_cond] + list(self.sort_name.keys())
        features = features + features_base
        if self.weight_method != "等权重":
            features = features + [WEIGHT_MAP[self.weight_method]]
        if self.stock_pool != "全市场":
            features = features + [STOCK_POOL_MAP[self.stock_pool]]
        return features

    def prepare_data(self, start_date: str, end_date: str, features: List[str]) -> DataSource:
        instruments = M.instruments.v2(
            start_date=start_date, end_date=end_date, market="CN_STOCK_A", instrument_list="")
        features = DataSource.write_pickle(features)
        general_feature = M.general_feature_extractor.v7(
            instruments=instruments.data, features=features, start_date="", end_date="", before_start_days=0)
        factor_data = M.derived_feature_extractor.v3(
            input_data=general_feature.data, features=features, date_col="date", instrument_col="instrument", drop_na=False, remove_extra_columns=False
        )
        return factor_data

    def filter_data(self, factor_data: DataSource) -> pd.DataFrame:
        factor_data = factor_data.data.read()
        factor_data = factor_data[factor_data["filter_cond"]]
        if self.stock_pool != "全市场":
            factor_data = factor_data[factor_data[STOCK_POOL_MAP[self.stock_pool]] == 1]
        if self.market != ["全市场"]:
            factor_data = factor_data[factor_data.instrument.str.startswith(
                tuple(MARKET_MAP[x] for x in self.market))]
        if self.industry != ["全行业"]:
            factor_data = factor_data[factor_data.industry_sw_level1_0.isin(
                [INDUSTRY_MAP[x] for x in self.industry])]

        if self.drop_st_status:
            factor_data = factor_data[factor_data["st_status_0"] == 0.0]

        factor_data = DataSource.write_df(factor_data)
        return factor_data

    def cal_score(self, df: pd.DataFrame) -> pd.DataFrame:
        # 均值填充nan
        df = df.fillna(df.mean())
        sort_name_list = list(self.sort_name.keys())
        df["total_score"] = 0
        for i in range(len(self.sort_weight)):
            df[sort_name_list[i] + "_score"] = df[sort_name_list[i]
                                                  ].rank(pct=True, method="max", ascending=self.sort_direction[i]) * 100
            df["total_score"] += df[sort_name_list[i] +
                                    "_score"] * self.sort_weight[i]
        df["total_score"] = df["total_score"].rank(
            pct=True, method="max", ascending=True) * 100
        return df

    def sort_data(self) -> pd.DataFrame:
        features = self.features_list()
        factor_data = self.prepare_data(
            self.start_date, self.end_date, features)
        factor_data = self.filter_data(factor_data)
        factor_data = factor_data.read().round(2)
        sorted_data = factor_data.groupby("date").apply(self.cal_score).reset_index(drop=True)
        sorted_data = sorted_data.groupby("date").apply(lambda x: x.sort_values(
            "total_score", ascending=False)).reset_index(drop=True)
        sorted_data = DataSource.write_df(sorted_data)
        return sorted_data

    def view_stock(self) -> pd.DataFrame:
        features = self.features_list()
        factor_data = self.prepare_data(
            self.view_date, self.view_date, features)
        factor_data = self.filter_data(factor_data)
        sorted_data = factor_data.read().round(2)
        sorted_data = self.cal_score(sorted_data)
        sorted_data = sorted_data.sort_values("total_score", ascending=False)
        sort_name_list = list(self.sort_name.keys())
        sort_dict = {{"total_score": "总得分"}}
        for i in range(len(sort_name_list)):
            sort_dict[sort_name_list[i] +
                      "_score"] = self.sort_name[sort_name_list[i]] + "得分"
        filter_dict = {{"instrument": "股票代码", "name": "股票名称", "date": "时间"}}
        filter_dict.update(FEATURE_MAP)
        filter_dict.update(self.filter_name)
        filter_dict.update(self.sort_name)
        filter_dict.update(sort_dict)
        filter_dict_list = list(filter_dict.keys())
        stock_name = DataSource("instruments_CN_STOCK_A").read(
            start_date=self.view_date, end_date=self.view_date)
        sorted_data = pd.merge(sorted_data, stock_name, on=[
                               'instrument', 'date'], how='left')
        sorted_data = sorted_data[filter_dict_list]
        sorted_data.rename(columns=filter_dict, inplace=True)
        return sorted_data

    def select_time(self) -> DataSource:
        df = DataSource("bar1d_index_CN_STOCK_A").read(
            MARKET_INDEX_MAP[self.market_index])
        df = DataSource.write_df(df)

        if self.is_select_time:
            features = self.select_time_parameters
        else:
            features = "is_hold = True"

        factor_data = M.derived_feature_extractor.v3(
            input_data=df, features=features, date_col="date", instrument_col="instrument", drop_na=False, remove_extra_columns=False
        )

        return factor_data


def get_weight(df: pd.DataFrame, n: int, weight_method: str) -> pd.DataFrame:
    pool = df.head(n)
    if weight_method == "等权重":
        pool["weight"] = 1 / n
    else:
        pool["weight"] = pool[WEIGHT_MAP[weight_method]] / \
            pool[WEIGHT_MAP[weight_method]].sum()
    return pool


def m3_initialize_bigquant_run(context):
    context.ranker_prediction = context.options["data"]["factor_data"].read()
    context.index_data = context.options["data"]["index_data"].data.read()
    context.set_commission(
        PerOrder(buy_cost=buy_cost, sell_cost=sell_cost, min_cost=min_cost))
    context.position_status = 0
    context.portfolio_num = portfolio_num
    context.rebalance_days = rebalance_days
    context.weight_method = weight_method


# 回测引擎：每日数据处理函数，每天执行一次


def m3_handle_data_bigquant_run(context, data):

    ranker_prediction = context.ranker_prediction[context.ranker_prediction.date == data.current_dt.strftime(
        "%Y-%m-%d")]

    prepare_buy_list = ranker_prediction.instrument[: context.portfolio_num].tolist(
    )
    hold_list = [equity.symbol for equity in context.portfolio.positions]
    buy_list = [i for i in prepare_buy_list if i not in hold_list]
    sell_list = [i for i in hold_list if i not in prepare_buy_list]
    index_data = context.index_data[context.index_data.date ==
                                    data.current_dt.strftime("%Y-%m-%d")]

    weight_df = get_weight(
        ranker_prediction, context.portfolio_num, context.weight_method)
    weights = weight_df[weight_df.instrument.isin(buy_list)]
    weights_dict = dict(weights.set_index("instrument")["weight"])

    if index_data["is_hold"].values[0] == True:
        if context.trading_day_index % context.rebalance_days == 0:
            context.position_status = 1
            # 如果有卖出信号
            if len(sell_list) > 0:
                for instrument in sell_list:
                    sid = context.symbol(instrument)  # 将标的转化为equity格式
                    # 持仓
                    cur_position = context.portfolio.positions[sid].amount
                    if cur_position > 0 and data.can_trade(sid):
                        context.order_target_percent(sid, 0)  # 全部卖出

            # 如果有买入信号/有持仓
            if len(buy_list) > 0:
                for instrument in buy_list:
                    weight = weights_dict[instrument]
                    sid = context.symbol(instrument)  # 将标的转化为equity格式
                    if data.can_trade(sid):
                        context.order_target_percent(sid, weight)  # 买入

    elif index_data["is_hold"].values[0] == False and context.position_status == 1:
        context.position_status = 0
        if len(sell_list) > 0:
            for instrument in sell_list:
                sid = context.symbol(instrument)  # 将标的转化为equity格式
                cur_position = context.portfolio.positions[sid].amount  # 持仓
                if cur_position > 0 and data.can_trade(sid):
                    context.order_target_percent(sid, 0)  # 全部卖出


# 回测引擎：准备数据，只执行一次


def m3_prepare_bigquant_run(context):
    pass


# 回测引擎：每个单位时间开始前调用一次，即每日开盘前调用一次。


def m3_before_trading_start_bigquant_run(context, data):
    pass



start_date = '{start_date}'
end_date = '{end_date}'
filter_cond = '{filter_cond_composition}'
filter_name = {filter_name}
sort_name = {sort_name}
sort_direction = {sort_direction}  # 升序False
sort_weight = {sort_weight}
drop_st_status = {drop_st_status}
stock_pool = '{stock_pool}'
market = {market}
industry = {industry}

capital_base = {capital_base}  # 初始资金
buy_cost = {buy_cost}  # 买入佣金
sell_cost = {sell_cost}  # 卖出佣金
min_cost = {min_cost}  # 最小佣金
portfolio_num = {portfolio_num}  # 持股数量
rebalance_days = {rebalance_days}  # 调仓天数
weight_method = '{weight_method}'  # 全部选项为 "流通市值加权"、"市值加权"、"等权重"
market_index = '{market_index}'

select_time_parameters = {select_time_parameters}
is_select_time = {is_select_time}
view_date = ''
benchmark = '{reference}'

bs = BuildStrategy(start_date, end_date, filter_cond, filter_name, sort_name, sort_direction, sort_weight, drop_st_status,
                   stock_pool, market, industry, rebalance_days, weight_method, market_index, select_time_parameters, is_select_time, view_date)

factor_data = {{"factor_data": bs.sort_data(), "index_data": bs.select_time()}}

M.trade.v4(
    instruments=bs.instruments.data,
    options_data=factor_data,
    start_date="",
    end_date="",
    initialize=m3_initialize_bigquant_run,
    handle_data=m3_handle_data_bigquant_run,
    prepare=m3_prepare_bigquant_run,
    before_trading_start=m3_before_trading_start_bigquant_run,
    volume_limit=0.025,
    order_price_field_buy="close",
    order_price_field_sell="close",
    capital_base=capital_base,
    auto_cancel_non_tradable_orders=True,
    data_frequency="daily",
    price_type="真实价格",
    product_type="股票",
    plot_charts=True,
    backtest_only=False,
    benchmark=BENCHMARK_MAP[benchmark],
)
"""
