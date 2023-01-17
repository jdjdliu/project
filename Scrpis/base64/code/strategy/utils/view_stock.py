from typing import Dict, List

import pandas as pd
from learning.module2.modules.derived_feature_extractor.v3 import \
    BigQuantModule as derived_feature_extractor
from learning.module2.modules.general_feature_extractor.v7 import \
    BigQuantModule as general_feature_extractor
from learning.module2.modules.instruments.v2 import bigquant_run as instrument
from sdk.datasource import DataSource
from sdk.strategy.constants import WeightMethod

pd.set_option('precision', 2)

STOCK_POOL_MAP = {
    "全市场": "all",
    "上证50": "in_sse50_0",
    "沪深300": "in_csi300_0",
    "中证500": "in_csi500_0",
    "中证800": "in_csi800_0",
}

MARKET_MAP = {
    "全市场": "all",
    "上证主板": "60",
    "深证主板": "00",
    "创业板": "3",
    "科创板": "68",
    "北交所": "8",
}

MARKET_INDEX_MAP = {
    "上证综指": "000001.HIX",
    "上证50": "000016.HIX",
    "沪深300": "000300.HIX",
    "中证500": "000905.HIX",
    "深证成指": "399001.ZIX",
}

# 行业分布映射
INDUSTRY_MAP = {
    "全行业": "all",
    "农林牧渔": 110000,
    "采掘": 210000,
    "化工": 220000,
    "基础化工": 220000,
    "钢铁": 230000,
    "有色金属": 240000,
    "电子": 270000,
    "汽车": 280000,
    "家用电器": 330000,
    "食品饮料": 340000,
    "纺织服装": 350000,
    "轻工制造": 360000,
    "医药生物": 370000,
    "公用事业": 410000,
    "交通运输": 420000,
    "房地产": 430000,
    "商业贸易": 450000,
    "商贸零售": 450000,
    "休闲服务": 460000,
    "社会服务": 460000,
    "银行": 480000,
    "非银金融": 490000,
    "综合": 510000,
    "建筑材料": 610000,
    "建筑装饰": 620000,
    "电气设备": 630000,
    "电力设备": 630000,
    "机械设备": 640000,
    "国防军工": 650000,
    "计算机": 710000,
    "传媒": 720000,
    "通信": 730000,
    "煤炭": 740000,
    "石油石化": 750000,
    "环保": 760000,
    "美容护理": 770000,
    "其他": 0,
}

WEIGHT_MAP = {
    "等权重": "all",
    "市值加权": "market_cap_0",
    "流通市值加权": "market_cap_float_0",
}

FEATURE_MAP = {
    "open_0/adjust_factor_0": "开盘价",
    "close_0/adjust_factor_0": "收盘价",
    "amount_0": "交易额",
    "daily_return_0 - 1": "当日收益",
}


class ViewStock:
    def __init__(
        self,
        start_date: str,
        end_date: str,
        filter_conde_composition: str,
        filter_name: Dict[str, str],
        sort_name: Dict[str, str],
        sort_direction: List[bool],
        sort_weight: List[int],
        drop_st_status: bool,
        stock_pool: str,
        market: List[str],
        industry: List[str],
        view_date: str,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.filter_cond = filter_conde_composition
        self.filter_name = filter_name
        self.sort_direction = sort_direction
        self.sort_name = sort_name
        self.sort_weight = sort_weight
        self.drop_st_status = drop_st_status  # 是否剔除ST票
        self.stock_pool = stock_pool
        self.market = market
        self.industry = industry
        self.weight_method = WeightMethod.WEIGHTED_MEAN  # 加权方式
        self.view_date = view_date
        self.date = DataSource("all_trading_days").read(start_date=start_date, end_date=end_date)[["date"]] if DataSource("all_trading_days") else None
        self.instruments = instrument(start_date=self.start_date, end_date=self.end_date, market="CN_STOCK_A", instrument_list="")

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
        instruments = instrument(
            start_date=start_date, end_date=end_date, market="CN_STOCK_A", instrument_list="")
        features = DataSource.write_pickle(features)
        general_feature = general_feature_extractor(
            instruments=instruments.data, features=features, start_date="", end_date="", before_start_days=0).run()
        factor_data = derived_feature_extractor(
        input_data=general_feature.data, features=features, date_col="date", instrument_col="instrument", drop_na=False, remove_extra_columns=False
        ).run()
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
        sorted_data = factor_data.groupby("date").apply(self.cal_score)
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
        sort_dict = {"total_score": "总得分"}
        for i in range(len(sort_name_list)):
            sort_dict[sort_name_list[i] +
                      "_score"] = self.sort_name[sort_name_list[i]] + "得分"
        filter_dict = {"instrument": "股票代码", "name": "股票名称", "date": "时间"}
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


# start_date = "2020-01-15"
# end_date = "2020-02-15"
# filter_cond = "filter_cond=(market_cap_0>1)"
# filter_name = {"market_cap_0": "总市值"}
# sort_name = {"market_cap_0": "总市值"}
# sort_direction = [False]  # 升序False
# sort_weight = [1]
# drop_st_status = False
# stock_pool = "全市场"
# market = ["全市场"]
# industry = ["全行业"]

# capital_base = 10000000  # 初始资金
# buy_cost = 0  # 买入佣金
# sell_cost = 0  # 卖出佣金
# min_cost = 0  # 最小佣金
# portfolio_num = 10  # 持股数量
# rebalance_days = 22  # 调仓天数
# weight_method = "等权重"  # 全部选项为 "流通市值加权"、"市值加权"、"等权重"
# market_index = "上证综指"
# select_time_parameters = "is_hold = mean(close,5)>mean(close,20)"
# #
# is_select_time = False
# view_date = "2020-01-16"

# bs = ViewStock(
#     start_date,
#     end_date,
#     filter_cond,
#     filter_name,
#     sort_name,
#     sort_direction,
#     sort_weight,
#     drop_st_status,
#     stock_pool,
#     market,
#     industry,
#     view_date,
# )
