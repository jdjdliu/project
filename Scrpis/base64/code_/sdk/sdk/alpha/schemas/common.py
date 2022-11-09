import os
from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field

from .. import constants as C
ALPHA_STRATE_DATE= os.getenv("ALPHA_STRATE_DATE", "2019-01-01")


class AlphaParameter(BaseModel):
    start_date: str = Field(ALPHA_STRATE_DATE, description="回测开始日期")
    end_date: str = Field(default_factory=lambda: date.today().strftime("%Y-%m-%d"), description="回测开始日期")
    rebalance_period: int = Field(22, description="调仓周期")
    delay_rebalance_days: int = Field(0, description="延迟建仓天数")
    rebalance_price: C.RebalancePrice = Field(C.RebalancePrice.CLOSE_0, description="收益价格")
    stock_pool: C.StockPool = Field(C.StockPool.ALL, description="股票池")
    quantile_count: int = Field(5, ge=1, description="分组数量")
    commission_rate: float = Field(0.0016, description="手续费及滑点")
    returns_calculation_method: C.ReturnsCalculationMethod = Field(C.ReturnsCalculationMethod.CUM_PROD, description="收益计算方式")
    benchmark: C.Benchmark = Field(C.Benchmark.NONE, description="收益率基准")
    drop_new_stocks: int = Field(60, ge=0, description="drop_new_stocks")
    drop_price_limit_stocks: bool = Field(True, description="移除涨跌停股票")
    drop_st_stocks: bool = Field(True, description="移除ST股票")
    drop_suspended_stocks: bool = Field(True, description="移除停牌股票")
    normalization: bool = Field(True, description="因子标准化")
    cutoutliers: bool = Field(False, description="因子去极值")
    neutralization: List[C.Neutralization] = Field(default_factory=lambda: [n for n in C.Neutralization], unique_items=True, description="中性化风险因子")
    metrics: List[C.Metric] = Field(default_factory=lambda: [m for m in C.Metric], unique_items=True, description="指标")
    factor_coverage: float = Field(0.5, ge=0.0, le=1.0, description="原始因子值覆盖率")
    user_data_merge: C.UserDataMergeMethod = Field(C.UserDataMergeMethod.LEFT, description="用户数据合并方式")

    class Config:
        use_enum_values = True


class IndexParameter(BaseModel):
    factor_name: Optional[List] = Field(..., description="选择因子")
    weight_method: C.WeightMethod = Field("市值加权", description="加权类型")
    stock_pool: C.StockPool = Field("全市场", description="股票池")
    benchmark: C.Benchmark = Field("中证500", description="收益率基准")
    sort: C.Sort = Field("升序", description="排序")
    rebalance_days: int = Field(2, ge=1, description="调仓天数")
    cost: float = Field(0.001, gt=0.0, description="交易成本")
    quantile_ratio: float = Field(0, ge=0.0, le=100.0, description="指数分位数")
    stock_num: int = Field(0, ge=0, description="指数成分数")

    class Config:
        use_enum_values = True


class CompositionAlpha(BaseModel):
    alpha: str = Field(..., description="因子名")
    weight: float = Field(..., description="权重")


class FilterQuery(BaseModel):
    key: str = Field(..., description="过滤字段")
    value: float = Field(..., description="比较值")
    operator: C.FilterOperator = Field(..., description="比较符")


class AlphaQuery(BaseModel):
    keyword: Optional[str] = Field(None, description="关键字")
    filters: List[FilterQuery] = Field(default_factory=list, description="过滤条件")

class AlphaInfoResponse(BaseModel):
    name: str = Field(...)
    alpha_id: str = Field(...)

    class Config:
        orm_mode = True


class ListAlphaResponse(BaseModel):
    alpha_list: List[AlphaInfoResponse] = Field(default_factory=list, description="因子列表")

    class Config:
        orm_mode = True