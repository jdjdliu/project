from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field

from ..constants import FilterOperator, IndexType, IndustryType, MarketIndex, MarketType, ReferenceType, StockPool, WeightMethod


class FilterCondParameter(BaseModel):
    factor: str = Field(..., description="策略")
    filter: FilterOperator = Field(..., description="选项")
    min_val: Optional[float] = Field(0.0, description="最小值")
    max_val: Optional[float] = Field(0.0, description="最大值")
    value: Optional[float] = Field(0.0, description="比较基准值")


class SelectTimeParam(BaseModel):
    index: IndexType = Field(..., description="择时指标名")
    val_1: Optional[int] = Field(0, description="数值一")
    val_2: Optional[int] = Field(0, description="数值二")
    val_3: Optional[int] = Field(0, description="数值三")


class StrategyParameter(BaseModel):
    start_date: Optional[str] = Field("2005-01-01", description="回测开始日期")
    end_date: Optional[str] = Field(default_factory=lambda: date.today().strftime("%Y-%m-%d"), description="回测结束日期")
    capital_base: Optional[int] = Field(1000, description="初始资金")
    reference: Optional[ReferenceType] = Field(None, description="参考基准")
    buy_cost: Optional[float] = Field(0.0003, description="买入费率")
    sell_cost: Optional[float] = Field(0.0003, description="卖出费率")
    min_cost: Optional[float] = Field(5.0, description="最小佣金")
    stock_pool: Optional[StockPool] = Field(StockPool.ALL, description="股票池")
    market: Optional[List[MarketType]] = Field([MarketType.ALL], description="上市板")
    industry: Optional[List[IndustryType]] = Field([IndustryType.ALL], description="行业")
    drop_st_status: Optional[bool] = Field(True, description="移除ST股票")
    filter_cond: Optional[List[FilterCondParameter]] = Field([], description="选股条件")
    portfolio_num: Optional[int] = Field(30, description="最大持有股票数量")
    rebalance_days: Optional[int] = Field(22, description="调仓天数")
    weight_method: Optional[WeightMethod] = Field(WeightMethod.WEIGHTED_MEAN, description="股票权重分配方式")
    market_index: Optional[MarketIndex] = Field(MarketIndex.HS_300, description="市场指数")
    select_time_parameters: Optional[List[SelectTimeParam]] = Field([], description="择时参数")
    is_third_match: Optional[bool] = Field(default=False, description="是否为第三方， 默认为0")
    filter_name: Optional[dict] = Field({}, description="过滤名字")
    sort_name: Optional[dict] = Field({}, description="排序名字")
    sort_direction: Optional[List[bool]] = Field([False], description="排序方向，升序False")
    sort_weight: Optional[List[float]] = Field([], description="排序权重")
    view_date: Optional[str] = Field("", description="预览日期")
    is_select_time: Optional[bool] = Field(False, description="是否择时")

    class Config:
        use_enum_values = True