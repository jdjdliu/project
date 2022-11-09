from datetime import datetime
from typing import Any, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field
from sdk.common import schemas

from ..constants import ProductType, StrategyBuildType, ReferenceType, StockPool, MarketType, WeightMethod, IndustryType
from .common import StrategyParameter,FilterCondParameter


class StrategyBacktestSchema(schemas.CreatedAtMixin, schemas.UpdatedAtMixin, schemas.CreatorMixin, schemas.UUIDIDMixin, BaseModel):
    name: str = Field(..., description="策略名称")
    description: Optional[str] = Field(..., description="策略备注")
    parameter: Optional[StrategyParameter] = Field(..., description="策略参数")

    build_type: Optional[StrategyBuildType] = Field(description="策略构建类型", default=StrategyBuildType.WIZARD)
    product_type: ProductType = Field(..., description="资产类型")

    task_id: UUID = Field()

    class Config:
        orm_mode = True
        use_enum_values = True


class ViewStockRequest(BaseModel):
    start_date: Optional[str] = Field("2005-01-01", description="回测开始日期")
    end_date: Optional[str] = Field(default_factory=lambda: date.today().strftime("%Y-%m-%d"), description="回测结束日期")
    filter_cond: Optional[List[FilterCondParameter]] = Field([], description="选股条件")
    filter_name: Optional[dict] = Field([], description="过滤名字")
    sort_direction: Optional[List[bool]] = Field(False, description="排序方向，升序False")
    sort_name: Optional[dict] = Field([], description="排序名字")
    sort_weight: Optional[List[float]] = Field([], description="排序权重")
    drop_st_status: Optional[bool] = Field(True, description="移除ST股票")
    stock_pool: Optional[StockPool] = Field(StockPool.ALL, description="股票池")
    market: Optional[List[MarketType]] = Field([MarketType.ALL], description="上市板")
    industry: Optional[List[IndustryType]] = Field([IndustryType.ALL], description="行业")
    view_date: Optional[str] = Field("2005-01-01", description="预览日期")


class CreateStrategyBacktestFromWizardRequest(BaseModel):
    name: str = Field(..., description="策略名称")
    description: str = Field(..., description="策略备注")
    parameter: StrategyParameter = Field(..., description="策略参数")

    product_type: Optional[ProductType] = Field(description="资产类型", default=ProductType.STOCK)

    class Config:
        orm_mode = True
        use_enum_values = True


# 代码式构建
class CreateBacktestByCodeRequest(BaseModel):
    name: str = Field(..., description="任务名称")

    notebook: str = Field(..., description="notebook内容")
    expression: Optional[str] = Field(..., description="")
    parameter: BaseModel = Field(..., description="")

    product_type: ProductType = Field(..., description="资产类型")
    # build_type: AlphaBuildType = Field(..., description="因子构建类型")

    class Config:
        use_enum_values = True

class UpdateBacktestRequest(BaseModel):
    name: str = Field(..., description="策略名称")
    description: str = Field(..., description="策略备注")
    parameter: Optional[StrategyParameter] = Field(default=None)

    product_type: Optional[ProductType] = Field(description="资产类型", default=ProductType.STOCK)

    class Config:
        use_enum_values = True


class BacktestDailyPerformanceSchema(BaseModel):
    algo_volatility: Any = Field(..., null=True)
    algorithm_period_return: Any = Field(..., null=True)
    alpha: Any = Field(..., null=True)
    benchmark_period_return: Any = Field(..., null=True)
    benchmark_volatility: Any = Field(..., null=True)
    beta: Any = Field(..., null=True)
    # buy_volume: Any = Field(..., null=True)
    capital_used: Any = Field(..., null=True)
    ending_cash: Any = Field(..., null=True)
    ending_exposure: Any = Field(..., null=True)
    ending_value: Any = Field(..., null=True)
    excess_return: Any = Field(..., null=True)
    gross_leverage: Any = Field(..., null=True)
    information: Any = Field(..., null=True)
    long_exposure: Any = Field(..., null=True)
    long_value: Any = Field(..., null=True)
    longs_count: Any = Field(..., null=True)
    max_drawdown: Any = Field(..., null=True)
    max_leverage: Any = Field(..., null=True)
    net_leverage: Any = Field(..., null=True)
    orders: Any = Field(..., null=True)
    period_close: Any = Field(..., null=True)
    period_label: Any = Field(..., null=True)
    period_open: Any = Field(..., null=True)
    pnl: Any = Field(..., null=True)
    portfolio_value: Any = Field(..., null=True)
    positions: Any = Field(..., null=True)
    returns: Any = Field(..., null=True)
    sharpe: Any = Field(..., null=True)
    short_exposure: Any = Field(..., null=True)
    short_value: Any = Field(..., null=True)
    shorts_count: Any = Field(..., null=True)
    sortino: Any = Field(..., null=True)
    starting_cash: Any = Field(..., null=True)
    starting_exposure: Any = Field(..., null=True)
    starting_value: Any = Field(..., null=True)
    trading_days: Any = Field(..., null=True)
    transactions: Any = Field(..., null=True)
    treasury_period_return: Any = Field(..., null=True)
    LOG: Any = Field(..., null=True)
    TRA_FAC: Any = Field(..., null=True)
    POS_FAC: Any = Field(..., null=True)
    need_settle: Any = Field(..., null=True)
    win_percent: Any = Field(..., null=True)
    pnl_ratio: Any = Field(..., null=True)
    trade_times: Any = Field(..., null=True)


class CreateBacktestPerformanceRequest(BaseModel):
    backtest_id: UUID = Field(...)
    run_date: datetime = Field(...)
    daily_data: Any = Field(...)

    return_ratio: Any = Field(...)
    annual_return_ratio: Any = Field(...)
    benchmark_ratio: Any = Field(...)
    alpha: float = Field(...)
    sharp: float = Field(...)
    ir: Any = Field(...)
    return_volatility: Any = Field(...)
    max_drawdown: float = Field(...)
    win_ratio: Any = Field(...)
    profit_loss_ratio: Any = Field(...)
    

class BacktestPerformanceSchema(
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    schemas.UUIDIDMixin,
    BaseModel,
):
    backtest_id: UUID = Field(...)
    run_date: datetime = Field(...)
    daily_data: Any = Field(...)

    return_ratio: Any = Field(...)
    annual_return_ratio: Any = Field(...)
    benchmark_ratio: Any = Field(...)
    alpha: float = Field(...)
    sharp: float = Field(...)
    ir: Any = Field(...)
    return_volatility: Any = Field(...)
    max_drawdown: float = Field(...)
    win_ratio: Any = Field(...)
    profit_loss_ratio: Any = Field(...)

    class Config:
        orm_mode = True
