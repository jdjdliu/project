from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field
from sdk.strategy import (
    ViewStockRequest,
    PlotCutSchema,
    CreateBacktestByCodeRequest,
    CreateStrategyDailySchema,
    CreateBacktestPerformanceRequest,
    BacktestDailyPerformanceSchema,
    BacktestPerformanceSchema,
    CollectionSchema,
    CreateMemberRequest,
    CreateRepoRequest,
    CreateStrategyBacktestFromWizardRequest,
    CreateStrategyFromBacktestRequest,
    DeleteMemberRequest,
    MemberSchema,
    OrderSchema,
    RepoSchema,
    RepoWithMember,
    StrategyBacktestSchema,
    StrategyDailySchema,
    StrategyDashboardSchema,
    StrategyPerformanceSchema,
    StrategySchema,
    StrategyShareCreateRequest,
    UpdateBacktestParamterRequest,
    UpdateBacktestRequest,
    UpdateRepoRequest,
    UpdateStrategyRequest,
)
from sdk.strategy.constants import RecordType
from sdk.task import TaskSchema
from sdk.strategy.schemas import DashboardQuery

__all__ = [
    "ViewStockRequest",
    "DashboardQuery",
    "CreateBacktestByCodeRequest",
    "CreateStrategyDailySchema",
    "CreateBacktestPerformanceRequest",
    "UpdateStrategyRequest",
    "StrategyShareCreateRequest",
    "BacktestPerformanceSchema",
    "BacktestDailyPerformanceSchema",
    "UpdateBacktestRequest",
    "CreateStrategyFromBacktestRequest",
    "CollectionSchema",
    "StrategySchema",
    "CreateStrategyBacktestFromWizardRequest",
    "StrategyBacktestSchema",
    "UpdateRepoRequest",
    "RepoSchema",
    "RepoWithMember",
    "CreateRepoRequest",
    "CreateMemberRequest",
    "UpdateBacktestParamterRequest",
    "DeleteMemberRequest",
    "MemberSchema",
    "StrategyPerformanceSchema",
    "StrategyDailySchema",
    "StrategyDashboardSchema",
    "OrderSchema",
    "PlotCutSchema",
]

class MyBacktestPerformanceSchema(BaseModel):
    backtest_id: UUID = Field(..., description="策略id")
    sharp: float = Field(..., description="夏普率")
    max_drawdown: float = Field(..., description="最大回撤")
    return_ratio: float = Field(..., description="累计收益")
    annual_return_ratio: float = Field(..., description="年化收益")

    class Config:
        orm_mode = True

class MyStrategyPerformanceSchema(BaseModel):
    strategy_id: UUID = Field(..., description="策略id")
    sharpe: float = Field(..., description="夏普率")
    max_drawdown: float = Field(..., description="最大回撤")
    cum_return: float = Field(..., description="累计收益")
    annual_return: float = Field(..., description="年化收益")

    class Config:
        orm_mode = True


class MyBacktest(BaseModel):
    backtest: Any = Field(...)
    performance: Union[BacktestPerformanceSchema,MyBacktestPerformanceSchema, None] = Field(...)
    task: TaskSchema = Field(...)
    notebook: Optional[str] = Field(None)


class MyStrategies(BaseModel):
    strategy: StrategySchema = Field(...)
    performance: Union[StrategyPerformanceSchema, MyStrategyPerformanceSchema, None] = Field(...)  # StrategyPerformanceSchema
    task: Optional[TaskSchema] = Field(...)

class MyStrategy(BaseModel):
    strategy: StrategySchema = Field(...)
    performance: Union[StrategyPerformanceSchema, MyStrategyPerformanceSchema, None] = Field(...)  # StrategyPerformanceSchema
    # daily: StrategyDailySchema = Field(...)
    daily: Any = Field(...)
    benchmark_profit: Optional[Dict] = Field(...)
    task: Optional[TaskSchema] = Field(...)
    overall_position: Optional[float] = Field(...)
    notebook: Optional[str] = Field(None)


class SharedRecord(BaseModel):
    strategy: Union[MyStrategy, None] = Field(...)
    notebook: Optional[str] = Field(default=None)
    record_id: UUID = Field(..., description="strategy or backtest id")
    record_type: RecordType = Field(..., description="stratgy/backtest")


class BulkDeletBacktestRequest(BaseModel):
    backtest_ids: List[UUID] = Field(..., description="回测ID列表")


class StrategyIdListRequest(BaseModel):
    strategy_ids: List[UUID] = Field(..., description="策略ID列表")


class BenchMarkSchema(BaseModel):
    cum_return: Optional[float] = Field(None, description="当前收益")
    sharpe: Optional[float] = Field(None, description="夏普比率")
    annual_return: Optional[float] = Field(None, description="年度收益")
    max_drawdown: Optional[float] = Field(None, description="最大回撤")
    today_return: Optional[float] = Field(None, description="今日收益")
    week_return: Optional[float] = Field(None, description="近1周收益")
    month_return: Optional[float] = Field(None, description="近1月收益")
    three_month_return: Optional[float] = Field(None, description="近3月收益")
    six_month_return: Optional[float] = Field(None, description="近6月收益")
    year_return: Optional[float] = Field(None, description="six_month_return")