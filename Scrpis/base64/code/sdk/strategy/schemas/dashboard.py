from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from ..constants import ProductType


class DashboardQuery(BaseModel):
    repos: Optional[List[UUID]] = Field(..., description="策略库")
    strategy_type: Optional[List[ProductType]] = Field(..., description="策略类型")
    keyword: Optional[str] = Field(..., description="模糊搜索， 适用于名字和描述")


class StrategyDashboardSchema(BaseModel):
    capital_base: Optional[int] = Field(1000, description="初始资金")
    strategy_id: UUID = Field(..., description="策略id")
    name: str = Field(..., description="策略名称")
    description: Optional[str] = Field(..., description="策略备注")
    sharpe: float = Field(default=None, description="夏普率")
    max_drawdown: float = Field(default=None, description="最大回撤")
    cum_return: float = Field(default=None, description="累计收益")
    annual_return: float = Field(default=None, description="年化收益")
    today_return: float = Field(default=None, description="当日收益")
    three_month_return: float = Field(default=None, description="三月收益")
    cum_return_plot: List[List[float]] = Field(default=None, description="策略累计收益曲线")
    benchmark_cum_return_plot: List[List[float]] = Field(default=None, description="基准累计收益曲线")
    relative_cum_return_plot: List[List[float]] = Field(default=None, description="相对累计收益曲线")
    collected: bool = Field(False, description="是否收藏")