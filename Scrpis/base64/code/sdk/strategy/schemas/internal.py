from datetime import datetime
from typing import List
from uuid import UUID

from pydantic import BaseModel, Field

from ...common import schemas
from ..constants import MarketType
from .common import StrategyParameter


class UpdateBacktestParamter(BaseModel):
    capital_base: int = Field(..., description="初始资金")


class UpdateBacktestParamterRequest(BaseModel):
    paramter: UpdateBacktestParamter = Field(...)
    backtest_id: UUID = Field(...)


class UpdateStrategyParameter(BaseModel):
    market: List[MarketType] = Field([MarketType.ALL], description="上市板")
    capital_base: int = Field(1000, description="初始资金")
    start_date: str = Field("2005-01-01", description="回测开始日期")


class UpdateStrategyRequest(schemas.CreatorMixin, BaseModel):
    strategy_id: UUID = Field(...)
    name: str = Field(...)
    description: str = Field(...)
    parameter: UpdateStrategyParameter = Field(...)


class DeleteDailyRequest(BaseModel):
    strategy_id: UUID = Field(...)
    run_date: datetime = Field(...)
