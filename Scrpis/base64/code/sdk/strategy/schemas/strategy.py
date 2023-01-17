from typing import List, Union, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, Field
from sdk.common import schemas

from ..constants import ProductType, StrategyBuildType
from .common import StrategyParameter


class StrategySchema(schemas.CreatedAtMixin, schemas.UpdatedAtMixin, schemas.CreatorMixin, schemas.UUIDIDMixin, BaseModel):
    name: str = Field(..., description="策略名称")
    description: Optional[str] = Field(..., description="策略备注")
    parameter: Union[StrategyParameter, Dict] = Field(..., description="策略参数")

    build_type: Optional[StrategyBuildType] = Field(description="策略构建类型", default=StrategyBuildType.WIZARD)
    product_type: ProductType = Field(..., description="资产类型")

    task_id: UUID = Field()

    class Config:
        orm_mode = True
        use_enum_values = True


class CreateStrategyFromBacktestRequest(BaseModel):
    name: str = Field(..., description="策略名称")
    backtest_id: UUID = Field(..., description="回测 ID")


class CollectionSchema(
    schemas.IntIDMixin,
    schemas.CreatorMixin,
    schemas.CreatedAtMixin,
):
    strategy_id: UUID = Field(..., description="策略ID")

    class Config:
        orm_mode = True


class PlotCutSchema(BaseModel):
    benchmark_cum_return_plot: List = Field(..., description="基准数据")
    cum_return_plot: List = Field(..., description="当前收益数据")
    max_drawdown_stamp: List = Field(..., description="最大回撤")
    relative_cum_return_plot: List = Field(..., description="相对收益数据")