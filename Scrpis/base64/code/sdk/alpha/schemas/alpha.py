from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from sdk.common import schemas

from ..constants import AlphaBuildType, AlphaType, ProductType
from .common import AlphaParameter, CompositionAlpha, IndexParameter


class AlphaSchema(
    schemas.IntIDMixin,
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    schemas.CreatorMixin,
    BaseModel,
):
    name: str = Field(..., description="因子名称")
    alpha_id: str = Field(..., description="因子 ID")

    column: str = Field(..., description="因子列名")
    alphas: Optional[List[CompositionAlpha]] = Field(default_factory=list, description="因子列表")
    expression: Optional[str] = Field(..., description="因子表达式")
    parameter: AlphaParameter = Field(..., description="回测参数")
    dependencies: Optional[List[str]] = Field(default_factory=list, description="因子依赖")

    catalog_id: Optional[UUID] = Field(..., description="因子目录 ID")
    alpha_type: AlphaType = Field(..., description="因子类型")
    product_type: ProductType = Field(..., description="资产类型")
    build_type: Optional[AlphaBuildType] = Field(description="因子构建类型", default=AlphaBuildType.WIZARD)

    task_id: UUID = Field()

    class Config:
        orm_mode = True
        use_enum_values = True


class IndexSchema(
    schemas.IntIDMixin,
    schemas.CreatedAtMixin,
    schemas.UpdatedAtMixin,
    schemas.CreatorMixin,
    BaseModel,
):
    name: str = Field(..., description="因子名称")
    alpha_id: str = Field(..., description="因子 ID")

    column: str = Field(..., description="因子列名")
    alphas: Optional[List[CompositionAlpha]] = Field(default_factory=list, description="因子列表")
    expression: Optional[str] = Field(..., description="因子表达式")
    parameter: IndexParameter = Field(..., description="回测参数")
    dependencies: Optional[List[str]] = Field(default_factory=list, description="因子依赖")

    catalog_id: Optional[UUID] = Field(..., description="因子目录 ID")
    alpha_type: AlphaType = Field(..., description="因子类型")
    product_type: ProductType = Field(..., description="资产类型")
    build_type: Optional[AlphaBuildType] = Field(description="因子构建类型", default=AlphaBuildType.WIZARD)

    task_id: UUID = Field()

    class Config:
        orm_mode = True
        use_enum_values = True


class CreateAlphaFromBacktestRequest(BaseModel):
    name: str = Field(..., description="因子名称")
    backtest_id: int = Field(..., description="回测 ID")


class CollectionSchema(
    schemas.IntIDMixin,
    schemas.CreatorMixin,
    schemas.CreatedAtMixin,
):
    alpha_id: int = Field(..., description="因子ID")

    class Config:
        orm_mode = True
